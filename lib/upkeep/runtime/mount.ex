defmodule Upkeep.Runtime.Mount do
  @moduledoc false

  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.Telemetry

  alias Upkeep.Runtime.Effects
  alias Upkeep.Runtime.Execution.Shared
  alias Upkeep.Runtime.Materializer
  alias Upkeep.Runtime.NodeSpec
  alias Upkeep.Runtime.Patch
  alias Upkeep.Runtime.Producer
  alias Upkeep.Runtime.SourceLoads
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Subscriptions

  def dispatch(socket, %NodeSpec{kind: :source, producer: %Producer.Source{} = producer} = spec) do
    %Materializer.Assign{assign_name: assign_name} = single_materializer(spec)
    source_id = producer.source_id

    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        socket =
          socket
          |> State.put_watch_assign(source_id, assign_name)
          |> State.put_assign_node(assign_name, spec.id)

        effects = [
          {:telemetry, [:source, :watch], %{count: 1},
           Telemetry.watch_alias_metadata(watch, assign_name)},
          {:assign, assign_name, Store.fetch!(State.store(socket), spec.id)}
        ]

        {:ok, socket, effects}

      :error ->
        instance = producer.instance
        policy = Subscriptions.tracking_policy(socket)
        tracked? = policy == :auto

        if tracked? do
          :ok = Subscriptions.track_source(source_id)
        end

        subscriber_count = if tracked?, do: Subscriptions.source_member_count(source_id), else: 0
        registered? = policy == :eager or subscriber_count > 1

        if tracked? and not registered? do
          :ok = Subscriptions.join_local_notifications()
        end

        result =
          SourceLoads.load_coalesced(
            instance,
            source_id,
            producer.component,
            :watch
          )

        patch =
          socket
          |> State.put_watch(source_id, %{
            assign_names: MapSet.new([assign_name]),
            instance: instance,
            component: producer.component,
            registered?: registered?,
            subscribed?: tracked?,
            surface: result.surface,
            tracked_deps: result.tracked_deps
          })
          |> Patch.new()
          |> Patch.put_source(source_id, result.value, spec.deps, metadata: spec.metadata)
          |> Patch.put_assign_node(assign_name, spec.id)

        effects =
          Effects.maybe_register_source(
            registered?,
            source_id,
            result.surface,
            producer
          ) ++
            [
              {:telemetry, [:source, :watch], %{count: 1},
               spec.metadata
               |> Map.put(:node_id, spec.id)
               |> Map.put(:kind, :new)
               |> Map.put(:registered?, registered?)
               |> Map.put(:surface_keys, Upkeep.InvalidationSurface.keys(result.surface))}
            ] ++
            Effects.assign_source(assign_name, result.value, source_id)

        {:ok, Patch.socket(patch), effects}
    end
  end

  def dispatch(
        socket,
        %NodeSpec{kind: :component, producer: %Producer.Compute{} = producer} = spec
      ) do
    %Materializer.Component{component_id: component_id} = single_materializer(spec)
    compute = compute_fun(producer)

    socket
    |> Patch.new()
    |> Patch.put_component(spec.id, spec.deps, compute, spec.metadata, component_id)
    |> Patch.result()
  end

  def dispatch(socket, %NodeSpec{kind: :derived, producer: %Producer.Compute{} = producer} = spec) do
    %Materializer.Assign{assign_name: assign_name} = single_materializer(spec)
    compute = compute_fun(producer)

    {initial_value, sharing_metadata} =
      Shared.initial_value(
        socket,
        assign_name,
        spec.deps,
        producer.dep_pairs,
        producer.fun,
        compute,
        spec.source_location
      )

    public_sharing_metadata = Map.delete(sharing_metadata, :compute_fn)
    sharing_plan = Shared.sharing_plan(socket, spec.deps, public_sharing_metadata)
    public_sharing_metadata = Map.put(public_sharing_metadata, :shareable_plan, sharing_plan)

    patch =
      socket
      |> Patch.new()
      |> Patch.register_derived(spec.id, spec.deps, compute, initial_value, spec.metadata)
      |> Patch.put_assign_node(assign_name, spec.id)
      |> Patch.put_derive_sharing(spec.id, public_sharing_metadata)

    value = Store.fetch!(State.store(Patch.socket(patch)), spec.id)

    Patch.socket(patch)
    |> then(fn socket ->
      effects =
        [
          {:telemetry, [:derive, :sharing], %{count: 1}, public_sharing_metadata},
          {:telemetry, [:derive, :sharing_plan], %{count: 1}, sharing_plan},
          {:assign, assign_name, value}
        ]

      {:ok, socket, effects}
    end)
  end

  def compute_fun(%Producer.Compute{dep_pairs: dep_pairs, fun: fun}) do
    fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end
  end

  def single_materializer(%NodeSpec{materializers: [materializer]}), do: materializer
end

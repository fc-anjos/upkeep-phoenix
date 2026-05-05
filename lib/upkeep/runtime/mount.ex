defmodule Upkeep.Runtime.Mount do
  @moduledoc false

  alias Upkeep.DAG.Store

  alias Upkeep.Live.{
    Components,
    Ids,
    Telemetry
  }

  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.Effects
  alias Upkeep.Runtime.Execution.Shared
  alias Upkeep.Runtime.Materializer
  alias Upkeep.Runtime.NodeSpec
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
           Telemetry.watch_metadata(watch, assign_name, :alias)},
          {:assign, assign_name, Map.fetch!(socket.assigns, primary_assign_name(watch))}
        ]

        {:ok, socket, effects}

      :error ->
        registered? = Subscriptions.register_interest?(socket)
        shared_initial_load? = Subscriptions.shared_initial_load?(socket)

        {value, tracked_deps, interest_keys} =
          SourceLoads.load_or_register(
            socket,
            shared_initial_load?,
            source_id,
            producer.source,
            producer.params,
            producer.component
          )

        socket =
          socket
          |> State.put_watch(source_id, %{
            assign_name: assign_name,
            assign_names: MapSet.new([assign_name]),
            source: producer.source,
            params: producer.params,
            component: producer.component,
            registered?: registered?,
            interest_keys: interest_keys,
            tracked_deps: tracked_deps
          })
          |> DAGOperations.put_source(source_id, value, spec.deps, spec.metadata)
          |> State.put_assign_node(assign_name, spec.id)

        effects =
          Effects.maybe_register_source(
            registered? and not shared_initial_load?,
            source_id,
            interest_keys,
            producer
          ) ++
            [
              {:telemetry, [:source, :watch], %{count: 1},
               spec.metadata
               |> Map.put(:node_id, spec.id)
               |> Map.put(:kind, :new)
               |> Map.put(:registered?, registered?)
               |> Map.put(:interest_keys, interest_keys)}
            ] ++
            Effects.assign_source(assign_name, value, source_id)

        {:ok, socket, effects}
    end
  end

  def dispatch(
        socket,
        %NodeSpec{kind: :component, producer: %Producer.Compute{} = producer} = spec
      ) do
    %Materializer.Component{component_id: component_id} = single_materializer(spec)
    compute = compute_fun(producer)

    store =
      socket
      |> State.store()
      |> Store.put_component(spec.id, spec.deps, compute)
      |> DAGOperations.put_runtime_metadata(spec.id, spec.metadata)

    value = Store.fetch!(store, spec.id)
    store = Components.put_assign_nodes(store, component_id, value)

    socket
    |> State.put_store(store)
    |> put_component_assign_nodes(component_id, value)
    |> then(fn socket -> {:ok, socket, Effects.component_assigns(value)} end)
  end

  def dispatch(socket, %NodeSpec{kind: :derived, producer: %Producer.Compute{} = producer} = spec) do
    %Materializer.Assign{assign_name: assign_name} = single_materializer(spec)
    compute = compute_fun(producer)

    {initial_value, graph_node_id, sharing_metadata} =
      Shared.initial_value(
        socket,
        assign_name,
        spec.deps,
        producer.dep_pairs,
        producer.fun,
        compute
      )

    public_sharing_metadata = Map.delete(sharing_metadata, :compute_fn)
    sharing_plan = Shared.sharing_plan(socket, spec.deps, public_sharing_metadata)
    public_sharing_metadata = Map.put(public_sharing_metadata, :shareable_plan, sharing_plan)

    store =
      socket
      |> State.store()
      |> Store.register_derived(spec.id, spec.deps, compute)
      |> DAGOperations.put_runtime_metadata(spec.id, spec.metadata)
      |> seed_initial_value(spec.id, initial_value)

    value = Store.fetch!(store, spec.id)

    socket
    |> State.put_store(store)
    |> State.put_assign_node(assign_name, spec.id)
    |> State.put_derive_sharing(spec.id, public_sharing_metadata)
    |> put_shared_derived_node(spec.id, graph_node_id)
    |> then(fn socket ->
      effects =
        [
          {:telemetry, [:derive, :sharing], %{count: 1}, public_sharing_metadata},
          {:telemetry, [:derive, :sharing_plan], %{count: 1}, sharing_plan}
        ] ++
          Effects.register_shared_derived(graph_node_id, sharing_metadata) ++
          [{:assign, assign_name, value}]

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

  defp put_component_assign_nodes(socket, component_id, value) when is_map(value) do
    Enum.reduce(value, socket, fn
      {assign_name, _assign_value}, socket when is_atom(assign_name) ->
        State.put_assign_node(
          socket,
          assign_name,
          Ids.component_assign_node_id(component_id, assign_name)
        )

      {_assign_name, _assign_value}, socket ->
        socket
    end)
  end

  defp put_component_assign_nodes(socket, _component_id, _value), do: socket

  defp put_shared_derived_node(socket, _node_id, nil), do: socket

  defp put_shared_derived_node(socket, node_id, graph_node_id) do
    State.put_shared_derived_node(socket, node_id, graph_node_id)
  end

  defp seed_initial_value(store, id, value) do
    {store, _changed?} = Store.seed(store, id, value)
    store
  end

  defp primary_assign_name(watch) do
    watch.assign_name || Enum.at(watch.assign_names, 0)
  end
end

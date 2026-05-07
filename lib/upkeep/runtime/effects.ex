defmodule Upkeep.Runtime.Effects do
  @moduledoc false

  alias Upkeep.Live.Ids
  alias Upkeep.Runtime.State

  def maybe_register_source(true, source_id, interest_keys, tracked_deps, producer) do
    [{:register_source, source_id, interest_keys, tracked_deps, producer.source, producer.params}]
  end

  def maybe_register_source(false, _source_id, _interest_keys, _tracked_deps, _producer), do: []

  def assign_source(assign_name, value, source_id) do
    [
      {:telemetry, [:live, :assign], %{count: 1},
       %{
         assign: assign_name,
         node_id: Ids.source_node_id(source_id),
         source_id: source_id,
         kind: :source
       }},
      {:assign, assign_name, value}
    ]
  end

  def assign_derived(assign_name, value, node_id) do
    [
      {:telemetry, [:live, :assign], %{count: 1},
       %{
         assign: assign_name,
         node_id: node_id,
         kind: :derived
       }},
      {:assign, assign_name, value}
    ]
  end

  def component_assigns(value) when is_map(value) do
    value
    |> Enum.flat_map(fn
      {assign_name, assign_value} when is_atom(assign_name) ->
        [{:assign, assign_name, assign_value}]

      {_assign_name, _assign_value} ->
        []
    end)
  end

  def component_assigns(_value), do: []

  def register_shared_derived(nil, _sharing_metadata), do: []

  def register_shared_derived(graph_node_id, sharing_metadata) do
    case Map.fetch(sharing_metadata, :compute_fn) do
      {:ok, compute_fn} ->
        [
          {:register_derived, graph_node_id, Map.fetch!(sharing_metadata, :graph_dep_node_ids),
           compute_fn}
        ]

      :error ->
        []
    end
  end

  def assign_watch(watch, value) do
    Enum.flat_map(watch.assign_names, fn assign_name ->
      assign_source(assign_name, value, watch.source_id)
    end)
  end

  def assign_shared_derived(socket, local_node_id, value) do
    socket
    |> State.assign_names_for_node(local_node_id)
    |> Enum.flat_map(fn assign_name ->
      assign_derived(assign_name, value, local_node_id)
    end)
  end
end

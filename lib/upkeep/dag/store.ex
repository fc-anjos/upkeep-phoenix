defmodule Upkeep.DAG.Store do
  @moduledoc false

  alias Upkeep.DAG.{Diff, Graph}

  defstruct graph: nil, values: %{}, compute_fns: %{}, metadata: %{}

  def new, do: %__MODULE__{graph: Graph.new()}

  def graph(%__MODULE__{graph: graph}), do: graph

  def has_node?(%__MODULE__{graph: graph}, id), do: Graph.has_node?(graph, id)

  def fetch!(%__MODULE__{values: values}, id), do: Map.fetch!(values, id)

  @doc """
  Attach an opaque metadata payload to an existing node. Domain code uses this
  for per-node payloads that aren't computed values (loaders, registration
  records, telemetry tags).
  """
  def put_metadata(%__MODULE__{} = store, id, metadata) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    %{store | metadata: Map.put(store.metadata, id, metadata)}
  end

  def update_metadata(%__MODULE__{} = store, id, default, fun) when is_function(fun, 1) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    %{store | metadata: Map.update(store.metadata, id, default, fun)}
  end

  def fetch_metadata!(%__MODULE__{metadata: metadata}, id), do: Map.fetch!(metadata, id)

  def get_metadata(%__MODULE__{metadata: metadata}, id, default \\ nil) do
    Map.get(metadata, id, default)
  end

  @doc """
  Register or update a source node and its value. Returns `{store, changed?}`
  reporting whether the cached value actually moved.
  """
  def put_source(%__MODULE__{} = store, id, value, deps \\ []) when is_list(deps) do
    store
    |> ensure_topology(id, :source, deps)
    |> put_value(id, value)
  end

  @doc """
  Register a derived node and compute its initial value from its deps' cached
  values.
  """
  def put_derived(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :derived, deps)
    |> put_compute(id, compute)
    |> compute_and_store(id)
  end

  @doc """
  Register a derived node without computing or seeding a value. Use when deps
  may not yet have values — the value will materialize on the first
  `recompute/3` that touches the node.
  """
  def register_derived(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :derived, deps)
    |> put_compute(id, compute)
  end

  @doc """
  Register a component node and compute its initial value. Behaves like
  `put_derived/4` but tags the node kind as `:component`.
  """
  def put_component(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :component, deps)
    |> put_compute(id, compute)
    |> compute_and_store(id)
  end

  @doc """
  Set a value on an existing node without recomputing dependents. Returns
  `{store, changed?}`.
  """
  def seed(%__MODULE__{} = store, id, value) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    put_value(store, id, value)
  end

  @doc """
  Drop a node and everything downstream of it from both topology and values.
  """
  def remove_subgraph(%__MODULE__{} = store, root_id) do
    plan = Graph.subgraph_plan(store.graph, root_id)

    Enum.reduce(plan.selected_node_ids, store, fn id, store ->
      %{
        store
        | graph: Graph.remove_node(store.graph, id),
          values: Map.delete(store.values, id),
          compute_fns: Map.delete(store.compute_fns, id),
          metadata: Map.delete(store.metadata, id)
      }
    end)
  end

  @doc """
  Walk downstream of `changed_ids` in topological order, recomputing any
  derived/component node whose deps changed. Returns `{store, %Diff{}}`.

  Options:

    * `:skip` — list of node ids to leave untouched. They appear in
      `diff.skipped_node_ids` and as boundaries in the diff.
  """
  def recompute(%__MODULE__{} = store, changed_ids, opts \\ []) do
    changed_ids = MapSet.new(changed_ids)
    skip_ids = opts |> Keyword.get(:skip, []) |> MapSet.new()
    order = Graph.affected_ids(store.graph, MapSet.to_list(changed_ids))

    {store, changed, recomputed} =
      Enum.reduce(order, {store, changed_ids, []}, fn id, {store, changed, recomputed} ->
        deps = Graph.deps(store.graph, id)
        kind = Graph.kind(store.graph, id)

        cond do
          MapSet.member?(skip_ids, id) ->
            {store, changed, recomputed}

          kind != :source and Enum.any?(deps, &MapSet.member?(changed, &1)) ->
            value = compute_value(store, id)
            {store, value_changed?} = put_value(store, id, value)
            changed = if value_changed?, do: MapSet.put(changed, id), else: changed

            {store, changed, [id | recomputed]}

          true ->
            {store, changed, recomputed}
        end
      end)

    derived_changed =
      recomputed
      |> Enum.reverse()
      |> Enum.filter(&MapSet.member?(changed, &1))

    recomputed = Enum.reverse(recomputed)

    skipped_node_ids =
      if MapSet.size(skip_ids) == 0,
        do: [],
        else: Graph.topological_subset(store.graph, skip_ids)

    diff = %Diff{
      roots: changed_ids |> MapSet.to_list() |> Enum.sort_by(&inspect/1),
      selected_node_ids: order,
      boundaries: Enum.map(skipped_node_ids, &%{node_id: &1, reason: :skipped}),
      changed_node_ids: derived_changed,
      recomputed_node_ids: recomputed,
      skipped_node_ids: skipped_node_ids
    }

    {store, diff}
  end

  ## Internals

  defp ensure_topology(store, id, kind, deps) do
    %{store | graph: Graph.put_node(store.graph, id, kind, deps)}
  end

  defp put_compute(store, id, compute) do
    %{store | compute_fns: Map.put(store.compute_fns, id, compute)}
  end

  defp put_value(store, id, value) do
    old = Map.get(store.values, id, :__upkeep_missing__)
    store = %{store | values: Map.put(store.values, id, value)}
    {store, old != value}
  end

  defp compute_and_store(store, id) do
    {store, _changed?} = put_value(store, id, compute_value(store, id))
    store
  end

  defp compute_value(store, id) do
    compute = Map.fetch!(store.compute_fns, id)

    store.graph
    |> Graph.deps(id)
    |> Map.new(fn dep -> {dep, Map.fetch!(store.values, dep)} end)
    |> compute.()
  end
end

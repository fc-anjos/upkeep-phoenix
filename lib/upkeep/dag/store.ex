defmodule Upkeep.DAG.Store do
  @moduledoc false

  alias Upkeep.DAG.{Diff, Graph}

  defstruct graph: nil, values: %{}, compute_fns: %{}, metadata: %{}

  @type node_id :: Graph.node_id()
  @type compute_fn :: (map() -> term())
  @opaque t :: %__MODULE__{
            graph: Graph.t(),
            values: %{optional(node_id()) => term()},
            compute_fns: %{optional(node_id()) => compute_fn()},
            metadata: %{optional(node_id()) => term()}
          }

  def new, do: %__MODULE__{graph: Graph.new()}

  @spec graph(t()) :: Graph.t()
  def graph(%__MODULE__{graph: graph}), do: graph

  @spec has_node?(t(), node_id()) :: boolean()
  def has_node?(%__MODULE__{graph: graph}, id), do: Graph.has_node?(graph, id)

  @spec fetch!(t(), node_id()) :: term()
  def fetch!(%__MODULE__{values: values}, id), do: Map.fetch!(values, id)

  @spec put_metadata(t(), node_id(), term()) :: t()
  def put_metadata(%__MODULE__{} = store, id, metadata) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    %{store | metadata: Map.put(store.metadata, id, metadata)}
  end

  @spec update_metadata(t(), node_id(), term(), (term() -> term())) :: t()
  def update_metadata(%__MODULE__{} = store, id, default, fun) when is_function(fun, 1) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    %{store | metadata: Map.update(store.metadata, id, default, fun)}
  end

  @spec fetch_metadata!(t(), node_id()) :: term()
  def fetch_metadata!(%__MODULE__{metadata: metadata}, id), do: Map.fetch!(metadata, id)

  @spec get_metadata(t(), node_id(), term()) :: term()
  def get_metadata(%__MODULE__{metadata: metadata}, id, default \\ nil) do
    Map.get(metadata, id, default)
  end

  @spec put_source(t(), node_id(), term(), [node_id()]) :: {t(), boolean()}
  def put_source(%__MODULE__{} = store, id, value, deps \\ []) when is_list(deps) do
    store
    |> ensure_topology(id, :source, deps)
    |> put_value(id, value)
  end

  @spec put_derived(t(), node_id(), [node_id()], compute_fn()) :: t()
  def put_derived(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :derived, deps)
    |> put_compute(id, compute)
    |> compute_and_store(id)
  end

  @spec register_derived(t(), node_id(), [node_id()], compute_fn()) :: t()
  def register_derived(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :derived, deps)
    |> put_compute(id, compute)
  end

  @spec put_component(t(), node_id(), [node_id()], compute_fn()) :: t()
  def put_component(%__MODULE__{} = store, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    store
    |> ensure_topology(id, :component, deps)
    |> put_compute(id, compute)
    |> compute_and_store(id)
  end

  @spec seed(t(), node_id(), term()) :: {t(), boolean()}
  def seed(%__MODULE__{} = store, id, value) do
    unless Graph.has_node?(store.graph, id) do
      raise ArgumentError, "unknown DAG node #{inspect(id)}"
    end

    put_value(store, id, value)
  end

  @spec remove_subgraph(t(), node_id()) :: t()
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

  @spec recompute(t(), Enumerable.t(), keyword()) :: {t(), Diff.t()}
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

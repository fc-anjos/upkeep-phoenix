defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Live.{Snapshot, Specs}

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Live,
        only: [
          watch: 4,
          watch: 5,
          component: 4,
          remove_component: 2,
          derive: 4,
          unwatch: 2,
          unwatch: 3,
          refresh: 4
        ]

      @impl true
      def handle_info({:dag_values, pairs}, socket) do
        {:noreply, Upkeep.Live.apply_dag_values(socket, pairs)}
      end

      defoverridable handle_info: 2
    end
  end

  def watch(socket, assign_name, source, params, opts \\ []) when is_atom(assign_name) do
    params = normalize_params(params)
    component = Keyword.get(opts, :under)
    Upkeep.Runtime.mount(socket, Specs.source(assign_name, source, params, component))
  end

  def component(socket, component_id, deps, fun)
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    Upkeep.Runtime.mount(socket, Specs.component(socket, component_id, deps, fun))
  end

  def remove_component(socket, component_id) when not is_nil(component_id) do
    Upkeep.Runtime.remove_component(socket, component_id)
  end

  def derive(socket, assign_name, deps, fun)
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    Upkeep.Runtime.mount(socket, Specs.derived(socket, assign_name, deps, fun))
  end

  def unwatch(socket, assign_name) when is_atom(assign_name) do
    Upkeep.Runtime.unwatch_assign(socket, assign_name)
  end

  def unwatch(socket, source, params) when is_atom(source) do
    Upkeep.Runtime.unwatch_source(socket, source, normalize_params(params))
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    {value, _tracked_deps} = Upkeep.Source.load(source, normalize_params(params))
    assign(socket, assign_name, value)
  end

  def refresh_matching(socket, event) when is_struct(event) do
    Upkeep.Runtime.refresh_matching(socket, event)
  end

  def graph_snapshot(socket) do
    Snapshot.build(socket)
  end

  def queue_matching(socket, event) when is_struct(event) do
    Upkeep.Runtime.queue_matching(socket, event)
  end

  def flush_refreshes(socket) do
    Upkeep.Runtime.flush_refreshes(socket)
  end

  def notify(event) when is_struct(event), do: Upkeep.notify(event)

  @doc """
  Apply a batch of Graph-pushed values to the LV in one pass.
  Reduces subscriber-side wakeups to one handle_info per shard flush.
  """
  def apply_dag_values(socket, pairs) when is_list(pairs) do
    Upkeep.Runtime.apply_dag_values(socket, pairs)
  end

  @doc """
  Apply a Graph-pushed value to the LV. Mirrors `maybe_refresh` but skips
  the source.load step — the coordinator already ran it once for everyone.
  """
  def apply_dag_value(socket, source_id, value) do
    Upkeep.Runtime.apply_dag_value(socket, source_id, value)
  end

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end

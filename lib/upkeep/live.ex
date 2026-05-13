defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.

  Use this module from a Phoenix LiveView:

      defmodule MyAppWeb.ProjectLive do
        use MyAppWeb, :live_view
        use Upkeep.Live

        def mount(%{"id" => id}, _session, socket) do
          project_id = String.to_integer(id)

          socket =
            socket
            |> watch(:items, MyApp.Catalog.Sources.ProjectItems, %{project_id: project_id})
            |> derive(:item_count, [:items], fn %{items: items} -> length(items) end)

          {:ok, socket}
        end
      end

  `watch/4` loads a source into a LiveView assign and keeps that assign fresh
  when matching domain writes commit. `derive/4` computes another assign from
  watched or derived values. `component/4` creates a component-scoped dependency
  boundary for repeated UI fragments that need their own stable identity.

  `use Upkeep.Live` also installs the runtime `handle_info/2` callbacks needed
  for graph-pushed values and registers an `on_mount` hook that exposes
  Phoenix's `:current_scope` assign to Upkeep.

  When viewer identity affects a source value, define the source with
  `load/2` or `query/2` and read scope through `Upkeep.current_scope!/1` inside
  the source callback. When the source value is safe to share and only the
  presentation differs by viewer, use a local derive that receives
  `%{current_scope: scope}`.
  """

  use Boundary,
    top_level?: true,
    exports: [
      Macros,
      ScopeHook
    ],
    deps: [
      Phoenix.Component,
      Phoenix.LiveView,
      Upkeep.Mutation,
      Upkeep.Runtime,
      Upkeep.Source,
      Upkeep.Source.Loader,
      {Mix, :compile}
    ],
    type: :strict

  alias Upkeep.Source.Loader, as: Source

  defmacro __using__(_opts \\ []) do
    quote do
      Phoenix.LiveView.on_mount(Upkeep.Live.ScopeHook)

      import Upkeep.Live.Macros,
        only: [
          watch: 4,
          watch: 5,
          component: 4,
          derive: 4
        ]

      import Upkeep.Live,
        only: [
          remove_component: 2,
          unwatch: 2,
          unwatch: 3,
          refresh: 4
        ]

      @impl true
      def handle_info({:dag_values, pairs}, socket) do
        {:noreply, Upkeep.Live.apply_dag_values(socket, pairs)}
      end

      def handle_info({:upkeep_invalidation, _origin, event}, socket) do
        {:noreply, Upkeep.Live.refresh_local_matching(socket, event)}
      end

      def handle_info({:upkeep_invalidation, event}, socket) do
        {:noreply, Upkeep.Live.refresh_local_matching(socket, event)}
      end

      defoverridable handle_info: 2
    end
  end

  def watch(socket, assign_name, source, params, opts \\ []) when is_atom(assign_name) do
    params = normalize_params(params)
    component = Keyword.get(opts, :under)
    location = Keyword.get(opts, :source_location)

    with_current_scope(socket, fn socket ->
      Source.verify_source!(
        source,
        params,
        current_scope: Map.get(socket.assigns, :current_scope),
        source_location: location
      )

      Upkeep.Runtime.mount_source(socket, assign_name, source, params, component, location)
    end)
  end

  def component(socket, component_id, deps, fun, opts \\ [])
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    location = Keyword.get(opts, :source_location)

    with_current_scope(socket, fn socket ->
      Upkeep.Runtime.mount_component(socket, component_id, deps, fun, location)
    end)
  end

  def remove_component(socket, component_id) when not is_nil(component_id) do
    with_current_scope(socket, &Upkeep.Runtime.remove_component(&1, component_id))
  end

  def derive(socket, assign_name, deps, fun, opts \\ [])
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    location = Keyword.get(opts, :source_location)

    with_current_scope(socket, fn socket ->
      Upkeep.Runtime.mount_derived(socket, assign_name, deps, fun, location)
    end)
  end

  def unwatch(socket, assign_name) when is_atom(assign_name) do
    with_current_scope(socket, &Upkeep.Runtime.unwatch_assign(&1, assign_name))
  end

  def unwatch(socket, source, params) when is_atom(source) do
    with_current_scope(
      socket,
      &Upkeep.Runtime.unwatch_source(&1, source, normalize_params(params))
    )
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    with_current_scope(
      socket,
      &Upkeep.Runtime.refresh(&1, assign_name, source, normalize_params(params))
    )
  end

  def refresh_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.refresh_matching(&1, event))
  end

  def refresh_local_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.refresh_local_matching(&1, event))
  end

  @doc """
  Return the current Upkeep graph state for diagnostics.
  """
  def graph_snapshot(socket) do
    Upkeep.Runtime.graph_snapshot(socket)
  end

  def inspecting?(socket) do
    Map.get(socket.assigns, :upkeep_inspector?, false)
  end

  def queue_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.queue_matching(&1, event))
  end

  def flush_refreshes(socket) do
    with_current_scope(socket, &Upkeep.Runtime.flush_refreshes/1)
  end

  def notify(event) when is_struct(event), do: Upkeep.Mutation.notify(event)

  @doc """
  Apply a batch of Graph-pushed values to the LV in one pass.
  Reduces subscriber-side wakeups to one handle_info per shard flush.
  """
  def apply_dag_values(socket, pairs) when is_list(pairs) do
    with_current_scope(socket, &Upkeep.Runtime.apply_dag_values(&1, pairs))
  end

  @doc """
  Apply a Graph-pushed value to the LV. Mirrors `maybe_refresh` but skips
  the source.load step — the coordinator already ran it once for everyone.
  """
  def apply_dag_value(socket, source_id, value) do
    with_current_scope(socket, &Upkeep.Runtime.apply_dag_value(&1, source_id, value))
  end

  defp with_current_scope(socket, fun) do
    {:ok, socket, scope_effects} = Upkeep.Runtime.sync_current_scope(socket)
    {:ok, socket, effects} = fun.(socket)
    Upkeep.Runtime.to_socket({:ok, socket, scope_effects ++ effects})
  end

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end

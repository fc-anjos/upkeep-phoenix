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
          refresh: 4,
          revoke_authorization: 2
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

  @doc """
  Load a source into `socket.assigns[assign_name]` and keep it fresh.

  `source` is a module using `Upkeep.Source` or `Upkeep.Ecto.Source`; `params`
  is a map (keyword lists are normalized to maps). Two LiveViews watching the
  same `{source, params}` pair share one loaded value. The assign refreshes
  whenever a captured write or explicit domain fact matches the source's
  surface.

  Options:

    * `:under` - scope the watch to a component boundary created with
      `component/4`.
  """
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

  @doc """
  Create a component-scoped dependency boundary.

  `deps` lists the watched or derived assigns the component reads; `fun`
  receives a map of those values and returns the component's assigns. Use for
  repeated UI fragments that need their own stable identity. Remove it with
  `remove_component/2`.
  """
  def component(socket, component_id, deps, fun, opts \\ [])
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    location = Keyword.get(opts, :source_location)

    with_current_scope(socket, fn socket ->
      Upkeep.Runtime.mount_component(socket, component_id, deps, fun, location)
    end)
  end

  @doc """
  Remove a component boundary created with `component/4`, dropping the
  watches scoped under it.
  """
  def remove_component(socket, component_id) when not is_nil(component_id) do
    with_current_scope(socket, &Upkeep.Runtime.remove_component(&1, component_id))
  end

  @doc """
  Compute `socket.assigns[assign_name]` as a pure function of other watched
  or derived assigns.

  `deps` lists the input assign names; `fun` receives a map of their current
  values. The derive recomputes whenever any input changes. Derives are local
  to this LiveView and never hit the database.
  """
  def derive(socket, assign_name, deps, fun, opts \\ [])
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    location = Keyword.get(opts, :source_location)

    with_current_scope(socket, fn socket ->
      Upkeep.Runtime.mount_derived(socket, assign_name, deps, fun, location)
    end)
  end

  @doc """
  Stop watching the source behind `assign_name` and drop the assign's
  subscription. The assign value itself is left in place.
  """
  def unwatch(socket, assign_name) when is_atom(assign_name) do
    with_current_scope(socket, &Upkeep.Runtime.unwatch_assign(&1, assign_name))
  end

  @doc """
  Stop watching every assign backed by the `{source, params}` identity.
  """
  def unwatch(socket, source, params) when is_atom(source) do
    with_current_scope(
      socket,
      &Upkeep.Runtime.unwatch_source(&1, source, normalize_params(params))
    )
  end

  @doc """
  Drop watches whose authorizing identity is no longer valid.

  Each watch is tagged at watch time with the identity that authorized it
  (derived from `:current_scope` via the `:authorizing_identity` extractor).
  Pass the revoked identity to drop every watch tagged with it, or a predicate
  receiving each watch's identity and returning `true` to drop it. Dropped
  watches leave their source groups and unregister like `unwatch/2`.
  """
  def revoke_authorization(socket, predicate) when is_function(predicate, 1) do
    with_current_scope(socket, &Upkeep.Runtime.revoke_authorization(&1, predicate))
  end

  def revoke_authorization(socket, identity) do
    revoke_authorization(socket, fn watch_identity -> watch_identity == identity end)
  end

  @doc """
  Reload a watched source now, regardless of invalidation state.
  """
  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    with_current_scope(
      socket,
      &Upkeep.Runtime.refresh(&1, assign_name, source, normalize_params(params))
    )
  end

  @doc """
  Refresh every watched source whose surface matches `event`, an
  `Upkeep.Change` struct.
  """
  def refresh_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.refresh_matching(&1, event))
  end

  @doc false
  def refresh_local_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.refresh_local_matching(&1, event))
  end

  @doc """
  Return the current Upkeep graph state for diagnostics.
  """
  def graph_snapshot(socket) do
    Upkeep.Runtime.graph_snapshot(socket)
  end

  @doc """
  Whether the Upkeep inspector is active for this socket.
  """
  def inspecting?(socket) do
    Map.get(socket.assigns, :upkeep_inspector?, false)
  end

  @doc false
  def queue_matching(socket, event) when is_struct(event) do
    with_current_scope(socket, &Upkeep.Runtime.queue_matching(&1, event))
  end

  @doc false
  def flush_refreshes(socket) do
    with_current_scope(socket, &Upkeep.Runtime.flush_refreshes/1)
  end

  @doc """
  Dispatch an `Upkeep.Change` event to the coordinator, as if a captured
  write had committed.
  """
  def notify(event) when is_struct(event), do: Upkeep.Mutation.notify(event)

  @doc false
  def apply_dag_values(socket, pairs) when is_list(pairs) do
    with_current_scope(socket, &Upkeep.Runtime.apply_dag_values(&1, pairs))
  end

  @doc false
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

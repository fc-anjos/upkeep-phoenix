defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Live, only: [watch: 4, refresh: 4]

      @impl true
      def handle_info({:upkeep_event, event}, socket) do
        socket =
          socket
          |> Upkeep.Live.queue_matching(event)
          |> Upkeep.Live.flush_refreshes()

        {:noreply, socket}
      end

      defoverridable handle_info: 2
    end
  end

  def watch(socket, assign_name, source, params) when is_atom(assign_name) do
    params = normalize_params(params)
    value = source.load(params)
    source_id = Source.source_id(source, params)
    interest_keys = source.__upkeep_interest_keys__(params)

    for key <- interest_keys do
      :ok =
        Group.join(@supervisor, Source.group_key(key), %{
          assign: assign_name,
          source: inspect(source)
        })
    end

    socket
    |> put_watch(source_id, %{
      assign_name: assign_name,
      source: source,
      params: params,
      interest_keys: interest_keys
    })
    |> assign(assign_name, value)
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    assign(socket, assign_name, source.load(params))
  end

  def refresh_matching(socket, event) when is_struct(event) do
    socket
    |> queue_matching(event)
    |> flush_refreshes()
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> watches()
    |> Enum.reduce(socket, fn {_source_id, watch}, socket ->
      if watch.source.reacts_to?(event, watch.params) do
        queue_refresh(socket, watch.source_id)
      else
        socket
      end
    end)
  end

  def flush_refreshes(socket) do
    socket
    |> pending_refreshes()
    |> Enum.reduce(clear_pending_refreshes(socket), fn source_id, socket ->
      case Map.fetch(watches(socket), source_id) do
        {:ok, watch} -> maybe_refresh(socket, watch)
        :error -> socket
      end
    end)
  end

  def notify(event) when is_struct(event), do: Upkeep.notify(event)

  defp put_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watch = Map.put(watch, :source_id, source_id)
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp queue_refresh(socket, source_id) do
    private = socket.private || %{}
    pending = MapSet.put(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

    %{socket | private: Map.put(private, :upkeep_pending_refreshes, pending)}
  end

  defp pending_refreshes(socket) do
    case socket.private do
      %{upkeep_pending_refreshes: pending} -> pending
      _ -> MapSet.new()
    end
  end

  defp clear_pending_refreshes(socket) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_pending_refreshes, MapSet.new())}
  end

  defp maybe_refresh(socket, watch) do
    refresh(socket, watch.assign_name, watch.source, watch.params)
  rescue
    _ -> socket
  end

  defp watches(socket) do
    case socket.private do
      %{upkeep_watches: watches} -> watches
      _ -> %{}
    end
  end

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end

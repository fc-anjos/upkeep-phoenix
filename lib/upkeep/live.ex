defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Live, only: [watch: 4, unwatch: 2, unwatch: 3, refresh: 4]

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
    source_id = Source.source_id(source, params)

    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        socket
        |> put_watch_assign(source_id, assign_name)
        |> assign(assign_name, Map.fetch!(socket.assigns, primary_assign_name(watch)))

      :error ->
        value = source.load(params)
        interest_keys = source.__upkeep_interest_keys__(params)

        join_interest(interest_keys, assign_name, source)

        socket
        |> put_watch(source_id, %{
          assign_name: assign_name,
          assign_names: MapSet.new([assign_name]),
          source: source,
          params: params,
          interest_keys: interest_keys
        })
        |> assign(assign_name, value)
    end
  end

  def unwatch(socket, assign_name) when is_atom(assign_name) do
    socket
    |> watches()
    |> Enum.filter(fn {_source_id, watch} -> MapSet.member?(watch.assign_names, assign_name) end)
    |> Enum.reduce(socket, fn {source_id, _watch}, socket ->
      remove_watch_assign(socket, source_id, assign_name)
    end)
  end

  def unwatch(socket, source, params) when is_atom(source) do
    params = normalize_params(params)
    source_id = Source.source_id(source, params)
    remove_watch(socket, source_id)
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

  defp join_interest(interest_keys, assign_name, source) do
    for key <- interest_keys do
      :ok =
        Group.join(@supervisor, Source.group_key(key), %{
          assign: assign_name,
          source: inspect(source)
        })
    end
  end

  defp put_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watch = Map.put(watch, :source_id, source_id)
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp put_watch_assign(socket, source_id, assign_name) do
    private = socket.private || %{}

    watches =
      Map.update!(Map.get(private, :upkeep_watches, %{}), source_id, fn watch ->
        Map.update!(watch, :assign_names, &MapSet.put(&1, assign_name))
      end)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp remove_watch(socket, source_id) do
    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        leave_interest(watch.interest_keys)

        private = socket.private || %{}
        watches = Map.delete(Map.get(private, :upkeep_watches, %{}), source_id)

        pending =
          MapSet.delete(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

        %{
          socket
          | private:
              private
              |> Map.put(:upkeep_watches, watches)
              |> Map.put(:upkeep_pending_refreshes, pending)
        }

      :error ->
        socket
    end
  end

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        assign_names = MapSet.delete(watch.assign_names, assign_name)

        if Enum.empty?(assign_names) do
          remove_watch(socket, source_id)
        else
          put_existing_watch(socket, source_id, %{watch | assign_names: assign_names})
        end

      :error ->
        socket
    end
  end

  defp put_existing_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp leave_interest(interest_keys) do
    for key <- interest_keys do
      case Group.leave(@supervisor, Source.group_key(key)) do
        :ok -> :ok
        {:error, :not_in_group} -> :ok
      end
    end

    :ok
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
    value = watch.source.load(watch.params)

    Enum.reduce(watch.assign_names, socket, fn assign_name, socket ->
      assign(socket, assign_name, value)
    end)
  rescue
    _ -> socket
  end

  defp primary_assign_name(watch) do
    watch.assign_name || Enum.at(watch.assign_names, 0)
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

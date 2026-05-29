defmodule Upkeep.Runtime.Result do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Runtime.Subscriptions
  alias Upkeep.Runtime.Telemetry

  @type effect ::
          {:assign, atom(), term()}
          | {:telemetry, [atom()], map(), map()}
          | {:register_source, term(), Upkeep.InvalidationSurface.t(), Upkeep.Source.Instance.t()}
          | {:unregister, term()}
          | {:track_source, term()}
          | {:join_local_notifications}
  @type result(socket) :: {:ok, socket, [effect()]}

  # External effects mutate shared cluster state (group membership, coordinator
  # registration). They are committed together with rollback so a raised effect
  # cannot leave a half-applied subscription behind; pure socket effects apply
  # only once the external side succeeds.
  @spec to_socket(result(socket)) :: socket when socket: term()
  def to_socket({:ok, socket, effects}) when is_list(effects) do
    started_at = System.monotonic_time()
    {external, internal} = Enum.split_with(effects, &external?/1)
    commit_external(external)
    socket = Enum.reduce(internal, socket, &apply_one/2)
    emit_effects_applied(effects, started_at)
    socket
  end

  defp external?({:register_source, _id, _surface, _instance}), do: true
  defp external?({:unregister, _id}), do: true
  defp external?({:track_source, _id}), do: true
  defp external?({:join_local_notifications}), do: true
  defp external?(_effect), do: false

  defp commit_external(effects) do
    Enum.reduce(effects, [], fn effect, compensations ->
      try do
        apply_external(effect)
        [compensation(effect) | compensations]
      rescue
        error ->
          Enum.each(compensations, & &1.())
          reraise error, __STACKTRACE__
      end
    end)
  end

  defp apply_external({:register_source, source_id, surface, instance}),
    do: Subscriptions.register(source_id, surface, instance)

  defp apply_external({:unregister, source_id}), do: Subscriptions.unregister(source_id)
  defp apply_external({:track_source, source_id}), do: Subscriptions.track_source(source_id)
  defp apply_external({:join_local_notifications}), do: Subscriptions.join_local_notifications()

  defp compensation({:register_source, source_id, _surface, _instance}),
    do: fn -> Subscriptions.unregister(source_id) end

  defp compensation({:track_source, source_id}),
    do: fn -> Subscriptions.untrack_source(source_id) end

  defp compensation(_effect), do: fn -> :ok end

  defp apply_one({:assign, name, value}, socket) do
    assign(socket, name, value)
  end

  defp apply_one({:telemetry, event, measurements, metadata}, socket) do
    Telemetry.emit(event, measurements, metadata)
    socket
  end

  defp emit_effects_applied(effects, started_at) do
    effect_counts = Enum.frequencies_by(effects, &effect_kind/1)

    :telemetry.execute(
      [:upkeep, :live, :effects, :apply],
      %{count: 1, duration: System.monotonic_time() - started_at},
      %{
        effect_count: length(effects),
        assign_count: Map.get(effect_counts, :assign, 0),
        telemetry_count: Map.get(effect_counts, :telemetry, 0),
        register_source_count: Map.get(effect_counts, :register_source, 0),
        unregister_count: Map.get(effect_counts, :unregister, 0)
      }
    )
  end

  defp effect_kind({:assign, _name, _value}), do: :assign
  defp effect_kind({:unregister, _source_id}), do: :unregister
  defp effect_kind({:telemetry, _event, _measurements, _metadata}), do: :telemetry
  defp effect_kind({:track_source, _source_id}), do: :track_source
  defp effect_kind({:join_local_notifications}), do: :join_local_notifications

  defp effect_kind({:register_source, _source_id, _surface, _instance}),
    do: :register_source

  defp effect_kind(_effect), do: :unknown
end

defmodule Upkeep.Coordinator.Graph.Shard.Retries do
  @moduledoc false

  @max_attempts 3
  @base_delay_ms 10
  @max_delay_ms 40

  def after_failure(state, node_id, %{loaded?: true}) do
    attempt = Map.get(state.retry_attempts, node_id, 0) + 1
    retry? = attempt <= @max_attempts

    state = put_in(state.retry_attempts[node_id], attempt)

    {state, delay_ms} =
      if retry? do
        delay_ms = retry_delay_ms(attempt)
        timer_ref = make_ref()
        process_ref = Process.send_after(self(), {:retry_source, node_id, timer_ref}, delay_ms)

        state =
          state
          |> cancel_timer(node_id)
          |> put_in([:retry_timers, node_id], {timer_ref, process_ref})

        {state, delay_ms}
      else
        {cancel_timer(state, node_id), nil}
      end

    {state,
     %{
       retry?: retry?,
       retry_attempt: attempt,
       retry_max_attempts: @max_attempts,
       retry_delay_ms: delay_ms
     }}
  end

  def after_failure(state, _node_id, _node) do
    {state,
     %{
       retry?: false,
       retry_attempt: 0,
       retry_max_attempts: @max_attempts,
       retry_delay_ms: nil
     }}
  end

  def clear(state, node_id) do
    state
    |> cancel_timer(node_id)
    |> update_in([:retry_attempts], &Map.delete(&1, node_id))
  end

  def reset(state, node_ids) do
    Enum.reduce(node_ids, state, &clear(&2, &1))
  end

  def cancel_all(state) do
    state.retry_timers
    |> Map.values()
    |> Enum.each(fn {_timer_ref, process_ref} -> Process.cancel_timer(process_ref) end)

    %{state | retry_attempts: %{}, retry_timers: %{}}
  end

  def pop_timer(state, node_id, timer_ref) do
    case Map.fetch(state.retry_timers, node_id) do
      {:ok, {^timer_ref, _process_ref}} ->
        {:ok, update_in(state.retry_timers, &Map.delete(&1, node_id))}

      _stale_or_missing ->
        :stale
    end
  end

  defp cancel_timer(state, node_id) do
    case Map.fetch(state.retry_timers, node_id) do
      {:ok, {_timer_ref, process_ref}} ->
        Process.cancel_timer(process_ref)
        update_in(state.retry_timers, &Map.delete(&1, node_id))

      :error ->
        state
    end
  end

  defp retry_delay_ms(attempt) do
    delay =
      @base_delay_ms
      |> Kernel.*(:math.pow(2, attempt - 1))
      |> round()
      |> min(@max_delay_ms)

    delay + jitter(delay)
  end

  defp jitter(delay), do: :rand.uniform(max(1, div(delay, 2) + 1)) - 1
end

defmodule Upkeep.Coordinator.Graph.Shard.Retries do
  @moduledoc false

  alias Upkeep.Retry

  def after_failure(state, node_id, %{loaded?: true, retry: retry}) do
    {retries, metadata} = Retry.after_failure(state.retries, node_id, retry, &schedule_retry/3)
    {%{state | retries: retries}, metadata}
  end

  def after_failure(state, _node_id, _node) do
    {state, Retry.no_retry_metadata(state.retries)}
  end

  def clear(state, node_id) do
    %{state | retries: Retry.clear(state.retries, node_id)}
  end

  def reset(state, node_ids) do
    %{state | retries: Retry.reset(state.retries, node_ids)}
  end

  def cancel_all(state) do
    %{state | retries: Retry.cancel_all(state.retries)}
  end

  def pop_timer(state, node_id, timer_ref) do
    case Retry.pop_timer(state.retries, node_id, timer_ref) do
      {:ok, retries} ->
        {:ok, %{state | retries: retries}}

      :stale ->
        :stale
    end
  end

  defp schedule_retry(node_id, timer_ref, delay_ms) do
    Process.send_after(self(), {:retry_source, node_id, timer_ref}, delay_ms)
  end
end

defmodule Upkeep.Retry do
  @moduledoc false

  defstruct attempts: %{}, timers: %{}, max_attempts: 3, base_delay_ms: 10, max_delay_ms: 40

  def new(opts \\ []) do
    %__MODULE__{
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      base_delay_ms: Keyword.get(opts, :base_delay_ms, 10),
      max_delay_ms: Keyword.get(opts, :max_delay_ms, 40)
    }
  end

  def after_failure(%__MODULE__{} = retry, key, schedule_fn) when is_function(schedule_fn, 3) do
    attempt = Map.get(retry.attempts, key, 0) + 1
    retry? = attempt <= retry.max_attempts
    retry = put_in(retry.attempts[key], attempt)

    {retry, delay_ms} =
      if retry? do
        delay_ms = retry_delay_ms(retry, attempt)
        timer_ref = make_ref()
        process_ref = schedule_fn.(key, timer_ref, delay_ms)

        retry =
          retry
          |> cancel_timer(key)
          |> put_in([Access.key!(:timers), key], {timer_ref, process_ref})

        {retry, delay_ms}
      else
        {cancel_timer(retry, key), nil}
      end

    {retry, metadata(retry?, attempt, retry.max_attempts, delay_ms)}
  end

  def no_retry_metadata(%__MODULE__{} = retry) do
    metadata(false, 0, retry.max_attempts, nil)
  end

  def clear(%__MODULE__{} = retry, key) do
    retry
    |> cancel_timer(key)
    |> update_in([Access.key!(:attempts)], &Map.delete(&1, key))
  end

  def reset(%__MODULE__{} = retry, keys) do
    Enum.reduce(keys, retry, &clear(&2, &1))
  end

  def cancel_all(%__MODULE__{} = retry) do
    retry.timers
    |> Map.values()
    |> Enum.each(fn {_timer_ref, process_ref} -> Process.cancel_timer(process_ref) end)

    %{retry | attempts: %{}, timers: %{}}
  end

  def pop_timer(%__MODULE__{} = retry, key, timer_ref) do
    case Map.fetch(retry.timers, key) do
      {:ok, {^timer_ref, _process_ref}} ->
        {:ok, update_in(retry.timers, &Map.delete(&1, key))}

      _stale_or_missing ->
        :stale
    end
  end

  defp cancel_timer(retry, key) do
    case Map.fetch(retry.timers, key) do
      {:ok, {_timer_ref, process_ref}} ->
        Process.cancel_timer(process_ref)
        update_in(retry.timers, &Map.delete(&1, key))

      :error ->
        retry
    end
  end

  defp retry_delay_ms(retry, attempt) do
    delay =
      retry.base_delay_ms
      |> Kernel.*(:math.pow(2, attempt - 1))
      |> round()
      |> min(retry.max_delay_ms)

    delay + jitter(delay)
  end

  defp jitter(delay), do: :rand.uniform(max(1, div(delay, 2) + 1)) - 1

  defp metadata(retry?, attempt, max_attempts, delay_ms) do
    %{
      retry?: retry?,
      retry_attempt: attempt,
      retry_max_attempts: max_attempts,
      retry_delay_ms: delay_ms
    }
  end
end

defmodule Upkeep.Coordinator.Retry do
  @moduledoc false

  defstruct attempts: %{}, timers: %{}, max_attempts: 3, base_delay_ms: 10, max_delay_ms: 40

  @type key :: term()
  @type policy :: :default | :source | :none | :not_loaded
  @type metadata :: %{
          retry?: boolean(),
          retry_attempt: non_neg_integer(),
          retry_max_attempts: non_neg_integer(),
          retry_delay_ms: non_neg_integer() | nil,
          retry_policy: policy()
        }
  @type schedule_fn :: (key(), reference(), non_neg_integer() -> reference())
  @type timer_entry :: {reference(), reference()}
  @type t :: %__MODULE__{
          attempts: %{optional(key()) => non_neg_integer()},
          timers: %{optional(key()) => timer_entry()},
          max_attempts: non_neg_integer(),
          base_delay_ms: non_neg_integer(),
          max_delay_ms: non_neg_integer()
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      base_delay_ms: Keyword.get(opts, :base_delay_ms, 10),
      max_delay_ms: Keyword.get(opts, :max_delay_ms, 40)
    }
  end

  @spec after_failure(t(), key(), schedule_fn()) :: {t(), metadata()}
  def after_failure(%__MODULE__{} = retry, key, schedule_fn) when is_function(schedule_fn, 3) do
    after_failure(retry, key, :default, schedule_fn)
  end

  @spec after_failure(t(), key(), false | keyword() | :default, schedule_fn()) ::
          {t(), metadata()}
  def after_failure(%__MODULE__{} = retry, key, false, _schedule_fn) do
    retry = clear(retry, key)
    {retry, metadata(false, 1, 0, nil, :none)}
  end

  def after_failure(%__MODULE__{} = retry, key, opts, schedule_fn)
      when is_function(schedule_fn, 3) do
    effective = apply_opts(retry, opts)
    attempt = Map.get(retry.attempts, key, 0) + 1
    retry? = attempt <= effective.max_attempts
    retry = put_in(retry.attempts[key], attempt)

    {retry, delay_ms} =
      if retry? do
        delay_ms = retry_delay_ms(effective, attempt)
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

    {retry, metadata(retry?, attempt, effective.max_attempts, delay_ms, policy(opts))}
  end

  @spec no_retry_metadata(t()) :: metadata()
  def no_retry_metadata(%__MODULE__{} = retry) do
    metadata(false, 0, retry.max_attempts, nil, :not_loaded)
  end

  @spec clear(t(), key()) :: t()
  def clear(%__MODULE__{} = retry, key) do
    retry
    |> cancel_timer(key)
    |> update_in([Access.key!(:attempts)], &Map.delete(&1, key))
  end

  @spec reset(t(), Enumerable.t()) :: t()
  def reset(%__MODULE__{} = retry, keys) do
    Enum.reduce(keys, retry, &clear(&2, &1))
  end

  @spec cancel_all(t()) :: t()
  def cancel_all(%__MODULE__{} = retry) do
    retry.timers
    |> Map.values()
    |> Enum.each(fn {_timer_ref, process_ref} ->
      process_ref
      |> cancel_retry_timer()
      |> accept_timer_cancellation()
    end)

    %{retry | attempts: %{}, timers: %{}}
  end

  @spec pop_timer(t(), key(), reference()) :: {:ok, t()} | :stale
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
        process_ref
        |> cancel_retry_timer()
        |> forget_timer_after_cancellation(retry, key)

      :error ->
        retry
    end
  end

  defp cancel_retry_timer(process_ref) do
    case Process.cancel_timer(process_ref) do
      false -> :already_fired_or_missing
      remaining_ms when is_integer(remaining_ms) -> {:cancelled, remaining_ms}
    end
  end

  defp accept_timer_cancellation({:cancelled, _remaining_ms}), do: :ok
  defp accept_timer_cancellation(:already_fired_or_missing), do: :ok

  defp forget_timer_after_cancellation({:cancelled, _remaining_ms}, retry, key) do
    delete_timer(retry, key)
  end

  defp forget_timer_after_cancellation(:already_fired_or_missing, retry, key) do
    delete_timer(retry, key)
  end

  defp delete_timer(retry, key) do
    update_in(retry.timers, &Map.delete(&1, key))
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

  defp apply_opts(retry, :default), do: retry
  defp apply_opts(retry, opts) when is_list(opts), do: struct!(retry, opts)

  defp policy(:default), do: :default
  defp policy(opts) when is_list(opts), do: :source

  defp metadata(retry?, attempt, max_attempts, delay_ms, policy) do
    %{
      retry?: retry?,
      retry_attempt: attempt,
      retry_max_attempts: max_attempts,
      retry_delay_ms: delay_ms,
      retry_policy: policy
    }
  end
end

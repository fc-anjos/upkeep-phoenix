defmodule Upkeep.RetryTest do
  use ExUnit.Case, async: true

  alias Upkeep.Retry

  test "first failure schedules with base delay metadata" do
    retry = Retry.new(base_delay_ms: 10, max_delay_ms: 40)

    {retry, metadata} =
      Retry.after_failure(retry, :source, fn key, timer_ref, delay_ms ->
        send(self(), {:scheduled, key, timer_ref, delay_ms})
        make_ref()
      end)

    assert_receive {:scheduled, :source, timer_ref, delay_ms}
    assert is_reference(timer_ref)
    assert delay_ms >= 10
    assert delay_ms <= 15
    assert metadata.retry? == true
    assert metadata.retry_attempt == 1
    assert metadata.retry_max_attempts == 3
    assert metadata.retry_delay_ms == delay_ms
    assert Map.has_key?(retry.timers, :source)
  end

  test "retry delay doubles and caps" do
    retry = Retry.new(base_delay_ms: 10, max_delay_ms: 20)
    schedule = fn _key, _timer_ref, _delay_ms -> make_ref() end

    {retry, first} = Retry.after_failure(retry, :source, schedule)
    {_retry, second} = Retry.after_failure(retry, :source, schedule)

    assert first.retry_delay_ms in 10..15
    assert second.retry_delay_ms in 20..30
  end

  test "gives up after max attempts" do
    retry = Retry.new(max_attempts: 1)
    schedule = fn _key, _timer_ref, _delay_ms -> make_ref() end

    {retry, first} = Retry.after_failure(retry, :source, schedule)
    {retry, second} = Retry.after_failure(retry, :source, schedule)

    assert first.retry? == true
    assert second.retry? == false
    assert second.retry_attempt == 2
    assert second.retry_delay_ms == nil
    refute Map.has_key?(retry.timers, :source)
  end

  test "clear cancels timer and resets attempts" do
    parent = self()

    {retry, _metadata} =
      Retry.after_failure(Retry.new(), :source, fn _key, _timer_ref, _delay_ms ->
        Process.send_after(parent, :should_not_arrive, 5_000)
      end)

    retry = Retry.clear(retry, :source)

    refute Map.has_key?(retry.timers, :source)
    refute Map.has_key?(retry.attempts, :source)
  end

  test "pop_timer accepts current timer and rejects stale refs" do
    {retry, _metadata} =
      Retry.after_failure(Retry.new(), :source, fn _key, _timer_ref, _delay_ms -> make_ref() end)

    {timer_ref, _process_ref} = Map.fetch!(retry.timers, :source)

    assert :stale = Retry.pop_timer(retry, :source, make_ref())
    assert {:ok, retry} = Retry.pop_timer(retry, :source, timer_ref)
    refute Map.has_key?(retry.timers, :source)
  end
end

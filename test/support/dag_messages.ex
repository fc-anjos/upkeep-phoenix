defmodule Upkeep.TestSupport.DagMessages do
  @moduledoc false

  import ExUnit.Assertions

  @buffer_key {__MODULE__, :buffer}
  @default_timeout 1_000

  def receive_batch(timeout \\ @default_timeout) do
    receive do
      {:dag_values, batch} -> batch
    after
      timeout -> flunk("did not receive :dag_values")
    end
  end

  def receive_value(source_id, timeout \\ @default_timeout) do
    case pop_buffered(source_id) do
      {:ok, value} -> value
      :error -> drain_until(source_id, timeout)
    end
  end

  def assert_values(expected, timeout \\ @default_timeout) when is_list(expected) do
    do_assert_values(expected, [], timeout)
  end

  def refute_value(source_id, timeout \\ 100) do
    refute_receive {:dag_values, [{^source_id, _value}]}, timeout
  end

  def refute_any(timeout \\ 0) do
    refute_receive {:dag_values, _batch}, timeout
  end

  def clear_buffer do
    Process.delete(@buffer_key)
    :ok
  end

  defp do_assert_values(expected, received, timeout) do
    if Enum.all?(expected, &(&1 in received)) do
      received
    else
      receive do
        {:dag_values, batch} -> do_assert_values(expected, received ++ batch, timeout)
      after
        timeout ->
          flunk("expected DAG values #{inspect(expected)}, got #{inspect(received)}")
      end
    end
  end

  defp drain_until(source_id, timeout) do
    receive do
      {:dag_values, batch} ->
        case Enum.split_with(batch, fn {id, _value} -> id == source_id end) do
          {[{^source_id, value} | rest_match], remaining} ->
            buffer(rest_match ++ remaining)
            value

          {[], _matches} ->
            buffer(batch)
            drain_until(source_id, timeout)
        end
    after
      timeout -> flunk("did not receive :dag_values for #{inspect(source_id)}")
    end
  end

  defp pop_buffered(source_id) do
    buffered = Process.get(@buffer_key, [])

    case Enum.split_with(buffered, fn {id, _value} -> id == source_id end) do
      {[{^source_id, value} | rest_match], remaining} ->
        Process.put(@buffer_key, rest_match ++ remaining)
        {:ok, value}

      {[], _remaining} ->
        :error
    end
  end

  defp buffer(pairs) do
    Process.put(@buffer_key, Process.get(@buffer_key, []) ++ pairs)
  end
end

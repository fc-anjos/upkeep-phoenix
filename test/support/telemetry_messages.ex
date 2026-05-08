defmodule Upkeep.TestSupport.TelemetryMessages do
  @moduledoc false

  import ExUnit.Assertions

  @default_timeout 1_000

  def assert_counted(event, expected_metadata \\ [], opts \\ []) do
    opts =
      opts
      |> Keyword.put(
        :measurements,
        Map.merge(%{count: 1}, expected_map(opts[:measurements] || %{}))
      )
      |> Keyword.put(:metadata, expected_metadata)

    assert_event(event, opts)
  end

  def assert_event(event, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    expected_measurements = expected_map(Keyword.get(opts, :measurements, %{}))
    expected_metadata = expected_map(Keyword.get(opts, :metadata, %{}))
    deadline = System.monotonic_time(:millisecond) + timeout

    receive_matching_event(event, expected_measurements, expected_metadata, [], deadline)
  end

  defp receive_matching_event(event, expected_measurements, expected_metadata, skipped, deadline) do
    receive do
      {:telemetry, ^event, measurements, metadata} = message ->
        if subset?(measurements, expected_measurements) and subset?(metadata, expected_metadata) do
          replay(skipped)
          {measurements, metadata}
        else
          receive_matching_event(
            event,
            expected_measurements,
            expected_metadata,
            [message | skipped],
            deadline
          )
        end
    after
      remaining_ms(deadline) ->
        replay(skipped)

        flunk("""
        expected telemetry event #{inspect(event)}
        with measurements including #{inspect(expected_measurements)}
        and metadata including #{inspect(expected_metadata)}
        """)
    end
  end

  defp remaining_ms(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp replay(skipped) do
    skipped
    |> Enum.reverse()
    |> Enum.each(&send(self(), &1))
  end

  defp subset?(actual, expected) do
    actual = Map.new(actual)

    Enum.all?(expected, fn {key, expected_value} ->
      Map.has_key?(actual, key) and value_matches?(Map.fetch!(actual, key), expected_value)
    end)
  end

  defp value_matches?(actual, expected)
       when is_map(actual) and is_map(expected) and not is_struct(actual) and
              not is_struct(expected) do
    subset?(actual, expected)
  end

  defp value_matches?(actual, expected), do: actual == expected

  defp expected_map(nil), do: %{}
  defp expected_map(expected) when is_map(expected), do: expected
  defp expected_map(expected) when is_list(expected), do: Map.new(expected)
end

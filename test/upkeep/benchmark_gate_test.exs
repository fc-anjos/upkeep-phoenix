defmodule Upkeep.BenchmarkGateTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 120_000
  @moduletag :benchmark
  @bench_watches 100

  test "sharing benchmarks fire and pass their gates" do
    benchmark_specs()
    |> Task.async_stream(
      fn {path, assertions} -> {path, assertions, run_benchmark!(path)} end,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity,
      ordered: false
    )
    |> Enum.each(fn
      {:ok, {path, assertions, output}} ->
        Enum.each(assertions, fn assertion ->
          assert output =~ assertion, """
          expected #{path} output to match #{inspect(assertion)}

          #{output}
          """
        end)

      {:exit, reason} ->
        flunk("benchmark task failed: #{inspect(reason)}")
    end)
  end

  defp run_benchmark!(path) do
    port = 41_000 + System.unique_integer([:positive, :monotonic])

    # The parent `mix test` run compiles the project before this test starts.
    # Child benchmark VMs skip repeated compilation and execute the .exs script.
    {output, status} =
      System.cmd("mix", ["run", "--no-compile", path],
        cd: File.cwd!(),
        env: [
          {"MIX_ENV", "test"},
          {"PORT", Integer.to_string(port)},
          {"BENCH_WATCHES", Integer.to_string(@bench_watches)}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    output
  end

  defp benchmark_specs do
    [
      {"bench/initial_source_sharing.exs",
       [
         "connected_watches=#{@bench_watches}",
         ~r/same\s+1\s+\d+\.\d+/,
         ~r/distinct\s+#{@bench_watches}\s+\d+\.\d+/,
         "\nOK\n"
       ]},
      {"bench/initial_derived_sharing.exs",
       [
         "connected_derives=#{@bench_watches}",
         ~r/same\s+1\s+1\s+\d+\.\d+/,
         ~r/distinct\s+#{@bench_watches}\s+#{@bench_watches}\s+\d+\.\d+/,
         "\nOK\n"
       ]},
      {"bench/initial_multi_source_derived_sharing.exs",
       [
         "connected_multi_source_derives=#{@bench_watches}",
         ~r/same\s+1\s+\d+\.\d+/,
         ~r/distinct\s+#{@bench_watches}\s+\d+\.\d+/,
         ~r/cross\s+#{@bench_watches}\s+\d+\.\d+/,
         "\nOK\n"
       ]},
      {"bench/steady_state_derived_sharing.exs",
       [
         "steady_state_subscribers=#{@bench_watches}",
         ~r/update\s+1\s+1\s+#{@bench_watches}\s+\d+\.\d+/,
         "\nOK\n"
       ]}
    ]
  end
end

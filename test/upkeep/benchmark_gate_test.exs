defmodule Upkeep.BenchmarkGateTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 120_000
  @moduletag :benchmark
  @bench_watches 100

  test "initial source sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/initial_source_sharing.exs")

    assert output =~ "connected_watches=#{@bench_watches}"
    assert output =~ ~r/same\s+1\s+\d+\.\d+/
    assert output =~ ~r/distinct\s+#{@bench_watches}\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  test "initial derived sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/initial_derived_sharing.exs")

    assert output =~ "connected_derives=#{@bench_watches}"
    assert output =~ ~r/same\s+1\s+1\s+\d+\.\d+/
    assert output =~ ~r/distinct\s+#{@bench_watches}\s+#{@bench_watches}\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  test "initial multi-source derived sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/initial_multi_source_derived_sharing.exs")

    assert output =~ "connected_multi_source_derives=#{@bench_watches}"
    assert output =~ ~r/same\s+1\s+\d+\.\d+/
    assert output =~ ~r/distinct\s+#{@bench_watches}\s+\d+\.\d+/
    assert output =~ ~r/cross\s+#{@bench_watches}\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  test "steady-state derived sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/steady_state_derived_sharing.exs")

    assert output =~ "steady_state_subscribers=#{@bench_watches}"
    assert output =~ ~r/update\s+1\s+1\s+#{@bench_watches}\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  defp run_benchmark!(path) do
    port = 41_000 + System.unique_integer([:positive, :monotonic])

    {output, status} =
      System.cmd("mix", ["run", path],
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
end

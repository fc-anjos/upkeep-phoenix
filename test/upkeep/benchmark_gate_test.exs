defmodule Upkeep.BenchmarkGateTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 120_000

  test "initial source sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/initial_source_sharing.exs")

    assert output =~ "connected_watches=1000"
    assert output =~ ~r/same\s+1\s+\d+\.\d+/
    assert output =~ ~r/distinct\s+1000\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  test "initial derived sharing benchmark fires and passes its gate" do
    output = run_benchmark!("bench/initial_derived_sharing.exs")

    assert output =~ "connected_derives=1000"
    assert output =~ ~r/same\s+1\s+1\s+\d+\.\d+/
    assert output =~ ~r/distinct\s+1000\s+1000\s+\d+\.\d+/
    assert output =~ "\nOK\n"
  end

  defp run_benchmark!(path) do
    port = 41_000 + System.unique_integer([:positive, :monotonic])

    {output, status} =
      System.cmd("mix", ["run", path],
        cd: File.cwd!(),
        env: [{"MIX_ENV", "test"}, {"PORT", Integer.to_string(port)}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    output
  end
end

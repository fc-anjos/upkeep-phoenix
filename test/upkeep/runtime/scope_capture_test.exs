defmodule Upkeep.Runtime.ScopeCaptureTest do
  use ExUnit.Case, async: false

  alias Upkeep.Runtime.ScopeCapture
  alias Upkeep.TestSupport.Config

  test "raise policy raises implicit scope errors" do
    Config.with_upkeep(:captured_scope_policy, :raise, fn ->
      assert_raise Upkeep.Runtime.ImplicitScopeError, ~r/captures :socket/, fn ->
        ScopeCapture.apply_policy({:captured_scope, {__MODULE__, :example, 1}, :socket}, %{
          assign_name: :label
        })
      end
    end)
  end

  test "telemetry policy allows captured scope analysis to continue" do
    Config.with_upkeep(:captured_scope_policy, :telemetry, fn ->
      assert :ok =
               ScopeCapture.apply_policy(
                 {:captured_scope, {__MODULE__, :example, 1}, :socket},
                 %{assign_name: :label}
               )
    end)
  end
end

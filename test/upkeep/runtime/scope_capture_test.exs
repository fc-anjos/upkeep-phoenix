defmodule Upkeep.Internal.Runtime.ScopeCaptureTest do
  use ExUnit.Case, async: false

  alias Upkeep.Internal.Runtime.ScopeCapture

  test "raise policy raises implicit scope errors" do
    with_policy(:raise, fn ->
      assert_raise Upkeep.ImplicitScopeError, ~r/captures :socket/, fn ->
        ScopeCapture.apply_policy({:captured_scope, {__MODULE__, :example, 1}, :socket}, %{
          assign_name: :label
        })
      end
    end)
  end

  test "telemetry policy allows captured scope analysis to continue" do
    with_policy(:telemetry, fn ->
      assert :ok =
               ScopeCapture.apply_policy(
                 {:captured_scope, {__MODULE__, :example, 1}, :socket},
                 %{assign_name: :label}
               )
    end)
  end

  defp with_policy(policy, fun) do
    previous = Application.get_env(:upkeep, :captured_scope_policy, :__missing__)
    Application.put_env(:upkeep, :captured_scope_policy, policy)

    try do
      fun.()
    after
      case previous do
        :__missing__ -> Application.delete_env(:upkeep, :captured_scope_policy)
        value -> Application.put_env(:upkeep, :captured_scope_policy, value)
      end
    end
  end
end

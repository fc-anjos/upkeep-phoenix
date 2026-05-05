defmodule Upkeep.Runtime.ScopeCaptureTest do
  use ExUnit.Case, async: false

  alias Upkeep.Runtime.ScopeCapture

  test "analyze identifies external functions" do
    assert ScopeCapture.analyze(&:erlang.phash2/1) == {:external, {:erlang, :phash2, 1}}
  end

  test "analyze identifies captured functions without scope-like values" do
    prefix = inspect(make_ref())

    assert {:captured, {_module, _name, 1}} =
             ScopeCapture.analyze(fn value -> prefix <> to_string(value) end)
  end

  test "analyze identifies captured current scope values" do
    context = %{assigns: %{current_scope: %{user_id: make_ref()}}}

    assert {:captured_scope, {_module, _name, 1}, :socket} =
             ScopeCapture.analyze(fn value -> {context.assigns.current_scope, value} end)
  end

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

  test "implicit scope metadata reports dependency state" do
    socket = %{assigns: %{current_scope: %{user_id: 1}}}

    assert ScopeCapture.implicit_scope_metadata(%{assigns: %{}}, []) == :missing

    assert ScopeCapture.implicit_scope_metadata(socket, [{:scope, :current_scope}]) ==
             :dependency

    assert ScopeCapture.implicit_scope_metadata(socket, []) == :available
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

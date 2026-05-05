defmodule Upkeep.Runtime.ScopeCaptureAnalysisTest do
  use ExUnit.Case, async: true

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

  test "implicit scope metadata reports dependency state" do
    socket = %{assigns: %{current_scope: %{user_id: 1}}}

    assert ScopeCapture.implicit_scope_metadata(%{assigns: %{}}, []) == :missing

    assert ScopeCapture.implicit_scope_metadata(socket, [{:scope, :current_scope}]) ==
             :dependency

    assert ScopeCapture.implicit_scope_metadata(socket, []) == :available
  end
end

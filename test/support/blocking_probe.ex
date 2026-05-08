defmodule Upkeep.TestSupport.BlockingProbe do
  @moduledoc false

  import ExUnit.Assertions

  def block(test_pid, event, message) do
    block(test_pid, event, [], message)
  end

  def block(test_pid, event, payload, message) do
    send(test_pid, List.to_tuple([event, self() | List.wrap(payload)]))

    receive do
      :continue -> :ok
    after
      1_000 -> raise message
    end
  end

  def await(event) do
    assert_receive {^event, pid}
    pid
  end

  def await_with(event, payload) do
    assert_receive {^event, pid, ^payload}
    pid
  end

  def continue(pid), do: send(pid, :continue)
end

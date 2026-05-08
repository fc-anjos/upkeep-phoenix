defmodule Upkeep.TestSupport do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [
      Config,
      DagMessages,
      LiveRefreshFixture,
      LiveSocket,
      TelemetryMessages
    ],
    check: [out: false]

  def attach_telemetry(events) do
    test_pid = self()
    handler_id = {__MODULE__, test_pid, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end

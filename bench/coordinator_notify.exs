# Publisher-side microbench for the three notify strategies.
#
#   mix run bench/coordinator_notify.exs
#
# Measures wall-clock cost of `notify(event)` from the publisher's POV
# under varying parallel publisher counts. Subscribers are fake processes
# that drain `{:upkeep_event, _}` to keep mailboxes from growing without
# bound — they do no work, so this isolates the publisher path.

Application.ensure_all_started(:upkeep)

# Wait for the durable supervisor + Group cluster to be ready.
{:ok, _} = Upkeep.Coordinator.ensure_started()
{:ok, _} = Upkeep.Coordinator.Cast.start_link()

defmodule Bench.FakeEvent do
  defstruct [:id, :name, :schema, :tenant_id]
end

defmodule Bench.Subscriber do
  @supervisor Upkeep.DurableSupervisor

  def spawn_many(count, keys) do
    for _ <- 1..count do
      spawn_link(fn ->
        Enum.each(keys, &Group.join(@supervisor, &1, %{kind: :bench}))
        loop()
      end)
    end
  end

  defp loop do
    receive do
      _ -> loop()
    end
  end
end

# Build a representative event and pre-compute its group keys so subscribers
# can join them. This mirrors what a LiveView would do on mount.
event =
  struct(Bench.FakeEvent, id: 1, name: :insert, schema: :widgets, tenant_id: 42)

keys =
  event
  |> Upkeep.Source.event_keys()
  |> Enum.map(&Upkeep.Source.group_key/1)
  |> Enum.uniq()

IO.puts("Event produces #{length(keys)} unique group keys")

# Spawn 100 fake subscribers, each joining all keys for this event shape.
_subs = Bench.Subscriber.spawn_many(100, keys)

# Give Group time to propagate joins before we start measuring.
Process.sleep(200)

Benchee.run(
  %{
    "stateless" => fn -> Upkeep.Coordinator.Stateless.notify(event) end,
    "durable_call" => fn -> Upkeep.Coordinator.notify(event) end,
    "plain_cast" => fn -> Upkeep.Coordinator.Cast.notify(event) end
  },
  time: 5,
  warmup: 2,
  memory_time: 1,
  parallel: System.schedulers_online(),
  print: [fast_warning: false]
)

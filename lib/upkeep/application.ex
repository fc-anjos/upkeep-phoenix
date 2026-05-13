defmodule Upkeep.Application do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Upkeep.Supervision
    ],
    type: :strict

  use Application

  @impl true
  def start(_type, _args) do
    Upkeep.Supervision.start_link(name: Upkeep.Supervisor)
  end

  @impl true
  def config_change(_changed, _new, _removed) do
    :ok
  end
end

defmodule Upkeep.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Upkeep, []}
    ]

    opts = [strategy: :one_for_one, name: Upkeep.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    _ = {changed, removed}
    :ok
  end
end

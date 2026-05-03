defmodule Upkeep.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      UpkeepWeb.Telemetry,
      Upkeep.Observability,
      Upkeep.Repo,
      {Ecto.Migrator,
       repos: Application.fetch_env!(:upkeep, :ecto_repos), skip: skip_migrations?()},
      {DNSCluster, query: Application.get_env(:upkeep, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Upkeep.PubSub},
      Upkeep.Kanban,
      durable_supervisor_child(),
      # Start to serve requests, typically the last entry
      UpkeepWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Upkeep.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    UpkeepWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp skip_migrations?() do
    # By default, sqlite migrations are run when using a release
    System.get_env("RELEASE_NAME") == nil
  end

  defp durable_supervisor_child do
    opts =
      :upkeep
      |> Application.fetch_env!(:durable_server)
      |> Keyword.put(:name, Upkeep.DurableSupervisor)

    {DurableServer.Supervisor, opts}
  end
end

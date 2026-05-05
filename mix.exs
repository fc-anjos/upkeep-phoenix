defmodule Upkeep.MixProject do
  use Mix.Project

  def project do
    [
      app: :upkeep,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      deps: deps()
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Upkeep.Application, []},
      extra_applications: [:logger]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:ecto, "~> 3.13"},
      {:ecto_sql, "~> 3.13"},
      {:phoenix_live_view, "~> 1.1.0"},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:ecto_sqlite3, ">= 0.0.0", only: :test},
      {:telemetry, "~> 1.0"},
      {:group, "~> 0.1"},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      "test.setup": ["ecto.create --quiet", "ecto.migrate --quiet"],
      precommit: ["compile --warnings-as-errors", "deps.unlock --unused", "format", "test"]
    ]
  end

  defp description do
    "Domain-reactive LiveView runtime for watching sources and refreshing server-rendered UI."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"Docs" => "https://hexdocs.pm/upkeep"},
      files: ~w(lib mix.exs README.md LICENSE* docs)
    ]
  end

  defp docs do
    [
      main: "Upkeep",
      extras: [
        "README.md",
        "docs/getting-started.md",
        "docs/public-api.md",
        "docs/known-gaps.md",
        "docs/query-coverage-known-gaps.md"
      ],
      groups_for_extras: [
        Guides: ["README.md", "docs/getting-started.md", "docs/public-api.md"],
        Reference: ["docs/known-gaps.md", "docs/query-coverage-known-gaps.md"]
      ]
    ]
  end
end

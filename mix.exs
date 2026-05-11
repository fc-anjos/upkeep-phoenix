defmodule Upkeep.MixProject do
  use Mix.Project

  def project do
    [
      app: :upkeep,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: compilers(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      docs: docs(),
      boundary: boundary(),
      dialyzer: dialyzer(),
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

  defp compilers(env) when env in [:dev, :test], do: [:boundary] ++ Mix.compilers()
  defp compilers(_env), do: Mix.compilers()

  defp boundary do
    [
      default: [
        check: [
          aliases: true,
          apps: [{:mix, :runtime}]
        ]
      ]
    ]
  end

  defp dialyzer do
    [
      flags: [
        :unmatched_returns,
        :error_handling,
        :underspecs,
        :extra_return,
        :missing_return,
        :overlapping_contract
      ],
      plt_add_apps: [:ex_unit]
    ]
  end

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
      {:postgrex, ">= 0.0.0", only: :test},
      {:telemetry, "~> 1.0"},
      {:group, "~> 0.1"},
      {:boundary, "~> 0.10.4", runtime: false},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      "test.setup": ["ecto.create --quiet", "ecto.migrate --quiet"],
      precommit: [
        "compile --warnings-as-errors",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "deps.unlock --unused",
        "format",
        "credo --strict",
        "test",
        "dialyzer"
      ]
    ]
  end

  defp description do
    "Domain-reactive LiveView runtime for watching sources and refreshing server-rendered UI."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"Docs" => "https://hexdocs.pm/upkeep"},
      files: ~w(lib mix.exs README.md LICENSE*)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end
end

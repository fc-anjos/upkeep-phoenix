defmodule Mix.Tasks.Upkeep.Audit do
  @shortdoc "Find writes that may bypass Upkeep repo capture"

  @moduledoc """
  Statically scan source for write paths that may bypass Upkeep repo capture and
  leave watched sources stale.

  Out-of-band writes (raw SQL, an unwrapped/second repo) do not emit
  `Upkeep.Change` events. This task parses each file's AST and reports the common
  shapes so each can be confirmed intentional or routed through
  `use Upkeep.Ecto.Repo`. Because it works on the parsed AST, comments and
  docstrings are never matched.

      mix upkeep.audit          # scans lib/
      mix upkeep.audit lib web  # scans the given roots

  It is advisory: it reports findings and always exits successfully. Findings are
  signals, not proof — a flagged call may be a deliberate `upkeep: false` write
  or a read-only query.
  """

  use Boundary, top_level?: true, deps: [{Mix, :runtime}], exports: []

  use Mix.Task

  @impl true
  def run(args) do
    roots = if args == [], do: ["lib"], else: args

    findings =
      roots
      |> Enum.flat_map(&elixir_files/1)
      |> Enum.flat_map(&scan_file/1)

    report(findings)
    :ok
  end

  defp elixir_files(root) do
    Path.wildcard(Path.join(root, "**/*.{ex,exs}"))
  end

  defp scan_file(path) do
    source = File.read!(path)

    case Code.string_to_quoted(source) do
      {:ok, ast} ->
        lines = String.split(source, "\n")

        path
        |> findings_in(ast)
        |> Enum.map(&add_snippet(&1, lines))

      {:error, _reason} ->
        []
    end
  end

  defp findings_in(path, ast) do
    {_ast, findings} =
      Macro.prewalk(ast, [], fn node, acc -> {node, node_findings(path, node) ++ acc} end)

    Enum.reverse(findings)
  end

  # `use Ecto.Repo` — a plain, non-capturing repo
  defp node_findings(path, {:use, meta, [{:__aliases__, _, [:Ecto, :Repo]} | _]}) do
    [finding(path, meta, "plain Ecto.Repo is not capture-enabled (use Upkeep.Ecto.Repo)")]
  end

  # `Ecto.Adapters.SQL.query/query!/stream` — raw SQL
  defp node_findings(
         path,
         {{:., _, [{:__aliases__, _, [:Ecto, :Adapters, :SQL]}, fun]}, meta, _args}
       )
       when fun in [:query, :query!, :stream] do
    [finding(path, meta, "raw SQL via Ecto.Adapters.SQL.#{fun} bypasses capture")]
  end

  # `<Anything>Repo.query/query!` — raw SQL through a repo
  defp node_findings(path, {{:., _, [{:__aliases__, _, mods}, fun]}, meta, _args})
       when fun in [:query, :query!] and is_list(mods) do
    if mods |> List.last() |> to_string() |> String.ends_with?("Repo") do
      [finding(path, meta, "raw SQL via #{Enum.join(mods, ".")}.#{fun} bypasses capture")]
    else
      []
    end
  end

  defp node_findings(_path, _node), do: []

  defp finding(path, meta, reason) do
    %{path: path, line: Keyword.get(meta, :line, 0), reason: reason}
  end

  defp add_snippet(%{line: line} = finding, lines) do
    snippet = lines |> Enum.at(line - 1, "") |> String.trim()
    Map.put(finding, :snippet, snippet)
  end

  defp report([]) do
    Mix.shell().info([:green, "upkeep.audit: no out-of-band write patterns found."])
  end

  defp report(findings) do
    Mix.shell().info([
      :yellow,
      "upkeep.audit: #{length(findings)} possible out-of-band write(s). ",
      "Confirm each is intentional, routed through `use Upkeep.Ecto.Repo`, or emits ",
      "`Upkeep.changed/3`.\n"
    ])

    Enum.each(findings, fn finding ->
      Mix.shell().info([
        :cyan,
        "  #{finding.path}:#{finding.line}",
        :reset,
        " — #{finding.reason}\n    #{finding.snippet}"
      ])
    end)
  end
end

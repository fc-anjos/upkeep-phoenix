defmodule Upkeep.Ecto.WriteGuard do
  @moduledoc false

  # Opt-in, always-on detection of out-of-band writes for a running repo.
  #
  # Attaches a handler to the repo's Ecto query telemetry and warns when a
  # write (`INSERT`/`UPDATE`/`DELETE`) reaches the database without flowing
  # through `Upkeep.Ecto.Repo` capture — for example raw SQL. It only ever warns:
  # a telemetry handler that raises is force-detached by `:telemetry`, so hard,
  # fail-the-build enforcement lives in `Upkeep.Test.assert_all_writes_captured/1`
  # instead.

  alias Upkeep.Ecto.RepoCapture

  @warned_key {__MODULE__, :warned}

  def attach(repo, opts \\ []) when is_atom(repo) do
    detach(repo)

    case Keyword.get(opts, :policy, configured_policy()) do
      :ignore ->
        :ok

      :warn ->
        :telemetry.attach(
          handler_id(repo),
          telemetry_prefix(repo) ++ [:query],
          &__MODULE__.handle_event/4,
          %{repo: repo}
        )

      other ->
        raise ArgumentError,
              "invalid :policy #{inspect(other)} for Upkeep write guard; use :warn or :ignore " <>
                "(hard enforcement belongs in Upkeep.Test.assert_all_writes_captured/1)"
    end
  end

  def detach(repo) when is_atom(repo) do
    case :telemetry.detach(handler_id(repo)) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end

  def handle_event(_event, _measurements, metadata, %{repo: repo}) do
    if RepoCapture.observed() == :none and write_sql?(metadata[:query]) do
      report(repo, metadata[:query])
    end
  end

  @doc false
  def telemetry_prefix(repo) do
    case Keyword.get(repo.config(), :telemetry_prefix) do
      nil -> Enum.map(Module.split(repo), &(&1 |> Macro.underscore() |> String.to_atom()))
      prefix -> prefix
    end
  end

  @doc false
  def write_sql?(query) when is_binary(query) do
    command = query |> String.trim_leading() |> String.upcase()

    String.starts_with?(command, "INSERT") or
      String.starts_with?(command, "UPDATE") or
      String.starts_with?(command, "DELETE")
  end

  def write_sql?(_query), do: false

  @doc false
  def reset_warnings(repo), do: :persistent_term.put({@warned_key, repo}, MapSet.new())

  defp report(repo, query) do
    :telemetry.execute(
      [:upkeep, :repo, :out_of_band_write],
      %{count: 1},
      %{repo: repo, query: query, policy: :warn}
    )

    warn_once(repo, query)
  end

  defp warn_once(repo, query) do
    key = {@warned_key, repo}
    seen = :persistent_term.get(key, MapSet.new())

    unless MapSet.member?(seen, query) do
      :persistent_term.put(key, MapSet.put(seen, query))

      require Logger
      Logger.warning(message(repo, query))
    end
  end

  defp message(repo, query) do
    """
    Out-of-band write through #{inspect(repo)} will not refresh watched Upkeep \
    sources:

      #{query}

    Route it through a repo built with `use Upkeep.Ecto.Repo`, emit the change \
    yourself (`Upkeep.updated/2`, `Upkeep.inserted/2`, `Upkeep.deleted/2`), or, \
    if the staleness is intentional, mark it `upkeep: false`.\
    """
  end

  defp configured_policy do
    Application.get_env(:upkeep, :out_of_band_writes, :warn)
  end

  defp handler_id(repo), do: {__MODULE__, repo}
end

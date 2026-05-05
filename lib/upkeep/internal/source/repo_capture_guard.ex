defmodule Upkeep.Internal.Source.RepoCaptureGuard do
  @moduledoc false

  @warn_dedup_key {Upkeep.Source, :repo_capture_warned}

  def verify_source!(source, params, opts \\ []) when is_atom(source) and is_map(params) do
    cond do
      query_source?(source) or explicit_repo?(source) ->
        source
        |> repo()
        |> ensure_repo_capture!(source, params, opts)

      true ->
        :ok
    end
  end

  def ensure_repo_capture!(repo, source, params, opts \\ []) do
    status = repo_capture_status(repo)
    source_location = Keyword.get(opts, :source_location)
    boundary = Keyword.get(opts, :boundary, :watch)

    emit_repo_capture_check(repo, source, params, status, boundary, source_location)

    case status do
      :ok ->
        :ok

      {:error, reason} ->
        handle_repo_capture_misconfiguration(repo, source, params, reason, source_location)
    end
  end

  defp repo(source) do
    source.__upkeep_repo__() || Application.get_env(:upkeep, :repo)
  end

  defp query_source?(source) do
    function_exported?(source, :__upkeep_query_source__?, 0) and source.__upkeep_query_source__?()
  end

  defp explicit_repo?(source) do
    function_exported?(source, :__upkeep_repo_explicit__?, 0) and
      source.__upkeep_repo_explicit__?()
  end

  defp repo_capture_status(nil), do: {:error, :missing_repo}

  defp repo_capture_status(repo) when is_atom(repo) do
    cond do
      not Code.ensure_loaded?(repo) ->
        {:error, :repo_not_loaded}

      Upkeep.Ecto.Repo.capture_enabled?(repo) ->
        :ok

      true ->
        {:error, :repo_capture_disabled}
    end
  end

  defp repo_capture_status(_repo), do: {:error, :invalid_repo}

  defp emit_repo_capture_check(repo, source, params, status, boundary, source_location) do
    :telemetry.execute(
      [:upkeep, :repo, :capture_check],
      %{count: 1},
      %{
        repo: repo,
        source: source,
        params: params,
        status: status_label(status),
        reason: reason(status),
        boundary: boundary,
        policy: repo_capture_policy(),
        source_location: source_location
      }
    )
  end

  defp handle_repo_capture_misconfiguration(repo, source, params, reason, source_location) do
    message = repo_capture_message(repo, source, params, reason, source_location)

    case repo_capture_policy() do
      :raise ->
        raise ArgumentError, message

      :warn ->
        warn_repo_capture_once({repo, source, reason}, message)
        :ok

      :ignore ->
        :ok
    end
  end

  defp warn_repo_capture_once(shape, message) do
    seen = :persistent_term.get(@warn_dedup_key, MapSet.new())

    unless MapSet.member?(seen, shape) do
      :persistent_term.put(@warn_dedup_key, MapSet.put(seen, shape))

      require Logger
      Logger.warning(message)
    end
  end

  defp repo_capture_policy do
    Application.get_env(:upkeep, :repo_capture_misconfiguration, default_repo_capture_policy())
  end

  defp default_repo_capture_policy do
    if Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and
         Mix.env() in [:dev, :test] do
      :raise
    else
      :warn
    end
  end

  defp repo_capture_message(repo, source, params, reason, source_location) do
    location = source_location_text(source_location)

    [
      "Upkeep source #{inspect(source)} with params #{inspect(params)} cannot rely on automatic Ecto reactivity because #{repo_capture_reason(repo, reason)}.",
      "Use `Upkeep.Ecto.Repo` in the configured repo, or make this source explicit-only with `invalidated_by`/`reacts_to` declarations.",
      location
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
  end

  defp repo_capture_reason(_repo, :missing_repo) do
    "no repo is configured"
  end

  defp repo_capture_reason(repo, :repo_not_loaded) do
    "repo #{inspect(repo)} could not be loaded"
  end

  defp repo_capture_reason(repo, :repo_capture_disabled) do
    "repo #{inspect(repo)} does not use `Upkeep.Ecto.Repo`"
  end

  defp repo_capture_reason(repo, :invalid_repo) do
    "#{inspect(repo)} is not a repo module"
  end

  defp source_location_text(nil), do: nil

  defp source_location_text(%{} = location) do
    file = Map.get(location, :file_label) || Map.get(location, :file)
    line = Map.get(location, :line)
    snippet = Map.get(location, :snippet) || Map.get(location, :code)

    label =
      cond do
        is_binary(file) and is_integer(line) -> "#{file}:#{line}"
        is_binary(file) -> file
        is_integer(line) -> "line #{line}"
        true -> nil
      end

    cond do
      is_binary(label) and is_binary(snippet) -> "Declared at #{label}\n#{snippet}"
      is_binary(label) -> "Declared at #{label}"
      true -> nil
    end
  end

  defp status_label(:ok), do: :ok
  defp status_label({:error, _reason}), do: :error

  defp reason(:ok), do: nil
  defp reason({:error, reason}), do: reason
end

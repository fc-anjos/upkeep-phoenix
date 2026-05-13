defmodule Upkeep.Ecto.Mutation do
  @moduledoc false

  alias Upkeep.Mutation

  def mutate(repo_or_fun_or_multi, fun_or_multi \\ nil)

  def mutate(fun, nil) when is_function(fun, 0), do: mutate(default_repo!(), fun)
  def mutate(%Ecto.Multi{} = multi, nil), do: mutate(default_repo!(), multi)

  def mutate(repo, fun) when is_atom(repo) and is_function(fun, 0) do
    if Mutation.journal_active?() do
      {:ok, fun.()}
    else
      run_outer_mutation(repo, fun)
    end
  end

  def mutate(repo, %Ecto.Multi{} = multi) when is_atom(repo) do
    if Mutation.journal_active?() do
      run_nested_multi(repo, multi)
    else
      run_outer_multi(repo, multi)
    end
  end

  defp run_outer_mutation(repo, fun) do
    Mutation.with_isolated_journal(fn ->
      repo
      |> transaction_with_journal(fun)
      |> dispatch_transaction_journal()
    end)
  end

  defp transaction_with_journal(repo, fun) do
    repo.transaction(fn -> {fun.(), Mutation.journal()} end)
  end

  defp dispatch_transaction_journal({:ok, {result, events}}) do
    Mutation.dispatch_journal(events)
    {:ok, result}
  end

  defp dispatch_transaction_journal({:error, reason}), do: {:error, reason}

  defp run_outer_multi(repo, multi) do
    Mutation.with_isolated_journal(fn ->
      case repo.transaction(multi) do
        {:ok, changes} ->
          Mutation.dispatch_journal(Mutation.journal())
          {:ok, changes}

        {:error, _operation, _value, _changes} = error ->
          error
      end
    end)
  end

  defp run_nested_multi(repo, multi) do
    previous = Mutation.journal()

    case repo.transaction(multi) do
      {:ok, _changes} = ok ->
        ok

      {:error, _operation, _value, _changes} = error ->
        Mutation.put_journal(previous)
        error
    end
  end

  defp default_repo! do
    Application.get_env(:upkeep, :repo) ||
      raise """
      No default repo configured for Upkeep. Either pass the repo as the
      first argument to `Upkeep.mutate/2`, or configure one in your app:

          config :upkeep, repo: MyApp.Repo
      """
  end
end

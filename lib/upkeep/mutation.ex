defmodule Upkeep.Mutation do
  @moduledoc """
  Transaction boundary for domain facts.

  Notifications emitted inside `mutate/2` are journaled in the calling process
  and flushed only after the database transaction commits.
  """

  @journal_key {__MODULE__, :journal}

  def mutate(repo_or_fun_or_multi, fun_or_multi \\ nil)

  def mutate(fun, nil) when is_function(fun, 0), do: mutate(Upkeep.Repo, fun)
  def mutate(%Ecto.Multi{} = multi, nil), do: mutate(Upkeep.Repo, multi)

  def mutate(repo, fun) when is_atom(repo) and is_function(fun, 0) do
    if journal_active?() do
      {:ok, fun.()}
    else
      run_outer_mutation(repo, fun)
    end
  end

  def mutate(repo, %Ecto.Multi{} = multi) when is_atom(repo) do
    if journal_active?() do
      run_nested_multi(repo, multi)
    else
      run_outer_multi(repo, multi)
    end
  end

  def notify(event) when is_struct(event) do
    if journal_active?() do
      put_journal(journal() ++ [event])
      :ok
    else
      Upkeep.Coordinator.notify(event)
    end
  end

  defp run_outer_mutation(repo, fun) do
    previous = Process.get(@journal_key, :upkeep_no_journal)
    put_journal([])

    try do
      case repo.transaction(fn -> {fun.(), journal()} end) do
        {:ok, {result, events}} ->
          Enum.each(events, &Upkeep.Coordinator.notify/1)
          {:ok, result}

        {:error, reason} ->
          {:error, reason}
      end
    after
      restore_journal(previous)
    end
  end

  defp run_outer_multi(repo, multi) do
    previous = Process.get(@journal_key, :upkeep_no_journal)
    put_journal([])

    try do
      case repo.transaction(multi) do
        {:ok, changes} ->
          journal()
          |> Enum.each(&Upkeep.Coordinator.notify/1)

          {:ok, changes}

        {:error, _operation, _value, _changes} = error ->
          error
      end
    after
      restore_journal(previous)
    end
  end

  defp run_nested_multi(repo, multi) do
    previous = journal()

    case repo.transaction(multi) do
      {:ok, _changes} = ok ->
        ok

      {:error, _operation, _value, _changes} = error ->
        put_journal(previous)
        error
    end
  end

  defp journal_active?, do: is_list(Process.get(@journal_key))
  defp journal, do: Process.get(@journal_key, [])
  defp put_journal(events), do: Process.put(@journal_key, events)

  defp restore_journal(:upkeep_no_journal), do: Process.delete(@journal_key)
  defp restore_journal(previous), do: Process.put(@journal_key, previous)
end

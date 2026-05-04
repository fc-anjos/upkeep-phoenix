defmodule Upkeep.Mutation do
  @moduledoc """
  Transaction boundary for domain facts.

  Notifications emitted inside `mutate/2` are journaled in the calling process
  and dispatched to graph shards only after the database transaction commits.
  Dispatch is synchronous to shard mailboxes (each shard observes the event
  before `mutate/2` returns), but shard flushes and subscriber dispatches
  happen on the shards' own ~1ms flush timers. Tests that need to assert on
  the absence of subscriber messages can still call
  `Upkeep.Coordinator.Graph.drain/0` explicitly.
  """

  @journal_key {__MODULE__, :journal}

  def mutate(repo_or_fun_or_multi, fun_or_multi \\ nil)

  def mutate(fun, nil) when is_function(fun, 0), do: mutate(default_repo!(), fun)
  def mutate(%Ecto.Multi{} = multi, nil), do: mutate(default_repo!(), multi)

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
      dispatch_journal([event])
    end
  end

  def changed(name, payload, opts \\ []) when is_atom(name) do
    name
    |> Upkeep.Change.changed(payload, opts)
    |> notify()
  end

  def inserted(record, opts \\ []) when is_struct(record) do
    record
    |> Upkeep.Change.inserted(opts)
    |> notify()
  end

  def updated(record, opts \\ []) when is_struct(record) do
    record
    |> Upkeep.Change.updated(opts)
    |> notify()
  end

  def deleted(record, opts \\ []) when is_struct(record) do
    record
    |> Upkeep.Change.deleted(opts)
    |> notify()
  end

  def with_transaction_journal(fun) when is_function(fun, 0) do
    if journal_active?() do
      run_nested_transaction_journal(fun)
    else
      run_outer_transaction_journal(fun)
    end
  end

  defp run_outer_mutation(repo, fun) do
    previous = Process.get(@journal_key, :upkeep_no_journal)
    put_journal([])

    try do
      case repo.transaction(fn -> {fun.(), journal()} end) do
        {:ok, {result, events}} ->
          dispatch_journal(events)
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
          dispatch_journal(journal())
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

  defp run_outer_transaction_journal(fun) do
    previous = Process.get(@journal_key, :upkeep_no_journal)
    put_journal([])

    try do
      result = fun.()

      if transaction_committed?(result) do
        dispatch_journal(journal())
      end

      result
    after
      restore_journal(previous)
    end
  end

  defp run_nested_transaction_journal(fun) do
    previous = journal()

    try do
      result = fun.()

      unless transaction_committed?(result) do
        put_journal(previous)
      end

      result
    rescue
      exception ->
        put_journal(previous)
        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        put_journal(previous)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp dispatch_journal([]), do: :ok

  defp dispatch_journal(events) do
    Enum.each(events, &Upkeep.Coordinator.Graph.notify/1)
    :ok
  end

  defp transaction_committed?({:ok, _result}), do: true
  defp transaction_committed?(_result), do: false

  defp journal_active?, do: is_list(Process.get(@journal_key))
  defp journal, do: Process.get(@journal_key, [])
  defp put_journal(events), do: Process.put(@journal_key, events)

  defp restore_journal(:upkeep_no_journal), do: Process.delete(@journal_key)
  defp restore_journal(previous), do: Process.put(@journal_key, previous)

  defp default_repo! do
    Application.get_env(:upkeep, :repo) ||
      raise """
      No default repo configured for Upkeep. Either pass the repo as the
      first argument to `Upkeep.mutate/2`, or configure one in your app:

          config :upkeep, repo: MyApp.Repo
      """
  end
end

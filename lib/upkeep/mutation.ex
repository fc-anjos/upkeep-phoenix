defmodule Upkeep.Mutation do
  @moduledoc false

  use Boundary,
    top_level?: true,
    deps: [
      Upkeep.Change,
      Upkeep.Invalidation
    ]

  @journal_key {__MODULE__, :journal}

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

  @doc false
  def with_isolated_journal(fun) when is_function(fun, 0) do
    previous = Process.get(@journal_key, :upkeep_no_journal)
    put_journal([])

    try do
      fun.()
    after
      restore_journal(previous)
    end
  end

  defp run_outer_transaction_journal(fun) do
    with_isolated_journal(fn ->
      result = fun.()

      if transaction_committed?(result) do
        dispatch_journal(journal())
      end

      result
    end)
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

  @doc false
  def dispatch_journal([]), do: :ok

  def dispatch_journal(events) do
    Upkeep.Invalidation.dispatch(events)
  end

  defp transaction_committed?({:ok, _result}), do: true
  defp transaction_committed?(_result), do: false

  @doc false
  def journal_active?, do: is_list(Process.get(@journal_key))

  @doc false
  def journal, do: Process.get(@journal_key, [])

  @doc false
  def put_journal(events), do: Process.put(@journal_key, events)

  defp restore_journal(:upkeep_no_journal), do: Process.delete(@journal_key)
  defp restore_journal(previous), do: Process.put(@journal_key, previous)
end

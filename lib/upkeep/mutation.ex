defmodule Upkeep.Mutation do
  @moduledoc false

  use Boundary,
    top_level?: true,
    deps: [
      Upkeep.Change,
      Upkeep.Invalidation
    ]

  @journal_key {__MODULE__, :journal}
  @capture_key {__MODULE__, :capture}

  @doc """
  Run `fun` with repo capture forced on or off for the current process.

  A process-scoped form of the per-write `upkeep: false` option. Every write
  made by code inside the block — including existing context functions called
  unchanged — uses `enabled?` as its capture default, so it can skip (or force)
  Upkeep notifications without threading `upkeep:` through each call. An explicit
  `upkeep:` option on an individual write still takes precedence. The previous
  default is restored afterward, so the helper nests and is safe across raises.
  """
  def with_upkeep(enabled?, fun) when is_boolean(enabled?) and is_function(fun, 0) do
    previous = Process.get(@capture_key, :upkeep_no_capture_default)
    Process.put(@capture_key, enabled?)

    try do
      fun.()
    after
      restore_capture_default(previous)
    end
  end

  @doc false
  def capture_default, do: Process.get(@capture_key, true)

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

  def dispatch_journal([]), do: :ok

  def dispatch_journal(events) do
    Upkeep.Invalidation.dispatch(events)
  end

  defp transaction_committed?({:ok, _result}), do: true
  defp transaction_committed?(_result), do: false
  def journal_active?, do: is_list(Process.get(@journal_key))
  def journal, do: Process.get(@journal_key, [])
  def put_journal(events), do: Process.put(@journal_key, events)

  defp restore_journal(:upkeep_no_journal), do: Process.delete(@journal_key)
  defp restore_journal(previous), do: Process.put(@journal_key, previous)

  defp restore_capture_default(:upkeep_no_capture_default), do: Process.delete(@capture_key)
  defp restore_capture_default(previous), do: Process.put(@capture_key, previous)
end

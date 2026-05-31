defmodule Upkeep.Ecto.RepoCapture do
  @moduledoc false

  alias Upkeep.Ecto.RepoCapture.{BulkWrites, Notify, Options}

  defmacro __using__(_opts) do
    quote do
      alias Upkeep.Ecto.RepoCapture

      def __upkeep_repo_capture_enabled__?, do: true

      defoverridable transaction: 2,
                     insert: 2,
                     update: 2,
                     insert_or_update: 2,
                     delete: 2,
                     insert_all: 3,
                     update_all: 3,
                     delete_all: 2,
                     insert!: 2,
                     update!: 2,
                     insert_or_update!: 2,
                     delete!: 2

      def transaction(fun_or_multi, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)

        if capture? do
          Upkeep.Mutation.with_transaction_journal(fn -> super(fun_or_multi, opts) end)
        else
          super(fun_or_multi, opts)
        end
      end

      def insert(changeset_or_struct, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)

        RepoCapture.run_write(:inserted, changeset_or_struct, capture?, fn ->
          super(changeset_or_struct, opts)
        end)
      end

      def update(changeset, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)
        RepoCapture.run_write(:updated, changeset, capture?, fn -> super(changeset, opts) end)
      end

      def insert_or_update(changeset, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)
        action = RepoCapture.insert_or_update_action(changeset)
        RepoCapture.run_write(action, changeset, capture?, fn -> super(changeset, opts) end)
      end

      def delete(changeset_or_struct, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)

        RepoCapture.run_write(:deleted, changeset_or_struct, capture?, fn ->
          super(changeset_or_struct, opts)
        end)
      end

      def insert_all(schema_or_source, entries, opts) do
        {capture?, capture_opts, opts} = RepoCapture.pop_capture_opts(opts)

        RepoCapture.observe(capture?, fn ->
          RepoCapture.run_insert_all(
            __MODULE__,
            schema_or_source,
            entries,
            capture_opts,
            capture?,
            fn ->
              super(schema_or_source, entries, opts)
            end
          )
        end)
      end

      def update_all(queryable, updates, opts) do
        {capture?, capture_opts, opts} = RepoCapture.pop_capture_opts(opts)

        RepoCapture.observe(capture?, fn ->
          RepoCapture.run_update_all(
            __MODULE__,
            queryable,
            updates,
            opts,
            capture_opts,
            capture?,
            fn ->
              super(queryable, updates, opts)
            end
          )
        end)
      end

      def delete_all(queryable, opts) do
        {capture?, capture_opts, opts} = RepoCapture.pop_capture_opts(opts)

        RepoCapture.observe(capture?, fn ->
          RepoCapture.run_delete_all(__MODULE__, queryable, capture_opts, capture?, fn ->
            super(queryable, opts)
          end)
        end)
      end

      def insert!(changeset_or_struct, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)

        RepoCapture.run_write!(:inserted, changeset_or_struct, capture?, fn ->
          super(changeset_or_struct, opts)
        end)
      end

      def update!(changeset, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)
        RepoCapture.run_write!(:updated, changeset, capture?, fn -> super(changeset, opts) end)
      end

      def insert_or_update!(changeset, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)
        action = RepoCapture.insert_or_update_action(changeset)
        RepoCapture.run_write!(action, changeset, capture?, fn -> super(changeset, opts) end)
      end

      def delete!(changeset_or_struct, opts) do
        {capture?, opts} = RepoCapture.pop_capture_opt(opts)

        RepoCapture.run_write!(:deleted, changeset_or_struct, capture?, fn ->
          super(changeset_or_struct, opts)
        end)
      end
    end
  end

  @observed_key {__MODULE__, :observed}

  @doc """
  Run a repo write `fun`, marking the current process so out-of-band write
  detection can tell writes that flowed through `Upkeep.Ecto.Repo` (`:captured`,
  or `:skipped` for `upkeep: false`) apart from raw SQL or unwrapped writes,
  which leave no mark (`:none`). Restores the previous mark afterward so the
  helper nests across transactions.
  """
  def observe(capture?, fun) when is_boolean(capture?) and is_function(fun, 0) do
    previous = Process.get(@observed_key, :none)
    Process.put(@observed_key, if(capture?, do: :captured, else: :skipped))

    try do
      fun.()
    after
      restore_observed(previous)
    end
  end

  @doc """
  Returns how the current process's in-flight write was observed by Upkeep:
  `:captured`, `:skipped`, or `:none` (no wrapped write in flight).
  """
  def observed, do: Process.get(@observed_key, :none)

  defp restore_observed(:none), do: Process.delete(@observed_key)
  defp restore_observed(previous), do: Process.put(@observed_key, previous)

  @doc false
  def run_write(action, input, capture?, super_fun) when is_function(super_fun, 0) do
    observe(capture?, fn -> capture_result(super_fun.(), action, input, capture?) end)
  end

  @doc false
  def run_write!(action, input, capture?, super_fun) when is_function(super_fun, 0) do
    observe(capture?, fn -> capture_record!(super_fun.(), action, input, capture?) end)
  end

  defdelegate pop_capture_opt(opts), to: Options
  defdelegate pop_capture_opts(opts), to: Options
  defdelegate capture_result(result, action, input, capture?), to: Notify
  defdelegate capture_record!(record, action, input, capture?), to: Notify

  defdelegate capture_insert_all(repo, schema_or_source, entries, capture_opts, run),
    to: BulkWrites

  defdelegate capture_update_all(repo, queryable, updates, opts, capture_opts, run),
    to: BulkWrites

  defdelegate capture_delete_all(repo, queryable, capture_opts, run), to: BulkWrites

  @doc false
  def run_insert_all(repo, schema_or_source, entries, capture_opts, true, run),
    do: capture_insert_all(repo, schema_or_source, entries, capture_opts, run)

  def run_insert_all(_repo, _schema_or_source, _entries, _capture_opts, false, run), do: run.()

  @doc false
  def run_update_all(repo, queryable, updates, opts, capture_opts, true, run),
    do: capture_update_all(repo, queryable, updates, opts, capture_opts, run)

  def run_update_all(_repo, _queryable, _updates, _opts, _capture_opts, false, run), do: run.()

  @doc false
  def run_delete_all(repo, queryable, capture_opts, true, run),
    do: capture_delete_all(repo, queryable, capture_opts, run)

  def run_delete_all(_repo, _queryable, _capture_opts, false, run), do: run.()

  def insert_or_update_action(%Ecto.Changeset{action: :insert}), do: :inserted
  def insert_or_update_action(%Ecto.Changeset{action: :update}), do: :updated

  def insert_or_update_action(%Ecto.Changeset{data: data}) do
    case ecto_state(data) do
      :loaded -> :updated
      _state -> :inserted
    end
  end

  defp ecto_state(%{__meta__: %Ecto.Schema.Metadata{state: state}}), do: state
  defp ecto_state(_record), do: nil
end

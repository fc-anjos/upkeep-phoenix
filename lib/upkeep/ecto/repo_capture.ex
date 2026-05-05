defmodule Upkeep.Ecto.RepoCapture do
  @moduledoc false

  alias Upkeep.Ecto.RepoCapture.{BulkWrites, Notify, Options}

  defmacro __using__(_opts) do
    quote do
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
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        if capture? do
          Upkeep.Mutation.with_transaction_journal(fn -> super(fun_or_multi, opts) end)
        else
          super(fun_or_multi, opts)
        end
      end

      def insert(changeset_or_struct, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset_or_struct
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_result(:inserted, changeset_or_struct, capture?)
      end

      def update(changeset, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_result(:updated, changeset, capture?)
      end

      def insert_or_update(changeset, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)
        action = Upkeep.Ecto.RepoCapture.insert_or_update_action(changeset)

        changeset
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_result(action, changeset, capture?)
      end

      def delete(changeset_or_struct, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset_or_struct
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_result(:deleted, changeset_or_struct, capture?)
      end

      def insert_all(schema_or_source, entries, opts) do
        {capture?, capture_opts, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opts(opts)

        if capture? do
          Upkeep.Ecto.RepoCapture.capture_insert_all(
            __MODULE__,
            schema_or_source,
            entries,
            capture_opts,
            fn -> super(schema_or_source, entries, opts) end
          )
        else
          super(schema_or_source, entries, opts)
        end
      end

      def update_all(queryable, updates, opts) do
        {capture?, capture_opts, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opts(opts)

        if capture? do
          Upkeep.Ecto.RepoCapture.capture_update_all(
            __MODULE__,
            queryable,
            capture_opts,
            fn -> super(queryable, updates, opts) end
          )
        else
          super(queryable, updates, opts)
        end
      end

      def delete_all(queryable, opts) do
        {capture?, capture_opts, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opts(opts)

        if capture? do
          Upkeep.Ecto.RepoCapture.capture_delete_all(
            __MODULE__,
            queryable,
            capture_opts,
            fn -> super(queryable, opts) end
          )
        else
          super(queryable, opts)
        end
      end

      def insert!(changeset_or_struct, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset_or_struct
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_record!(:inserted, changeset_or_struct, capture?)
      end

      def update!(changeset, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_record!(:updated, changeset, capture?)
      end

      def insert_or_update!(changeset, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)
        action = Upkeep.Ecto.RepoCapture.insert_or_update_action(changeset)

        changeset
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_record!(action, changeset, capture?)
      end

      def delete!(changeset_or_struct, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        changeset_or_struct
        |> super(opts)
        |> Upkeep.Ecto.RepoCapture.capture_record!(:deleted, changeset_or_struct, capture?)
      end
    end
  end

  defdelegate pop_capture_opt(opts), to: Options
  defdelegate pop_capture_opts(opts), to: Options
  defdelegate capture_result(result, action, input, capture?), to: Notify
  defdelegate capture_record!(record, action, input, capture?), to: Notify

  defdelegate capture_insert_all(repo, schema_or_source, entries, capture_opts, run),
    to: BulkWrites

  defdelegate capture_update_all(repo, queryable, capture_opts, run), to: BulkWrites
  defdelegate capture_delete_all(repo, queryable, capture_opts, run), to: BulkWrites

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

defmodule Upkeep.Ecto.RepoCapture do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defoverridable transaction: 2,
                     insert: 2,
                     update: 2,
                     insert_or_update: 2,
                     delete: 2,
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

  def pop_capture_opt(opts) do
    case Keyword.pop(opts, :upkeep, true) do
      {false, opts} -> {false, opts}
      {_value, opts} -> {true, opts}
    end
  end

  def capture_result({:ok, record} = result, action, input, true) do
    notify(action, record, input)
    result
  end

  def capture_result(result, _action, _input, _capture?), do: result

  def capture_record!(record, action, input, true) do
    notify(action, record, input)
    record
  end

  def capture_record!(record, _action, _input, _capture?), do: record

  def insert_or_update_action(%Ecto.Changeset{action: :insert}), do: :inserted
  def insert_or_update_action(%Ecto.Changeset{action: :update}), do: :updated

  def insert_or_update_action(%Ecto.Changeset{data: data}) do
    case ecto_state(data) do
      :loaded -> :updated
      _state -> :inserted
    end
  end

  defp notify(:inserted, record, _input), do: Upkeep.Mutation.inserted(record)
  defp notify(:deleted, record, _input), do: Upkeep.Mutation.deleted(record)

  defp notify(:updated, record, input) do
    Upkeep.Mutation.updated(record, from: before_record(input))
  end

  defp before_record(%Ecto.Changeset{data: data}), do: data
  defp before_record(record), do: record

  defp ecto_state(%{__meta__: %Ecto.Schema.Metadata{state: state}}), do: state
  defp ecto_state(_record), do: nil
end

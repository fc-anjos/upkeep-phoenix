defmodule Upkeep.Ecto.RepoCapture do
  @moduledoc false

  import Ecto.Query

  defmacro __using__(_opts) do
    quote do
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
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        schema_or_source
        |> super(entries, opts)
        |> Upkeep.Ecto.RepoCapture.capture_insert_all(schema_or_source, entries, capture?)
      end

      def update_all(queryable, updates, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        if capture? do
          Upkeep.Ecto.RepoCapture.capture_update_all(__MODULE__, queryable, fn ->
            super(queryable, updates, opts)
          end)
        else
          super(queryable, updates, opts)
        end
      end

      def delete_all(queryable, opts) do
        {capture?, opts} = Upkeep.Ecto.RepoCapture.pop_capture_opt(opts)

        if capture? do
          Upkeep.Ecto.RepoCapture.capture_delete_all(__MODULE__, queryable, fn ->
            super(queryable, opts)
          end)
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

  def capture_insert_all(result, schema_or_source, entries, true) do
    schema = source_schema(schema_or_source)

    result
    |> inserted_records(schema, entries)
    |> Enum.each(&notify_change(:inserted, schema, &1))

    result
  end

  def capture_insert_all(result, _schema_or_source, _entries, _capture?), do: result

  def capture_update_all(repo, queryable, run) when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           before_records = bulk_records(repo, queryable)
           result = run.()
           schema = queryable_schema(queryable)
           after_records = reload_records(repo, schema, before_records)

           before_records
           |> Enum.zip(after_records)
           |> Enum.each(fn {before_record, after_record} ->
             notify_change(:updated, schema, after_record, before_record)
           end)

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_delete_all(repo, queryable, run) when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           before_records = bulk_records(repo, queryable)
           result = run.()
           schema = queryable_schema(queryable)

           Enum.each(before_records, &notify_change(:deleted, schema, &1))

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def insert_or_update_action(%Ecto.Changeset{action: :insert}), do: :inserted
  def insert_or_update_action(%Ecto.Changeset{action: :update}), do: :updated

  def insert_or_update_action(%Ecto.Changeset{data: data}) do
    case ecto_state(data) do
      :loaded -> :updated
      _state -> :inserted
    end
  end

  defp notify(:inserted, record, _input),
    do: notify_change(:inserted, record_schema(record), record)

  defp notify(:deleted, record, _input),
    do: notify_change(:deleted, record_schema(record), record)

  defp notify(:updated, record, input) do
    notify_change(:updated, record_schema(record), record, before_record(input))
  end

  defp before_record(%Ecto.Changeset{data: data}), do: data
  defp before_record(record), do: record

  defp notify_change(action, schema, record), do: notify_change(action, schema, record, nil)

  defp notify_change(_action, nil, _record, _from), do: :ok

  defp notify_change(action, schema, record, from) do
    action
    |> Upkeep.Change.changed(record,
      action: action,
      schema: schema,
      record: record,
      from: from
    )
    |> Upkeep.Mutation.notify()
  end

  defp inserted_records({_count, records}, schema, entries)
       when is_atom(schema) and is_list(records) and is_list(entries) do
    records
    |> Enum.zip(entries)
    |> Enum.map(fn {record, entry} -> merge_insert_record(schema, record, entry) end)
  end

  defp inserted_records({_count, records}, _schema, _entries) when is_list(records), do: records

  defp inserted_records(_result, schema, entries) when is_atom(schema) and is_list(entries) do
    Enum.map(entries, &entry_record(schema, &1))
  end

  defp inserted_records(_result, _schema, _entries), do: []

  defp entry_record(schema, entry) when is_map(entry), do: struct(schema, entry)
  defp entry_record(schema, entry) when is_list(entry), do: struct(schema, Map.new(entry))

  defp merge_insert_record(schema, %schema{} = record, entry) do
    entry_map =
      schema
      |> entry_record(entry)
      |> Map.from_struct()

    record_map = Map.from_struct(record)

    schema
    |> struct(
      Map.merge(entry_map, record_map, fn _key, entry_value, record_value ->
        if is_nil(record_value), do: entry_value, else: record_value
      end)
    )
  end

  defp merge_insert_record(_schema, record, _entry), do: record

  defp bulk_records(repo, queryable) do
    case queryable_schema(queryable) do
      nil ->
        []

      _schema ->
        queryable
        |> bulk_select_query()
        |> repo.all()
    end
  end

  defp bulk_select_query(queryable) do
    queryable
    |> Ecto.Queryable.to_query()
    |> exclude(:select)
    |> select([record], record)
  end

  defp reload_records(_repo, nil, _before_records), do: []
  defp reload_records(_repo, _schema, []), do: []

  defp reload_records(repo, schema, before_records) do
    case schema.__schema__(:primary_key) do
      [] ->
        before_records

      primary_keys ->
        schema
        |> primary_key_query(primary_keys, before_records)
        |> repo.all()
        |> Map.new(fn record -> {primary_key_values(record, primary_keys), record} end)
        |> then(fn records_by_key ->
          Enum.map(before_records, fn record ->
            Map.get(records_by_key, primary_key_values(record, primary_keys), record)
          end)
        end)
    end
  end

  defp primary_key_query(schema, [primary_key], records) do
    values = Enum.map(records, &Map.fetch!(&1, primary_key))

    from record in schema,
      where: field(record, ^primary_key) in ^values
  end

  defp primary_key_query(schema, primary_keys, records) do
    predicate =
      Enum.reduce(records, dynamic(false), fn record, predicate ->
        record_predicate =
          Enum.reduce(primary_keys, dynamic(true), fn primary_key, record_predicate ->
            value = Map.fetch!(record, primary_key)
            dynamic([candidate], ^record_predicate and field(candidate, ^primary_key) == ^value)
          end)

        dynamic([candidate], ^predicate or ^record_predicate)
      end)

    from record in schema,
      where: ^predicate
  end

  defp primary_key_values(record, primary_keys) do
    Enum.map(primary_keys, &Map.fetch!(record, &1))
  end

  defp queryable_schema(queryable) do
    queryable
    |> Ecto.Queryable.to_query()
    |> Map.get(:from)
    |> then(fn
      %{source: source} -> source_schema(source)
      _from -> nil
    end)
  rescue
    _ -> nil
  end

  defp source_schema({_source, schema}) when is_atom(schema), do: schema
  defp source_schema(schema) when is_atom(schema), do: schema
  defp source_schema(_source), do: nil

  defp record_schema(%schema{}), do: schema
  defp record_schema(_record), do: nil

  defp ecto_state(%{__meta__: %Ecto.Schema.Metadata{state: state}}), do: state
  defp ecto_state(_record), do: nil
end

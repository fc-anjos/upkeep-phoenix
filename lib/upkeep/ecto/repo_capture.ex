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

  def pop_capture_opt(opts) do
    {capture?, _capture_opts, opts} = pop_capture_opts(opts)
    {capture?, opts}
  end

  def pop_capture_opts(opts) do
    {upkeep, opts} = Keyword.pop(opts, :upkeep, true)
    {schema, opts} = Keyword.pop(opts, :upkeep_schema, nil)

    case upkeep do
      false -> {false, [], opts}
      value -> {true, capture_opts(value, schema), opts}
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

  def capture_insert_all(repo, schema_or_source, %Ecto.Query{} = query, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = source_schema(schema_or_source, capture_opts)
           entries = insert_query_entries(repo, query, schema)
           result = run.()

           notify_insert_all(result, schema, entries)

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_insert_all(_repo, schema_or_source, entries, capture_opts, run)
      when is_function(run, 0) do
    schema = source_schema(schema_or_source, capture_opts)
    result = run.()

    notify_insert_all(result, schema, entries)

    result
  end

  def capture_update_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = queryable_schema(queryable, capture_opts)
           before_records = bulk_records(repo, queryable, schema)
           result = run.()
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

  def capture_delete_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = queryable_schema(queryable, capture_opts)
           before_records = bulk_records(repo, queryable, schema)
           result = run.()

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

  defp capture_opts(value, schema) do
    value
    |> normalize_capture_opts()
    |> put_capture_schema(schema)
  end

  defp normalize_capture_opts(value) when is_list(value), do: value
  defp normalize_capture_opts(value) when is_map(value), do: Map.to_list(value)
  defp normalize_capture_opts(_value), do: []

  defp put_capture_schema(opts, nil), do: opts
  defp put_capture_schema(opts, schema), do: Keyword.put_new(opts, :schema, schema)

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

  defp notify_insert_all(result, schema, entries) do
    result
    |> inserted_records(schema, entries)
    |> Enum.each(&notify_change(:inserted, schema, &1))
  end

  defp insert_query_entries(repo, query, schema) do
    query
    |> repo.all()
    |> Enum.map(&entry_record(schema, &1))
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

  defp inserted_records(_result, schema, entries) when is_binary(schema) and is_list(entries) do
    Enum.map(entries, &entry_record(schema, &1))
  end

  defp inserted_records(_result, _schema, _entries), do: []

  defp entry_record(nil, _entry), do: nil
  defp entry_record(schema, entry) when is_binary(schema), do: entry_map(entry)
  defp entry_record(schema, %schema{} = entry) when is_atom(schema), do: entry

  defp entry_record(schema, %_{} = entry) when is_atom(schema),
    do: struct(schema, Map.from_struct(entry))

  defp entry_record(schema, entry) when is_atom(schema) and is_map(entry),
    do: struct(schema, entry)

  defp entry_record(schema, entry) when is_atom(schema) and is_list(entry),
    do: struct(schema, Map.new(entry))

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

  defp merge_insert_record(schema, record, entry)
       when is_binary(schema) and is_map(record) do
    Map.merge(entry_map(entry), entry_map(record), fn _key, entry_value, record_value ->
      if is_nil(record_value), do: entry_value, else: record_value
    end)
  end

  defp merge_insert_record(_schema, record, _entry), do: record

  defp entry_map(%_{} = entry), do: Map.from_struct(entry)
  defp entry_map(entry) when is_map(entry), do: entry
  defp entry_map(entry) when is_list(entry), do: Map.new(entry)
  defp entry_map(_entry), do: %{}

  defp bulk_records(repo, queryable, schema) do
    case schema do
      nil ->
        []

      _schema ->
        case bulk_select_query(repo, queryable, schema) do
          nil -> []
          query -> repo.all(query)
        end
    end
  end

  defp bulk_select_query(_repo, queryable, schema) when is_atom(schema) do
    queryable
    |> Ecto.Queryable.to_query()
    |> put_query_schema(schema)
    |> exclude(:select)
    |> select([record], record)
  end

  defp bulk_select_query(repo, queryable, source) when is_binary(source) do
    fields = table_fields(repo, source)

    if fields == [] do
      nil
    else
      queryable
      |> Ecto.Queryable.to_query()
      |> exclude(:select)
      |> select([record], map(record, ^fields))
    end
  end

  defp reload_records(_repo, nil, _before_records), do: []
  defp reload_records(_repo, _schema, []), do: []

  defp reload_records(repo, schema, before_records) when is_atom(schema) do
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

  defp reload_records(repo, source, before_records) when is_binary(source) do
    fields = before_records |> List.first() |> Map.keys()

    case table_primary_keys(repo, source) do
      [] ->
        before_records

      _primary_keys when fields == [] ->
        before_records

      primary_keys ->
        source
        |> primary_key_query(primary_keys, before_records, fields)
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

  defp primary_key_query(source, [primary_key], records, fields) when is_binary(source) do
    values = Enum.map(records, &Map.fetch!(&1, primary_key))

    from record in source,
      where: field(record, ^primary_key) in ^values,
      select: map(record, ^fields)
  end

  defp primary_key_query(source, primary_keys, records, fields) when is_binary(source) do
    predicate =
      Enum.reduce(records, dynamic(false), fn record, predicate ->
        record_predicate =
          Enum.reduce(primary_keys, dynamic(true), fn primary_key, record_predicate ->
            value = Map.fetch!(record, primary_key)
            dynamic([candidate], ^record_predicate and field(candidate, ^primary_key) == ^value)
          end)

        dynamic([candidate], ^predicate or ^record_predicate)
      end)

    from record in source,
      where: ^predicate,
      select: map(record, ^fields)
  end

  defp primary_key_values(record, primary_keys) do
    Enum.map(primary_keys, &Map.fetch!(record, &1))
  end

  defp queryable_schema(queryable, capture_opts) do
    case Keyword.get(capture_opts, :schema) do
      nil -> queryable_source_schema(queryable)
      schema -> schema
    end
  end

  defp queryable_source_schema(queryable) do
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

  defp put_query_schema(%Ecto.Query{from: %{source: {source, nil}} = from} = query, schema)
       when is_atom(schema) and not is_nil(schema) do
    %{query | from: %{from | source: {source, schema}}}
  end

  defp put_query_schema(query, _schema), do: query

  defp source_schema(source, capture_opts) do
    case Keyword.get(capture_opts, :schema) do
      nil -> source_schema(source)
      schema -> schema
    end
  end

  defp source_schema({source, nil}) when is_binary(source), do: source
  defp source_schema({_source, schema}) when is_atom(schema) and not is_nil(schema), do: schema
  defp source_schema(schema) when is_atom(schema), do: schema
  defp source_schema(source) when is_binary(source), do: source
  defp source_schema(_source), do: nil

  defp record_schema(%schema{}), do: schema
  defp record_schema(_record), do: nil

  defp table_fields(repo, source) do
    case table_metadata(repo, source) do
      %{fields: fields} -> fields
      _metadata -> []
    end
  end

  defp table_primary_keys(repo, source) do
    case table_metadata(repo, source) do
      %{primary_keys: primary_keys} -> primary_keys
      _metadata -> []
    end
  end

  defp table_metadata(repo, source) do
    case repo.__adapter__() do
      Ecto.Adapters.SQLite3 -> sqlite_table_metadata(repo, source)
      Ecto.Adapters.Postgres -> postgres_table_metadata(repo, source)
      _adapter -> %{fields: [], primary_keys: []}
    end
  rescue
    _ -> %{fields: [], primary_keys: []}
  end

  defp sqlite_table_metadata(repo, source) do
    source = quoted_sqlite_identifier(source)
    result = repo.query!("PRAGMA table_info(#{source})", [])

    fields =
      Enum.map(result.rows, fn [_cid, name | _rest] ->
        String.to_atom(name)
      end)

    primary_keys =
      result.rows
      |> Enum.filter(fn [_cid, _name, _type, _not_null, _default, pk] -> pk > 0 end)
      |> Enum.sort_by(fn [_cid, _name, _type, _not_null, _default, pk] -> pk end)
      |> Enum.map(fn [_cid, name, _type, _not_null, _default, _pk] -> String.to_atom(name) end)

    %{fields: fields, primary_keys: primary_keys}
  end

  defp postgres_table_metadata(repo, source) do
    result =
      repo.query!(
        """
        SELECT column_name, ordinal_position,
               EXISTS (
                 SELECT 1
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = c.table_schema
                   AND tc.table_name = c.table_name
                   AND kcu.column_name = c.column_name
               ) AS primary_key
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema()
          AND c.table_name = $1
        ORDER BY ordinal_position
        """,
        [source]
      )

    fields = Enum.map(result.rows, fn [name, _position, _primary_key] -> String.to_atom(name) end)

    primary_keys =
      result.rows
      |> Enum.filter(fn [_name, _position, primary_key] -> primary_key end)
      |> Enum.map(fn [name, _position, _primary_key] -> String.to_atom(name) end)

    %{fields: fields, primary_keys: primary_keys}
  end

  defp quoted_sqlite_identifier(source) do
    ~s("#{String.replace(source, ~s("), ~s(""))}")
  end

  defp ecto_state(%{__meta__: %Ecto.Schema.Metadata{state: state}}), do: state
  defp ecto_state(_record), do: nil
end

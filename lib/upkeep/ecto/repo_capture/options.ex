defmodule Upkeep.Ecto.RepoCapture.Options do
  @moduledoc false

  def pop_capture_opt(opts) do
    {capture?, _capture_opts, opts} = pop_capture_opts(opts)
    {capture?, opts}
  end

  def pop_capture_opts(opts) do
    {upkeep, opts} = Keyword.pop(opts, :upkeep, Upkeep.Mutation.capture_default())
    {schema, opts} = Keyword.pop(opts, :upkeep_schema, nil)

    case upkeep do
      false -> {false, [], opts}
      value -> {true, capture_opts(value, schema), opts}
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
end

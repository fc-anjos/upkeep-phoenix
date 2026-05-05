defmodule Upkeep.Source.Identity do
  @moduledoc false

  def source_id(source, params) when is_atom(source) and is_map(params), do: {source, params}

  def sharing_partition(source, params) when is_atom(source) and is_map(params) do
    if function_exported?(source, :__upkeep_sharing_partition__, 1) do
      source.__upkeep_sharing_partition__(params)
    else
      {:params, params}
    end
  end

  def retry_config(source) when is_atom(source) do
    if function_exported?(source, :__upkeep_retry__, 0) do
      source.__upkeep_retry__()
    else
      :default
    end
  end
end

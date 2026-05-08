defmodule Upkeep.Source.Identity do
  @moduledoc false

  @type source_id :: {module(), map()}

  @spec source_id(module(), map()) :: source_id()
  def source_id(source, params) when is_atom(source) and is_map(params), do: {source, params}

  @spec sharing_partition(module(), map()) :: term()
  def sharing_partition(source, params) when is_atom(source) and is_map(params) do
    if function_exported?(source, :__upkeep_sharing_partition__, 1) do
      source.__upkeep_sharing_partition__(params)
    else
      {:params, params}
    end
  end

  @spec retry_config(module()) :: Upkeep.Source.retry_config()
  def retry_config(source) when is_atom(source) do
    if function_exported?(source, :__upkeep_retry__, 0) do
      source.__upkeep_retry__()
    else
      :default
    end
  end
end

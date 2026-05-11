defmodule Upkeep.Source.Identity do
  @moduledoc false

  alias Upkeep.Source.Context

  @type source_id :: {module(), map()} | {:identity, {module(), map()}, term()}

  @spec source_id(module(), map()) :: source_id()
  def source_id(source, params) when is_atom(source) and is_map(params), do: {source, params}

  @spec source_id(module(), map(), Context.t() | nil) :: source_id()
  def source_id(source, params, nil) when is_atom(source) and is_map(params) do
    source_id(source, params)
  end

  def source_id(source, params, %Context{identity_envelope: identity_envelope})
      when is_atom(source) and is_map(params) do
    {:identity, source_id(source, params), identity_envelope}
  end

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

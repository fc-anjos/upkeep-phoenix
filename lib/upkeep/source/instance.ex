defmodule Upkeep.Source.Instance do
  @moduledoc false

  alias Upkeep.Source.Identity

  @enforce_keys [
    :source,
    :params,
    :id,
    :repo,
    :repo_explicit?,
    :query_source?,
    :retry,
    :sharing_partition,
    :surface,
    :explicit_surface
  ]
  defstruct source: nil,
            params: %{},
            id: nil,
            repo: nil,
            repo_explicit?: false,
            query_source?: false,
            retry: :default,
            sharing_partition: nil,
            surface: Upkeep.InvalidationSurface.empty(),
            explicit_surface: Upkeep.InvalidationSurface.empty()

  @type t :: %__MODULE__{
          source: module(),
          params: Upkeep.Source.params(),
          id: term(),
          repo: module() | nil,
          repo_explicit?: boolean(),
          query_source?: boolean(),
          retry: Upkeep.Source.retry_config(),
          sharing_partition: term(),
          surface: Upkeep.InvalidationSurface.t(),
          explicit_surface: Upkeep.InvalidationSurface.t()
        }

  @spec build(module(), map() | keyword()) :: t()
  def build(source, params) when is_atom(source) do
    params = normalize_params(params)

    %__MODULE__{
      source: source,
      params: params,
      id: Identity.source_id(source, params),
      repo: repo(source),
      repo_explicit?: repo_explicit?(source),
      query_source?: query_source?(source),
      retry: Identity.retry_config(source),
      sharing_partition: Identity.sharing_partition(source, params),
      surface: surface(source, params),
      explicit_surface: explicit_surface(source, params)
    }
  end

  @spec verify!(t(), keyword()) :: :ok
  def verify!(%__MODULE__{} = instance, opts \\ []) do
    if function_exported?(instance.source, :__upkeep_verify__!, 2) do
      instance.source.__upkeep_verify__!(instance.params, opts)
    else
      :ok
    end
  end

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params

  defp repo(source) do
    source_repo(source) || Application.get_env(:upkeep, :repo)
  end

  defp source_repo(source) do
    if function_exported?(source, :__upkeep_repo__, 0), do: source.__upkeep_repo__(), else: nil
  end

  defp repo_explicit?(source) do
    function_exported?(source, :__upkeep_repo_explicit__?, 0) and
      source.__upkeep_repo_explicit__?()
  end

  defp query_source?(source) do
    function_exported?(source, :__upkeep_query_source__?, 0) and source.__upkeep_query_source__?()
  end

  defp surface(source, params) do
    if function_exported?(source, :__upkeep_surface__, 1),
      do: source.__upkeep_surface__(params),
      else: Upkeep.InvalidationSurface.empty()
  end

  defp explicit_surface(source, params) do
    if function_exported?(source, :__upkeep_explicit_surface__, 1),
      do: source.__upkeep_explicit_surface__(params),
      else: Upkeep.InvalidationSurface.empty()
  end
end

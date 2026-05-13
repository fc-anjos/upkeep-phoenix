defmodule Upkeep.Ecto.Source.QueryAdapter do
  @moduledoc false

  alias Upkeep.Ecto.Source.{QueryDeps, RepoCaptureGuard}

  @spec read(term()) :: term()
  defdelegate read(value), to: Upkeep.Ecto.Source.Reader

  @spec verify_source!(module(), map(), keyword()) :: :ok
  def verify_source!(source, params, opts \\ []) do
    RepoCaptureGuard.verify_source!(source, params, opts)
  end

  @spec query_surface(module(), map(), Upkeep.Source.Context.t() | nil) ::
          Upkeep.InvalidationSurface.t()
  def query_surface(source, params, context \\ nil) when is_atom(source) do
    source
    |> source_query(params, context)
    |> QueryDeps.surface()
  end

  @spec query_reacts_to?(module(), struct(), map(), Upkeep.Source.Context.t() | nil) :: boolean()
  def query_reacts_to?(source, event, params, context \\ nil)
      when is_atom(source) and is_struct(event) do
    deps =
      source
      |> source_query(params, context)
      |> QueryDeps.from_query()

    QueryDeps.matches_change?(deps, event)
  end

  defp source_query(source, params, context) do
    cond do
      function_exported?(source, :query, 2) -> source.query(params, context)
      function_exported?(source, :query, 1) -> source.query(params)
      true -> nil
    end
  end
end

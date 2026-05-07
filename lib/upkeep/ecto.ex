defmodule Upkeep.Ecto do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [
      Mutation,
      Repo
    ],
    deps: [
      Ecto.Adapters.Postgres,
      Ecto.Adapters.SQL,
      Ecto.Changeset,
      Ecto.Migration.SchemaMigration,
      Ecto.Multi,
      Ecto.Query,
      Ecto.QueryError,
      Ecto.Queryable,
      Ecto.Repo,
      Ecto.Schema.Metadata,
      Upkeep.Change,
      Upkeep.Mutation
    ],
    type: :strict
end

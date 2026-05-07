defmodule Upkeep.Ecto.Source.QueryDeps.BindingsTest do
  use ExUnit.Case, async: true

  alias Upkeep.Ecto.Source.QueryDeps.Bindings

  defmodule Issue do
    use Ecto.Schema

    schema "issues" do
      has_many :comments, Upkeep.Ecto.Source.QueryDeps.BindingsTest.Comment
    end
  end

  defmodule Comment do
    use Ecto.Schema

    schema "comments" do
      belongs_to :issue, Upkeep.Ecto.Source.QueryDeps.BindingsTest.Issue
    end
  end

  defmodule NotSchema do
  end

  defmodule BrokenSchema do
    def __schema__(:association, :comments), do: raise("broken schema metadata")
  end

  test "resolves association joins from Ecto metadata" do
    import Ecto.Query

    query =
      from i in Issue,
        join: c in assoc(i, :comments),
        where: c.issue_id == i.id

    assert %Bindings{by_index: %{0 => Issue, 1 => Comment}, diagnostics: []} =
             Bindings.from_query(query)
  end

  test "association metadata failures return diagnostics" do
    assert {:error, %{reason: :unknown_association, owner_schema: Issue, assoc: :missing}} =
             Bindings.association_dependencies(Issue, :missing)

    assert {:error, %{reason: :non_schema_owner, owner_schema: NotSchema, assoc: :comments}} =
             Bindings.association_dependencies(NotSchema, :comments)

    assert {:error,
            %{
              reason: :association_lookup_failed,
              owner_schema: BrokenSchema,
              assoc: :comments,
              exception: RuntimeError
            }} = Bindings.association_dependencies(BrokenSchema, :comments)
  end
end

defmodule Upkeep.SourceEctoPropertyTest do
  @moduledoc """
  Property-based coverage for surface derivation and write -> surface matching.

  The safety-critical invariant is SOUNDNESS: a surface derived from a source's
  query must never miss a committed change to a row that satisfies the query's
  equality/membership filters. A false negative silently serves stale UI.

  Generating valid Ecto query *syntax* at runtime is intractable (queries are
  macros), so we keep a small fixed set of representative query shapes and use
  `stream_data` to vary the filter values, the change record values, their
  types/representations, and the action. For every generated (query, change)
  pair we assert the must-react direction strictly (no false negatives) and the
  must-not-react direction only where the derived surface is precise (over-broad
  is sound; false-negative is not).
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Upkeep.InvalidationSurface

  # --- Schemas -------------------------------------------------------------

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_property_test_issues" do
      field :project_id, :integer
      field :assignee_id, :integer
      field :status, :string
      field :title, :string
    end
  end

  defmodule EnumIssue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_property_test_enum_issues" do
      field :project_id, :integer
      field :state, Ecto.Enum, values: [:open, :closed, :blocked]
    end
  end

  # --- Sources -------------------------------------------------------------

  # Equality on a single integer field, value taken from params.
  defmodule EqualityIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue, where: i.project_id == ^project_id
    end
  end

  # Conjunction across two fields (integer + string).
  defmodule MultiFieldIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.Issue

    def query(%{project_id: project_id, status: status}) do
      from i in Issue, where: i.project_id == ^project_id and i.status == ^status
    end
  end

  # Membership filter on a string field.
  defmodule MembershipIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.Issue

    def query(%{project_id: project_id, statuses: statuses}) do
      from i in Issue, where: i.project_id == ^project_id and i.status in ^statuses
    end
  end

  # Disjunction across two integer fields (precise alternative surfaces).
  defmodule OrIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.Issue

    def query(%{project_id: project_id, assignee_id: assignee_id}) do
      from i in Issue, where: i.project_id == ^project_id or i.assignee_id == ^assignee_id
    end
  end

  # Equality on an Ecto.Enum field, queried with an atom literal.
  defmodule EnumStateIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.EnumIssue

    def query(%{project_id: project_id}) do
      from i in EnumIssue, where: i.project_id == ^project_id and i.state == :open
    end
  end

  # Fragment in the where clause: unanalyzable, must fall back to broad.
  defmodule FragmentIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoPropertyTest.Issue

    def query(%{term: term}) do
      from i in Issue, where: fragment("lower(?)", i.title) == ^term
    end
  end

  @actions [:inserted, :updated, :deleted]

  # --- Generators ----------------------------------------------------------

  # Small integer pool keeps collisions (matches) and misses both likely.
  defp small_int, do: integer(1..4)

  # An integer that may arrive as a plain integer or its string form, so the
  # canonicalization path (string param vs integer column, and vice versa) is
  # exercised on both the filter and record sides.
  defp int_repr(value) do
    member_of([value, Integer.to_string(value)])
  end

  defp status_value,
    do: member_of(["open", "closed", "blocked", "archived", "draft", "review", "done"])

  defp enum_state, do: member_of([:open, :closed, :blocked])

  # An enum value as it can appear on a change record: loaded atom or dumped
  # string (bulk insert_all/update_all paths produce the dumped string).
  defp enum_repr(value) do
    member_of([value, Atom.to_string(value)])
  end

  # --- Helpers -------------------------------------------------------------

  defp surface_keys(source, params) do
    source.__upkeep_surface__(params)
    |> InvalidationSurface.keys()
  end

  # Build a struct-backed change for one of the schemas above, so schema-aware
  # canonicalization (the Ecto type lookup) applies during matching.
  defp struct_change(action, schema, attrs) do
    record = struct(schema, attrs)

    case action do
      :inserted -> Upkeep.Change.inserted(record)
      :deleted -> Upkeep.Change.deleted(record)
      # A full update with no prior state (record == from) so the change is a
      # precise (non-broad) update keyed on the record's current field values.
      :updated -> Upkeep.Change.updated(record, from: record)
    end
  end

  # Build a map-backed change carrying possibly-dumped values, mirroring what
  # bulk write paths produce. The schema is still supplied so type lookup runs.
  # Updates use `from: record` so the change stays precise (keyed on its current
  # field values) rather than collapsing to a broad, always-reacting update.
  defp dumped_change(action, schema, record) do
    opts =
      case action do
        :updated -> [schema: schema, record: record, from: record, action: action]
        _other -> [schema: schema, record: record, action: action]
      end

    Upkeep.Change.changed(action, record, opts)
  end

  # --- Properties ----------------------------------------------------------

  property "equality match soundness: a row matching the filter always reacts" do
    check all action <- member_of(@actions),
              filter_value <- small_int(),
              record_value <- small_int(),
              filter_repr <- int_repr(filter_value),
              record_repr <- int_repr(record_value),
              max_runs: 300 do
      params = %{project_id: filter_repr}

      change =
        struct_change(action, Issue, project_id: record_repr, status: "open", title: "x")

      reacts = EqualityIssues.reacts_to?(change, params)

      if record_value == filter_value do
        assert reacts,
               "SOUNDNESS VIOLATION: row with project_id=#{inspect(record_repr)} matches " <>
                 "filter project_id=#{inspect(filter_repr)} but did not react (#{action})"
      else
        # Surface is precise here, so a non-matching row must NOT react.
        refute reacts,
               "over-broad: row project_id=#{inspect(record_repr)} reacted to filter " <>
                 "project_id=#{inspect(filter_repr)} (#{action})"
      end
    end
  end

  property "multi-field AND soundness: a row reacts iff it satisfies every filter" do
    check all action <- member_of(@actions),
              filter_project <- small_int(),
              filter_status <- status_value(),
              record_project <- small_int(),
              record_status <- status_value(),
              project_repr <- int_repr(record_project),
              max_runs: 300 do
      params = %{project_id: filter_project, status: filter_status}

      change =
        struct_change(action, Issue,
          project_id: project_repr,
          status: record_status,
          title: "x"
        )

      reacts = MultiFieldIssues.reacts_to?(change, params)
      matches? = record_project == filter_project and record_status == filter_status

      if matches? do
        assert reacts,
               "SOUNDNESS VIOLATION: row #{inspect({record_project, record_status})} matches " <>
                 "AND filter #{inspect({filter_project, filter_status})} but did not react"
      else
        refute reacts,
               "over-broad: row #{inspect({record_project, record_status})} reacted to AND " <>
                 "filter #{inspect({filter_project, filter_status})}"
      end
    end
  end

  property "membership (in) soundness: a row whose value is in the list always reacts" do
    check all action <- member_of(@actions),
              filter_project <- small_int(),
              statuses <- uniq_list_of(status_value(), min_length: 1, max_length: 3),
              record_project <- small_int(),
              record_status <- status_value(),
              max_runs: 300 do
      params = %{project_id: filter_project, statuses: statuses}

      change =
        struct_change(action, Issue,
          project_id: record_project,
          status: record_status,
          title: "x"
        )

      reacts = MembershipIssues.reacts_to?(change, params)
      matches? = record_project == filter_project and record_status in statuses

      if matches? do
        assert reacts,
               "SOUNDNESS VIOLATION: row #{inspect({record_project, record_status})} satisfies " <>
                 "project_id=#{filter_project} and status in #{inspect(statuses)} but did not react"
      else
        refute reacts,
               "over-broad: row #{inspect({record_project, record_status})} reacted to " <>
                 "project_id=#{filter_project}, status in #{inspect(statuses)}"
      end
    end
  end

  property "OR soundness: a row satisfying either disjunct always reacts" do
    check all action <- member_of(@actions),
              filter_project <- small_int(),
              filter_assignee <- small_int(),
              record_project <- small_int(),
              record_assignee <- small_int(),
              max_runs: 300 do
      params = %{project_id: filter_project, assignee_id: filter_assignee}

      change =
        struct_change(action, Issue,
          project_id: record_project,
          assignee_id: record_assignee,
          status: "open",
          title: "x"
        )

      reacts = OrIssues.reacts_to?(change, params)
      matches? = record_project == filter_project or record_assignee == filter_assignee

      if matches? do
        assert reacts,
               "SOUNDNESS VIOLATION: row #{inspect({record_project, record_assignee})} satisfies " <>
                 "the OR filter #{inspect({filter_project, filter_assignee})} but did not react"
      else
        refute reacts,
               "over-broad: row #{inspect({record_project, record_assignee})} reacted to OR " <>
                 "filter #{inspect({filter_project, filter_assignee})}"
      end
    end
  end

  property "type canonicalization: int vs string representations match regardless of side" do
    check all action <- member_of(@actions),
              value <- small_int(),
              filter_repr <- int_repr(value),
              record_repr <- int_repr(value),
              max_runs: 200 do
      # Same logical value on both sides, but possibly skewed representation.
      params = %{project_id: filter_repr}

      change =
        struct_change(action, Issue, project_id: record_repr, status: "open", title: "x")

      assert EqualityIssues.reacts_to?(change, params),
             "CANONICALIZATION VIOLATION: filter #{inspect(filter_repr)} and record " <>
               "#{inspect(record_repr)} are the same logical integer but did not match"
    end
  end

  property "type canonicalization: Ecto.Enum atom vs dumped string match" do
    check all action <- member_of(@actions),
              project <- small_int(),
              record_repr <- enum_repr(:open),
              max_runs: 150 do
      # EnumStateIssues filters state == :open (atom literal). A change carrying
      # the loaded atom OR the dumped string for :open must react.
      params = %{project_id: project}

      change =
        dumped_change(action, EnumIssue, %{project_id: project, state: record_repr})

      assert EnumStateIssues.reacts_to?(change, params),
             "ENUM CANONICALIZATION VIOLATION: state=#{inspect(record_repr)} (logically :open) " <>
               "did not match filter state == :open"
    end
  end

  property "Ecto.Enum filter rejects non-matching states (precise must-not-react)" do
    check all action <- member_of(@actions),
              project <- small_int(),
              state <- enum_state(),
              record_repr <- enum_repr(state),
              max_runs: 200 do
      params = %{project_id: project}

      change =
        dumped_change(action, EnumIssue, %{project_id: project, state: record_repr})

      reacts = EnumStateIssues.reacts_to?(change, params)

      if state == :open do
        assert reacts,
               "SOUNDNESS VIOLATION: state #{inspect(record_repr)} (==:open) did not react"
      else
        refute reacts,
               "over-broad: state #{inspect(record_repr)} reacted to filter state == :open"
      end
    end
  end

  property "unanalyzable fragment shapes fall back to broad and always react" do
    check all action <- member_of(@actions),
              title <- string(:alphanumeric, min_length: 1, max_length: 8),
              project <- small_int(),
              term <- string(:alphanumeric, min_length: 0, max_length: 8),
              max_runs: 150 do
      params = %{term: term}

      change =
        struct_change(action, Issue, project_id: project, status: "open", title: title)

      # The fragment defeats precise analysis, so the surface must be broad for
      # the Issue schema: ANY Issue change must react (no false negatives).
      assert FragmentIssues.reacts_to?(change, params),
             "SOUNDNESS VIOLATION: fragment-based query failed to broadly react to an " <>
               "Issue change (#{action})"

      # And the derived keys must be schema-broad (no value-indexing).
      keys = surface_keys(FragmentIssues, params)

      assert {:upkeep_change, action, Issue} in keys,
             "expected broad schema key for #{action}, got #{inspect(keys)}"
    end
  end
end

defmodule Upkeep.TestSupport.Schema do
  @moduledoc false

  alias Upkeep.TestSupport.Repo

  @repo_capture_tables [
    "upkeep_repo_capture_test_imports",
    "upkeep_repo_capture_test_issues"
  ]

  @source_ecto_tables [
    "upkeep_source_ecto_test_issue_tags",
    "upkeep_source_ecto_test_tags",
    "upkeep_source_ecto_test_comments",
    "upkeep_source_ecto_test_columns",
    "upkeep_source_ecto_test_issues"
  ]

  def reset! do
    Enum.each(@repo_capture_tables ++ @source_ecto_tables, fn table ->
      :ok = execute_ddl!("DROP TABLE IF EXISTS #{table}")
    end)

    :ok = create_repo_capture_tables()
    :ok = create_source_ecto_tables()
  end

  defp create_repo_capture_tables do
    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_repo_capture_test_issues (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL,
        assignee_id INTEGER,
        status TEXT NOT NULL,
        title TEXT NOT NULL,
        position INTEGER NOT NULL
      )
      """)

    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_repo_capture_test_imports (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL,
        assignee_id INTEGER,
        status TEXT NOT NULL,
        title TEXT NOT NULL,
        position INTEGER NOT NULL
      )
      """)
  end

  defp create_source_ecto_tables do
    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_source_ecto_test_issues (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL,
        column_id INTEGER,
        assignee_id INTEGER,
        status TEXT NOT NULL,
        title TEXT NOT NULL,
        position INTEGER NOT NULL
      )
      """)

    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_source_ecto_test_columns (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        position INTEGER NOT NULL
      )
      """)

    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_source_ecto_test_comments (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL,
        issue_id INTEGER NOT NULL,
        body TEXT NOT NULL
      )
      """)

    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_source_ecto_test_tags (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      )
      """)

    :ok =
      execute_ddl!("""
      CREATE TABLE upkeep_source_ecto_test_issue_tags (
        issue_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL
      )
      """)
  end

  defp execute_ddl!(sql) do
    case Repo.query!(sql) do
      %{rows: rows, num_rows: num_rows} when rows in [nil, []] and is_integer(num_rows) ->
        :ok
    end
  end
end

defmodule Upkeep.TestSchema do
  @moduledoc false

  alias Upkeep.Repo

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
      Repo.query!("DROP TABLE IF EXISTS #{table}")
    end)

    create_repo_capture_tables()
    create_source_ecto_tables()
  end

  defp create_repo_capture_tables do
    Repo.query!("""
    CREATE TABLE upkeep_repo_capture_test_issues (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      assignee_id INTEGER,
      status TEXT NOT NULL,
      title TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
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
    Repo.query!("""
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

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_columns (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_comments (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      issue_id INTEGER NOT NULL,
      body TEXT NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_tags (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_issue_tags (
      issue_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL
    )
    """)
  end
end

defmodule Upkeep.Kanban.Issue do
  @moduledoc false

  use Ecto.Schema

  @primary_key {:id, :integer, autogenerate: false}
  schema "kanban_issues" do
    field :project_id, :integer
    field :column_id, :integer
    field :assignee_id, :integer
    field :title, :string
    field :status, :string
    field :position, :integer
  end
end

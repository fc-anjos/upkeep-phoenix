defmodule Upkeep.Kanban.Comment do
  @moduledoc false

  use Ecto.Schema

  @primary_key {:id, :integer, autogenerate: false}
  schema "kanban_comments" do
    field :project_id, :integer
    field :issue_id, :integer
    field :body, :string
  end
end

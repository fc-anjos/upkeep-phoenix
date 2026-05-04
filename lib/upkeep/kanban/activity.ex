defmodule Upkeep.Kanban.Activity do
  @moduledoc false

  use Ecto.Schema

  @primary_key {:id, :integer, autogenerate: false}
  schema "kanban_activity" do
    field :project_id, :integer
    field :message, :string
  end
end

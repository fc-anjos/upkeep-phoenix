defmodule Upkeep.Kanban.Column do
  @moduledoc false

  use Ecto.Schema

  @primary_key {:id, :integer, autogenerate: false}
  schema "kanban_columns" do
    field :project_id, :integer
    field :name, :string
    field :position, :integer
  end
end

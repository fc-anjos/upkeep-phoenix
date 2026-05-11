defmodule Upkeep.DemoApp.Note do
  @moduledoc false

  use Ecto.Schema

  @primary_key {:id, :integer, autogenerate: false}
  schema "upkeep_demo_app_notes" do
    field :user_id, :integer
    field :title, :string
  end
end

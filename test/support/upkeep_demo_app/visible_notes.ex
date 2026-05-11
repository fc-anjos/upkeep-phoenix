defmodule Upkeep.DemoApp.VisibleNotes do
  @moduledoc false

  use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

  import Ecto.Query

  alias Upkeep.DemoApp.Note

  def query(%{user_id: user_id}) do
    from note in Note,
      where: note.user_id == ^user_id,
      order_by: [asc: note.id]
  end
end

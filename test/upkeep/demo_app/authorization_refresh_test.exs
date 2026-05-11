defmodule Upkeep.DemoApp.AuthorizationRefreshTest do
  use Upkeep.DemoAppWeb.ConnCase, async: false

  alias Upkeep.DemoApp.Note
  alias Upkeep.TestSupport.Repo

  test "broad invalidations refresh without leaking another user's records", %{conn: conn} do
    seed_notes()

    {:ok, user_one, _html} =
      conn
      |> signed_in_conn(1)
      |> live("/notes")

    {:ok, user_two, _html} =
      Phoenix.ConnTest.build_conn()
      |> signed_in_conn(2)
      |> live("/notes")

    assert has_element?(user_one, "#note-1", "User one private")
    refute has_element?(user_one, "#note-2")

    assert has_element?(user_two, "#note-2", "User two private")
    refute has_element?(user_two, "#note-1")

    rename_note_with_broad_update(1, "User one revised")
    sync_live_views(user_one, user_two)

    assert has_element?(user_one, "#note-1", "User one revised")
    refute has_element?(user_two, "#note-1")
    assert has_element?(user_two, "#note-2", "User two private")

    rename_note_with_broad_update(2, "User two revised")
    sync_live_views(user_one, user_two)

    assert has_element?(user_two, "#note-2", "User two revised")
    refute has_element?(user_one, "#note-2")
    assert has_element?(user_one, "#note-1", "User one revised")
  end

  defp seed_notes do
    Repo.insert_all(
      Note,
      [
        %{id: 1, user_id: 1, title: "User one private"},
        %{id: 2, user_id: 2, title: "User two private"}
      ],
      upkeep: false
    )
  end

  defp rename_note_with_broad_update(id, title) do
    note =
      Note
      |> Repo.get!(id)
      |> Ecto.Changeset.change(title: title)
      |> Repo.update!(upkeep: false)

    Upkeep.updated(note)
  end

  defp sync_live_views(views) do
    :ok = Upkeep.Test.drain()
    Enum.each(views, &:sys.get_state(&1.pid))
  end

  defp sync_live_views(view, other_view), do: sync_live_views([view, other_view])
end

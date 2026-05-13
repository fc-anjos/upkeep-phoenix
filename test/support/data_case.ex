defmodule Upkeep.TestSupport.DataCase do
  @moduledoc """
  Shared SQL sandbox setup for Upkeep tests.

  Each test starts with a reset Upkeep graph, allows coordinator processes to
  use the checked-out repo connection, and drains/resets runtime state on exit.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Upkeep.Test, as: UpkeepTest
  alias Upkeep.TestSupport.DataCase
  alias Upkeep.TestSupport.Repo

  using do
    quote do
      alias Upkeep.TestSupport.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Upkeep.TestSupport.DataCase
    end
  end

  setup tags do
    DataCase.setup_sandbox(tags)
    :ok
  end

  @doc """
  Sets up the sandbox based on the test tags.
  """
  def setup_sandbox(tags) do
    UpkeepTest.reset_graph()

    pid = Sandbox.start_owner!(Repo, shared: not tags[:async])

    UpkeepTest.allow_sandbox()

    on_exit(fn ->
      try do
        UpkeepTest.await_idle()
      after
        UpkeepTest.reset_graph()
        Sandbox.stop_owner(pid)
      end
    end)
  end

  @doc """
  A helper that transforms changeset errors into a map of messages.

      assert {:error, changeset} = Accounts.create_user(%{password: "short"})
      assert "password is too short" in errors_on(changeset).password
      assert %{password: ["password is too short"]} = errors_on(changeset)

  """
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end

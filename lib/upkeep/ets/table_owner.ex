defmodule Upkeep.ETS.TableOwner do
  @moduledoc false

  use Boundary, top_level?: true

  # Long-lived owner for a set of named ETS tables, paired with a heir process
  # so the tables outlive an owner crash.
  #
  # ETS tables die with their owning process. When tables are created inside a
  # supervisor's `init/1`, the supervisor process owns them, so any restart of
  # that supervisor wipes the tables (and recreates them empty) — leaving any
  # surviving processes pointing at empty state.
  #
  # This module runs two cooperating, named processes (started FIRST in their
  # subtree, before any sibling uses the tables):
  #
  #   * a `:heir` keeper, started first, which never touches the tables in
  #     steady state. It only exists to receive the tables if the owner dies.
  #   * an `:owner`, started second, which creates and owns the tables with the
  #     keeper as their ETS `:heir`.
  #
  # If the OWNER crashes, ETS transfers the tables to the keeper (so the data
  # survives); when the supervisor restarts the owner, the keeper hands the
  # tables back and the owner re-arms the keeper as heir. This survives repeated
  # owner crashes.
  #
  # Residual limitation: if the whole subtree restarts (owner AND keeper die),
  # the tables are still wiped. Fully surviving that requires hoisting ownership
  # above the coordinator supervisor, which is intentionally out of scope.

  use GenServer

  @give_back_retry_ms 5

  @doc """
  Child specs for the owner/keeper pair.

  Options:

    * `:name` (required) — base name; the keeper is `Module.concat(name, Heir)`
      and the owner is `name`.
    * `:tables` (required) — list of `{table_name, ets_opts}` to create/own.

  Place these at the FRONT of the subtree's child list so the tables exist
  before any sibling reads or writes them.
  """
  def child_specs(opts) do
    name = Keyword.fetch!(opts, :name)
    tables = Keyword.fetch!(opts, :tables)
    heir_name = heir_name(name)

    [
      %{id: heir_name, start: {__MODULE__, :start_keeper, [heir_name]}, type: :worker},
      %{id: name, start: {__MODULE__, :start_owner, [name, heir_name, tables]}, type: :worker}
    ]
  end

  def start_keeper(name), do: GenServer.start_link(__MODULE__, {:keeper, name}, name: name)

  def start_owner(name, heir_name, tables) do
    GenServer.start_link(__MODULE__, {:owner, name, heir_name, tables}, name: name)
  end

  @impl true
  def init({:keeper, _name}), do: {:ok, %{role: :keeper}}

  def init({:owner, _name, heir_name, tables}) do
    heir = Process.whereis(heir_name)
    Enum.each(tables, fn {table, ets_opts} -> adopt_or_create(table, ets_opts, heir) end)
    {:ok, %{role: :owner, heir: heir, tables: tables}}
  end

  @impl true
  # Keeper inherits a table after the owner dies: hold it, then hand it back once
  # the owner has been restarted (so the owner re-owns and re-arms heirship).
  def handle_info({:"ETS-TRANSFER", table, _from, owner_name}, %{role: :keeper} = state)
      when is_atom(owner_name) do
    _ = give_back_when_available(table, owner_name)
    {:noreply, state}
  end

  def handle_info({:give_back, table, owner_name}, %{role: :keeper} = state) do
    _ = give_back_when_available(table, owner_name)
    {:noreply, state}
  end

  # Owner receives a table back from the keeper after a restart: re-arm the
  # keeper as heir for the next crash.
  def handle_info({:"ETS-TRANSFER", table, _from, _data}, %{role: :owner} = state) do
    if is_pid(state.heir) and Process.alive?(state.heir) and table_exists?(table) do
      :ets.setopts(table, {:heir, state.heir, owner_self_name()})
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp adopt_or_create(table, ets_opts, heir) do
    cond do
      not table_exists?(table) ->
        # First boot: create and own the table with the keeper as heir. The heir
        # data carries this owner's registered name so the keeper knows where to
        # return the table.
        ^table = :ets.new(table, with_heir(ets_opts, heir))

      :ets.info(table, :owner) == self() ->
        # Already ours (e.g. handed back by the keeper): just re-arm the heir.
        if is_pid(heir), do: :ets.setopts(table, {:heir, heir, owner_self_name()})

      true ->
        # The table exists but is held by the keeper (post-crash). The keeper
        # hands it back via give_away once it sees us restart; nothing to do
        # synchronously here. The data is safe with the keeper meanwhile.
        :ok
    end

    :ok
  end

  defp give_back_when_available(table, owner_name) do
    case Process.whereis(owner_name) do
      nil ->
        # Owner not yet restarted; retry shortly. The keeper owns the table in
        # the meantime, so the data is safe.
        Process.send_after(self(), {:give_back, table, owner_name}, @give_back_retry_ms)

      pid when is_pid(pid) ->
        safe_give_away(table, pid, owner_name)
    end
  end

  defp safe_give_away(table, pid, owner_name) do
    if table_exists?(table) and :ets.info(table, :owner) == self() do
      :ets.give_away(table, pid, owner_name)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp with_heir(ets_opts, heir) when is_pid(heir) do
    [{:heir, heir, owner_self_name()} | ets_opts]
  end

  defp with_heir(ets_opts, _heir), do: ets_opts

  # The registered name of THIS owner process, embedded as ETS heir data so the
  # keeper can hand the table back to the right name after a restart.
  defp owner_self_name do
    case Process.info(self(), :registered_name) do
      {:registered_name, name} when is_atom(name) -> name
      _ -> nil
    end
  end

  defp table_exists?(table), do: :ets.whereis(table) != :undefined

  @doc false
  def heir_name(name), do: Module.concat(name, Heir)
end

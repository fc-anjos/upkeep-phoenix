defmodule Upkeep.Internal.Runtime.ScopeCapture do
  @moduledoc false

  alias Upkeep.Live.Ids

  def analyze(fun) when is_function(fun) do
    info = :erlang.fun_info(fun)

    with {:env, []} <- List.keyfind(info, :env, 0),
         {:type, :external} <- List.keyfind(info, :type, 0),
         {:module, module} <- List.keyfind(info, :module, 0),
         {:name, name} <- List.keyfind(info, :name, 0),
         {:arity, arity} <- List.keyfind(info, :arity, 0) do
      {:external, {module, name, arity}}
    else
      {:env, env} when is_list(env) and env != [] ->
        case scope_like_capture(env) do
          nil -> {:captured, fun_identity_from_info(info)}
          scope_capture -> {:captured_scope, fun_identity_from_info(info), scope_capture}
        end

      _ ->
        :not_external
    end
  end

  def policy do
    Application.get_env(:upkeep, :captured_scope_policy) || default_policy()
  end

  def apply_policy({:captured_scope, _fun_identity, scope_capture}, context) do
    case policy() do
      :raise ->
        assign_name = Map.fetch!(context, :assign_name)
        location_suffix = location_suffix(Map.get(context, :source_location))

        raise Upkeep.ImplicitScopeError,
              "Upkeep derive #{inspect(assign_name)}#{location_suffix} captures " <>
                "#{inspect(scope_capture)}. " <>
                "Use an external function that receives current_scope from the dependency map " <>
                "instead of closing over socket/session/current_scope values."

      :telemetry ->
        :ok
    end
  end

  def apply_policy(_analysis, _context), do: :ok

  defp location_suffix(nil), do: ""

  defp location_suffix(%{} = location) do
    file = Map.get(location, :file_label) || Map.get(location, :file)
    line = Map.get(location, :line)

    cond do
      is_binary(file) and is_integer(line) -> " (declared at #{file}:#{line})"
      is_binary(file) -> " (declared at #{file})"
      true -> ""
    end
  end

  def implicit_scope_metadata(socket, dep_node_ids) do
    cond do
      not Map.has_key?(socket.assigns, :current_scope) ->
        :missing

      Ids.scope_node_id(:current_scope) in dep_node_ids ->
        :dependency

      true ->
        :available
    end
  end

  defp fun_identity_from_info(info) do
    module = info |> List.keyfind(:module, 0) |> elem(1)
    name = info |> List.keyfind(:name, 0) |> elem(1)
    arity = info |> List.keyfind(:arity, 0) |> elem(1)
    {module, name, arity}
  end

  defp default_policy do
    case runtime_env() do
      :dev -> :raise
      :prod -> :telemetry
      _other -> :telemetry
    end
  end

  defp runtime_env do
    if Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) do
      Mix.env()
    else
      :prod
    end
  end

  defp scope_like_capture(values) do
    Enum.find_value(values, &scope_like_value/1)
  end

  defp scope_like_value(%Phoenix.LiveView.Socket{}), do: :socket
  defp scope_like_value(%{assigns: %{current_scope: _}}), do: :socket
  defp scope_like_value(%{current_scope: _}), do: :current_scope
  defp scope_like_value(%{current_user: _}), do: :current_user
  defp scope_like_value(%{"current_scope" => _}), do: :current_scope
  defp scope_like_value(%{"current_user" => _}), do: :current_user

  defp scope_like_value(map) when is_map(map) do
    map
    |> Map.values()
    |> scope_like_capture()
  end

  defp scope_like_value(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> scope_like_capture()
  end

  defp scope_like_value(list) when is_list(list), do: scope_like_capture(list)
  defp scope_like_value(_value), do: nil
end

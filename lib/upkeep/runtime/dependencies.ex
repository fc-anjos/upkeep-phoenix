defmodule Upkeep.Runtime.Dependencies do
  @moduledoc false

  alias Upkeep.Runtime.State

  def dependency_nodes(socket, deps, source_location \\ nil) do
    deps
    |> Enum.map(fn dep ->
      node_id =
        Map.get(State.assign_nodes(socket), dep) ||
          raise ArgumentError, unknown_dep_message(dep, source_location)

      {node_id, {dep, node_id}}
    end)
    |> Enum.unzip()
  end

  defp unknown_dep_message(dep, nil) do
    "unknown Upkeep dependency assign #{inspect(dep)}"
  end

  defp unknown_dep_message(dep, %{} = location) do
    where = location_label(location)
    code = Map.get(location, :code)

    base = "unknown Upkeep dependency assign #{inspect(dep)} (declared at #{where}"

    if is_binary(code) and code != "" do
      base <> ": " <> code <> ")"
    else
      base <> ")"
    end
  end

  defp location_label(%{file_label: file, line: line}) when is_binary(file) and is_integer(line),
    do: "#{file}:#{line}"

  defp location_label(%{file: file, line: line}) when is_binary(file) and is_integer(line),
    do: "#{file}:#{line}"

  defp location_label(%{file_label: file}) when is_binary(file), do: file
  defp location_label(%{file: file}) when is_binary(file), do: file
  defp location_label(_), do: "unknown source"
end

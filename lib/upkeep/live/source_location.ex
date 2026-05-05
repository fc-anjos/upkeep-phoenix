defmodule Upkeep.Live.SourceLocation do
  @moduledoc false

  @context_radius 2

  @doc """
  Compile-time capture from a `Macro.Env`. Used by `Upkeep.Live.Macros`.
  """
  def capture(%Macro.Env{} = caller, kind, call_name, args) when is_atom(call_name) do
    if capture?() do
      %{
        kind: kind,
        file: caller.file,
        file_label: file_label(caller.file),
        line: caller.line,
        module: caller.module,
        module_label: module_label(caller.module),
        function: caller.function,
        function_label: function_label(caller.function),
        code: Macro.to_string({call_name, [], args}),
        snippet: source_snippet(caller.file, caller.line)
      }
    end
  end

  def capture? do
    Application.get_env(:upkeep, :capture_source_locations, default_capture?())
  end

  defp default_capture? do
    Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() in [:dev, :test]
  end

  defp file_label(nil), do: nil

  defp file_label(file) when is_binary(file) do
    case File.cwd() do
      {:ok, cwd} -> Path.relative_to(file, cwd)
      {:error, _reason} -> file
    end
  end

  defp module_label(nil), do: nil

  defp module_label(module) when is_atom(module) do
    module
    |> Module.split()
    |> Enum.join(".")
  rescue
    ArgumentError -> Atom.to_string(module)
  end

  defp function_label(nil), do: nil

  defp function_label({name, arity}) do
    "#{name}/#{arity}"
  end

  defp source_snippet(file, line_number) when is_binary(file) and is_integer(line_number) do
    with {:ok, contents} <- File.read(file) do
      lines = String.split(contents, "\n", trim: false)
      first = max(line_number - @context_radius, 1)
      last = min(line_number + @context_radius, length(lines))
      width = last |> Integer.to_string() |> String.length()

      first..last
      |> Enum.map(fn number ->
        marker = if number == line_number, do: ">", else: " "
        source_line = Enum.at(lines, number - 1, "")
        "#{marker} #{String.pad_leading(Integer.to_string(number), width)}  #{source_line}"
      end)
      |> Enum.join("\n")
    else
      {:error, _reason} -> nil
    end
  end

  defp source_snippet(_file, _line_number), do: nil
end

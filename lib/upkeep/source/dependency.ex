defmodule Upkeep.Source.Dependency do
  @moduledoc false

  def coverage(deps) do
    apply_dependency(deps, :coverage, [deps], Upkeep.Source.Coverage.new(nil, %{}))
  end

  def interest_keys(deps) do
    apply_dependency(deps, :interest_keys, [deps], [])
  end

  def coarse_keys(deps) do
    apply_dependency(deps, :coarse_keys, [deps], [])
  end

  def matches_change?(deps, event) do
    apply_dependency(deps, :matches_change?, [deps, event], false)
  end

  defp apply_dependency(%{__struct__: module}, function, args, default)
       when is_atom(module) and is_atom(function) do
    if function_exported?(module, function, length(args)) do
      apply(module, function, args)
    else
      default
    end
  end

  defp apply_dependency(_deps, _function, _args, default), do: default
end

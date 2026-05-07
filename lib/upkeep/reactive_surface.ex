defmodule Upkeep.ReactiveSurface do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Upkeep.Change,
      Upkeep.Source
    ],
    type: :strict

  defstruct source: nil,
            params: nil,
            deps: [],
            matcher: nil,
            index_keys: []

  def source(source, params, interest_keys, deps \\ [])
      when is_atom(source) and is_map(params) and is_list(interest_keys) and is_list(deps) do
    %__MODULE__{
      source: source,
      params: params,
      deps: deps,
      index_keys: build_index_keys(interest_keys, deps)
    }
  end

  def deps(deps) when is_list(deps) do
    %__MODULE__{deps: deps, index_keys: build_index_keys([], deps)}
  end

  def manual(index_keys, matcher) when is_list(index_keys) and is_function(matcher, 1) do
    %__MODULE__{matcher: matcher, index_keys: build_index_keys(index_keys, [])}
  end

  def from_source_loader({:source, source, params}, interest_keys, deps)
      when is_atom(source) and is_map(params) do
    source(source, params, interest_keys, deps)
  end

  def index_keys(%__MODULE__{index_keys: keys}), do: keys

  def candidate_keys(%Upkeep.Change{} = change) do
    [
      Upkeep.Source.Keys.notification_key(%{name: change.name, schema: change.schema}),
      Upkeep.Source.Keys.notification_key(%{name: change.name, schema: :_})
    ]
    |> Enum.uniq()
  end

  def candidate_keys(event) when is_struct(event) do
    [Upkeep.Source.Keys.notification_key(%{legacy: event.__struct__})]
  end

  def matches?(%__MODULE__{} = surface, event) when is_struct(event) do
    source_matches?(surface.source, surface.params, event) or
      deps_match?(surface.deps, event) or
      manual_match?(surface.matcher, event)
  end

  defp build_index_keys(interest_keys, deps) do
    (Enum.flat_map(interest_keys, &coarse_key/1) ++
       Enum.flat_map(deps, &dependency_index_keys/1))
    |> Enum.uniq()
  end

  defp dependency_index_keys(deps) do
    deps
    |> Upkeep.Source.Dependency.coarse_keys()
    |> Enum.flat_map(&coarse_key/1)
  end

  defp coarse_key({:upkeep_change, name, schema}), do: [{:upkeep_change, name, schema}]
  defp coarse_key({:upkeep_change, name, schema, _values}), do: [{:upkeep_change, name, schema}]
  defp coarse_key({:upkeep_event, event}), do: [{:upkeep_event, event}]
  defp coarse_key({:upkeep_event, event, _values}), do: [{:upkeep_event, event}]
  defp coarse_key({action, schema}) when is_atom(action), do: [{:upkeep_change, action, schema}]
  defp coarse_key(_key), do: []

  defp source_matches?(nil, _params, _event), do: false

  defp source_matches?(source, params, event) when is_atom(source) and is_map(params) do
    function_exported?(source, :reacts_to?, 2) and source.reacts_to?(event, params)
  end

  defp deps_match?(deps, event) do
    Enum.any?(deps, &Upkeep.Source.Dependency.matches_change?(&1, event))
  end

  defp manual_match?(nil, _event), do: false
  defp manual_match?(matcher, event) when is_function(matcher, 1), do: matcher.(event)
end

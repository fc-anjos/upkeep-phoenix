defmodule Upkeep.Live.Specs do
  @moduledoc false

  alias Upkeep.Live.Ids
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.NodeSpec
  alias Upkeep.Runtime.Materializer
  alias Upkeep.Runtime.Producer
  alias Upkeep.Runtime.ScopeCapture
  alias Upkeep.Runtime.State
  alias Upkeep.Source.Runtime, as: Source

  def source(assign_name, source, params, component, source_location \\ nil)
      when is_atom(assign_name) and is_map(params) do
    source_id = Ids.scoped_source_id(source, params, component)
    node_id = Ids.source_node_id(source_id)

    %NodeSpec{
      id: node_id,
      kind: :source,
      deps: Ids.source_deps(component),
      producer: %Producer.Source{
        source: source,
        params: params,
        source_id: source_id,
        component: component
      },
      scope: :shared,
      materializers: [
        %Materializer.Assign{assign_name: assign_name, node_id: node_id, kind: :source}
      ],
      metadata:
        %{
          assign_name: assign_name,
          source_id: source_id,
          source: source,
          params: params,
          component: component,
          sharing_partition: Source.sharing_partition(source, params)
        }
        |> put_source_location(source_location)
    }
  end

  def component(socket, component_id, deps, fun, source_location \\ nil)
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = DAGOperations.dependency_nodes(socket, deps)
    node_id = Ids.component_node_id(component_id)

    %NodeSpec{
      id: node_id,
      kind: :component,
      deps: dep_node_ids,
      producer: %Producer.Compute{deps: dep_node_ids, dep_pairs: dep_pairs, fun: fun},
      scope: :local,
      materializers: [%Materializer.Component{component_id: component_id, node_id: node_id}],
      metadata: %{component_id: component_id} |> put_source_location(source_location)
    }
  end

  def derived(socket, assign_name, deps, fun, source_location \\ nil)
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    identity = external_fun_identity(fun)
    deps = maybe_add_implicit_scope_dep(socket, deps, identity)
    {dep_node_ids, dep_pairs} = DAGOperations.dependency_nodes(socket, deps)
    node_id = Ids.derived_node_id(assign_name)

    %NodeSpec{
      id: node_id,
      kind: :derived,
      deps: dep_node_ids,
      producer: %Producer.Compute{
        deps: dep_node_ids,
        dep_pairs: dep_pairs,
        fun: fun,
        identity: identity
      },
      scope: :local_or_shared,
      materializers: [
        %Materializer.Assign{assign_name: assign_name, node_id: node_id, kind: :derived}
      ],
      metadata: %{assign_name: assign_name} |> put_source_location(source_location)
    }
  end

  defp put_source_location(metadata, nil), do: metadata

  defp put_source_location(metadata, source_location) when is_map(source_location) do
    Map.put(metadata, :source_location, source_location)
  end

  defp external_fun_identity(fun) do
    case ScopeCapture.analyze(fun) do
      {:external, identity} -> identity
      _other -> nil
    end
  end

  defp maybe_add_implicit_scope_dep(socket, deps, nil) do
    if Map.has_key?(State.assign_nodes(socket), :current_scope) and :current_scope not in deps do
      deps ++ [:current_scope]
    else
      deps
    end
  end

  defp maybe_add_implicit_scope_dep(_socket, deps, _identity), do: deps
end

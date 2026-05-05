defmodule Upkeep.Live.Macros do
  @moduledoc false

  alias Upkeep.Live.SourceLocation

  defmacro watch(socket, assign_name, source, params) do
    location =
      SourceLocation.capture(__CALLER__, :source, :watch, [socket, assign_name, source, params])

    quote do
      Upkeep.Live.watch(
        unquote(socket),
        unquote(assign_name),
        unquote(source),
        unquote(params),
        source_location: unquote(Macro.escape(location))
      )
    end
  end

  defmacro watch(socket, assign_name, source, params, opts) do
    location =
      SourceLocation.capture(__CALLER__, :source, :watch, [
        socket,
        assign_name,
        source,
        params,
        opts
      ])

    quote do
      Upkeep.Live.watch(
        unquote(socket),
        unquote(assign_name),
        unquote(source),
        unquote(params),
        Keyword.put(unquote(opts), :source_location, unquote(Macro.escape(location)))
      )
    end
  end

  defmacro component(socket, component_id, deps, fun) do
    location =
      SourceLocation.capture(__CALLER__, :component, :component, [socket, component_id, deps, fun])

    quote do
      Upkeep.Live.component(
        unquote(socket),
        unquote(component_id),
        unquote(deps),
        unquote(fun),
        source_location: unquote(Macro.escape(location))
      )
    end
  end

  defmacro derive(socket, assign_name, deps, fun) do
    location =
      SourceLocation.capture(__CALLER__, :derived, :derive, [socket, assign_name, deps, fun])

    quote do
      Upkeep.Live.derive(
        unquote(socket),
        unquote(assign_name),
        unquote(deps),
        unquote(fun),
        source_location: unquote(Macro.escape(location))
      )
    end
  end
end

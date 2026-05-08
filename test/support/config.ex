defmodule Upkeep.TestSupport.Config do
  @moduledoc false

  def with_env(app, key, value, fun) when is_function(fun, 0) do
    previous = Application.get_env(app, key, :__missing__)
    Application.put_env(app, key, value)

    try do
      fun.()
    after
      case previous do
        :__missing__ -> Application.delete_env(app, key)
        value -> Application.put_env(app, key, value)
      end
    end
  end

  def with_upkeep(key, value, fun) when is_function(fun, 0) do
    with_env(:upkeep, key, value, fun)
  end
end

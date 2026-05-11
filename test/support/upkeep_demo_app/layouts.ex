defmodule Upkeep.DemoAppWeb.Layouts do
  @moduledoc false

  use Upkeep.DemoAppWeb, :html

  attr :current_scope, :map, required: true
  attr :flash, :map, default: %{}
  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <main id="demo-app" data-user-id={@current_scope.user_id}>
      {render_slot(@inner_block)}
    </main>
    """
  end
end

defmodule Upkeep.Runtime.Producer.Source do
  @moduledoc false

  @enforce_keys [:instance, :source_id, :component]
  defstruct [:instance, :source_id, :component]
end

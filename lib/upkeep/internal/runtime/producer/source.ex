defmodule Upkeep.Internal.Runtime.Producer.Source do
  @moduledoc false

  @enforce_keys [:source, :params, :source_id, :component]
  defstruct [:source, :params, :source_id, :component]
end

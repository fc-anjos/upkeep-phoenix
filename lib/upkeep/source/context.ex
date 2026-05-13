defmodule Upkeep.Source.Context do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [],
    type: :strict

  @enforce_keys [:current_scope, :identity_envelope]
  defstruct current_scope: nil,
            identity_envelope: nil

  @type t :: %__MODULE__{
          current_scope: term(),
          identity_envelope: term()
        }

  def new(current_scope) do
    %__MODULE__{
      current_scope: current_scope,
      identity_envelope: current_scope_envelope(current_scope)
    }
  end

  def current_scope!(%__MODULE__{current_scope: nil}) do
    raise ArgumentError,
          "Upkeep.current_scope!/1 was called by an identity-aware source, " <>
            "but Phoenix did not assign :current_scope for this socket"
  end

  def current_scope!(%__MODULE__{current_scope: current_scope}), do: current_scope

  defp current_scope_envelope(current_scope) do
    {:current_scope, :crypto.hash(:sha256, :erlang.term_to_binary(current_scope))}
  end
end

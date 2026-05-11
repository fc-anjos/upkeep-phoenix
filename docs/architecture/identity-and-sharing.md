# Identity And Sharing Contract

**Status:** decided for the current runtime. Future optimization can make more
nodes share, but it must preserve this contract.

Upkeep can only share work when every value that can affect the result is
represented in graph identity. That is the central safety rule. If a value is
hidden in a closure, process dictionary, session blob, or socket capture, the
runtime cannot prove who owns the result and must not share it.

Phoenix applications already model request identity elsewhere. In Phoenix 1.8
generated auth, that identity normally reaches LiveViews as
`socket.assigns.current_scope`, assigned through `assign_new/3` in an
`on_mount` hook. Upkeep uses that Phoenix shape directly. It does not ask host
applications to declare identity a second time.

## Package-Level Identity

Application identity layers such as:

```text
global < account < project < user < session < component/socket
```

are app-specific. A package cannot know whether `project_id` belongs above
`account_id`, whether a user belongs to one account or many, or whether a
session flag affects authorization.

Upkeep's package-level identities are therefore:

- **Source identity:** source module plus normalized params, optionally scoped
  under a component identity, and for identity-aware sources an opaque
  `current_scope` envelope.
- **Source sharing partition:** the source-defined partition used to colocate
  source and derived graph work.
- **Scope identity:** the opaque value assigned at `:current_scope`.
- **Component identity:** the explicit non-nil identity passed to
  `component/4` or `watch(..., under: component_id)`.
- **Function identity:** external module/function/arity for shareable derived
  computations.

The runtime can combine these identities conservatively. It must not inspect a
project-specific `current_scope` struct and decide which fields mean "account",
"project", or "user".

## Phoenix Hook Point

`use Upkeep.Live` registers `Phoenix.LiveView.on_mount(Upkeep.Live.ScopeHook)`.
The hook calls:

```elixir
assign_new(socket, :current_scope, fn -> nil end)
```

Every public `Upkeep.Live` operation then syncs `socket.assigns.current_scope`
into the local DAG as the source node:

```elixir
{:scope, :current_scope}
```

If `current_scope` changes, downstream local nodes depending on that scope node
recompute. This composes with the host app's auth hook because Phoenix
`assign_new/3` reuses an existing assign when the auth layer already provided
one.

## Sharing Rules

### Sources

A source load can be shared when subscribers watch the same source identity:

```elixir
watch(:stats, Sources.ProjectStats, project_id: 123)
```

If 100 connected LiveViews call that with the same source module and normalized
params, the Graph coordinator can run the initial source load once and feed the
loaded value to every subscriber. On later writes, Graph can also reload the
dirty source once per flush and fan out the value.

If stable domain identity affects the source value, it can be part of the
source params. For example:

```elixir
watch(:my_issues, Sources.MyIssues, project_id: 123, user_id: 456)
```

does not share across users because `user_id` is part of the source params. It
can still share across two sockets for the same user and same project.

When viewer identity or permissions come from Phoenix `:current_scope`, the
source uses `load/2` or `query/2` and reads the scope through the Upkeep source
context:

```elixir
def query(%{project_id: project_id}, upkeep) do
  scope = Upkeep.current_scope!(upkeep)

  from card in Card,
    where:
      card.project_id == ^project_id and
        card.account_id == ^scope.account_id and
        card.value <= ^scope.max_card_value
end
```

The callback shape is the pre-load signal that the source is identity-aware.
Before coalescing or graph registration, Upkeep qualifies the source identity
with an opaque envelope for the whole `current_scope` value. Two subscribers
watching the same source params but different scopes therefore do not share the
source load or graph node.

### External Derived Values

A derived value can be shared by Graph when all of these are true:

- the compute function is an external function with module/function/arity
  identity;
- the LiveView is connected and can subscribe to Graph;
- all dependencies have graph identities;
- dependencies are in the same sharing partition;
- no dependency is component-scoped or local-only.

This is the package-level safe path for shared intermediate view models:

```elixir
socket
|> watch(:issues, Sources.ProjectIssues, project_id: 123)
|> derive(:stats, [:issues], &DashboardStats.from_issues/1)
```

Every subscriber with the same source value and same external function identity
can share the initial derived compute. During steady-state updates, Graph can
recompute the derived node once before dispatch.

### Local And Private Derived Values

Private derives are allowed to depend on `current_scope` without making the
host app spell that dependency out:

```elixir
socket
|> watch(:stories, Sources.ProjectStories, project_id: 123)
|> derive(:story_cards, [:stories], fn %{stories: stories, current_scope: scope} ->
  StoryCards.for_scope(stories, scope)
end)
```

When `current_scope` is present, Upkeep adds `:current_scope` to the local DAG
deps for non-external derive functions. That keeps the value socket-local and
also makes it reactive when identity changes.

The useful split is:

```text
shared project source -> local scope-aware projection -> LiveView assign
```

For a dashboard, `Sources.ProjectStories(project_id: 123)` can still be loaded
once for everyone. The per-user read markers, permissions, labels, or ordering
that depend on `current_scope` stay local unless a later scoped-sharing pass can
prove the same scope identity.

### Captured Scope Values

Captured socket/session/current-scope values are fail-closed:

- in dev, Upkeep raises `Upkeep.Runtime.ImplicitScopeError`;
- in prod, Upkeep keeps the derive local and emits
  `[:upkeep, :derive, :sharing]` with `severity: :error` and
  `reason: :captured_scope`.

This code is rejected in dev and never shared in prod:

```elixir
socket
|> derive(:label, [:issues], fn %{issues: issues} ->
  Label.build(issues, socket.assigns.current_scope)
end)
```

The correct shape receives identity from the dependency map:

```elixir
socket
|> derive(:label, [:issues], fn %{issues: issues, current_scope: scope} ->
  Label.build(issues, scope)
end)
```

## When Identity Is Unambiguous

Upkeep can derive identity without app-specific knowledge when:

- two watches use the same source module and normalized params;
- an identity-aware `load/2` or `query/2` source reads `current_scope` through
  the Upkeep source context, causing the whole scope value to qualify source
  identity before sharing;
- a source declares a sharing partition and dependencies are in that same
  partition;
- Phoenix has assigned `:current_scope`, and Upkeep treats the whole value as
  an opaque dependency;
- a component has an explicit stable identity;
- a derived compute is an external function, so module/function/arity can be
  part of the graph node id.

Examples:

- `watch(:stats, Sources.ProjectStats, project_id: 123)` is shared across all
  sockets that watch that exact project stats source.
- `watch(:my_issues, Sources.MyIssues, project_id: 123, user_id: 456)` is
  shared only across sockets with that exact user-specific source identity.
- A `query/2` source that calls `Upkeep.current_scope!(upkeep)` is shared only
  across sockets with the same source params and the same opaque scope envelope.
- `derive(:stats, [:issues], &Stats.from_issues/1)` is shareable when `:issues`
  is graph-backed and partition-compatible.
- A private function that reads `%{current_scope: scope}` from its dependency
  map is local and reactive to scope changes.

## When Identity Is Ambiguous

Upkeep cannot prove identity when:

- a derive function closes over `socket`, `current_scope`, `current_user`, or a
  session map;
- a source load reads identity from process state, application state, or a value
  that is not in source params or the Upkeep source context;
- a function component boundary has no stable id;
- source params contain extra values whose effect on the loaded value cannot be
  proven;
- `current_scope` is absent and private code expects identity anyway;
- authorization changes data after a shared source load but before the socket
  sends the value to the browser, outside the graph.

The runtime response is conservative for shapes it can detect: derived scope
captures stay local or raise by policy. Hidden identity reads inside a source
are outside Upkeep's observation boundary; source authors must use `load/2` or
`query/2` for identity-sensitive source values.

## How The DAG Finds The Biggest Safe Unit

The DAG helps by separating upstream shared work from downstream private work.
It does not need to know the app's domain hierarchy. It only needs visible
dependencies.

```text
Sources.ProjectStats(project_id: 123)
  shared by all subscribers with that source identity

current_scope
  local opaque identity value

derive(:dashboard_model, [:stats, :current_scope], ...)
  local today; future scoped sharing can key by exact scope identity
```

This lets Upkeep deduplicate the largest prefix of the computation that has no
private dependency. If a later node depends on `current_scope`, sharing stops at
that boundary unless the runtime has a scoped graph identity for that exact
scope value.

## Telemetry Contract

Sharing decisions must stay observable. Current events include:

- `[:upkeep, :graph, :initial_load, :hit | :miss]`
- `[:upkeep, :graph, :derived_initial, :hit | :miss]`
- `[:upkeep, :graph, :dispatch, :start | :stop]`
- `[:upkeep, :derive, :sharing]`

Important `[:upkeep, :derive, :sharing]` metadata:

- `result: :shared | :local`
- `reason: :shareable | :captured_scope | :captured_fun | :local_fun |
  :component_scoped_dep | :local_only_dep | :cross_partition_dep | ...`
- `severity: :error` for captured scope fallbacks
- `implicit_scope: :current_scope | :dependency | :available | :missing`
- `scope_present?: boolean`

Benchmark gates should assert these events fire for any optimization they are
intended to prove.

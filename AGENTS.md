# AGENTS.md

Single-binary Go gateway: OpenCode agents behind a central hub, reachable over
Nostr and a small HTTP API. `plan.md` is the original spec; `README.md` is the
protocol/config reference. This file is only the things you cannot infer from
reading the code.

## Commands

```bash
export PATH=$HOME/.local/go/bin:$PATH   # go is not on PATH by default
gofmt -l . && go vet ./... && go test ./...   # the full check, in that order
go test -race ./...                            # green; see the race note below
go test -run TestPermissionV2Server -v .      # one test; ./... is a single package
go build -o /tmp/gateway .                     # build outside the tree, do not litter
```

`go.mod`'s `go` directive is the single source of truth for the Go version: the
Dockerfile tag and CI's `go-version-file` both read it. It is `1.27.1` and it has
to stay at or above the patch release that fixes the current stdlib CVEs — the
CI `govulncheck` step exists to catch a *lowered* directive, because that is how
the gateway ends up running an unpatched `net/http`, `crypto/tls` and `crypto/x509`
with nothing saying so. Add a check to `.github/workflows/ci.yml`, not a third step
in your head, and pin any tool version rather than using `@latest` so a new release
cannot break main.

`go test` and `go test -race` have different file sets: `nostr_test.go` carries
`//go:build !race`, so `-race` skips it. Reason: go-nostr v0.52.3 has a data race
in its own connect path (`relay.go:175` vs `relay.go:576`) that fires as soon as
the pool subscribes, and it is the newest published version. If you ever see that
race, it is upstream — do not try to "fix" it in gateway code. `go test ./...`
covers the Nostr path.

The same version builds *identical* NIP-42 auth events when two subscriptions
answer a challenge in the same second, and it keys the `OK` waiters by event id
(`relay.go:371`), so the second `Store` overwrites the first and the loser of the
pair waits out its context instead of returning. This is why each agent has its
own pool **per role**: the `buzzPools` entry is a second, separate pool for
`BUZZ_RELAYS`, so the kind-30078 listener and the Buzz discovery never answer a
challenge on one connection. Sharing one is what made the gateway deaf — with a
relay in both lists (the normal production setup, since the kind-30078 answers
have to land somewhere the agent is a member) the discovery lost the race, sat
out its whole 15s `buzzFetchTimeout`, returned empty with the member list right
there, and the agent subscribed to no channel at all.
`TestBuzzDiscoveryOnARelaySharedWithTheMessageListener` is the guard.

## Wiring

Everything crosses at exactly two functions: `hub.Handle` (inbound, all
transports) and `hub.emit` (outbound, all transports). `opencode.go` is the only
file allowed to speak the OpenCode HTTP API; `reduce.go` is the only place that
translates OpenCode events into protocol events. Keep it that way — the point of
the gateway is that no transport or agent can see OpenCode's wire format or its
`:4096` address. The `/v1/chat/completions` face in `http.go` is a translation of
the same two functions, not a path around them.

Data flow: transport → `hub.Handle` → `conversation.ensureSession` (creates the
OpenCode session once, atomically) → `opencodeClient.promptAsync` → SSE from
`opencodeClient.events` → `hub.onEvent` → `reduceEvent` → `hub.emit` → subscribers
(SSE) + the Nostr publish queue.

`buzz.go` is a transport like `nostr.go`, and it is the one place that both reads
and writes Buzz. It publishes two things and no more: the NIP-OA agent profile
(kind `10100`), once per agent, so Buzz knows the pubkey is an agent, and the
agent's answers, as kind `9` in the channel the mention came from. The profile
must stay signed by the agent's own key, kind `10100` must not grow any channel
traffic in its content, and `buzzProfile` must keep firing on a member list that
names no channel — Buzz's channel UI is how an agent gets added to a channel, so
an agent that waits to be a member to be registered is an agent that can never
join.
Inbound is unchanged — a mention becomes a prompt through `hub.Handle`, so
`allow`, the lock order and one-session-per-conversation all still hold.

The route out is the conversation id, not the hub: `buzzConvPrefix` marks a
conversation as a channel, and `nostr.send` branches on it. `hub` stays
transport-blind, so **if a second transport ever needs its own outbound path it
gets its own conversation-id prefix, not a field on the hub.** A kind-30078
envelope is what a Nostr subscriber renders, and Buzz renders kind 9, so the
wrong kind in a channel is a silent no-op — the agent answers and the human sees
nothing, which is the failure this whole path exists to prevent.

`buzzJob` assembles a turn and posts it once, because a channel message is one
message: OpenCode streams the answer as deltas and posting each one would put a
message per token in front of the reader. It therefore runs on the `worker`
goroutine, which is its only writer — that is why `nostr.turns` needs no lock and
why `send` may not publish a channel message from its own goroutine. A turn that
never reaches `completed` posts nothing; a debounce is the upgrade if that
matters. `buzzReceive` drops events authored by the agent's own key, because a
relay fans an event out to the connection that published it, and in a DM every
message counts as addressed to the agent — without that check the agent answers
itself once per answer.

`buzzDiscover` is a one-shot `FetchMany`, and go-nostr never re-sends the REQ when
its NIP-42 handshake comes back empty: it answers the challenge once and returns
(pool.go:662-673). Combined with the identical-auth-event note above, that turns
"the relay sent nothing" into a 60-second wait in the old code. Discovery is
therefore bounded (`buzzFetchTimeout`) and re-asked once on an empty answer
(`buzzRetryDelay`). It reports three states, not two, and the distinction is load
bearing: no member lists at all (a lost handshake, or a relay withholding the
optional kind 39002) versus member lists that name no channel of ours (a wait for
a join). Collapsing them sends the operator to fix the wrong thing — the log line
used to claim "add it to a channel" when the relay had nothing to add it to.
`buzzDiscover` returns the roster count for exactly that reason. A discovery
query is the one call in the gateway whose failure mode is silence, so anything
that swallows its result makes the agent deaf with nothing in the log.

## Security invariants — break these and it is a vulnerability, not a style nit

- **Identity comes from the transport, never the envelope.** `hub.Handle`'s
  `peer` argument is the authenticated Nostr pubkey and is `""` for HTTP. The
  `sender` field in an envelope is ignored for both authorization and reply
  addressing. Do not "helpfully" trust it.
- **`allow` is a Nostr-only gate, checked before decryption** (cheaper, and it
  keeps blocked senders away from the cipher). HTTP callers have no Nostr
  identity; their gate is the `GATEWAY_TOKEN` bearer. An empty `allow` accepts
  anyone, so the gateway logs a warning when `GATEWAY_TOKEN` is unset.
- **`allow` is enforced gateway-side and must never go into a subscription
  filter.** Both `nostr.listen` and `buzzListen` used to set `Authors: a.Allow`,
  which is the same deafness one layer out: a relay applies `authors` before
  delivering, so a blocked sender — or a pubkey that is not in the list because
  of a typo — is dropped by the relay and never reaches the gateway's own
  `allows()` check. Every log line explaining that refusal was therefore
  unreachable, and the agent was deaf on both transports with a clean log. The
  gateway-side check is the real boundary, so keep the filter wide and take the
  fan-out; a relay-side filter is only safe paired with a periodic self-REQ to
  prove delivery. Note that the fake relay ignores filters entirely — the REQ
  handler replays every stored event to every subscriber — so it can only catch a
  filter that was never sent, never one the relay would honour.
- **The rule is: a filter may address, never adjudicate. `authors` is out; the two
  tags the relays themselves route on stay.** A relay applies `authors` and every
  `#tag` before it delivers, so anything the gateway *decides* with in a filter is
  a refusal it can never log. Two tags are the exception and are not an
  optimisation, because the relay delivers on them and omitting one does not widen
  delivery, it ends it:
  - `#p` on the kind-30078 listener, enforced twice (the REQ and go-nostr's
    client-side `Filter.Matches`, which does run — verified, do not re-derive this
    by grepping for `Matches` outside `filter.go`).
  - `#h` on the kind-9 listener, listing every channel `buzzDiscover` found. Buzz's
    relay hands a channel message only to the subscriptions that **name** that
    channel, so a REQ with kinds alone receives none of them. `buzzListen` used to
    send `h` and it was removed as "a guess at Buzz's tag convention that
    buzzDiscover contradicts" — and that removal made an agent deaf in every
    channel it was a member of, with the cleanest log in the system: the relay
    reported the message ingested, the gateway logged nothing, and no amount of
    debugging the *processing* could have found it because nothing was processed.
    `h` is what NIP-29 puts on a chat message and what `buzzPost` publishes, so it
    is the name the relay routes on. Asking for `h` costs nothing on a relay that
    used `d`: such a relay could not route those messages to a channel-scoped
    subscription either. `buzzChannelTag` still accepts `h` or `d` and
    `buzzReceive` still does the membership check.
  `TestGatewayNeverAsksTheRelayToFilter` and `TestNostrRoutesByPTag` are the two
  guards, and between them they cover both halves — the first also asserts the
  kind-9 REQ names the discovered channel, so the deafness cannot come back as
  an empty list.
- **Keys are normalised to hex exactly once**, in `loadRegistry`. A bech32 pubkey
  reaching go-nostr produces a `p` tag filter that never matches a real relay —
  the agent goes silently deaf, with no error anywhere. `TestNostrRoutesByPTag`
  is the guard; it fails if this ever regresses.
- `loadRegistry` also refuses an `npub` that is not the pubkey of its
  `nsec_env`, naming both keys in the error. Same silence one step further
  out: the relay authenticates the connection as the nsec's key while every
  p-tag filter asks about the npub, so Buzz discovery returns no member lists
  and the log blames the relay. `TestLoadRegistry` is the guard.
- `sessionMap.lookup` intentionally does **not** set the reply peer. Setting it
  there let a request for another agent's conversation re-point that
  conversation's reply target before being rejected. Set the peer in `Handle`,
  after the agent-ownership check.
- Secrets (`nsec_env`) live in env/Kubernetes Secrets and must never be logged.
  Only agent names and short pubkeys are logged.

## Concurrency invariants

- One goroutine per agent consumes its OpenCode event stream, and that goroutine
  is the only writer of the conversation's `seen` dedupe map.
- `ensureSession` holds the conversation lock across session creation on purpose:
  two simultaneous first messages must produce one session, and the second
  goroutine's events must not land in an unbound conversation. Do not hoist the
  create call out of the lock; `TestConcurrentFirstMessageCreatesOneSession`
  covers it (4 sessions vs 1).
- **The lock order is one-way: `c.mu` may be held while `m.mu` is taken, never the
  reverse.** `ensureSession`'s closure calls `m.index` (`hub.go:248`) while it holds
  `c.mu`, so the nesting `c.mu → m.mu` exists and is deliberate. Nothing takes
  `m.mu` and then `c.mu`: `lookup`, `get`, `getHistory` and `byOpenCodeSession` all
  return the `*conversation` and drop `m.mu` (`sessions.go:104-122`) before any
  conversation method runs on it. A future change that holds `m.mu` across a
  `c.*` call deadlocks; one that only ever nests `c.mu → m.mu` does not.
- A conversation has one reply channel: the last inbound Nostr sender. Two
  allow-listed users on one conversation id is not a group chat, and the docs say
  so. Fixing that needs a reply-routing decision, not a patch.
- `subscribe` registers the subscriber and snapshots its history **under one
  `h.mu` hold**. They were two separate steps, and `emit` records into the
  conversation and then pushes under `h.mu`, so an event landing in that gap was
  dropped for that subscriber with no error anywhere. Do not split them back out
  to "avoid holding the lock longer".
- `buzzJob` runs on the single publish `worker` goroutine that serves every
  agent, so a panic there is a process crash, not a failed message. Every
  `n.turns[...]` read in it must tolerate the entry being absent: a turn reaches
  `completed` with nothing accumulated whenever it errored before its first delta
  or a `permission_request` posted on its own and took the turn with it, and
  `*strings.Builder.String()` dereferences its own fields. `TestBuzzJobCompletesATurnThatNeverAccumulated`
  is the guard.
- Every relay call on a listener's own goroutine carries `callTimeout`. A
  `context.Background()` publish to a relay that never answers stalls that
  agent's entire inbound stream — the listener is the only reader.

## OpenCode compatibility

Two API generations are live in the wild and both are handled on purpose:

- `permission.updated` + `permissionID` + `response` →
  `POST /session/:id/permissions/:id`
- `permission.asked` + `requestID` + `reply` →
  `POST /session/:id/permission/:id/reply`

`replyPermission` tries the legacy endpoint and falls back on 404/405 — matched by
string search in `isMissingRoute`, not by a typed status error, so a non-404
failure whose text happens to contain "404" also falls back. Removing either
generation breaks a real OpenCode version. `session.status: idle` is the
completion signal on newer builds; older ones send `session.idle`.

Other numbers that are deliberate, not arbitrary: `maxEventBytes` 32 MB per SSE
line (an oversized line is dropped, the stream survives), `callTimeout` 30 s on
every non-streaming call (the event stream is deliberately unbounded),
`historyDepth` 64 replayed events per conversation, Nostr publish queue 5 s
backpressure.

## Conventions

- Reject with `invalid(...)` from `hub.go` for anything the caller got wrong;
  `http.go` maps that to 400 and everything else to 502. Returning a bare error
  silently changes the status code. A transport that wants a different status
  code checks `errors.Is(err, errInvalid)` itself.
- `untilTurnEnd` is how a synchronous request turns an asynchronous turn into a
  response, and the ack id is the cursor: `subscribe` replays the conversation's
  history, so without cutting at the ack a continued conversation would answer
  with the previous turn. Do not replace that with a timestamp comparison.
- In-memory only. No database, no eviction, no persistence — a restart drops
  conversation→session state. Keep it that way unless asked.
- Config is static JSON (`AGENTS_FILE` or inline `AGENTS`), not a CRD watcher.
  The `ponytail:` comments in the source mark the deliberate shortcuts and name
  the upgrade path; read them before "improving" one.
- Tests use no framework: `httptest` fake OpenCode (`newFakeOC`) and an
  in-process fake Nostr relay (`newFakeRelay`). Event assertions go through
  `next`/`want` on the subscription channel; async conditions use `waitFor`.
  The fake relay deliberately ignores subscription filters, so filter behaviour
  has to be tested by what the gateway does or does not process.
- The fake relay is intentionally not closed at test cleanup; the comment there
  explains why. Leave it.

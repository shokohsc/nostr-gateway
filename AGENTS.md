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

`go test` and `go test -race` have different file sets: `nostr_test.go` carries
`//go:build !race`, so `-race` skips it. Reason: go-nostr v0.52.3 has a data race
in its own connect path (`relay.go:175` vs `relay.go:576`) that fires as soon as
the pool subscribes, and it is the newest published version. If you ever see that
race, it is upstream — do not try to "fix" it in gateway code. `go test ./...`
covers the Nostr path.

The same version builds *identical* NIP-42 auth events when two subscriptions
answer a challenge in the same second, and it keys the `OK` waiters by event id,
so one of the two waits out the 7s `Relay.publish` timeout and resubscribes. Two
subscriptions sharing one closed relay therefore start a few seconds apart, which
is why the Buzz test gives `BUZZ_RELAYS` its own fake relay instead of pointing
both relay lists at the same one.

## Wiring

Everything crosses at exactly two functions: `hub.Handle` (inbound, all
transports) and `hub.emit` (outbound, all transports). `opencode.go` is the only
file allowed to speak the OpenCode HTTP API; `reduce.go` is the only place that
translates OpenCode events into protocol events. Keep it that way — the point of
the gateway is that no transport or agent can see OpenCode's wire format or its
`:4096` address.

Data flow: transport → `hub.Handle` → `conversation.ensureSession` (creates the
OpenCode session once, atomically) → `opencodeClient.promptAsync` → SSE from
`opencodeClient.events` → `hub.onEvent` → `reduceEvent` → `hub.emit` → subscribers
(SSE) + the Nostr publish queue.

`buzz.go` is a transport like `nostr.go`, and it is subscribe-only on purpose.
Buzz relays come from `BUZZ_RELAYS` and are never in `NOSTR_RELAYS`: nothing is
published to them, because a kind-30078 envelope stored on a Buzz relay surfaces
in Buzz's own read-state view, and posting kind-9 answers back into a channel is
not built yet. Inbound is unchanged — a mention becomes a prompt through
`hub.Handle`, so `allow`, the lock order and one-session-per-conversation all
still hold.

## Security invariants — break these and it is a vulnerability, not a style nit

- **Identity comes from the transport, never the envelope.** `hub.Handle`'s
  `peer` argument is the authenticated Nostr pubkey and is `""` for HTTP. The
  `sender` field in an envelope is ignored for both authorization and reply
  addressing. Do not "helpfully" trust it.
- **`allow` is a Nostr-only gate, checked before decryption** (cheaper, and it
  keeps blocked senders away from the cipher). HTTP callers have no Nostr
  identity; their gate is the `GATEWAY_TOKEN` bearer. An empty `allow` accepts
  anyone, so the gateway logs a warning when `GATEWAY_TOKEN` is unset.
- **Keys are normalised to hex exactly once**, in `loadRegistry`. A bech32 pubkey
  reaching go-nostr produces a `p` tag filter that never matches a real relay —
  the agent goes silently deaf, with no error anywhere. `TestNostrRoutesByPTag`
  is the guard; it fails if this ever regresses.
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
- `sessionMap.index` must stay outside the conversation lock — taking both, in
  the other order, deadlocks.
- A conversation has one reply channel: the last inbound Nostr sender. Two
  allow-listed users on one conversation id is not a group chat, and the docs say
  so. Fixing that needs a reply-routing decision, not a patch.

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
  silently changes the status code.
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

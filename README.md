# nostr-gateway

Talks to [KubeOpenCode](https://opencode.ai/docs/server) agents over Nostr, and to
anyone else over plain HTTP. One process, one small JSON protocol, every transport
swappable — Mattermost or a webhook only has to call `hub.Handle` to join in.

Implements plan.md.

## What it does

- **Nostr in, Nostr out.** Kind `30078`, NIP-44 encrypted between the user's key
  and the agent's key. Relays see ciphertext, tags and timestamps, nothing else.
  NIP-42 `AUTH` is answered with the agent's own key, so closed relays work.
- **Async conversations.** A message gets an OpenCode session, an `ack`, and
  later events: `message`, `thinking`, `tool_started`, `tool_finished`,
  `permission_request`, `progress`, `completed`, `error`. Nothing waits on a
  30-minute HTTP request.
- **OpenCode's event stream, reduced.** The gateway does not forward
  `message.part.updated` and friends. It collapses them onto the protocol above,
  because that surface changes and a chat client should not have to track it.
- **Approvals.** `permission_request` goes out over the conversation, the reply
  (`once` / `always` / `reject`) comes back in and is delivered to OpenCode.
  Both OpenCode generations are handled: `permission.updated`/`permissionID`/
  `response` and `permission.asked`/`requestID`/`reply`, and the reply endpoint
  falls back from `POST /session/:id/permissions/:id` to
  `POST /session/:id/permission/:id/reply` when the first is a 404.
- **Plain HTTP API** for curl, tests and non-Nostr callers: `POST /v1/messages`
  and `GET /v1/conversations/{id}/events` (SSE).

## Protocol

```json
{
  "v": 1,
  "id": "evt_01J...",
  "conversation": "conv_01J...",
  "agent": "frontend-agent",
  "sender": "npub1...",
  "type": "message",
  "timestamp": "2026-09-28T16:30:00Z",
  "payload": { "text": "..." }
}
```

Inbound types: `message` (`prompt` is accepted too) and `permission_response`
(`payload.permission_id` + `payload.text`). `conversation` may be omitted — the
gateway allocates one on the first message and returns it in the `ack`.

Send a message over HTTP:

```bash
curl -sS -X POST localhost:8080/v1/messages \
  -H "Authorization: Bearer $GATEWAY_TOKEN" \
  -d '{"agent":"frontend-agent","type":"message","payload":{"text":"why is the build red?"}}'
# {"v":1,"id":"evt_...","conversation":"conv_...","agent":"frontend-agent","type":"ack",...}

# The last 64 events of a conversation are replayed to a new subscriber, so
# opening the stream after the first message does not lose the beginning.
curl -N localhost:8080/v1/conversations/conv_.../events
# event: tool_started
# data: {"v":1,"id":"evt_...","conversation":"conv_...","type":"tool_started","payload":{"tool":"bash",...}}
```

Over Nostr, publish the same JSON as the content of a kind `30078` event with a
`p` tag for the agent's `npub` and the agent's `nsec` as the signing key:

```jsonc
["EVENT", {
  "kind": 30078,
  "created_at": 1790000000,
  "tags": [["p", "<agent npub hex>"], ["d", "<envelope id>"]],
  "content": "<nip44 ciphertext of the envelope>",
  ...
}]
```

The `d` tag is what keeps history. `30078` is inside the NIP-33
parameterized-replaceable range, so a relay keys those events by
`(kind, author, d)` and keeps only the newest one for each key — with no `d` tag
that is every message the author has ever sent, and a Buzz relay will in fact
treat the kind as its own `KIND_READ_STATE` and store it as such. The gateway
stamps `d` with the envelope id on every event it publishes; clients should do
the same.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `AGENTS_FILE` | — | Path to the agent registry (required) |
| `AGENTS` | — | Same JSON inline, used when `AGENTS_FILE` is unset |
| `NOSTR_RELAYS` | `wss://nos.lol,wss://relay.damus.io` | Comma-separated relay URLs |
| `GATEWAY_ADDR` | `:8080` | HTTP listen address |
| `GATEWAY_TOKEN` | unset | Bearer token for the HTTP API; unset disables auth |
| `OPENCODE_USER` / `OPENCODE_PASSWORD` | unset | Basic auth for OpenCode servers that require it |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

### Relays that require NIP-42

No configuration: each agent answers a relay's `AUTH` challenge with its own
`nsec` automatically, and gives up on the connection when that fails. Closed
relays are stricter than that, though — Buzz (and anything else with
`BUZZ_REQUIRE_RELAY_MEMBERSHIP`) also requires the authenticating pubkey to be a
member, so the agent's `npub` has to be enrolled on the relay or its
subscription is closed with `auth-required: verification failed`.

Each agent holds its own pool and therefore its own connection, because a
closed relay accepts only events signed by the key that authenticated that
connection.

```json
{
  "frontend-agent": {
    "opencode": "http://frontend-agent.kubeopencode.svc.cluster.local:4096",
    "npub": "npub1...",
    "nsec_env": "FRONTEND_NSEC",
    "allow": ["<hex pubkey>"],
    "model": "anthropic/claude-sonnet-4-5",
    "opencode_agent": "build"
  }
}
```

`nsec_env` names the variable holding the private key, so keys stay in a
Kubernetes Secret while this config stays in a ConfigMap. Keys are written as
`npub1…`/`nsec1…` or hex; the registry normalises them to hex, and a malformed
key is a startup error rather than an agent that silently hears nothing.

`allow` is a **Nostr** gate. On the Nostr path identity is the pubkey on the
event signature — never the envelope's `sender` field, which is ignored — so a
blocked sender is dropped before any decryption happens. HTTP callers carry no
Nostr identity at all: `GATEWAY_TOKEN` is their gate, and an HTTP envelope's
`sender` can never make the gateway encrypt that agent's replies to a Nostr key.
An empty `allow` list accepts anyone.

```bash
go build -o nostr-gateway .

export AGENTS='{"frontend-agent":{"opencode":"http://localhost:4096","npub":"<npub-or-hex>","nsec_env":"FRONTEND_NSEC","allow":[],"model":"anthropic/claude-sonnet-4-5","opencode_agent":"build"}}'
export FRONTEND_NSEC='<nsec-or-hex>'
export GATEWAY_TOKEN=$(openssl rand -hex 32)
./nostr-gateway
```

`AGENTS_FILE` points at the same JSON in a file, which is what the ConfigMap
below does. `deploy/k8s.yaml` has the whole set: Secret, ConfigMap, KubeOpenCode
`Agent`, cluster-internal Service for the agent, and a central gateway Deployment.

## Design

`main` wires four things, and they only meet in one place — `hub.Handle` (inbound)
and `hub.emit` (outbound):

| File | Job |
| --- | --- |
| `protocol.go` | Envelope, payload, event types |
| `opencode.go` | The only code that speaks OpenCode's HTTP API |
| `reduce.go` | OpenCode events → protocol events |
| `hub.go` | Routing, allow list, fan-out to subscribers and transports |
| `sessions.go` | conversation ↔ OpenCode session mapping |
| `nostr.go` | Nostr transport: subscribe, decrypt, publish |
| `http.go` | `POST /v1/messages`, SSE stream, bearer auth |
| `registry.go` | Agent config, keys, per-peer NIP-44 key cache |

OpenCode's `:4096` is never exposed. The gateway calls it cluster-internal and
republishes under its own protocol.

## Known limits

- Conversations live in memory. A restart drops the conversation→session map and
  dedupe state; agents start fresh conversations. Persist the map if that matters.
- The Nostr subscription only looks 5 seconds back, so a gateway restart misses
  messages sent while it was down.
- `go test -race` skips `nostr_test.go`: go-nostr v0.52.3 has a data race in its
  own connect path (`Relay.ConnectWithTLS` writing `r.Connection` while
  `Relay.close` reads it, `relay.go:175` vs `relay.go:576`) that fires as soon as
  the pool subscribes, and it is the latest published version. The rest of the
  suite is race-clean; the Nostr tests run in the normal suite.
- Tool output is sent whole (capped at 32 MB per event) and not truncated.
- `model` and `opencode_agent` are global per agent, not per conversation.
- A conversation has one reply channel: replies go to whoever spoke last. Two
  allow-listed users sharing a conversation is not a group chat.

## Tests

```bash
go test ./...
```

Covers the prompt flow end to end over HTTP, the permission approval loop on
both OpenCode API generations, session reuse (including four simultaneous first
messages creating exactly one session), cross-agent conversation rejection, the
reducer's delta/dedupe rules, replay for late SSE subscribers, protocol version
and body-size rejection, an oversized event surviving the stream, the allow list
on the Nostr path, HTTP being unable to redirect Nostr replies, registry
loading and key normalisation from env, and a full Nostr round trip (NIP-44,
kind, `p` tag routing, encrypted reply) against an in-process fake relay,
including a relay that demands NIP-42 and authenticates each agent separately.

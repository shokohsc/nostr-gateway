# nostr-gateway

Talks to [KubeOpenCode](https://opencode.ai/docs/server) agents over Nostr, and to
anyone else over plain HTTP. One process, one small JSON protocol, every transport
swappable — a new transport only has to call `hub.Handle` and `hub.emit` to join in,
and both are the sole place any transport meets the others.

`plan.md` is the original spec this was built from. Where the two disagree, the code
and this file win.

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
- **OpenAI-compatible API** for anything that already speaks it — a chat client,
  a bot framework, an agent tool: `GET /v1/models` and
  `POST /v1/chat/completions` (streaming and not). See below.

## Chat clients: the OpenAI-compatible API

`/v1/chat/completions` is the same agent, the same conversation and the same
session as everything else here — only the wire format is the one every chat
client and every agent framework already speaks, so nothing has to be written to
drive it. Set `model` to the agent (`GET /v1/models` lists them, prefixed with
`opencode-nostr-gateway/`) and `user` to whoever is talking, which is what scopes
the conversation, since OpenAI has no id for one. A request with no `user` gets
its own conversation, which is what a stateless client should get.

```bash
curl -sS localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_TOKEN" \
  -d '{"model":"opencode-nostr-gateway/frontend-agent","user":"alice",
       "messages":[{"role":"user","content":"why is the build red?"}]}'
# {"object":"chat.completion","model":"opencode-nostr-gateway/frontend-agent",
#  "choices":[{"message":{"role":"assistant","content":"…"}}],
#  "conversation":"oai-alice", ...}

curl -N -sS localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_TOKEN" \
  -d '{"model":"…","stream":true,"messages":[…]}'
# data: {"object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"}}], …}
# data: {"object":"chat.completion.chunk","choices":[{"delta":{"content":"the "}}], …}
# data: [DONE]
```

`"stream": true` returns `text/event-stream` with `chat.completion.chunk`
objects and a `data: [DONE]` terminator. `user` is echoed back as
`conversation`, so a client that wants the full protocol — every event, tool
calls included — can follow the same conversation on
`GET /v1/conversations/{id}/events`.

The mapping is deliberately lossy in one direction: OpenAI has no way to express
a tool call, a progress event or a permission request, so the response carries
the answer text and nothing else. Anything more than the answer comes back over
the protocol stream above. That is also why this surface is not how a permission
request is answered — a client here cannot approve one, and the request arrives
as an error rather than a hang.

## Protocol

The gateway's own envelope protocol. Use it when you want every event; use
`/v1/chat/completions` above when you want a chat client.

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
| `BUZZ_RELAYS` | unset | Comma-separated Buzz relay URLs. Set it and each agent joins the Buzz channels it is a member of, answers mentions in the channel, and publishes its agent profile. A relay may be named in both this and `NOSTR_RELAYS`: Buzz gets its own connection either way |
| `GATEWAY_ADDR` | `:8080` | HTTP listen address |
| `GATEWAY_TOKEN` | unset | Bearer token for the HTTP API. **Unset means the HTTP API is open to anything that can reach it**, so the gateway logs a warning at startup. An empty `allow` list has the same consequence on the Nostr path |
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
connection. A relay in `BUZZ_RELAYS` gets a second, separate pool per agent, so
the Buzz handshake never collides with the kind-30078 one on a shared
connection — see `AGENTS.md` for what that collision costs.

### Buzz channels

[Buzz](https://github.com/block/buzz) is a Nostr workspace that speaks NIP-29, so
its chats are not kind `30078` envelopes: a message is a kind `9` event tagged
`#h <channel-uuid>`, an @mention is that message with a `p` tag for the agent's
`npub`, and a DM is just a channel the agent shares with one other person. Set
`BUZZ_RELAYS` and the gateway joins in:

```bash
export BUZZ_RELAYS='ws://buzz.example.internal:3000'
```

- A relay only hands channel-scoped events to a subscription that names the
  channel, so the agent's channels are discovered first, from the NIP-29 member
  lists (kind `39002`) the agent's own pubkey appears in. Membership is re-read
  every minute, so a channel the agent was added to later needs no restart.
- That discovered list **is** the `#h` filter on the kind-9 subscription, because
  omitting it does not widen what the agent hears, it ends it: the relay delivers
  a channel message only to subscriptions naming that channel, so a REQ with kinds
  and nothing else receives none of them, with a clean log on both sides — the
  relay reports the message ingested and the gateway has nothing to say about an
  event that never arrived. `h` is the name NIP-29 gives a channel on a chat
  message and the name the agent's own answers carry, so it is the one the relay
  routes on. The gateway still reads the channel from `h` or `d` and checks
  membership itself, so nothing about *whether to answer* moves into the filter.
- Nothing the gateway decides goes into a filter, `authors` included: a blocked
  sender dropped by the relay never reaches the `allow` check that would have
  named it. That decision, and the mention check, are logged where they happen.
- A discovery has three outcomes, and they are logged apart because their fixes
  are opposites. No member lists at all is re-asked a few seconds later, since
  that is what a lost NIP-42 handshake looks like (see `AGENTS.md`) rather than
  an answer. Still none after the retry means the relay is not serving kind
  `39002` to this pubkey — NIP-29 calls it optional and lets a relay restrict
  who may fetch it — and is reported as `buzz: relay serves no member lists for
  this agent`. Member lists that name no channel is a real answer and is
  reported as `buzz: agent is in no channel yet`: add the agent to a channel,
  then reconcile the rosters.
- A message is a prompt when it carries a `p` tag for the agent, or when it
  arrives in a two-member channel. Everything else in a group channel is
  ignored, and the `allow` list still applies.
- The channel uuid is the conversation id (`buzz-<channel-uuid>`), so one channel is one
  OpenCode session. The same rules work over the HTTP API:
  `POST /v1/messages` with `"conversation": "buzz-<channel-uuid>"` and
  `GET /v1/conversations/buzz-<channel-uuid>/events`.
- The agent's `npub` has to be a member of the Buzz relay
  (`buzz-admin add-member`) or the subscription is closed, same as above.

**What is published to the Buzz relays**: the agent's answers, and one other kind
of event. An answer to a channel message is a kind `9` in that same channel,
signed by the agent's own key, with an `h` tag for the channel and a `p` tag for
the person who asked — a chat client renders nothing else there. A channel message
is one message per turn however many deltas the turn streamed, so the answer is
assembled and posted when the turn ends. Nothing else is: the tool calls, the
reasoning and the acks stay off the channel, and a conversation that is not a
Buzz channel still answers with an encrypted kind-`30078` envelope on
`NOSTR_RELAYS`.

The second kind is the NIP-OA agent profile (kind `10100`), which is what Buzz
reads to know which pubkeys are agents, and
which has to be signed by the agent's own key — something no operator can do by
hand, because the `nsec` belongs to the gateway. It is replaceable, so it is
published once, as soon as the relay has served a member list — that is what
proves the connection passed NIP-42. It does not wait for the agent to be in a
channel: Buzz's channel UI is how an agent gets added to one, so an agent
waiting to be a member to be registered is an agent that can never join.


**Inbound otherwise.** A permission
request raised by a mention is posted into the channel so the reader can see it,
but it cannot be answered there: a reply in a channel is a prompt, not a
`permission_response`, so approving still needs `POST /v1/messages` with
`"type": "permission_response"`, or a client that speaks the envelope protocol.

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

`npub` and `nsec_env` are one identity, and the registry checks it: a well-formed
npub that is not the pubkey of that private key is a startup error naming both
keys, because the relay authenticates the connection as the nsec's key while
every p-tag filter asks about the npub. That mismatch is otherwise silent — no
member lists, no kind-`30078` answers, and nothing in the log but the relay
being blamed for it.

`model` is written `"provider/model"` and split in two on the way out:
`prompt_async` takes `{"providerID":…,"modelID":…}`, so a model with no slash in
it is a startup error rather than a `400` on every message.

`allow` is a **Nostr** gate. On the Nostr path identity is the pubkey on the
event signature — never the envelope's `sender` field, which is ignored — so a
blocked sender is dropped before any decryption happens. HTTP callers carry no
Nostr identity at all: `GATEWAY_TOKEN` is their gate, and an HTTP envelope's
`sender` can never make the gateway encrypt that agent's replies to a Nostr key.
An empty `allow` list accepts anyone.

The list is enforced by the gateway, **not** handed to the relay as a
subscription filter. A relay drops a non-matching event before delivering it, so
a relay-side `authors` filter would make a blocked sender — or a typo'd pubkey —
look identical to a broken relay: the agent goes deaf and nothing is logged. The
same is true of every other tag filter, which is why the only one the gateway
sends is the `p` that *addresses* an envelope to an agent. The gateway applies
`allow` itself, which is both the real security boundary and the place where the
refusal can be logged with the pubkey that caused it. The cost is fan-out: the
relay streams every event in the subscribed kinds to the gateway and the gateway
throws the blocked ones away.

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
| `buzz.go` | Buzz channels: discover, subscribe, mention to prompt, prompt to channel message |
| `http.go` | `POST /v1/messages`, SSE stream, bearer auth, `/v1/chat/completions` |
| `registry.go` | Agent config, keys, per-peer NIP-44 key cache |

OpenCode's `:4096` is never exposed. The gateway calls it cluster-internal and
republishes under its own protocol.

## Known limits

- Conversations live in memory. A restart drops the conversation→session map and
  dedupe state; agents start fresh conversations. Persist the map if that matters.
- Conversation ids are one namespace across every transport, and
  `/v1/chat/completions` derives one from the client-chosen `user` field, so an
  allow-listed Nostr peer can send `"conversation":"oai-alice"` and join an HTTP
  caller's conversation — and the gateway will then address its replies to that
  peer. The cause is the single shared `GATEWAY_TOKEN`: every HTTP caller is one
  trust domain, so there is no per-caller identity to namespace with. Per-caller
  tokens are the fix; until then, do not run the HTTP API and a shared Nostr
  relay for agents that must not see each other's traffic.
- Nothing evicts. Every invented `conversation` id costs a permanent map entry
  *and* a real OpenCode session on the agent's backend, so an unauthenticated
  caller (or a `GATEWAY_TOKEN`-holder inventing ids) can spend agent quota with
  one request per id. There is no rate limit and no cap either.
- The Nostr subscription only looks 5 seconds back, so a gateway restart misses
  messages sent while it was down. Same for a Buzz channel subscription.
- `allow` is not a relay-side filter, and neither is a channel or kind
  narrowing, so the relay streams every event in the subscribed kinds to the
  gateway and the gateway discards the ones it does not want. A busy channel with
  a short allow list pays that fan-out. See the `allow` section above for why it is
  not pushed down to the relay.
- Buzz carries the answer back as a kind `9` in the channel, but only the answer:
  tool calls, reasoning and progress are not posted. A permission request raised
  by a mention is posted and cannot be answered from the channel.
- Mentions sent as Buzz's rich-content kind (`40002`) rather than kind `9` are not
  seen. Neither is a message that arrives between one membership rediscovery and
  the next: a channel the agent has just joined only starts delivering on the
  subscription that named it.
- A kind-9 envelope has to carry the channel in an `h` or `d` tag, and the
  gateway only accepts it if the channel is one the agent is currently a member
  of. A message that names the channel any other way, or a channel the agent has
  not been added to yet, is refused — with the reason in the log, which is the
  one thing a relay-side filter could never have told you.
- A channel the agent is added to is picked up within a minute; a channel it is
  removed from keeps its conversation until the next rediscovery. A discovery
  query itself is capped at 15 seconds, so an unresponsive Buzz relay delays the
  agent rather than deafening it permanently.
- A Buzz relay that serves no kind `39002` to the agent leaves the agent deaf
  with a working connection — NIP-29 makes the member list optional. The gateway
  says which of the two cases it is in the log; it cannot work around a relay
  that withholds the roster.
- `go test -race` skips `nostr_test.go`: go-nostr v0.52.3 has a data race in its
  own connect path (`Relay.ConnectWithTLS` writing `r.Connection` while
  `Relay.close` reads it, `relay.go:175` vs `relay.go:576`) that fires as soon as
  the pool subscribes, and it is the latest published version. The rest of the
  suite is race-clean; the Nostr tests run in the normal suite.
- Tool output is sent whole (capped at 32 MB per event) and not truncated. The cap
  bounds what the stream does, not what it allocates: the oversized line is read
  before it is measured. OpenCode is a cluster-internal peer, so this is a note
  rather than a boundary you can hit from outside.
- `model` and `opencode_agent` are global per agent, not per conversation, and
  not per request: an OpenAI `model` picks the agent, never the model behind it.
- The OpenAI surface cannot express a tool call, a progress event or a permission
  request, so it carries the answer text only. `usage` is always zero — nothing
  counts tokens here.
- Two clients with the same `user` share one conversation, and therefore one
  OpenCode session and one reply channel.
- A conversation has one reply channel: replies go to whoever spoke last. Two
  allow-listed users sharing a conversation is not a group chat.

## Tests

```bash
go test ./...
```

Covers the prompt flow end to end over HTTP, the permission approval loop on
both OpenCode API generations, session reuse (including four simultaneous first
messages creating exactly one session), cross-agent conversation rejection, the
reducer's delta/dedupe rules, replay for late SSE subscribers, the OpenAI surface
(`/v1/models`, a non-streaming completion, a streamed one ending in `[DONE]`, an
unknown model rejected, and a continued conversation not carrying the previous
turn's answer), protocol version
and body-size rejection, an oversized event surviving the stream, the allow list
on the Nostr path and the absence of *any* relay-side filter that could swallow
an event before the gateway could log it (the `p` address tag excepted, and a
kind-9 message tagged with `d` reaching OpenCode), HTTP being unable to redirect
Nostr replies, registry
loading and key normalisation from env, a Buzz mention and a Buzz DM reaching
OpenCode (and group chatter not reaching it) with one session per channel, a
streamed answer assembled into one kind-9 channel message with nothing published
to `NOSTR_RELAYS`, the agent not answering its own channel replies, a Buzz
discovery recovering from a refused NIP-42 handshake, a Buzz discovery on a relay
that is also a `NOSTR_RELAYS` entry, the agent profile published before the agent
is in any channel, a Buzz turn that reaches `completed` with nothing accumulated
without taking the process down, and a full Nostr round trip
(NIP-44, kind, `p` tag routing, encrypted reply) against an in-process fake relay,
including a relay that demands NIP-42 and authenticates each agent separately.

Yes. The clean architecture is to treat **Nostr as the external messaging/event transport**, and keep **OpenCode completely HTTP-based**.

OpenCode already exposes a headless HTTP API and an SSE event stream, so there is no reason to put `stdio` between the agent and your bridge. ([OpenCode][1])

### Recommended architecture

```text
                    Nostr network
                         │
                  WebSocket / NIP-01
                         │
                         ▼
              ┌─────────────────────┐
              │   Nostr Buzz Relay  │
              │  / Nostr transport  │
              └──────────┬──────────┘
                         │
                         │ WS
                         ▼
              ┌─────────────────────┐
              │   opencode-bridge   │
              │                     │
              │ Nostr ↔ HTTP/SSE    │
              └────────┬─────┬──────┘
                       │     │
                 HTTP  │     │ SSE
                       │     │
                       ▼     ▼
              ┌─────────────────────┐
              │   OpenCode server   │
              │     :4096           │
              │                     │
              │ sessions / prompts  │
              │ events / tools      │
              └─────────────────────┘
                       │
                       ▼
              KubeOpenCode Agent Pod
```

The important part is that **the bridge is a separate container/service**.

KubeOpenCode's `AgentTemplate` already lets you define the OpenCode command, ports, plugins, credentials, Pod configuration, etc., and an `Agent` can inherit that template. ([Plaud][2])

---

# 1. Don't make the relay "talk OpenCode"

I'd define a small protocol between the bridge and OpenCode:

```text
Nostr event
     │
     ▼
bridge
     │
     ├── POST /session
     ├── POST /session/:id/prompt
     └── GET  /global/event
             │
             ▼
         OpenCode
```

OpenCode's server already provides an HTTP API and SSE event stream. ([OpenCode][1])

The bridge therefore has two responsibilities:

### Inbound

```text
Nostr EVENT
    ↓
validate
    ↓
decrypt
    ↓
resolve agent
    ↓
OpenCode HTTP API
```

For example:

```json
{
  "kind": 30078,
  "agent": "frontend-agent",
  "session": "abc123",
  "type": "prompt",
  "text": "Fix the failing Vue tests"
}
```

The bridge turns that into an OpenCode API call.

### Outbound

```text
OpenCode SSE event
       ↓
bridge
       ↓
Nostr EVENT
```

For example:

```json
{
  "agent": "frontend-agent",
  "session": "abc123",
  "type": "assistant",
  "text": "I fixed the failing tests."
}
```

---

# 2. Nostr is a good transport, but I'd separate "Buzz" from the protocol

Nostr relays fundamentally expose WebSockets and clients send `EVENT`, `REQ`, and `CLOSE` messages. ([NIPs][3])

So I'd make your bridge speak:

```text
Nostr
  │
  │ WebSocket
  ▼
bridge
  │
  │ HTTP/HTTPS
  ▼
OpenCode
```

rather than:

```text
OpenCode
   │
   │ Nostr
   ▼
Relay
```

This gives you an important property:

> **You can replace Nostr later without changing the OpenCode integration.**

Your bridge could eventually support:

```text
Nostr
Mattermost
Matrix
Slack
Webhooks
HTTP API
       │
       ▼
   Agent Bridge
       │
       ▼
    OpenCode
```

That fits particularly well with the messaging/agent architecture you've been considering.

---

# 3. I'd make the bridge a sidecar

For KubeOpenCode, I would initially deploy:

```text
Pod
┌──────────────────────────────────────────────┐
│                                              │
│  opencode-agent                              │
│  ┌────────────────────────────────────────┐  │
│  │ opencode serve :4096                   │  │
│  └────────────────────────────────────────┘  │
│                    ▲                         │
│                    │ localhost HTTP          │
│                    │                         │
│  ┌────────────────────────────────────────┐  │
│  │ opencode-nostr-bridge                  │  │
│  │ :8080                                  │  │
│  │                                        │  │
│  │ Nostr WS ────────────► relay           │  │
│  │ HTTP ────────────────► OpenCode        │  │
│  │ SSE ◄──────────────── OpenCode        │  │
│  └────────────────────────────────────────┘  │
│                                              │
└──────────────────────────────────────────────┘
```

This is particularly attractive because every Agent gets its own bridge identity.

For example:

```text
Agent A
  npub: npub1agentA...

Agent B
  npub: npub1agentB...

Agent C
  npub: npub1agentC...
```

The bridge owns the corresponding private key.

---

# 4. Agent identity should be a Nostr key

I'd make the Nostr key the **identity of the OpenCode agent**.

Something like:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: frontend-agent-nostr
stringData:
  nsec: "..."
```

Then:

```text
Agent
  │
  ├── OpenCode identity
  │
  └── Nostr identity
         │
         └── npub1...
```

Don't put the private key into the `Agent` CRD itself.

Instead, use KubeOpenCode credentials / Secret references. Its CRDs already support credentials backed by Kubernetes Secrets. ([Plaud][2])

---

# 5. I would use NIP-44 encryption

For private agent conversations, don't rely on the relay being private.

Use:

```text
Nostr event
 ├── sender pubkey
 ├── recipient pubkey
 └── NIP-44 encrypted payload
```

NIP-44 defines versioned encrypted payloads intended to be carried inside signed Nostr events. ([NIPs][4])

So your topology could be:

```text
User/client
   │
   │ encrypted Nostr event
   ▼
public/private relay
   │
   ▼
agent bridge
   │
   │ decrypt
   ▼
OpenCode
```

The relay therefore doesn't need to know:

```text
"Fix the Vue authentication bug"
```

It only sees the encrypted event.

---

# 6. Don't map every OpenCode event directly to a Nostr event

This is important.

OpenCode produces considerably more granular events than you want your messaging layer to expose.

I'd introduce an abstraction:

```text
OpenCode events
       │
       ▼
 Event reducer
       │
       ▼
 Agent protocol
       │
       ▼
 Nostr
```

For example, internally OpenCode might generate:

```text
session.created
message.created
message.part.updated
tool.started
tool.completed
permission.requested
session.idle
session.error
```

Your bridge could collapse these into:

```text
agent.started
agent.message
agent.tool
agent.permission
agent.completed
agent.error
```

That makes the external protocol stable even if OpenCode changes.

---

# 7. The really useful part: asynchronous conversations

You can then have:

```text
Nostr user
     │
     │ "implement feature X"
     ▼
agent
     │
     │ ACK
     ▼
Nostr
     │
     │ "working"
     ▼
agent
     │
     ├── tool events
     ├── progress
     ├── questions
     │
     ▼
Nostr
     │
     │ "I need permission to..."
     ▼
user
     │
     │ approval
     ▼
Nostr
     │
     ▼
agent
```

This is much more appropriate for an autonomous agent than treating the interaction as:

```text
HTTP request
   ↓
wait 30 minutes
   ↓
HTTP response
```

---

# 8. Session mapping

I'd maintain a mapping like:

```text
Nostr conversation
        │
        ▼
agent ID
        │
        ▼
OpenCode session ID
```

For example:

```json
{
  "conversation": "nostr:abc...",
  "agent": "frontend-agent",
  "opencodeSession": "ses_01J...",
  "participants": [
    "npub1user..."
  ]
}
```

The first message:

```text
"Build a login page"
```

creates:

```text
OpenCode session
```

Subsequent Nostr messages:

```text
"Use Bulma instead"
"Also add dark mode"
"Run the tests"
```

all go to the same OpenCode session.

---

# 9. Don't expose OpenCode directly

I would **not** do:

```text
Internet
   │
   ▼
Ingress
   │
   ▼
OpenCode :4096
```

Instead:

```text
Internet
   │
   ▼
Nostr relay
   │
   ▼
Bridge
   │
   ▼
OpenCode :4096
```

OpenCode's HTTP server supports password authentication, but keeping it cluster-internal gives you a much smaller attack surface. ([OpenCode][1])

The bridge can call:

```text
http://127.0.0.1:4096
```

and OpenCode doesn't need an externally accessible Service.

---

# 10. KubeOpenCode implementation

Given the current KubeOpenCode model, I'd start with an `AgentTemplate` along these lines:

```yaml
apiVersion: kubeopencode.io/v1alpha1
kind: AgentTemplate
metadata:
  name: nostr-agent
spec:
  agentImage: ghcr.io/kubeopencode/kubeopencode-agent-opencode:latest
  executorImage: ghcr.io/kubeopencode/kubeopencode-agent-devbox:latest

  command:
    - opencode
    - serve
    - --hostname
    - 127.0.0.1
    - --port
    - "4096"

  credentials:
    - name: nostr-identity
      secretRef:
        name: nostr-agent-identity
        key: nsec
      env: NOSTR_NSEC

  extraPorts:
    - name: bridge
      port: 8080
      targetPort: 8080
      protocol: TCP

  podSpec:
    # bridge sidecar would go here if/when KubeOpenCode
    # exposes arbitrary sidecar/container configuration
```

There is one caveat: **the documented `AgentTemplate` API gives you `podSpec`, `extraPorts`, command, credentials, etc., but I don't see an officially documented arbitrary sidecar-container field in the current CRD**. ([Plaud][2])

So I would consider two implementations.

### Option A — modify/extend KubeOpenCode

Add something like:

```yaml
spec:
  sidecars:
    - name: nostr-bridge
      image: ghcr.io/your-org/opencode-nostr-bridge:v1
      ports:
        - containerPort: 8080
```

This is the architecture I'd ultimately use.

### Option B — separate Deployment/Service

If you don't want to modify KubeOpenCode:

```text
                   ┌────────────────────┐
                   │  Nostr Bridge      │
                   │                    │
Nostr ────────────►│ HTTP/WS            │
                   └─────────┬──────────┘
                             │
                             │ HTTP
                             ▼
                   ┌────────────────────┐
                   │ KubeOpenCode      │
                   │ Agent Service      │
                   └────────────────────┘
```

The bridge discovers Agents through Kubernetes and talks to their OpenCode Services.

This actually becomes interesting if you want **one bridge for many agents**.

---

# 11. I think a central bridge is actually better for your use case

Given your earlier idea of having agents interact with Mattermost/other messaging systems, I'd eventually use:

```text
                       ┌─────────────┐
                       │   Nostr     │
                       │   relays    │
                       └──────┬──────┘
                              │
                              │ WS
                              ▼
                    ┌──────────────────┐
                    │ Agent Gateway    │
                    │                  │
                    │ Nostr            │
                    │ HTTP API         │
                    │ auth             │
                    │ routing          │
                    │ sessions         │
                    └───────┬──────────┘
                            │
                 Kubernetes │ API
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
        Agent A         Agent B         Agent C
        OpenCode        OpenCode        OpenCode
```

The gateway maintains:

```text
nostr pubkey
      ↓
agent ID
      ↓
namespace
      ↓
OpenCode Service
      ↓
session ID
```

This has a major advantage for KubeOpenCode: **you don't need a Nostr client in every agent Pod.**

---

# 12. And expose an ordinary HTTP API too

I'd make the gateway API something like:

```http
POST /v1/messages
```

```json
{
  "agent": "frontend-agent",
  "conversation": "abc123",
  "text": "Fix the login page"
}
```

and:

```http
GET /v1/conversations/abc123/events
```

using SSE.

Then:

```text
                  ┌── Nostr
                  │
                  ├── Mattermost
                  │
Clients ──────────┼── Matrix
                  │
                  ├── HTTP
                  │
                  └── Web UI
                         │
                         ▼
                 Agent Gateway
                         │
                         ▼
                    OpenCode
```

This also means your Mattermost relay from your previous architecture can become just another gateway adapter instead of another bespoke OpenCode integration.

---

## One protocol I'd use

I'd define a small envelope:

```json
{
  "v": 1,
  "id": "evt_01J...",
  "conversation": "conv_01J...",
  "agent": "frontend-agent",
  "sender": "npub1...",
  "type": "message",
  "timestamp": "2026-09-28T16:30:00Z",
  "payload": {
    "text": "Fix the authentication tests"
  }
}
```

with event types:

```text
message
ack
thinking
tool_started
tool_finished
permission_request
question
progress
completed
error
```

Then map them to Nostr events.

That gives you a **transport-independent agent protocol** rather than accidentally making Nostr your application protocol.

---

### My recommended final architecture

For your KubeOpenCode setup, I'd build:

```text
                       Nostr relay
                           ▲
                           │ WebSocket
                           │
                    ┌──────┴──────┐
                    │ Nostr       │
                    │ Gateway     │
                    │             │
                    │ Go          │
                    │ NIP-44      │
                    │ NIP-01      │
                    │ routing     │
                    │ sessions    │
                    └──────┬──────┘
                           │
                           │ HTTP/HTTPS
                           ▼
                 ┌───────────────────┐
                 │ KubeOpenCode      │
                 │ Agent Service     │
                 └─────────┬─────────┘
                           │
                           │ HTTP
                           ▼
                 ┌───────────────────┐
                 │ OpenCode :4096    │
                 │                   │
                 │ HTTP API          │
                 │ SSE events        │
                 └───────────────────┘
```

**No stdio anywhere.**

OpenCode's HTTP API is explicitly designed for programmatic/network clients, and its `/global/event` endpoint provides the SSE side needed to turn agent activity back into messages. ([OpenCode][1])

For your environment, I'd implement the gateway in **Go**, with `nsec` identities stored as Kubernetes Secrets, NIP-44 for private messages, and Kubernetes discovery of `Agent` → OpenCode Service/session mappings.

The one design decision I'd make early is **central gateway vs one bridge sidecar per agent**. For the KubeOpenCode + Mattermost + Nostr architecture you're building, I'd choose the **central gateway**, while keeping the protocol compatible with a sidecar later.

[1]: https://dev.opencode.ai/docs/server/?utm_source=chatgpt.com "Server | OpenCode"
[2]: https://download.plaud.ai/kubeopencode/kubeopencode/blob/main/website/docs/features/agent-templates.md?utm_source=chatgpt.com "kubeopencode/website/docs/features/agent-templates.md at main · kubeopencode/kubeopencode · GitHub"
[3]: https://nips.nostr.com/1?utm_source=chatgpt.com "NIP 1 - Basic protocol flow description — Nostr Implementation Possibilities"
[4]: https://nips.nostr.com/44?utm_source=chatgpt.com "NIP 44 - Encrypted Payloads (Versioned) — Nostr Implementation Possibilities"


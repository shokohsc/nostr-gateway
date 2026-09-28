package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// errInvalid marks a client mistake; everything else Handle returns is an
// upstream failure, and the HTTP layer reports those as 502 rather than 400.
var errInvalid = errors.New("invalid request")

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errInvalid, fmt.Sprintf(format, a...))
}

// hub is the transport-independent core: inbound envelope in, reduced protocol
// events out, addressed by conversation.
type hub struct {
	reg     *Registry
	clients map[string]*opencodeClient
	convs   *sessionMap
	nostr   *nostrTransport // nil when Nostr is not configured
	log     *slog.Logger

	mu   sync.Mutex
	subs map[string]map[chan Envelope]struct{}
}

func newHub(reg *Registry, log *slog.Logger) *hub {
	user, pass := os.Getenv("OPENCODE_USER"), os.Getenv("OPENCODE_PASSWORD")
	h := &hub{reg: reg, convs: newSessionMap(), subs: map[string]map[chan Envelope]struct{}{}, log: log}
	h.clients = map[string]*opencodeClient{}
	for name, a := range reg.byName {
		h.clients[name] = newOpencodeClient(a.OpenCode, user, pass)
	}
	return h
}

// Reconnect policy, shared by the OpenCode stream and the Nostr subscription:
// start at reconnectBase, double while the peer keeps failing, and start over
// as soon as a connection outlived the wait we had queued up. Without the reset
// a few restarts in a day pin an agent at the ceiling for the rest of the
// process's life, deaf for 5 minutes per drop even after healthy days.
const (
	reconnectBase = 2 * time.Second
	reconnectMax  = 5 * time.Minute
)

func reconnectDelay(prev, connectedFor time.Duration) time.Duration {
	if connectedFor > prev {
		return reconnectBase
	}
	return min(prev*2, reconnectMax)
}

// run consumes each agent's OpenCode event stream, forever, reconnecting when
// the server restarts.
func (h *hub) run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, name := range h.reg.names() {
		a := h.reg.byName[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := reconnectBase
			for ctx.Err() == nil {
				connected := time.Now()
				ch, errs, err := h.clients[a.Name].events(ctx)
				switch {
				case err != nil:
					h.log.Warn("opencode stream", "agent", a.Name, "err", err)
				default:
					for ev := range ch {
						h.onEvent(a, ev)
					}
					select {
					case err := <-errs:
						h.log.Warn("opencode stream closed", "agent", a.Name, "err", err)
					default:
					}
				}
				if ctx.Err() != nil {
					return
				}
				interval = reconnectDelay(interval, time.Since(connected))
				h.log.Warn("opencode reconnecting", "agent", a.Name, "in", interval)
				select {
				case <-time.After(interval):
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()
}

// onEvent reduces one OpenCode event and fans the result out to whoever is
// watching that conversation. A given agent's stream is a single goroutine, so
// the conversation's dedupe set needs no extra locking here.
func (h *hub) onEvent(a *Agent, ev opencodeEvent) {
	c := h.convs.byOpenCodeSession(a.Name, sessionOf(ev))
	if c == nil {
		h.log.Debug("opencode event for an unmapped session", "agent", a.Name, "type", ev.Type)
		return
	}
	for _, r := range reduceEvent(ev, c.seen) {
		h.emit(c, a, r)
	}
}

// emit stamps one protocol event and delivers it to SSE subscribers and Nostr.
func (h *hub) emit(c *conversation, a *Agent, r reduced) Envelope {
	env := newEnvelope(a.Name, a.npub(), c.ID, r.Type, r.Payload)
	peer := c.peerPub()

	c.record(env)
	h.mu.Lock()
	for ch := range h.subs[c.ID] {
		select {
		case ch <- env:
		default:
			h.log.Warn("subscriber too slow, dropping event", "conversation", c.ID, "type", r.Type)
		}
	}
	h.mu.Unlock()

	if peer != "" && h.nostr != nil {
		if err := h.nostr.send(a, peer, env); err != nil {
			h.log.Error("nostr send", "agent", a.Name, "err", err)
		}
	}
	return env
}

// subscribe returns a stream of a conversation's events, starting with the
// replay buffer, so a client that subscribes after its first message still sees
// the beginning of the answer.
func (h *hub) subscribe(convID string) (<-chan Envelope, func()) {
	ch := make(chan Envelope, 256)
	for _, e := range h.convs.getHistory(convID) {
		select {
		case ch <- e:
		default:
		}
	}
	h.mu.Lock()
	if h.subs[convID] == nil {
		h.subs[convID] = map[chan Envelope]struct{}{}
	}
	h.subs[convID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs[convID], ch)
		if len(h.subs[convID]) == 0 {
			delete(h.subs, convID)
		}
		h.mu.Unlock()
	}
}

// Handle is the single inbound entry point for every transport. `peer` is the
// authenticated Nostr pubkey of the sender ("" for HTTP, which authenticates
// with a bearer token instead): identity never travels inside the envelope, so
// no caller can name someone else as the sender.
func (h *hub) Handle(ctx context.Context, a *Agent, in Envelope, peer string) (Envelope, error) {
	if in.V != 0 && in.V != protocolVersion {
		return Envelope{}, invalid("unsupported protocol version %d", in.V)
	}
	// The allow list is a Nostr-identity gate. HTTP callers carry no Nostr
	// identity (they authenticate with the bearer token instead), and the
	// envelope's `sender` field is never trusted for either decision.
	if peer != "" && !a.allows(peer) {
		return Envelope{}, invalid("sender %s not allowed for agent %s", shortPub(peer), a.Name)
	}
	if in.Agent != "" && in.Agent != a.Name {
		return Envelope{}, invalid("envelope agent %q routed to %q", in.Agent, a.Name)
	}
	if in.Conversation == "" {
		in.Conversation = newID("conv")
	}
	c := h.convs.lookup(in.Conversation, a.Name)
	if c.Agent != a.Name {
		// The conversation id belongs to another agent; sending it to this one
		// would send a session id from a different OpenCode server.
		return Envelope{}, invalid("conversation %s belongs to agent %s", c.ID, c.Agent)
	}
	c.setPeer(peer)

	switch in.Type {
	case TypeMessage, "prompt":
		if in.Payload.Text == "" {
			return Envelope{}, invalid("empty message text")
		}
		if err := h.prompt(ctx, a, c, in.Payload.Text); err != nil {
			return Envelope{}, err
		}
		return h.emit(c, a, reduced{Type: TypeAck, Payload: Payload{Text: "queued"}}), nil

	case TypePermissionResp:
		id := in.Payload.PermissionID
		if id == "" {
			return Envelope{}, invalid("permission_response needs payload.permission_id")
		}
		sessionID := c.sessionID()
		if sessionID == "" {
			return Envelope{}, invalid("conversation %s has no session yet", c.ID)
		}
		if err := h.clients[a.Name].replyPermission(ctx, sessionID, id, in.Payload.Text); err != nil {
			return Envelope{}, err
		}
		return h.emit(c, a, reduced{Type: TypeProgress, Payload: Payload{PermissionID: id, Text: in.Payload.Text}}), nil

	default:
		return Envelope{}, invalid("unsupported inbound type %q", in.Type)
	}
}

// prompt queues a message. The session is created under the conversation lock,
// so two simultaneous first messages share one session instead of orphaning
// one. The call outlives the caller's context: a client that hangs up must not
// leave a created session without its prompt.
func (h *hub) prompt(ctx context.Context, a *Agent, c *conversation, text string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancel()

	client := h.clients[a.Name]
	sessionID, err := c.ensureSession(func() (string, error) {
		title := text
		if len(title) > 80 {
			title = title[:80]
		}
		id, err := client.createSession(ctx, title)
		if err != nil {
			return "", fmt.Errorf("create session: %w", err)
		}
		h.convs.index(c, a.Name, id)
		return id, nil
	})
	if err != nil {
		return err
	}
	return client.promptAsync(ctx, sessionID, text, a.Model, a.OpencodeAgent)
}

func shortPub(h string) string {
	if h == "" {
		return "anonymous"
	}
	if len(h) > 12 {
		return h[:12] + "..."
	}
	return h
}

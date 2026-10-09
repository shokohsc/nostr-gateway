//go:build !race

// The Nostr transport is not exercised under -race: go-nostr v0.52.3 races its
// own connect path (Relay.ConnectWithTLS writing r.Connection while Relay.close
// reads it, relay.go:175 vs :576) as soon as the pool subscribes. v0.52.3 is the
// latest release, so the race cannot be fixed from here. Everything below still
// runs in the normal suite; only the race detector skips it.

package main

import (
	"bytes"
	"context"
	"encoding/json"

	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip44"
)

// A relay just enough for the gateway: it accepts EVENT and REQ frames and
// fans events out to the other connections. This lets the Nostr path be tested
// end to end — encryption, kind, p-tag routing and reply — with no network.
type fakeRelay struct {
	*httptest.Server
	mu    sync.Mutex
	conns map[*relayConn]struct{}
	subs  int
	// requireAuth makes the relay behave like a closed one (Buzz, and most
	// relays that bill or rate-limit): it challenges on connect, refuses every
	// REQ until the connection presents a signed kind-22242 event, and refuses
	// any other event afterwards.
	requireAuth bool
	auths       int
	// dropAuths refuses that many NIP-42 answers. A refused handshake is the
	// fast stand-in for a lost one — either way go-nostr gives up on the
	// subscription instead of sending the REQ again (pool.go:662-673) — and it
	// is what a client sees when its auth event loses the race with another
	// subscription's on the same connection.
	dropAuths int
	// store makes the relay answer a REQ with everything injected so far before
	// its EOSE, like a real relay's stored events. It is off by default because
	// the other tests only ever need live fan-out; a client that has to discover
	// state by querying (the Buzz channel list) needs it.
	store     bool
	stored    []*nostr.Event
	published []int          // kinds the gateway pushed here, as opposed to broadcast
	posts     []*nostr.Event // the same events, whole, for tag and content assertions
	opened    int            // websocket connections ever accepted
	// filters records what each REQ actually asked for. The relay enforces one
	// narrowing only (relayRoutes, the `#h` scope), so these tests can still
	// inject anything from anyone, and without this the part of a filter that
	// matters most in production — what the relay is asked to route — is
	// invisible to every test here.
	filters []nostr.Filter
	// kickMessage closes that many kind-30078 subscriptions after their EOSE,
	// which is the cheapest stand-in for a relay restart: the listener's range
	// ends, it backs off, and it issues a second REQ. Whether that second REQ
	// is built from scratch or reuses the first one's `since` is not visible
	// any other way, and reusing it replays the whole stored history.
	kickMessage int
}

type relayConn struct {
	ws   *websocket.Conn
	ctx  context.Context
	mu   sync.Mutex // one writer, the subids and the identity, under one lock
	subs []string
	// authPub is the pubkey whose NIP-42 event authenticated this connection,
	// empty until it does. A closed relay only accepts events signed by exactly
	// this key, which is why one pool cannot serve two agents.
	authPub string
}

func (c *relayConn) write(v any) {
	b, _ := json.Marshal(v)
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.ws.Write(c.ctx, websocket.MessageText, b)
}

func (c *relayConn) writeRaw(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.ws.Write(c.ctx, websocket.MessageText, b)
}

// nextEventFrom returns the next event pushed to this connection that is not
// from skipPub (the relay echoes our own publishes back at us), or nil.
func (c *relayConn) nextEventFrom(skipPub string, d time.Duration) *nostr.Event {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(c.ctx, time.Until(deadline))
		_, data, err := c.ws.Read(ctx)
		cancel()
		if err != nil {
			return nil
		}
		var msg []json.RawMessage
		if json.Unmarshal(data, &msg) != nil || len(msg) < 2 || string(msg[0]) != `"EVENT"` {
			continue // EOSE / OK chatter
		}
		var ev nostr.Event // the event is the last element, with or without a subid
		if json.Unmarshal(msg[len(msg)-1], &ev) != nil {
			continue
		}
		if ev.PubKey == skipPub {
			continue
		}
		return &ev
	}
	return nil
}

func newFakeRelay(t *testing.T) *fakeRelay {
	t.Helper()
	r := &fakeRelay{conns: map[*relayConn]struct{}{}}
	r.Server = httptest.NewServer(http.HandlerFunc(r.handle))
	// Deliberately not closed: killing the server drops the gateway's relay
	// connection, and go-nostr's own close-then-reconnect is a data race
	// (relay.go:175 vs :576) that -race reports against the test. The test
	// binary exiting reclaims the listener anyway.
	return r
}

func (r *fakeRelay) url() string { return "ws" + strings.TrimPrefix(r.URL, "http") }

func (r *fakeRelay) handle(w http.ResponseWriter, req *http.Request) {
	ws, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	c := &relayConn{ws: ws, ctx: context.Background()}
	r.mu.Lock()
	r.conns[c] = struct{}{}
	r.opened++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.conns, c)
		r.mu.Unlock()
		ws.CloseNow()
	}()
	if r.requireAuth {
		// Before the first REQ, exactly like a real relay: the client has to
		// have the challenge in hand by the time the refusal arrives.
		c.write([]any{"AUTH", authChallenge})
	}

	for {
		_, data, err := ws.Read(c.ctx)
		if err != nil {
			return
		}
		var msg []json.RawMessage
		if json.Unmarshal(data, &msg) != nil || len(msg) == 0 {
			continue
		}
		switch string(msg[0]) {
		case `"AUTH"`:
			var ev nostr.Event
			if json.Unmarshal(msg[1], &ev) != nil {
				continue
			}
			r.mu.Lock()
			drop := r.dropAuths > 0
			if drop {
				r.dropAuths--
			}
			r.mu.Unlock()
			if drop {
				// Refused before verify, so the connection stays unauthenticated
				// and the client has to ask again from scratch.
				c.write([]any{"OK", ev.ID, false, "auth-required: refused on purpose"})
				continue
			}
			c.write([]any{"OK", ev.ID, r.verify(c, &ev), "auth-required: verification failed"})
		case `"EVENT"`:
			var ev nostr.Event
			if json.Unmarshal(msg[1], &ev) != nil {
				continue
			}
			if id := c.identity(); r.requireAuth && id != ev.PubKey {
				c.write([]any{"OK", ev.ID, false, "invalid: event pubkey does not match authenticated identity"})
				continue
			}
			r.mu.Lock()
			r.published = append(r.published, ev.Kind)
			post := ev
			r.posts = append(r.posts, &post)
			r.mu.Unlock()
			c.write([]any{"OK", ev.ID, true, ""})
			r.broadcast(&ev, c)
		case `"REQ"`:
			var subID string
			_ = json.Unmarshal(msg[1], &subID)
			if r.requireAuth && c.identity() == "" {
				c.write([]any{"NOTICE", "auth-required: authenticate before subscribing"})
				c.write([]any{"CLOSED", subID, "auth-required: not authenticated"})
				continue
			}
			var f nostr.Filter
			if len(msg) > 2 {
				_ = json.Unmarshal(msg[2], &f)
			}
			c.mu.Lock()
			c.subs = append(c.subs, subID)
			c.mu.Unlock()
			r.mu.Lock()
			r.subs++
			r.filters = append(r.filters, f)
			stored := append([]*nostr.Event(nil), r.stored...)
			r.mu.Unlock()
			for _, ev := range stored {
				if !relayRoutes(f, ev) {
					continue
				}
				c.write([]any{"EVENT", subID, ev})
			}
			c.write([]any{"EOSE", subID})
			if hasKind(f, messageKind) {
				r.mu.Lock()
				kick := r.kickMessage > 0
				if kick {
					r.kickMessage--
				}
				r.mu.Unlock()
				if kick {
					c.write([]any{"CLOSED", subID, "relay going away"})
				}
			}
		case `"CLOSE"`:
			// Only this subscription goes away, like a real relay: closing the
			// whole connection here would tear down the gateway's other
			// subscriptions every time a one-shot REQ ends.
			var subID string
			_ = json.Unmarshal(msg[1], &subID)
			c.mu.Lock()
			c.subs = slices.DeleteFunc(c.subs, func(s string) bool { return s == subID })
			c.mu.Unlock()
		}
	}
}

func (c *relayConn) identity() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authPub
}

const authChallenge = "nostr-gateway-test-challenge"

// verify accepts a NIP-42 event that answers this relay's challenge and pins
// the connection to the key that signed it.
func (r *fakeRelay) verify(c *relayConn, ev *nostr.Event) bool {
	if _, err := ev.CheckSignature(); err != nil ||
		tagValue(ev.Tags, "challenge") != authChallenge ||
		ev.Content != "" {
		return false
	}
	c.mu.Lock()
	c.authPub = ev.PubKey
	c.mu.Unlock()
	r.mu.Lock()
	r.auths++
	r.mu.Unlock()
	return true
}

// broadcast fans an event out to every other connection, tagged with the
// subscription id each one asked for (clients drop events with an unknown id).
func (r *fakeRelay) broadcast(ev *nostr.Event, except *relayConn) {
	r.mu.Lock()
	conns := make([]*relayConn, 0, len(r.conns))
	for c := range r.conns {
		conns = append(conns, c)
	}
	r.mu.Unlock()
	for _, c := range conns {
		if c == except {
			continue
		}
		c.mu.Lock()
		subs := append([]string(nil), c.subs...)
		c.mu.Unlock()
		if len(subs) == 0 {
			frame, err := json.Marshal([]any{"EVENT", ev}) // plain listener
			if err == nil {
				c.writeRaw(frame)
			}
			continue
		}
		for _, sub := range subs {
			if frame, err := json.Marshal([]any{"EVENT", sub, ev}); err == nil {
				c.writeRaw(frame)
			}
		}
	}
}

// inject simulates another publisher's event arriving at the relay.
func (r *fakeRelay) inject(ev *nostr.Event) {
	r.mu.Lock()
	if r.store {
		r.stored = append(r.stored, ev)
	}
	r.mu.Unlock()
	r.broadcast(ev, nil)
}

func (r *fakeRelay) subscribeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.subs
}

// publishKinds returns the kinds the gateway pushed here, in order, so a test can
// say which events crossed to the relay rather than only how many.
func (r *fakeRelay) publishKinds() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.published)
}

// channelPublishKinds is publishKinds without the presence heartbeat. Presence
// (kind 20001) goes out on its own goroutine on every refresh, so it interleaves
// with the profile and the answers unpredictably and a test that asserts on the
// order of channel traffic must not see it.
func (r *fakeRelay) channelPublishKinds() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(r.published), func(k int) bool { return k == buzzPresenceKind })
}

// hasPublished reports whether the gateway pushed an event of this kind here.
func (r *fakeRelay) hasPublished(kind int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.published, kind)
}

// postsOf returns the events of one kind the gateway pushed here, whole.
func (r *fakeRelay) postsOf(kind int) []*nostr.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(r.posts), func(e *nostr.Event) bool { return e.Kind != kind })
}

func (r *fakeRelay) authCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.auths
}

// opened returns how many websocket connections this relay has ever accepted,
// so a test can assert that two subscriptions did not share one.
func (r *fakeRelay) openedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.opened
}

func (r *fakeRelay) dial() *relayConn {
	ws, _, err := websocket.Dial(context.Background(), r.url(), nil)
	if err != nil {
		panic(err)
	}
	c := &relayConn{ws: ws, ctx: context.Background()}
	// Not registered, unlike a client of a real relay: the httptest handler
	// behind this websocket has already put the *server* end of this same
	// socket in conns, so broadcast delivers to the test client through it.
	// Registering this end too wrote every event twice on one socket — once out
	// to the client, once back into the relay, which then read its own echo as
	// a publish and fanned it out again. Fifty thousand copies of one event
	// before the first assertion, and a socket too flooded for the client to
	// read its answer.
	return c
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNostrRoundTrip(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)

	usk := nostr.GeneratePrivateKey()
	upk, err := nostr.GetPublicKey(usk)
	if err != nil {
		t.Fatal(err)
	}
	ask := nostr.GeneratePrivateKey()
	apk, err := nostr.GetPublicKey(ask)
	if err != nil {
		t.Fatal(err)
	}

	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// A second connection standing in for the user's other device.
	user := relay.dial()
	waitFor(t, "gateway subscription", func() bool { return relay.subscribeCount() > 0 })

	// User -> agent, addressed to the agent's pubkey, NIP-44 encrypted.
	ck, err := nip44.GenerateConversationKey(apk, usk)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(Envelope{
		V: protocolVersion, Conversation: "nostr-1", Agent: "frontend-agent",
		Type: TypeMessage, Payload: Payload{Text: "hello from nostr"},
	})
	cipher, err := nip44.Encrypt(string(body), ck)
	if err != nil {
		t.Fatal(err)
	}
	in := nostr.Event{
		Kind: messageKind, CreatedAt: nostr.Now(),
		Tags: nostr.Tags{{"p", apk}}, Content: cipher,
	}
	if err := in.Sign(usk); err != nil {
		t.Fatal(err)
	}
	relay.inject(&in)

	waitFor(t, "prompt delivered to opencode", func() bool {
		_, prompts, _ := f.counts()
		return prompts == 1
	})
	f.mu.Lock()
	text := f.prompts[0]["parts"].([]any)[0].(map[string]any)["text"]
	f.mu.Unlock()
	if text != "hello from nostr" {
		t.Fatalf("prompt text %v", text)
	}

	// Agent -> user: the ack, encrypted to the user, tagged to them.
	ev := user.nextEventFrom(upk, 5*time.Second) // our own event comes back to us first
	if ev == nil {
		t.Fatal("no event came back over nostr")
	}
	if ev.Kind != messageKind {
		t.Fatalf("kind %d", ev.Kind)
	}
	if got := tagValue(ev.Tags, "p"); got != upk {
		t.Fatalf("p tag %q, want %q", got, upk)
	}
	if strings.Contains(ev.Content, "hello") {
		t.Fatal("content is not encrypted")
	}
	plain, err := nip44.Decrypt(ev.Content, ck) // same key, from the user's side
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	var out Envelope
	if err := json.Unmarshal([]byte(plain), &out); err != nil {
		t.Fatal(err)
	}
	want(t, out, TypeAck, "queued")
	if out.Conversation != "nostr-1" || out.Sender != agent.npub() {
		t.Fatalf("stamped envelope wrong: %+v", out)
	}
}

func TestNostrPermissionReply(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)
	waitFor(t, "gateway subscription", func() bool { return relay.subscribeCount() > 0 })

	ck, _ := nip44.GenerateConversationKey(apk, usk)
	relay.injectFrom(t, usk, apk, ck, Envelope{
		Conversation: "nostr-2", Agent: "frontend-agent", Type: TypeMessage, Payload: Payload{Text: "clean up"},
	})
	waitFor(t, "session", func() bool { s, _, _ := f.counts(); return s == 1 })

	// The user approves a permission over Nostr.
	relay.injectFrom(t, usk, apk, ck, Envelope{
		Conversation: "nostr-2", Agent: "frontend-agent", Type: TypePermissionResp,
		Payload: Payload{PermissionID: "per_9", Text: "once"},
	})
	waitFor(t, "permission reply", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.perms) == 1 && f.perms[0] == "ses_1/per_9:once"
	})
}

// injectFrom signs and injects an encrypted envelope from a user key.
func (r *fakeRelay) injectFrom(t *testing.T, sk, to string, ck [32]byte, env Envelope) {
	t.Helper()
	body, _ := json.Marshal(env)
	cipher, err := nip44.Encrypt(string(body), ck)
	if err != nil {
		t.Fatal(err)
	}
	ev := nostr.Event{Kind: messageKind, CreatedAt: nostr.Now(), Tags: nostr.Tags{{"p", to}}, Content: cipher}
	if err := ev.Sign(sk); err != nil {
		t.Fatal(err)
	}
	r.inject(&ev)
}

// A user outside the agent's allow list is dropped even though the message is
// correctly signed and encrypted: the allow list is checked against the event's
// pubkey, not against anything the envelope claims.
func TestNostrAllowList(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, err := nostr.GetPublicKey(ask)
	if err != nil {
		t.Fatal(err)
	}
	agent := &Agent{
		Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask,
		Allow: []string{strings.Repeat("cd", 32)},
		ck:    map[string][32]byte{},
	}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)
	waitFor(t, "gateway subscription", func() bool { return relay.subscribeCount() > 0 })

	ck, _ := nip44.GenerateConversationKey(apk, usk)
	relay.injectFrom(t, usk, apk, ck, Envelope{
		Conversation: "nostr-3", Agent: "frontend-agent", Type: TypeMessage, Payload: Payload{Text: "let me in"},
	})
	time.Sleep(500 * time.Millisecond) // give it time to arrive and be refused

	if s, p, _ := f.counts(); s != 0 || p != 0 {
		t.Fatalf("blocked nostr user reached opencode: sessions=%d prompts=%d", s, p)
	}
	if h.convs.get("nostr-3") != nil {
		t.Fatal("blocked user created a conversation")
	}
}

// An HTTP caller that asserts someone else's pubkey as `sender` must not make
// the gateway publish that agent's output to that key: replies follow the
// authenticated transport identity, never the envelope.
func TestHTTPCannotRedirectNostrReplies(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)

	ask := nostr.GeneratePrivateKey()
	apk, err := nostr.GetPublicKey(ask)
	if err != nil {
		t.Fatal(err)
	}
	victim := strings.Repeat("ef", 32) // someone else's key
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)
	waitFor(t, "gateway subscription", func() bool { return relay.subscribeCount() > 0 })

	srv := httptest.NewServer(h.handler(""))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader(
		`{"conversation":"exfil","agent":"frontend-agent","sender":"`+victim+`","type":"message","payload":{"text":"secret"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)

	if h.convs.get("exfil").peerPub() != "" {
		t.Fatal("an HTTP envelope's claimed sender became the reply peer")
	}
	if c := h.convs.get("exfil"); c != nil {
		if c.peerPub() == victim {
			t.Fatal("replies would be encrypted to the claimed sender")
		}
	}
}

// Routing by p tag is what keeps agents apart, and the subscription filter is
// where it is enforced: the client's p tag must equal the agent's hex pubkey.
// This is the test that fails if the registry ever hands go-nostr a bech32
// pubkey instead of hex — the agent would silently never hear anything.
// A kind-30078 envelope is a message to one agent, and the p tag is the only
// thing that says which one. The conversation key is derived from the sender and
// this agent, never from the tag, so an envelope addressed elsewhere decrypts
// perfectly and would become a prompt here if it ever arrived — which is why
// the `#p` filter in nostr.listen is load-bearing addressing, not a narrowing to
// be trimmed for fan-out. Two layers enforce it, the REQ and go-nostr's own
// client-side match, and this test fails if either goes: the fake relay replays
// every stored event to every subscriber, so nothing in the harness hides a
// missing filter.
func TestNostrRoutesByPTag(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)
	waitFor(t, "gateway subscription", func() bool { return relay.subscribeCount() > 0 })

	ck, _ := nip44.GenerateConversationKey(apk, usk)
	relay.injectFrom(t, usk, strings.Repeat("ef", 32), ck, Envelope{ // addressed to another key
		Conversation: "wrong-agent", Agent: "frontend-agent", Type: TypeMessage, Payload: Payload{Text: "hello?"},
	})
	time.Sleep(300 * time.Millisecond)
	if s, p, _ := f.counts(); s != 0 || p != 0 {
		t.Fatalf("event addressed elsewhere reached opencode: sessions=%d prompts=%d", s, p)
	}

	relay.injectFrom(t, usk, apk, ck, Envelope{ // addressed to this agent
		Conversation: "right-agent", Agent: "frontend-agent", Type: TypeMessage, Payload: Payload{Text: "hello"},
	})
	waitFor(t, "message addressed to the agent", func() bool { _, p, _ := f.counts(); return p == 1 })
}

// A closed relay — Buzz, or anything that requires NIP-42 — refuses every REQ
// from an unauthenticated connection, and then only accepts events signed by
// the key that authenticated. Without this the gateway resubscribes forever and
// is silently deaf; with a single shared pool it would work for one agent and
// leave the other one deaf, so each agent needs its own authenticated pool.
func TestNostrAuthenticatesPerAgentOnAClosedRelay(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.requireAuth = true

	reg := &Registry{byName: map[string]*Agent{}}
	for _, name := range []string{"alpha", "beta"} {
		sk := nostr.GeneratePrivateKey()
		pk, _ := nostr.GetPublicKey(sk)
		reg.byName[name] = &Agent{Name: name, OpenCode: f.URL, PubKey: pk, sk: sk, ck: map[string][32]byte{}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// One challenge answered per agent, and both subscriptions through. Each
	// agent has two pools over one relay list, so there are two connections per
	// agent to authenticate and to subscribe on — the split is what keeps their
	// NIP-42 handshakes from colliding on one websocket.
	waitFor(t, "every agent authenticated", func() bool {
		return relay.authCount() >= 4 && relay.subscribeCount() >= 4
	})

	usk := nostr.GeneratePrivateKey()
	apk := reg.byName["alpha"].PubKey
	ck, _ := nip44.GenerateConversationKey(apk, usk)
	relay.injectFrom(t, usk, apk, ck, Envelope{
		Conversation: "closed-1", Agent: "alpha", Type: TypeMessage, Payload: Payload{Text: "still there?"},
	})
	waitFor(t, "message through a closed relay", func() bool { _, p, _ := f.counts(); return p == 1 })
}

// The gateway must not ask the relay to *narrow* its listeners. A relay applies
// `authors` and every `#tag` before it delivers, so anything the gateway
// adjudicates with is a way to be deaf with a clean log: there is no code left
// to run, so nothing logs, and the only evidence is the relay's own view of a
// REQ. Both transports shipped one of these. `authors` dropped a blocked sender
// before allows() could name it. `#p` on the kind-30078 filter was pure
// downside — receive() routes on the conversation key derived from the sender's
// pubkey and never reads the p tag, so the filter could only lose events, and a
// client that does not tag `p` for this agent is indistinguishable from a dead
// relay.
//
// What stays is a tag the relay itself routes on, because dropping it does not
// narrow delivery, it ends it: `#p` on the kind-30078 listener, which the relay
// and go-nostr both deliver on, and `#h` on the kind-9 listener, which is how
// Buzz's relay hands a channel message to the subscriptions that *name* that
// channel. A kind-9 REQ carrying kinds and nothing else receives nothing at
// all — not the wrong channels, none — and the failure is silent on both sides,
// because the relay logs the message ingested and the gateway has nothing to
// log about an event that never arrived. Removing `#h` on the grounds that the
// channel name is a guess is what made an agent deaf in every channel it was a
// member of.
//
// Everything the gateway *decides* — allow, membership, the mention — stays in
// buzzReceive and receive(), where a refusal can be named. That is the line this
// test draws: the filter may address, never adjudicate.
//
// The kind-9 listener sends both halves, because neither one covers the other: a
// scoped REQ hears what the relay routed to a channel and a channel-less one
// hears what it could not route, and the relay keeps the two subscriptions in
// separate scopes so nothing arrives twice. The scoped half is one REQ PER
// channel: the relay reads a REQ naming two channels as global scope, so a
// single filter listing them all is deaf to every one of them.
// ponytail: a relay filtering a large public channel set would make the wide
// fan-out expensive; a periodic self-REQ to prove delivery buys it back.
func TestGatewayNeverAsksTheRelayToFilter(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.store = true
	relay.requireAuth = true

	allowed := strings.Repeat("ab", 32)
	reg := &Registry{byName: map[string]*Agent{
		"frontend-agent": {
			Name: "frontend-agent", OpenCode: f.URL, PubKey: strings.Repeat("cd", 32),
			sk: nostr.GeneratePrivateKey(), ck: map[string][32]byte{},
			Allow: []string{allowed},
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// Two channels the agent is in, because the relay reads a single-filter
	// subscription naming more than one channel as global scope and then delivers
	// no channel message at all — so one channel would not exercise the bug.
	relay.store = true
	relay.inject(buzzMembers(t, nostr.GeneratePrivateKey(), reg.byName["frontend-agent"].PubKey, "chan-1", 2))
	relay.inject(buzzMembers(t, nostr.GeneratePrivateKey(), reg.byName["frontend-agent"].PubKey, "chan-2", 2))

	// One subscription per role, and one channel-scoped kind-9 REQ per channel
	// plus its channel-less companion for the Buzz listener.
	waitFor(t, "both transports subscribed", func() bool {
		relay.mu.Lock()
		defer relay.mu.Unlock()
		var chat int
		for _, f := range relay.filters {
			for _, k := range f.Kinds {
				if k == 9 {
					chat++
				}
			}
		}
		return len(relay.filters) >= 4 && chat >= 3
	})

	relay.mu.Lock()
	defer relay.mu.Unlock()
	var scoped, unrouted int
	for i, f := range relay.filters {
		listener := false
		for _, k := range f.Kinds {
			if k == messageKind || k == buzzChatKind {
				listener = true
			}
		}
		if !listener {
			// The kind-39002 member-list query is a different animal: a one-shot
			// FetchMany whose `p` filter is how a member list names its members,
			// and whose failure buzzDiscover already reports in three states. A
			// wrong filter there costs a channel, and it says so.
			continue
		}
		if len(f.Authors) != 0 {
			t.Errorf("REQ %d asks the relay for authors %v: a blocked sender is dropped "+
				"by the relay and the gateway never logs the refusal", i, f.Authors)
		}
		if hasKind(f, buzzChatKind) {
			// One channel-scoped REQ per discovered channel, plus one channel-less
			// one, and they are not interchangeable: a scoped REQ can hear only
			// what the relay routed to its one channel, the unrouted one only what
			// it could not route. Between them every kind 9 the relay holds is
			// either delivered or refused in buzzReceive, which logs it. A REQ
			// naming two channels in one filter is the bug this guards: the relay
			// falls back to global scope and hears neither.
			switch h := f.Tags["h"]; len(h) {
			case 0:
				if len(f.Tags) != 0 {
					t.Errorf("REQ %d asks for kind 9 with no #h but with tags %v: a relay "+
						"reads one filter without #h as global scope and then delivers only "+
						"the events it could not route to a channel", i, f.Tags)
				}
				unrouted++
			case 1:
				if h[0] != "chan-1" && h[0] != "chan-2" {
					t.Errorf("REQ %d asks for channel %q, want one of the discovered channels", i, h[0])
				}
				scoped++
			default:
				t.Errorf("REQ %d asks for %d channels in one filter %v: the relay reads more "+
					"than one channel as global scope and then delivers no channel message at all",
					i, len(h), h)
			}
		}
		for tag, vals := range f.Tags {
			// The addressing that stays, because the relay routes on it: `#p` for
			// the envelope, and `#h` for a channel message. `#h` also has to be
			// the whole discovered list, because a REQ naming a channel the agent
			// left, or none of them, is the same deafness one step further out.
			if tag == "p" && hasKind(f, messageKind) {
				continue
			}
			if tag == "h" && hasKind(f, buzzChatKind) {
				continue // the exact value was asserted above, per REQ
			}
			t.Errorf("REQ %d asks the relay for %s in %v: the relay drops a non-match "+
				"before delivery, so a wrong tag name is silence, not a message", i, tag, vals)
		}
	}
	if scoped != 2 || unrouted != 1 {
		t.Errorf("kind-9 REQs: %d scoped, %d unrouted, want one scoped per discovered channel "+
			"plus one unrouted — a REQ naming more than one channel is global scope and reaches "+
			"neither a routed nor an unrouted message", scoped, unrouted)
	}
}

// relayRoutes is the one narrowing a real relay applies before delivery, and it
// is the one the Buzz deafness is made of: a subscription naming channels in #h
// receives only the events naming one of them, and a subscription naming no
// channel receives only the events naming none (buzz-relay keeps a
// channel-scoped subscription and a global one in separate scopes, and its
// channel id comes from the `h` tag alone). It also falls back to global scope
// as soon as a filter names more than one channel, which is why every scoped
// kind-9 REQ has to name exactly one. Handing every stored event to every
// subscriber makes the two subscriptions indistinguishable, so a REQ the relay
// would answer with nothing looks here like one it answers with everything —
// which is how a `#h`-only listener passed every test in this file while a
// message the relay could not route never reached the gateway.
//
// Live broadcast is still unconditional, which is why every test publishes
// before it starts the gateway; a broadcast has no filter to apply because the
// fake stores the sub ids, not the filters.
func relayRoutes(f nostr.Filter, ev *nostr.Event) bool {
	h := f.Tags["h"]
	// Channel-scoped only when exactly one channel is named, like the real
	// relay's extract_channel_id_from_filters: no `#h` is global, and so is two
	// or more distinct channels — it cannot index the subscription under one
	// channel, so it falls back to global scope, which never sees a
	// channel-routed event. A listener that packs every channel into one filter
	// is therefore deaf exactly here, which is what this model has to reproduce.
	if len(h) != 1 {
		return tagValue(ev.Tags, "h") == ""
	}
	return tagValue(ev.Tags, "h") == h[0]
}

func hasKind(f nostr.Filter, kind int) bool {
	for _, k := range f.Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// buzzChannelTag still resolves either name, because buzzDiscover builds the
// channel list from `d` and buzzPost writes `h`, so a delivered message has to be
// addressable whichever its author used.
//
// The end-to-end version of this — a `d`-only chat message reaching OpenCode —
// needs the channel-less half of the listener, because a relay takes the channel
// id off `h` and a message with none of those is stored with no channel at all,
// where only a subscription naming no channel can hear it. That is
// TestBuzzChatMessageTheRelayCouldNotRouteReachesOpenCode.
func TestBuzzChannelTagAcceptsEitherName(t *testing.T) {
	for _, tc := range []struct {
		name string
		tags nostr.Tags
		want string
	}{
		{"h, as chat messages are written", nostr.Tags{{"h", "chan-h"}}, "chan-h"},
		{"d, as member lists name it", nostr.Tags{{"d", "chan-d"}}, "chan-d"},
		{"h wins when both are there", nostr.Tags{{"d", "chan-d"}, {"h", "chan-h"}}, "chan-h"},
		{"neither", nostr.Tags{{"p", "x"}}, ""},
	} {
		if got := buzzChannelTag(tc.tags); got != tc.want {
			t.Errorf("%s: buzzChannelTag = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A kind-9 chat message the relay could not route to a channel: the channel name
// is under `d`, and a relay reads a channel off `h` alone, so it stores the event
// with no channel and can only hand it to a subscription that names no channel.
// Before the channel-less half of the listener, that message was deafness with
// nothing to look at: the relay logged it ingested, the gateway logged that it
// asked for channel messages, and no line anywhere said the two did not meet. The
// answer still goes back under `h`, because buzzPost writes what NIP-29 says a
// chat message carries.
func TestBuzzChatMessageTheRelayCouldNotRouteReachesOpenCode(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.requireAuth = true
	relay.store = true

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	group := "channel-unrouted"
	relay.inject(buzzMembers(t, usk, apk, group, 2))
	relay.inject(buzzMessage(t, usk, "d", group, apk, "@frontend-agent named under d only"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	waitFor(t, "the unroutable mention", func() bool { _, p, _ := f.counts(); return p == 1 })
	// The channel came from `d` on both events: the member list names it, and the
	// message names it, so the turn belongs to the channel the agent is in.
	if h.convs.get(buzzConversation(group)) == nil {
		t.Fatalf("no conversation for %s: the channel name was not read off the message", group)
	}

	f.push("message.part.updated", map[string]any{
		"sessionID": "ses_1", "delta": "heard you",
		"part": map[string]any{"id": "prt_1", "type": "text", "sessionID": "ses_1", "messageID": "msg_asst"},
	})
	f.push("session.idle", map[string]any{"sessionID": "ses_1"})
	waitFor(t, "the answer", func() bool { return len(relay.postsOf(buzzChatKind)) == 1 })
	if got := tagValue(relay.postsOf(buzzChatKind)[0].Tags, "h"); got != group {
		t.Fatalf("h tag %q, want %q", got, group)
	}
}

// A Buzz mention is a kind-9 message carrying a p tag for the agent, and a Buzz
// DM is a two-member channel. The listener finds the agent's channels in the
// NIP-29 member lists (kind 39002) and subscribes to them by #h, because a relay
// does not hand channel-scoped events to a subscription that does not name the
// channel. Group chatter nobody addressed to the agent is not for the agent.
func TestBuzzMentionsAndDMsReachOpenCode(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	buzz := newFakeRelay(t)
	buzz.requireAuth = true // a closed relay, like Buzz with BUZZ_REQUIRE_RELAY_MEMBERSHIP
	buzz.store = true       // discovery has to read the member lists back

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	// Everything is published to the Buzz relay before the gateway starts,
	// because a channel message is only ever delivered to a subscription that
	// already names the channel — the fake relay replays its stored events to
	// every REQ, as a real relay does. A three-member channel the agent is in,
	// a two-member one (a DM), and one message each way.
	group, dm := "channel-group", "channel-dm"
	buzz.inject(buzzMembers(t, usk, apk, group, 3))
	buzz.inject(buzzMembers(t, usk, apk, dm, 2))
	buzz.inject(buzzMessage(t, usk, "h", group, "", "morning everyone"))
	buzz.inject(buzzMessage(t, usk, "h", group, apk, "@frontend-agent why is the build red?"))
	buzz.inject(buzzMessage(t, usk, "h", group, apk, "and now?"))
	buzz.inject(buzzMessage(t, usk, "h", dm, "", "just the two of us"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url(), buzz.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// The two mentions and the DM are prompts; the group chatter is not — the
	// mention is the p tag, not the @name in the text.
	waitFor(t, "the three addressed messages", func() bool { _, p, _ := f.counts(); return p == 3 })
	sessions, _, _ := f.counts()
	f.mu.Lock()
	texts := make([]string, 0, len(f.prompts))
	for _, p := range f.prompts {
		texts = append(texts, p["parts"].([]any)[0].(map[string]any)["text"].(string))
	}
	f.mu.Unlock()
	slices.Sort(texts) // stored events are dispatched concurrently, so unordered
	for _, want := range []string{"@frontend-agent why is the build red?", "and now?", "just the two of us"} {
		if !slices.Contains(texts, want) {
			t.Fatalf("prompt %q missing from %q", want, texts)
		}
	}

	// One session per channel: the group had two mentions and reused its own.
	if sessions != 2 {
		t.Fatalf("two channels produced %d sessions", sessions)
	}
	for _, channel := range []string{group, dm} {
		if h.convs.get(buzzConversation(channel)) == nil {
			t.Fatalf("no conversation for channel %s", channel)
		}
	}

	// Nothing but the profile crosses to the Buzz relay when the agent has said
	// nothing yet, and no envelope crosses the other way either: a channel
	// conversation answers into the channel, not as a kind-30078.
	if kinds := buzz.channelPublishKinds(); !slices.Equal(kinds, []int{buzzProfileKind}) {
		t.Fatalf("published %v to the buzz relay, want just [%d]", kinds, buzzProfileKind)
	}
	// Presence, so Buzz's UI shows the agent as online rather than offline. It
	// is a heartbeat, not a one-off: the relay keys presence with a 180s TTL
	// against a 60s heartbeat interval, so one event on connect goes stale.
	if !buzz.hasPublished(buzzPresenceKind) {
		t.Fatal("no presence published, so Buzz shows the agent offline")
	}
	// Not nothing this time: one relay list means the message relay is also the
	// channel relay, so the profile and presence reach it too. What must not
	// cross is the envelope — a channel conversation answers into the channel,
	// never as a kind-30078.
	if kinds := relay.channelPublishKinds(); slices.Contains(kinds, messageKind) {
		t.Fatalf("published %v for a buzz conversation, want no envelope", kinds)
	}
}

// A mention reached the agent, and a Buzz client shows nothing, because the
// answer went out as a kind-30078 envelope on NOSTR_RELAYS — a kind Buzz does not
// render in a channel. The answer to a channel message is channel traffic: a
// kind-9 in the same channel, signed by the agent, one message per turn however
// many deltas the turn streamed.
func TestBuzzAnswerIsPostedBackIntoTheChannel(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	buzz := newFakeRelay(t)
	buzz.requireAuth = true
	buzz.store = true

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	// A DM: the human and the agent, which is the shape the report was about.
	dm := "channel-dm-out"
	buzz.inject(buzzMembers(t, usk, apk, dm, 2))
	buzz.inject(buzzMessage(t, usk, "h", dm, "", "@frontend-agent hello"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url(), buzz.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	waitFor(t, "the mention to reach OpenCode", func() bool { _, p, _ := f.counts(); return p == 1 })

	// OpenCode streams the prompt back as its own message before the model runs:
	// a `message.updated` carrying the role, then the text the gateway just sent
	// as a text part of that message. Nothing on the part says whose it is — the
	// user message's part and the assistant's are the same shape — so a reducer
	// that reads parts alone posts the human's own words back into the channel.
	f.push("message.updated", map[string]any{
		"sessionID": "ses_1",
		"info":      map[string]any{"id": "msg_usr", "sessionID": "ses_1", "role": "user"},
	})
	f.push("message.part.updated", map[string]any{
		"sessionID": "ses_1", "delta": "@frontend-agent hello",
		"part": map[string]any{"id": "prt_0", "type": "text", "sessionID": "ses_1",
			"messageID": "msg_usr", "text": "@frontend-agent hello"},
	})

	// The answer streams in as deltas, so the gateway has to assemble it rather
	// than post one message per delta.
	part := func(delta string) {
		f.push("message.part.updated", map[string]any{
			"sessionID": "ses_1", "delta": delta,
			"part": map[string]any{"id": "prt_1", "type": "text", "sessionID": "ses_1", "messageID": "msg_asst"},
		})
	}
	part("on it, ")
	part("one sec")
	f.push("session.idle", map[string]any{"sessionID": "ses_1"})

	waitFor(t, "the answer to reach the buzz relay", func() bool {
		return len(buzz.postsOf(buzzChatKind)) == 1
	})
	post := buzz.postsOf(buzzChatKind)[0]
	if post.Content != "on it, one sec" {
		t.Fatalf("posted %q, want the whole turn in one message", post.Content)
	}
	if post.PubKey != apk {
		t.Fatalf("posted by %s, want the agent's own key %s", shortPub(post.PubKey), shortPub(apk))
	}
	if channel := tagValue(post.Tags, "h"); channel != dm {
		t.Fatalf("h tag %q, want channel %q", channel, dm)
	}
	upk, _ := nostr.GetPublicKey(usk)
	if !post.Tags.ContainsAny("p", []string{upk}) {
		t.Fatalf("no p tag for the person who asked: %v", post.Tags)
	}
	// A kind-30078 envelope for the same conversation would be the old, invisible
	// answer, and it would also land in the same relay as a read-state event.
	// One relay list means this relay also gets the channel answer as a kind 9
	// and the profile and presence on top; only the envelope is wrong here.
	if kinds := relay.publishKinds(); slices.Contains(kinds, messageKind) {
		t.Fatalf("published %v to the message relays, want no envelope", kinds)
	}
	if kinds := buzz.channelPublishKinds(); !slices.Equal(kinds, []int{buzzProfileKind, buzzChatKind}) {
		t.Fatalf("published %v to the buzz relay, want the profile then the answer", kinds)
	}
}

// A relay fans an event out to the connection that published it, so the agent's
// own answer comes back on its own channel subscription. In a DM every message
// counts as addressed to the agent, so without the self-check the agent answers
// itself once per answer.
func TestBuzzIgnoresItsOwnChannelReplies(t *testing.T) {
	f := newFakeOC(t)
	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(context.Background(), nil, reg, h, log)

	// The agent's own kind-9, in a two-member channel, addressed to the agent.
	own := buzzMessage(t, ask, "h", "channel-dm", apk, "the agent's own answer")
	nt.buzzReceive(context.Background(), agent, own, buzzChannels{"channel-dm": 2})
	if _, p, _ := f.counts(); p != 0 {
		t.Fatal("the agent prompted itself on its own channel reply")
	}
	// The same message from the human is still a prompt.
	nt.buzzReceive(context.Background(), agent, buzzMessage(t, usk, "h", "channel-dm", "", "hello"),
		buzzChannels{"channel-dm": 2})
	waitFor(t, "the human's message to reach OpenCode", func() bool { _, p, _ := f.counts(); return p == 1 })
}

// A message the relay delivers but the gateway refuses must say so at Warn,
// naming the pubkey or the channel and what to change: this failure looks
// exactly like an agent ignoring its sender, and all four refusals logged at
// Debug, so a configuration fault was indistinguishable from a quiet channel.
// Only the two configuration faults are Warn — a group channel refuses most of
// what it carries, and a Warn per group message would be noise.
func TestBuzzRefusalsNameTheFault(t *testing.T) {
	usk := nostr.GeneratePrivateKey()
	upk, _ := nostr.GetPublicKey(usk)
	stranger := nostr.GeneratePrivateKey()
	spk, _ := nostr.GetPublicKey(stranger)
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", PubKey: apk, sk: ask,
		Allow: []string{upk}, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	nt := newNostrTransport(context.Background(), nil, reg, newHub(reg, log), log)
	chans := buzzChannels{"channel-dm": 2, "channel-group": 5}

	nt.buzzReceive(context.Background(), agent,
		buzzMessage(t, stranger, "h", "channel-dm", "", "hi"), chans)
	nt.buzzReceive(context.Background(), agent,
		buzzMessage(t, usk, "h", "channel-elsewhere", "", "hi"), chans)
	// A group message with no mention is not a fault, and must stay quiet.
	nt.buzzReceive(context.Background(), agent,
		buzzMessage(t, usk, "h", "channel-group", "", "hi"), chans)

	for _, want := range []string{
		"level=WARN msg=\"buzz: inbound message from a pubkey the agent does not allow\"",
		"from=" + shortPub(spk),
		"msg=\"buzz: inbound message in a channel the agent is not in\"",
		"channel=channel-elsewhere in=channel-dm,channel-group",
		"level=DEBUG msg=\"inbound message not for this agent\"",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log is missing %q:\n%s", want, buf.String())
		}
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 2 {
		t.Errorf("got %d warnings, want the 2 configuration faults:\n%s", n, buf.String())
	}
}

// the same second collide and the loser of the pair gets nothing back — and the
// one-shot REQ that asked for it is never sent again. With NOSTR_RELAYS and
// BUZZ_RELAYS naming the same relay that is a guaranteed race at startup, so the
// first Buzz discovery can come back empty while the agent is in several
// channels. An empty answer has to be re-asked, not believed: the gateway used
// to wait out the whole refresh interval on it and log "agent is in no channel
// yet", which is a lie and leaves the agent deaf to every mention.
func TestBuzzDiscoverySurvivesALostNIP42Handshake(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	buzz := newFakeRelay(t)
	buzz.requireAuth = true
	buzz.store = true
	buzz.dropAuths = 1 // the first handshake is refused, the second is not

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	group := "channel-after-retry"
	buzz.inject(buzzMembers(t, usk, apk, group, 3))
	buzz.inject(buzzMessage(t, usk, "h", group, apk, "@frontend-agent did you get that?"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url(), buzz.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// The mention only arrives on a discovery that worked, so reaching OpenCode
	// is proof the retry happened: the member list was there all along.
	waitFor(t, "the mention after a refused handshake", func() bool { _, p, _ := f.counts(); return p == 1 })
	if h.convs.get(buzzConversation(group)) == nil {
		t.Fatal("the channel the agent is in has no conversation")
	}
	if kinds := buzz.channelPublishKinds(); !slices.Equal(kinds, []int{buzzProfileKind}) {
		t.Fatalf("published %v to the buzz relay, want just [%d]", kinds, buzzProfileKind)
	}
}

// Production wires NOSTR_RELAYS and BUZZ_RELAYS to the same relay — the Buzz
// relay is where the kind-30078 answers have to land, and it is also the only
// one the agent is a member of. That puts the message listener and the Buzz
// discovery on one websocket, and go-nostr keys its NIP-42 OK waiters by event
// id (relay.go:371): the two handshakes are built in the same second, so they
// are byte-identical, the second Store overwrites the first, and the loser of
// the pair waits out its entire context instead of returning. The Buzz
// discovery holds a 15s deadline, so it came back empty after 15s with the
// member list sitting right there, the agent subscribed to no channel at all,
// and every mention went unheard for a whole refresh interval.
func TestBuzzDiscoveryOnARelaySharedWithTheMessageListener(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.requireAuth = true
	relay.store = true

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	group := "channel-shared-relay"
	relay.inject(buzzMembers(t, usk, apk, group, 3))
	relay.inject(buzzMessage(t, usk, "h", group, apk, "@frontend-agent are you there?"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// The mention only arrives on a discovery that answered, so reaching
	// OpenCode inside waitFor's 5s is proof the handshake did not stall: a
	// stalled one takes the whole buzzFetchTimeout.
	waitFor(t, "the mention on a relay that is also a message relay", func() bool {
		_, p, _ := f.counts()
		return p == 1
	})
	if h.convs.get(buzzConversation(group)) == nil {
		t.Fatal("the channel the agent is in has no conversation")
	}
	// And the two roles must not have shared a connection. Which of the
	// colliding handshakes loses is a coin flip, so this is what makes the
	// assertion above deterministic.
	if n := relay.openedCount(); n < 2 {
		t.Fatalf("%d connection to a relay filling both roles, want one per role", n)
	}
}

// The agent profile (kind 10100) is what makes the pubkey show up as an agent in
// Buzz, and the Buzz UI is how an agent gets added to a channel in the first
// place. Gating the profile on a discovery that found a channel is a bootstrap
// deadlock: a relay that serves member lists naming no channel in particular
// leaves the agent unregistered, and an unregistered agent can never be added to
// the first one. A member list is all the proof of NIP-42 the profile needs.
func TestBuzzProfileIsPublishedWhenNoRosterNamesTheAgent(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.requireAuth = true
	relay.store = true

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	// A member list with no channel identifier: the relay answered the
	// discovery, and nothing it sent names a channel this agent is in.
	roster := &nostr.Event{Kind: buzzMemberKind, CreatedAt: nostr.Now(), Tags: nostr.Tags{{"p", apk}}}
	if err := roster.Sign(usk); err != nil {
		t.Fatal(err)
	}
	relay.inject(roster)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	waitFor(t, "the agent profile before any channel exists", func() bool {
		return slices.Equal(relay.channelPublishKinds(), []int{buzzProfileKind})
	})
}

// buzzMembers is a NIP-29 member list: d is the channel uuid and there is one p
// tag per member, the agent among them. The other members only need to exist —
// the gateway counts them, it does not resolve them.
func buzzMembers(t *testing.T, sk, agent, channel string, members int) *nostr.Event {
	t.Helper()
	tags := nostr.Tags{{"d", channel}, {"name", channel}, {"p", agent}}
	for i := 1; i < members; i++ {
		tags = append(tags, nostr.Tag{"p", strings.Repeat("ef", 32)})
	}
	ev := &nostr.Event{Kind: buzzMemberKind, CreatedAt: nostr.Now(), Tags: tags}
	if err := ev.Sign(sk); err != nil { // go-nostr drops an event whose signature fails
		t.Fatal(err)
	}
	return ev
}

// buzzMessage is a NIP-29 chat message: h is the channel, and p is set only when
// the agent was mentioned.
// tag names the channel explicitly: the gateway reads the channel off both "h"
// and "d" because buzzDiscover uses "d" on the member list and nothing in the
// protocol pins chat messages to one of them.
func buzzMessage(t *testing.T, sk, tag, channel, mention, text string) *nostr.Event {
	t.Helper()
	tags := nostr.Tags{{tag, channel}}
	if mention != "" {
		tags = append(tags, nostr.Tag{"p", mention})
	}
	ev := &nostr.Event{Kind: buzzChatKind, CreatedAt: nostr.Now(), Tags: tags, Content: text}
	if err := ev.Sign(sk); err != nil {
		t.Fatal(err)
	}
	return ev
}

// One list, one relay, both roles, both directions. This is what the two
// relay variables could not say: nothing here marks a relay as the encrypted
// kind or the channel kind, so a single relay has to carry both listeners,
// both kinds of answer, and the profile and presence that used to need a
// BUZZ_RELAYS of their own.
func TestOneRelayListServesBothRoles(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.requireAuth = true
	relay.store = true

	usk := nostr.GeneratePrivateKey()
	upk, _ := nostr.GetPublicKey(usk)
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask, ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	// The agent is in a channel, so the channel listener has a name to ask for.
	channel := "channel-one-relay"
	relay.inject(buzzMembers(t, usk, apk, channel, 3))
	relay.inject(buzzMessage(t, usk, "h", channel, apk, "@frontend-agent and you?"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	waitFor(t, "the channel mention", func() bool { _, p, _ := f.counts(); return p == 1 })

	// The turn, answered as a kind 9 in the channel it came from and addressed
	// to the person who asked.
	f.push("message.part.updated", map[string]any{
		"sessionID": "ses_1", "delta": "on it",
		"part": map[string]any{"id": "prt_1", "type": "text", "sessionID": "ses_1", "messageID": "msg_asst"},
	})
	f.push("session.idle", map[string]any{"sessionID": "ses_1"})
	waitFor(t, "the channel answer", func() bool { return len(relay.postsOf(buzzChatKind)) == 1 })
	post := relay.postsOf(buzzChatKind)[0]
	if got := tagValue(post.Tags, "h"); got != channel {
		t.Fatalf("h tag %q, want channel %q", got, channel)
	}
	if !post.Tags.ContainsAny("p", []string{upk}) {
		t.Fatalf("no p tag for the person who asked: %v", post.Tags)
	}

	// And the encrypted side, on the same relay: an inbound envelope becomes a
	// prompt, and its answer goes back out as a kind-30078 envelope.
	ck, err := nip44.GenerateConversationKey(apk, usk)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(Envelope{
		V: protocolVersion, Conversation: "nostr-1", Agent: "frontend-agent",
		Type: TypeMessage, Payload: Payload{Text: "hello over nostr"},
	})
	cipher, err := nip44.Encrypt(string(body), ck)
	if err != nil {
		t.Fatal(err)
	}
	in := nostr.Event{
		Kind: messageKind, CreatedAt: nostr.Now(),
		Tags: nostr.Tags{{"p", apk}}, Content: cipher,
	}
	if err := in.Sign(usk); err != nil {
		t.Fatal(err)
	}
	relay.inject(&in)
	waitFor(t, "the encrypted message", func() bool {
		_, p, _ := f.counts()
		return p == 2
	})
	waitFor(t, "the envelope answer", func() bool { return relay.hasPublished(messageKind) })

	// Plus the two events a Buzz relay used to be needed for at all.
	if !relay.hasPublished(buzzProfileKind) {
		t.Fatal("no agent profile, so the pubkey does not show up in Buzz as an agent")
	}
	if !relay.hasPublished(buzzPresenceKind) {
		t.Fatal("no presence, so Buzz shows the agent offline")
	}
}

// listen built its filter once, before the reconnect loop, so every
// resubscribe carried the original `since` — one relay restart and the relay
// re-sent everything published since process start, each event decrypted and
// re-prompted into the session as a duplicate. buzzListen has always rebuilt
// its own filter per pass; this is the guard that the envelope listener does
// too. The fake relay cannot honour `since` (it replays its store regardless),
// so what is asserted is the thing that decides it: a second REQ that asks
// again from now, not from startup.
func TestResubscribeRebuildsItsSince(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.store = true
	relay.kickMessage = 1

	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	reg := &Registry{byName: map[string]*Agent{
		"frontend-agent": {Name: "frontend-agent", OpenCode: f.URL, PubKey: apk,
			sk: ask, ck: map[string][32]byte{}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	messageFilters := func() []nostr.Timestamp {
		var out []nostr.Timestamp
		for _, f := range relay.filters {
			if hasKind(f, messageKind) && f.Since != nil {
				out = append(out, *f.Since)
			}
		}
		return out
	}
	// The kick lands right after the first EOSE, then the listener backs off
	// before asking again — at least reconnectBase, so the two timestamps are
	// never the same second.
	waitFor(t, "the listener resubscribed", func() bool {
		relay.mu.Lock()
		defer relay.mu.Unlock()
		return len(messageFilters()) >= 2
	})

	relay.mu.Lock()
	defer relay.mu.Unlock()
	since := messageFilters()
	if since[1] <= since[0] {
		t.Fatalf("the second REQ asked from %d, not later than the first's %d: the filter was "+
			"built outside the reconnect loop, so this resubscribe replays everything since "+
			"process start", since[1], since[0])
	}
}

// The kind-39002 query used to carry `#p=<agent>`, which is membership — a
// gateway decision — pushed into a relay-side filter, the same rule the rest of
// the gateway keeps for `authors` and every other tag: the relay drops what does
// not match before delivering, so a refusal and a broken relay come back
// identical. The concrete damage was that the two answers became
// indistinguishable. An agent in no channel has no member list naming it, so a
// `#p` filter returned nothing and the log blamed the relay for withholding
// 39002 — "check the relay serves it to this pubkey, and that the pubkey is a
// relay member" — while the fix the operator needed was `buzz-admin
// add-member`. The roster that branch exists to report was never reachable at
// all, because a `#p` query can only return a roster that names the agent.
//
// Membership is read off the rosters instead. It also keeps working on a relay
// that does not route 39002 on `p`.
func TestBuzzDiscoveryAsksForRostersNotMembership(t *testing.T) {
	f := newFakeOC(t)
	relay := newFakeRelay(t)
	relay.store = true

	usk := nostr.GeneratePrivateKey()
	ask := nostr.GeneratePrivateKey()
	apk, _ := nostr.GetPublicKey(ask)
	other, _ := nostr.GetPublicKey(nostr.GeneratePrivateKey())
	agent := &Agent{Name: "frontend-agent", OpenCode: f.URL, PubKey: apk, sk: ask,
		ck: map[string][32]byte{}}
	reg := &Registry{byName: map[string]*Agent{agent.Name: agent}}

	// The relay answers discovery with a member list, and the agent is not on it.
	relay.inject(buzzMembers(t, usk, other, "channel-others", 3))

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	waitFor(t, "the member-list REQ", func() bool {
		relay.mu.Lock()
		defer relay.mu.Unlock()
		return slices.ContainsFunc(relay.filters, func(f nostr.Filter) bool {
			return hasKind(f, buzzMemberKind)
		})
	})
	relay.mu.Lock()
	for _, got := range relay.filters {
		if !hasKind(got, buzzMemberKind) {
			continue
		}
		if len(got.Tags) != 0 || len(got.Authors) != 0 {
			t.Errorf("discovery asked the relay to adjudicate membership: %#v; the relay drops "+
				"what does not match before delivering, so the refusal is unloggable", got)
		}
	}
	relay.mu.Unlock()

	// A roster the agent is not on is a real answer, and has to be reported as
	// the wait for a join it is — not as a relay that serves nothing.
	waitFor(t, "the no-channel report", func() bool {
		return strings.Contains(buf.String(), "buzz: agent is in no channel yet")
	})
	for _, notWanted := range []string{
		"buzz: discovery came back with no member lists",
		"buzz: relay serves no member lists for this agent",
	} {
		if strings.Contains(buf.String(), notWanted) {
			t.Errorf("a served member list was reported as %q:\n%s", notWanted, buf.String())
		}
	}
}

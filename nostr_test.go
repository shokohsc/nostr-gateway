//go:build !race

// The Nostr transport is not exercised under -race: go-nostr v0.52.3 races its
// own connect path (Relay.ConnectWithTLS writing r.Connection while Relay.close
// reads it, relay.go:175 vs :576) as soon as the pool subscribes. v0.52.3 is the
// latest release, so the race cannot be fixed from here. Everything below still
// runs in the normal suite; only the race detector skips it.

package main

import (
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
	// store makes the relay answer a REQ with everything injected so far before
	// its EOSE, like a real relay's stored events. It is off by default because
	// the other tests only ever need live fan-out; a client that has to discover
	// state by querying (the Buzz channel list) needs it.
	store     bool
	stored    []*nostr.Event
	published int // events the gateway pushed here, as opposed to broadcast
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
			r.published++
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
			c.mu.Lock()
			c.subs = append(c.subs, subID)
			c.mu.Unlock()
			r.mu.Lock()
			r.subs++
			stored := append([]*nostr.Event(nil), r.stored...)
			r.mu.Unlock()
			for _, ev := range stored {
				c.write([]any{"EVENT", subID, ev})
			}
			c.write([]any{"EOSE", subID})
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
		ev.Tags.Find("challenge").Value() != authChallenge ||
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

func (r *fakeRelay) publishCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.published
}

func (r *fakeRelay) authCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.auths
}

func (r *fakeRelay) dial() *relayConn {
	ws, _, err := websocket.Dial(context.Background(), r.url(), nil)
	if err != nil {
		panic(err)
	}
	c := &relayConn{ws: ws, ctx: context.Background()}
	r.mu.Lock()
	r.conns[c] = struct{}{}
	r.mu.Unlock()
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
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
	if got := ev.Tags.Find("p").Value(); got != upk {
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
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
	nt := newNostrTransport(ctx, []string{relay.url()}, nil, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	// One challenge answered per agent, and both subscriptions through.
	waitFor(t, "every agent authenticated", func() bool {
		return relay.authCount() == 2 && relay.subscribeCount() == 2
	})

	usk := nostr.GeneratePrivateKey()
	apk := reg.byName["alpha"].PubKey
	ck, _ := nip44.GenerateConversationKey(apk, usk)
	relay.injectFrom(t, usk, apk, ck, Envelope{
		Conversation: "closed-1", Agent: "alpha", Type: TypeMessage, Payload: Payload{Text: "still there?"},
	})
	waitFor(t, "message through a closed relay", func() bool { _, p, _ := f.counts(); return p == 1 })
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
	buzz.inject(buzzMessage(t, usk, group, "", "morning everyone"))
	buzz.inject(buzzMessage(t, usk, group, apk, "@frontend-agent why is the build red?"))
	buzz.inject(buzzMessage(t, usk, group, apk, "and now?"))
	buzz.inject(buzzMessage(t, usk, dm, "", "just the two of us"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	nt := newNostrTransport(ctx, []string{relay.url()}, []string{buzz.url()}, reg, h, log)
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

	// The Buzz relays are subscribed to and nothing else: the agent's answers
	// are kind-30078 envelopes for the sender's key on the normal relays.
	if n := buzz.publishCount(); n != 0 {
		t.Fatalf("published %d events to the buzz relay, want 0", n)
	}
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
func buzzMessage(t *testing.T, sk, channel, mention, text string) *nostr.Event {
	t.Helper()
	tags := nostr.Tags{{"h", channel}}
	if mention != "" {
		tags = append(tags, nostr.Tag{"p", mention})
	}
	ev := &nostr.Event{Kind: buzzChatKind, CreatedAt: nostr.Now(), Tags: tags, Content: text}
	if err := ev.Sign(sk); err != nil {
		t.Fatal(err)
	}
	return ev
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/nbd-wtf/go-nostr/nip44"
)

// messageKind is an application-defined longform-content kind (the 30000-39999
// range of NIP-31 is meant for exactly this).
const messageKind = 30078

// nostrTransport carries the agent protocol over Nostr. Every envelope is
// NIP-44 encrypted between the user's key and the agent's key, so relays and
// observers see only ciphertext: kind, timestamps and tags.
//
// It is one transport among others, not a special case — it calls the same hub
// Handle/emit the HTTP API does.
type nostrTransport struct {
	pools  map[string]*nostr.SimplePool
	relays []string
	// buzzRelays are subscribed to and never published to, apart from the
	// agent profile; see buzz.go.
	buzzRelays []string
	// buzzPools are the same agents' pools for buzzRelays, one connection
	// each. They are separate from pools because a relay named in both lists
	// would otherwise put the kind-30078 listener and the Buzz discovery on one
	// websocket, where their NIP-42 handshakes collide: go-nostr keys its OK
	// waiters by event id (relay.go:371), two handshakes built in the same
	// second are byte-identical, the second Store overwrites the first, and the
	// loser waits out its whole context instead of returning. The Buzz
	// discovery holds a 15s deadline, so it stalled for all of it, came back
	// empty with the member list right there, and the agent heard nothing.
	buzzPools map[string]*nostr.SimplePool
	// profiled remembers which agents have published their Buzz agent profile.
	// One writer per key — that agent's buzzListen goroutine.
	profiled map[string]bool
	reg      *Registry
	hub      *hub
	log      *slog.Logger
	out      chan job
}

type job struct {
	agent *Agent
	peer  string
	env   Envelope
}

// One pool per agent, not one pool for the gateway. A closed relay authenticates
// a connection as exactly one NIP-42 identity and then rejects any event signed
// by a different key, so a shared pool would have the agents fight over the one
// authenticated identity — the losers go deaf with no error anywhere.
//
// And one pool per role, for the same reason one layer down: a relay in both
// NOSTR_RELAYS and BUZZ_RELAYS gets a second pool, so the message listener and
// the Buzz discovery never answer a challenge on the same connection.
func newNostrTransport(ctx context.Context, relays, buzzRelays []string, reg *Registry, h *hub, log *slog.Logger) *nostrTransport {
	n := &nostrTransport{
		pools:      map[string]*nostr.SimplePool{},
		relays:     relays,
		buzzRelays: buzzRelays,
		buzzPools:  map[string]*nostr.SimplePool{},
		profiled:   map[string]bool{},
		reg:        reg,
		hub:        h,
		log:        log,
		out:        make(chan job, 256),
	}
	for _, name := range reg.names() {
		a := reg.byName[name]
		if a.sk == "" {
			log.Warn("agent has no nsec, skipping nostr", "agent", name)
			continue
		}
		n.pools[name] = newAgentPool(ctx, a, log)
		if len(buzzRelays) > 0 {
			n.buzzPools[name] = newAgentPool(ctx, a, log)
		}
	}
	return n
}

// newAgentPool builds one agent's pool: it authenticates as that agent's key and
// nothing else, and keeps relay NOTICEs inside the gateway's own log level.
func newAgentPool(ctx context.Context, a *Agent, log *slog.Logger) *nostr.SimplePool {
	return nostr.NewSimplePool(ctx,
		nostr.WithAuthHandler(func(_ context.Context, authEvent nostr.RelayEvent) error {
			// The pool builds the kind-22242 event; answering the
			// challenge with the agent's own key is the whole job.
			return authEvent.Sign(a.sk)
		}),
		// Without this go-nostr prints NOTICEs to the stdlib logger,
		// straight past the gateway's own log level.
		nostr.WithRelayOptions(nostr.WithNoticeHandler(func(msg string) {
			log.Warn("nostr notice", "agent", a.Name, "notice", msg)
		})),
	)
}

func (n *nostrTransport) run(ctx context.Context) {
	go n.worker(ctx)
	for _, name := range n.reg.names() {
		if n.pools[name] != nil {
			a := n.reg.byName[name]
			go n.listen(ctx, a)
			if n.buzzPools[name] != nil {
				// Its own pool, so the Buzz connection authenticates as the
				// agent's own key on a connection of its own, which is what a
				// closed Buzz relay checks and what keeps its handshake from
				// colliding with the listener's.
				go n.buzzListen(ctx, a)
			}
		}
	}
	<-ctx.Done()
	for _, p := range n.pools {
		p.Close("shutdown")
	}
	for _, p := range n.buzzPools {
		p.Close("shutdown")
	}
}

// worker publishes queued envelopes one at a time so relay traffic keeps the
// order the agent produced it in.
func (n *nostrTransport) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-n.out:
			if err := n.publish(ctx, j.agent, j.peer, j.env); err != nil {
				n.log.Error("nostr publish", "agent", j.agent.Name, "err", err)
			}
		}
	}
}

// listen subscribes to an agent's kind-30078 events. A CLOSED from a relay (or
// a dropped connection) ends the subscription rather than the agent, so the
// loop reconnects instead of leaving the agent deaf. The pool re-authenticates
// and re-subscribes on its own when the closed reason is an auth challenge, so
// a closed relay reaches the second REQ without help from here.
func (n *nostrTransport) listen(ctx context.Context, a *Agent) {
	pool := n.pools[a.Name]
	filter := nostr.Filter{
		Kinds:   []int{messageKind},
		Tags:    nostr.TagMap{"p": []string{a.PubKey}},
		Authors: a.Allow,
		// A small Since avoids replaying a fresh deployment's stored history
		// into brand-new OpenCode sessions. ponytail: a few seconds of slack
		// because relay clocks differ; a durable cursor is the upgrade.
		Since: ptr(nostr.Now() - 5),
	}
	interval := reconnectBase
	for ctx.Err() == nil {
		connected := time.Now()
		for ie := range pool.SubscribeMany(ctx, n.relays, filter) {
			if ie.Event == nil {
				continue
			}
			if err := n.receive(ctx, a, ie.Event); err != nil {
				n.log.Warn("nostr inbound", "agent", a.Name, "err", err)
			}
		}
		if ctx.Err() != nil {
			return
		}
		interval = reconnectDelay(interval, time.Since(connected))
		n.log.Warn("nostr subscription ended, resubscribing", "agent", a.Name, "in", interval)
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}
	}
}

// receive decrypts one relay event and hands the envelope to the hub. Handling
// is synchronous so a conversation's messages stay in the order they arrived.
func (n *nostrTransport) receive(ctx context.Context, a *Agent, ev *nostr.Event) error {
	// The allow list is checked before any key work, so a blocked sender costs
	// nothing beyond the frame the relay already delivered.
	if !a.allows(ev.PubKey) {
		return nil
	}
	ck, err := a.conversationKey(ev.PubKey)
	if err != nil {
		return fmt.Errorf("conversation key: %w", err)
	}
	plain, err := nip44.Decrypt(ev.Content, ck)
	if err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}
	var in Envelope
	if err := json.Unmarshal([]byte(plain), &in); err != nil {
		return fmt.Errorf("parse envelope: %w", err)
	}
	// The event signature is the identity, not the envelope.
	if npub, err := nip19.EncodePublicKey(ev.PubKey); err == nil {
		in.Sender = npub
	}
	if in.Agent == "" {
		in.Agent = a.Name
	}
	conv := in.Conversation
	if conv == "" {
		conv = newID("conv")
	}
	if _, err := n.hub.Handle(ctx, a, in, ev.PubKey); err != nil {
		// A protocol is asynchronous, so a rejected message must come back as
		// an answer, not as silence the sender cannot distinguish from a lost
		// relay event.
		n.reply(a, ev.PubKey, Envelope{
			V: protocolVersion, Conversation: conv, Agent: a.Name, Sender: a.npub(),
			Type: TypeError, Timestamp: time.Now().UTC(),
			Payload: Payload{Error: err.Error(), Text: err.Error()},
		})
	}
	return nil
}

func (n *nostrTransport) send(a *Agent, peer string, env Envelope) error {
	if a.sk == "" {
		return fmt.Errorf("agent %s has no private key", a.Name)
	}
	select {
	case n.out <- job{agent: a, peer: peer, env: env}:
		return nil
	case <-time.After(5 * time.Second):
		// Backpressure beats silently dropping: the emitter is an OpenCode
		// stream reader and tolerates a short wait better than a lost event.
		return fmt.Errorf("nostr send queue full, blocked event %s", env.ID)
	}
}

// reply sends one envelope straight out, bypassing the ordered queue: it is a
// terminal answer to a message that will produce nothing else.
func (n *nostrTransport) reply(a *Agent, peer string, env Envelope) {
	if err := n.publish(context.Background(), a, peer, env); err != nil {
		n.log.Error("nostr reply", "agent", a.Name, "err", err)
	}
}

func (n *nostrTransport) publish(ctx context.Context, a *Agent, peer string, env Envelope) error {
	pool, ok := n.pools[a.Name]
	if !ok {
		return fmt.Errorf("agent %s has no nostr pool", a.Name)
	}
	ck, err := a.conversationKey(peer)
	if err != nil {
		return err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	content, err := nip44.Encrypt(string(body), ck)
	if err != nil {
		return err
	}
	ev := nostr.Event{
		Kind:      messageKind,
		CreatedAt: nostr.Now(),
		// 30078 is inside the NIP-33 parameterized-replaceable range, so a
		// conforming relay keys it by (kind, author, d) and keeps only the
		// newest event for each key — with no d tag that is every message
		// this agent has ever sent. One d tag per envelope gives each message
		// its own coordinate, so history stays append-only.
		Tags:    nostr.Tags{{"p", peer}, {"d", env.ID}},
		Content: content,
	}
	if err := ev.Sign(a.sk); err != nil {
		return err
	}

	var firstErr error
	for res := range pool.PublishMany(ctx, n.relays, ev) {
		if res.Error != nil && firstErr == nil {
			firstErr = res.Error
		}
	}
	return firstErr
}

func ptr[T any](v T) *T { return &v }

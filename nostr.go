package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	// buzzPools are the same agents' pools for the channel events — kind 9, the
	// member-list discovery, the agent profile and presence — one connection each
	// over the same relay list. They are separate from pools not because those
	// relays are a different kind of relay (there is no such kind here, see
	// relayList) but because two subscriptions answering a NIP-42 challenge on one
	// websocket collide: go-nostr keys its OK waiters by event id
	// (relay.go:371), two handshakes built in the same second are byte-identical,
	// the second Store overwrites the first, and the loser waits out its whole
	// context instead of returning. The Buzz discovery holds a 15s deadline, so it
	// stalled for all of it, came back empty with the member list right there, and
	// the agent heard nothing.
	buzzPools map[string]*nostr.SimplePool
	// turns holds the answer in flight per Buzz conversation, so a turn becomes
	// one channel message instead of one per streaming delta. Owned by the
	// publish worker goroutine, which is its only writer.
	turns map[string]*strings.Builder
	// profiled remembers which agents have published their Buzz agent profile.
	// One writer per key — that agent's buzzListen goroutine.
	profiled map[string]bool
	// warned remembers which (agent, fault) pairs have already been logged at
	// Warn, so a state that repeats every minute is said once loudly and then at
	// debug. Same one-writer-per-key rule as profiled.
	warned map[string]bool
	// stateMu guards warned and profiled. "One writer per key" is not a safe
	// rule for a Go map: both maps are shared across every agent's buzzListen
	// goroutine, and a grow in one goroutine relocates the buckets another is
	// reading. Disjoint keys do not make concurrent access to one map legal —
	// it is a race with two agents, and the runtime terminates the process on
	// it rather than corrupting a value.
	stateMu sync.Mutex
	reg     *Registry
	hub     *hub
	log     *slog.Logger
	out     chan job
}

// job is one outbound envelope. buzz is the channel uuid when the conversation
// is a Buzz channel and "" otherwise, which is what decides whether the answer
// goes into a channel or out as a kind-30078 envelope.
type job struct {
	agent *Agent
	peer  string
	buzz  string
	env   Envelope
}

// One pool per agent, not one pool for the gateway. A closed relay authenticates
// a connection as exactly one NIP-42 identity and then rejects any event signed
// by a different key, so a shared pool would have the agents fight over the one
// authenticated identity — the losers go deaf with no error anywhere.
//
// And two pools per agent, for the same reason one layer down: the encrypted
// listener and the channel listener never share a websocket, so their NIP-42
// handshakes cannot collide (see buzzPools). Both are opened against every relay
// in relays — one list, both roles, no relay type.
func newNostrTransport(ctx context.Context, relays []string, reg *Registry, h *hub, log *slog.Logger) *nostrTransport {
	n := &nostrTransport{
		pools:     map[string]*nostr.SimplePool{},
		relays:    relays,
		buzzPools: map[string]*nostr.SimplePool{},
		turns:     map[string]*strings.Builder{},
		profiled:  map[string]bool{},
		warned:    map[string]bool{},
		reg:       reg,
		hub:       h,
		log:       log,
		out:       make(chan job, 256),
	}
	for _, name := range reg.names() {
		a := reg.byName[name]
		if a.sk == "" {
			log.Warn("agent has no nsec, skipping nostr", "agent", name)
			continue
		}
		n.pools[name] = newAgentPool(ctx, a, log)
		n.buzzPools[name] = newAgentPool(ctx, a, log)
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
			// Its own pool, so the channel connection authenticates as the
			// agent's own key on a connection of its own, which is what a closed
			// relay checks and what keeps its handshake from colliding with the
			// listener's.
			go n.buzzListen(ctx, a)
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
// order the agent produced it in. It is also the only writer of the in-flight
// Buzz turns, so an answer is posted as one message the moment its turn ends.
func (n *nostrTransport) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-n.out:
			var err error
			if j.buzz != "" {
				err = n.buzzJob(ctx, j)
			} else {
				err = n.publish(ctx, j.agent, j.peer, j.env)
			}
			if err != nil {
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
	interval := reconnectBase
	for ctx.Err() == nil {
		connected := time.Now()
		// Built inside the loop, not outside it. A filter constructed once
		// carried its original `Since` for the life of the process, so every
		// resubscribe asked the relay for everything published since startup
		// and replayed it: months of stored history decrypted and re-prompted
		// into the session, one duplicate answer per event, from one relay
		// restart. `buzzListen` has always rebuilt its own filter per pass;
		// this one did not, and nothing in the tests ever resubscribes.
		filter := nostr.Filter{
			Kinds: []int{messageKind},
			// The p tag is the addressing, not a narrowing: a kind-30078 envelope is
			// a message to one agent, and the relay matches `#p` to decide who gets
			// it. go-nostr applies the same filter client-side, so this is enforced
			// twice and dropping it here would hand every agent on the relay every
			// other agent's mail — receive() would decrypt it, because the
			// conversation key comes from the sender and the agent, not from the tag.
			// receive() checks the tag as well, so a client that does not tag `p` for
			// this agent produces a log line instead of silence.
			//
			// `authors` must never go here, and neither may any other tag: a relay
			// applies those before delivery, so nothing runs and nothing logs, and
			// the only evidence is the relay's own view of a REQ. That cost a day for
			// the allow list, which belongs in allows() and nowhere else.
			// A small Since avoids replaying a fresh deployment's stored history
			// into brand-new OpenCode sessions. ponytail: a few seconds of slack
			// because relay clocks differ; a durable cursor is the upgrade.
			Tags:  nostr.TagMap{"p": []string{a.PubKey}},
			Since: ptr(nostr.Now() - 5),
		}
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
		n.drop(a, ev, "sender is not on the allow list")
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
	// Minted here rather than left for Handle: Handle mints its own when the
	// envelope carries none, and the failure reply below then named a
	// conversation the gateway had never registered — the sender's retry opened
	// a second conversation and a second OpenCode session, orphaning the first.
	if in.Conversation == "" {
		in.Conversation = newID("conv")
	}
	if _, err := n.hub.Handle(ctx, a, in, ev.PubKey); err != nil {
		// A protocol is asynchronous, so a rejected message must come back as
		// an answer, not as silence the sender cannot distinguish from a lost
		// relay event.
		n.reply(a, ev.PubKey, Envelope{
			V: protocolVersion, Conversation: in.Conversation, Agent: a.Name, Sender: a.npub(),
			Type: TypeError, Timestamp: time.Now().UTC(),
			Payload: Payload{Error: err.Error(), Text: err.Error()},
		})
	}
	return nil
}

// send queues one outbound envelope. A conversation that is a Buzz channel goes
// into that channel as a kind-9 message; anything else goes out as the encrypted
// kind-30078 envelope the protocol has always used. The conversation id carries
// the route, so the hub — which is transport-blind — does not have to know.
func (n *nostrTransport) send(a *Agent, peer string, env Envelope) error {
	if a.sk == "" {
		return fmt.Errorf("agent %s has no private key", a.Name)
	}
	buzz, _ := buzzChannelOf(env.Conversation)
	select {
	case n.out <- job{agent: a, peer: peer, buzz: buzz, env: env}:
		return nil
	case <-time.After(5 * time.Second):
		// Backpressure beats silently dropping: the emitter is an OpenCode
		// stream reader and tolerates a short wait better than a lost event.
		return fmt.Errorf("nostr send queue full, blocked event %s", env.ID)
	}
}

// reply sends one envelope straight out, bypassing the ordered queue: it is a
// terminal answer to a message that will produce nothing else. The deadline is
// the point — this runs on the listener goroutine, so a publish with no deadline
// is a relay that never answers holding that agent's inbound stream open.
func (n *nostrTransport) reply(a *Agent, peer string, env Envelope) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	var err error
	if channel, ok := buzzChannelOf(env.Conversation); ok {
		err = n.buzzPost(ctx, a, channel, peer, env.Payload.Text)
	} else {
		err = n.publish(ctx, a, peer, env)
	}
	if err != nil {
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
	return n.publishTo(ctx, pool, ev)
}

// publishTo sends one event to every relay on one pool.
//
// A refusal from one relay is not a failure of the publish. Every relay in the
// list now serves both roles, so a channel answer goes to relays that have never
// heard of that channel and to public ones that hold no membership of this agent,
// and a refusal from those says nothing about the reader having seen the answer
// — it only says this relay is not the one that hosts it. Each refusal is
// therefore logged at debug with the relay that made it, and the caller only ever
// hears about a publish that every relay in the list refused, which is the case
// worth a Warn. A pool with no relays to ask publishes nothing and is not a
// failure: relayList never hands one out (it falls back to the defaults), and
// buzzProfile is retried on the next discovery.
func (n *nostrTransport) publishTo(ctx context.Context, pool *nostr.SimplePool, ev nostr.Event) error {
	var firstErr error
	taken := false
	for res := range pool.PublishMany(ctx, n.relays, ev) {
		if res.Error == nil {
			taken = true
			continue
		}
		if firstErr == nil {
			firstErr = res.Error
		}
		n.log.Debug("relay refused an event", "kind", ev.Kind, "relay", res.RelayURL, "err", res.Error)
	}
	if taken || firstErr == nil {
		return nil
	}
	return firstErr
}

// warnOnce says a fault at Warn the first time an agent runs into it and at debug
// after that. The Buzz listener wakes every buzzRefresh — a minute — and finds
// the same state waiting for it, so a relay list that serves no NIP-29 at all
// (which any list of public relays now does) used to put a Warn per agent per
// minute on top of each other. The first line says what is wrong and what to do
// about it; the repeats are the same line again, and they bury the next fault.
// The first time is the whole point of a Warn, so a state that clears and comes
// back later still only gets said once.
func (n *nostrTransport) warnOnce(a *Agent, fault, msg string, args ...any) {
	key := a.Name + "|" + fault
	args = append([]any{"agent", a.Name}, args...)
	n.stateMu.Lock()
	first := !n.warned[key]
	n.warned[key] = true
	n.stateMu.Unlock()
	if !first {
		n.log.Debug(msg, args...)
		return
	}
	n.log.Warn(msg, args...)
}

// tagValue is Tags.Find + Tag.Value, which go-nostr has deprecated in favour of
// writing the indexing inline. Find already guarantees a two-element tag, so the
// only thing worth keeping is the empty case.
func tagValue(tags nostr.Tags, key string) string {
	if t := tags.Find(key); t != nil {
		return t[1]
	}
	return ""
}

func ptr[T any](v T) *T { return &v }

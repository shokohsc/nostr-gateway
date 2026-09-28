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
	pool   *nostr.SimplePool
	relays []string
	reg    *Registry
	hub    *hub
	log    *slog.Logger
	out    chan job
}

type job struct {
	agent *Agent
	peer  string
	env   Envelope
}

func newNostrTransport(ctx context.Context, relays []string, reg *Registry, h *hub, log *slog.Logger) *nostrTransport {
	return &nostrTransport{
		pool:   nostr.NewSimplePool(ctx),
		relays: relays,
		reg:    reg,
		hub:    h,
		log:    log,
		out:    make(chan job, 256),
	}
}

func (n *nostrTransport) run(ctx context.Context) {
	go n.worker(ctx)
	for _, name := range n.reg.names() {
		a := n.reg.byName[name]
		if a.sk == "" {
			n.log.Warn("agent has no nsec, skipping nostr", "agent", name)
			continue
		}
		go n.listen(ctx, a)
	}
	<-ctx.Done()
	n.pool.Close("shutdown")
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
// loop reconnects instead of leaving the agent deaf.
func (n *nostrTransport) listen(ctx context.Context, a *Agent) {
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
		for ie := range n.pool.SubscribeMany(ctx, n.relays, filter) {
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
		Tags:      nostr.Tags{{"p", peer}},
		Content:   content,
	}
	if err := ev.Sign(a.sk); err != nil {
		return err
	}

	var firstErr error
	for res := range n.pool.PublishMany(ctx, n.relays, ev) {
		if res.Error != nil && firstErr == nil {
			firstErr = res.Error
		}
	}
	return firstErr
}

func ptr[T any](v T) *T { return &v }

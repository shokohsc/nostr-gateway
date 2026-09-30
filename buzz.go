package main

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// Buzz (block/buzz) is a Nostr workspace that speaks NIP-29: a channel is a
// group, a message is kind 9 with an #h tag for the channel uuid, an @mention is
// that message with a p tag for the mentioned pubkey, and a DM is just a channel
// the agent shares with one other person. Relay fan-out keeps channel-scoped and
// global subscriptions apart, so a subscription only receives a channel's
// messages when it names that channel in #h — the agent's channels have to be
// discovered before it can hear anything.
//
// This is inbound, except for one thing: the NIP-OA agent profile (buzzProfileKind)
// that tells Buzz which pubkeys are agents. The gateway holds the agent's nsec, so
// it is the only party that can sign that profile at all. Answers still go out the
// way they always have: NIP-44 encrypted kind-30078 envelopes to the sender's key
// on NOSTR_RELAYS. Posting back into a Buzz channel is a separate feature (kind 9,
// the same h tag, no encryption) and is deliberately not here, so no channel
// traffic is ever published to a Buzz relay.
const (
	buzzChatKind   = 9     // NIP-29 message: #h = channel uuid, p = mention
	buzzMemberKind = 39002 // NIP-29 member list: d = channel uuid, p = every member
	// buzzProfileKind is the NIP-OA agent profile Buzz reads to tell an agent
	// apart from a person, and the only thing the gateway publishes to a Buzz
	// relay. Replaceable (10000-19999), so one event per author is enough.
	buzzProfileKind = 10100
	// buzzDMMembers is what makes a channel a DM: the agent and one other person.
	buzzDMMembers = 2
	// buzzRefresh re-reads the member lists, so a channel the agent joined after
	// the subscription came up is picked up without a restart.
	// ponytail: a timer per agent; a kind-44100 subscription ("member added",
	// p-gated to our own pubkey) is the upgrade if joins have to be instant.
	buzzRefresh = time.Minute
	// buzzFetchTimeout caps one discovery query. FetchMany is a one-shot REQ that
	// ends at EOSE, so a relay that stops answering would otherwise leave the
	// agent deaf with no log line at all.
	buzzFetchTimeout = 15 * time.Second
	// buzzRetryDelay is how long to wait before asking a relay that answered a
	// discovery with no member lists whatsoever. No rosters at all is not the
	// same answer as "no membership": go-nostr keys its NIP-42 OK waiters by
	// event id, so when two subscriptions on one connection answer the same
	// challenge in the same second one of them gets nothing back — and the
	// one-shot REQ is never sent again (pool.go:662-673). Asking a second time
	// on an already-authenticated connection costs a second and means the race
	// cannot leave the agent deaf for a whole refresh interval.
	buzzRetryDelay = 3 * time.Second
)

// buzzChannels maps a channel uuid to the member count its member list carried,
// which is all a DM is: a channel with the agent and one other person in it.
type buzzChannels map[string]int

// buzzConversation is the conversation id of a Buzz channel, so one channel maps
// to one OpenCode session — the session scope buzz-acp defaults to as well. The
// prefix keeps channel uuids clear of the ids the gateway mints itself.
func buzzConversation(channel string) string { return "buzz-" + channel }

// buzzDiscover lists the channels the agent is in. NIP-29 publishes one member
// list per channel, and the agent's own pubkey on it means the agent is a
// member, so a single filter fetches them all. FetchMany is a one-shot REQ that
// ends at EOSE and answers a NIP-42 challenge on the way, so a closed relay
// works here exactly as it does for the kind-30078 subscription. The timeout is
// the point: this is the one call in the gateway whose failure mode is silence.
//
// It also returns how many member lists the relay served, and that is not
// cosmetic. "No channel" and "no member lists at all" have opposite fixes — add
// the agent to a channel, versus the relay is not serving kind 39002 to us —
// and NIP-29 calls 39002 optional, with relays free to restrict who may fetch
// it. Collapsing the two into one log line is what sends an operator to fix the
// wrong thing.
func (n *nostrTransport) buzzDiscover(ctx context.Context, a *Agent) (buzzChannels, int) {
	ctx, cancel := context.WithTimeout(ctx, buzzFetchTimeout)
	defer cancel()
	chans, rosters := buzzChannels{}, 0
	filter := nostr.Filter{Kinds: []int{buzzMemberKind}, Tags: nostr.TagMap{"p": []string{a.PubKey}}}
	for ie := range n.buzzPools[a.Name].FetchMany(ctx, n.buzzRelays, filter) {
		if ie.Event == nil {
			continue // EOSE, or a subscription the relay closed
		}
		rosters++
		channel := ie.Event.Tags.GetD()
		if channel == "" {
			continue
		}
		members := 0
		for range ie.Event.Tags.FindAll("p") {
			members++
		}
		chans[channel] = members
	}
	return chans, rosters
}

// buzzListen subscribes to the agent's channels, rediscovering them on a timer
// and whenever the subscription ends. Same reconnect shape as the kind-30078
// listener, with one difference: the #h list is only as good as the last
// discovery, so a channel joined later only arrives on the next one.
func (n *nostrTransport) buzzListen(ctx context.Context, a *Agent) {
	pool := n.buzzPools[a.Name]
	interval, seen := reconnectBase, ""
	for ctx.Err() == nil {
		connected := time.Now()
		chans, rosters := n.buzzDiscover(ctx, a)
		if rosters == 0 {
			// A relay that returns no member lists at all has not really
			// answered — see buzzRetryDelay. Ask again before believing it.
			n.log.Warn("buzz: discovery came back with no member lists", "agent", a.Name, "relays", n.buzzRelays)
			if !wait(ctx, buzzRetryDelay) {
				return
			}
			chans, rosters = n.buzzDiscover(ctx, a)
		}
		if rosters == 0 {
			// Still nothing at all, and now twice over, so the retry has had
			// its chance. NIP-29 makes 39002 optional and lets a relay restrict
			// who may fetch it, so this is the case where the relay is not
			// serving the member lists — not the case where the agent is in no
			// channel. Say so, and keep looking.
			n.log.Warn("buzz: relay serves no member lists for this agent", "agent", a.Name,
				"relays", n.buzzRelays, "pubkey", a.PubKey[:8],
				"hint", "NIP-29 kind 39002 is optional and a relay may restrict who may fetch it: check the relay serves it to this pubkey, and that the pubkey is a relay member (buzz-admin add-member)")
			if !wait(ctx, buzzRefresh) {
				return
			}
			continue
		}
		// The relay answered with member lists, which is also what proves the
		// connection passed NIP-42. Register the agent even when none of those
		// rosters name it: Buzz reads kind 10100 to know which pubkeys are
		// agents, and until it is published the agent is invisible in the
		// channel UI, so the operator cannot even add it to one. Gating this
		// on a non-empty result is a bootstrap deadlock.
		n.buzzProfile(ctx, a)

		ids := make([]string, 0, len(chans))
		for id := range chans {
			ids = append(ids, id)
		}
		slices.Sort(ids) // a stable filter, so the REQ does not reshuffle itself
		if len(ids) == 0 {
			// A real answer: the relay has member lists and none of them name
			// this pubkey. A wait for a join, not a failed connection, so it
			// does not back off.
			n.log.Warn("buzz: agent is in no channel yet", "agent", a.Name,
				"rosters", rosters,
				"hint", "the relay's kind-39002 member lists do not name this pubkey: add it to a channel, then reconcile the rosters")
			if !wait(ctx, buzzRefresh) {
				return
			}
			continue
		}
		if joined := strings.Join(ids, ","); joined != seen {
			seen = joined
			n.log.Info("buzz channels", "agent", a.Name, "channels", joined)
		}

		filter := nostr.Filter{
			Kinds:   []int{buzzChatKind},
			Tags:    nostr.TagMap{"h": ids},
			Authors: a.Allow,
			// The same 5s slack as the kind-30078 subscription: a longer Since
			// would replay channel history into brand-new OpenCode sessions.
			Since: ptr(nostr.Now() - 5),
		}
		// The subscription gets its own context so a refresh can end it on the
		// wire: go-nostr turns a cancelled context into a NIP-01 CLOSE, and
		// leaving the old REQ open would keep the relay fanning out to a stale
		// channel list once a minute, forever.
		subCtx, endSub := context.WithCancel(ctx)
		ended := make(chan struct{})
		go func() {
			defer close(ended)
			for ie := range pool.SubscribeMany(subCtx, n.buzzRelays, filter) {
				if ie.Event == nil {
					continue // EOSE
				}
				n.buzzReceive(ctx, a, ie.Event, chans)
			}
		}()

		select {
		case <-time.After(buzzRefresh):
		case <-ended: // CLOSED, or the connection dropped
		case <-ctx.Done():
		}
		endSub()
		<-ended
		if ctx.Err() != nil {
			return
		}
		if time.Since(connected) < buzzRefresh {
			interval = reconnectDelay(interval, time.Since(connected))
			n.log.Warn("buzz subscription ended, rediscovering", "agent", a.Name, "in", interval)
			if !wait(ctx, interval) {
				return
			}
			continue
		}
		interval = reconnectBase
	}
}

// buzzProfile publishes the NIP-OA agent profile, once per agent per process,
// the first time a Buzz relay has answered a discovery with a member list.
// Buzz derives an agent's channel pills from these events (kind 10100), and
// without one the agent is a member of the relay but invisible in the agent UI —
// which is the one thing an operator cannot fix by hand, because the profile has
// to be signed by the agent's own key and the gateway is the only holder of it.
//
// This is the single exception to "a Buzz relay is subscribed to, never
// published to". It is safe because 10100 is replaceable (one event per author),
// carries no channel traffic, and does not show up in Buzz's read-state view,
// which is what that rule protects. It is only published once a discovery has
// come back with a member list, because that is what proves the connection
// passed NIP-42 — a profile pushed onto an unauthenticated connection is simply
// refused. An empty *result* is not required: rosters that name no channel still
// prove the point, and waiting for one is how an unregistered agent never gets
// added to a channel in the first place.
func (n *nostrTransport) buzzProfile(ctx context.Context, a *Agent) {
	// One writer per agent: buzzListen is the only caller, one goroutine each.
	if n.profiled[a.Name] {
		return
	}
	ev := nostr.Event{
		Kind: buzzProfileKind, CreatedAt: nostr.Now(),
		// Buzz parses the content as JSON and rejects the event outright
		// without this field. It says who may add the agent to a channel, and
		// "anyone" is the relay's own default, so publishing it changes nothing
		// about who can add the agent.
		// ponytail: the default policy; a config knob per agent if a deployment
		// ever wants the agent to refuse channel additions.
		Content: `{"channel_add_policy":"anyone"}`,
	}
	if err := ev.Sign(a.sk); err != nil {
		n.log.Warn("buzz: agent profile not signed", "agent", a.Name, "err", err)
		return
	}
	var firstErr error
	for res := range n.buzzPools[a.Name].PublishMany(ctx, n.buzzRelays, ev) {
		if res.Error != nil && firstErr == nil {
			firstErr = res.Error
		}
	}
	if firstErr != nil {
		n.log.Warn("buzz: agent profile not published, retrying with the next discovery", "agent", a.Name, "err", firstErr)
		return
	}
	n.profiled[a.Name] = true
	n.log.Info("buzz: published agent profile", "agent", a.Name, "kind", buzzProfileKind)
}

// buzzReceive decides whether one channel message is addressed to this agent and
// turns it into a protocol envelope. Identity is still the event signature: the
// allow list is checked before anything else, and the mention is the p tag, not
// the @name in the text.
func (n *nostrTransport) buzzReceive(ctx context.Context, a *Agent, ev *nostr.Event, chans buzzChannels) {
	if !a.allows(ev.PubKey) {
		return
	}
	channel := ev.Tags.Find("h").Value()
	members, member := chans[channel]
	if !member {
		return // not a channel the agent is in
	}
	// In a two-member channel every message is addressed to the agent, which is
	// what a DM is; in a group only a p tag for the agent is.
	if !ev.Tags.ContainsAny("p", []string{a.PubKey}) && members > buzzDMMembers {
		return
	}
	text := strings.TrimSpace(ev.Content)
	if text == "" {
		return
	}
	in := Envelope{
		V: protocolVersion, Conversation: buzzConversation(channel), Agent: a.Name,
		Type: TypeMessage, Timestamp: time.Unix(int64(ev.CreatedAt), 0).UTC(),
		Payload: Payload{Text: text},
	}
	if npub, err := nip19.EncodePublicKey(ev.PubKey); err == nil {
		in.Sender = npub
	}
	if _, err := n.hub.Handle(ctx, a, in, ev.PubKey); err != nil {
		// Nowhere to answer: an error envelope would have to be posted into the
		// channel, and this transport is subscribe-only.
		n.log.Warn("buzz inbound", "agent", a.Name, "channel", channel, "err", err)
	}
}

// wait sleeps for d and reports whether the wait finished rather than the
// process shutting down.
func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

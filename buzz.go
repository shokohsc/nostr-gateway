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
// This is inbound only. Answers still go out the way they always have: NIP-44
// encrypted kind-30078 envelopes to the sender's key on NOSTR_RELAYS. Posting
// back into a Buzz channel is a separate feature (kind 9, the same h tag, no
// encryption) and is deliberately not here, so the Buzz relays are subscribed to
// and never published to.
const (
	buzzChatKind   = 9     // NIP-29 message: #h = channel uuid, p = mention
	buzzMemberKind = 39002 // NIP-29 member list: d = channel uuid, p = every member
	// buzzDMMembers is what makes a channel a DM: the agent and one other person.
	buzzDMMembers = 2
	// buzzRefresh re-reads the member lists, so a channel the agent joined after
	// the subscription came up is picked up without a restart.
	// ponytail: a timer per agent; a kind-44100 subscription ("member added",
	// p-gated to our own pubkey) is the upgrade if joins have to be instant.
	buzzRefresh = time.Minute
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
// works here exactly as it does for the kind-30078 subscription.
func (n *nostrTransport) buzzDiscover(ctx context.Context, a *Agent) buzzChannels {
	chans := buzzChannels{}
	filter := nostr.Filter{Kinds: []int{buzzMemberKind}, Tags: nostr.TagMap{"p": []string{a.PubKey}}}
	for ie := range n.pools[a.Name].FetchMany(ctx, n.buzzRelays, filter) {
		if ie.Event == nil {
			continue // EOSE
		}
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
	return chans
}

// buzzListen subscribes to the agent's channels, rediscovering them on a timer
// and whenever the subscription ends. Same reconnect shape as the kind-30078
// listener, with one difference: the #h list is only as good as the last
// discovery, so a channel joined later only arrives on the next one.
func (n *nostrTransport) buzzListen(ctx context.Context, a *Agent) {
	pool := n.pools[a.Name]
	interval, seen := reconnectBase, ""
	for ctx.Err() == nil {
		connected := time.Now()
		chans := n.buzzDiscover(ctx, a)
		ids := make([]string, 0, len(chans))
		for id := range chans {
			ids = append(ids, id)
		}
		slices.Sort(ids) // a stable filter, so the REQ does not reshuffle itself
		if joined := strings.Join(ids, ","); joined != seen {
			seen = joined
			n.log.Info("buzz channels", "agent", a.Name, "channels", joined)
		}
		if len(ids) == 0 {
			// No membership to listen to, and an empty #h is a filter no relay
			// agrees on. This is a wait for a join, not a failed connection, so
			// it does not back off.
			n.log.Warn("buzz: agent is in no channel yet", "agent", a.Name)
			if !wait(ctx, buzzRefresh) {
				return
			}
			continue
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

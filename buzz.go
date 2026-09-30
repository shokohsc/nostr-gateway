package main

import (
	"context"
	"fmt"
	"maps"
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
// Outbound is a kind-9 message in the same channel, signed by the agent's own
// key, plus the NIP-OA agent profile (buzzProfileKind) that tells Buzz which
// pubkeys are agents. The gateway holds the agent's nsec, so it is the only party
// that can sign either. A kind-30078 envelope is not what a Buzz client renders
// in a channel, so an answer to a mention has to be posted as channel traffic —
// one message per turn, not one per streaming delta.
const (
	buzzChatKind   = 9     // NIP-29 message: #h = channel uuid, p = mention
	buzzMemberKind = 39002 // NIP-29 member list: d = channel uuid, p = every member
	// buzzProfileKind is the NIP-OA agent profile Buzz reads to tell an agent
	// apart from a person. Replaceable (10000-19999), so one event per author
	// is enough.
	buzzProfileKind = 10100
	// buzzConvPrefix marks a conversation that is a Buzz channel, so the answer
	// goes back into the channel instead of out as a kind-30078 envelope.
	buzzConvPrefix = "buzz-"
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
// prefix keeps channel uuids clear of the ids the gateway mints itself, and it is
// also how the outbound half recognises the conversation: the hub is
// transport-blind, so the id is the only thing that says where an answer goes.
func buzzConversation(channel string) string { return buzzConvPrefix + channel }

// buzzChannelOf is the inverse: the channel a conversation belongs to, or false
// for a conversation that is not a Buzz channel at all.
func buzzChannelOf(conv string) (string, bool) {
	channel, ok := strings.CutPrefix(conv, buzzConvPrefix)
	return channel, ok && channel != ""
}

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
			Authors: a.Allow, // ponytail: a membership change needs a restart or a config reload, log the count not the list
			// The same 5s slack as the kind-30078 subscription: a longer Since
			// would replay channel history into brand-new OpenCode sessions.
			Since: ptr(nostr.Now() - 5),
		}
		// The REQ that carries this filter goes out on every refresh, but a
		// refresh that changes nothing logs nothing, so a silent gateway is
		// indistinguishable from a healthy one. Log what it last asked for:
		// compared against the relay's own publish log, that is the difference
		// between a message the relay never delivered and one the gateway
		// refused (which buzzReceive logs).
		n.log.Debug("buzz: asked for channel messages", "agent", a.Name,
			"channels", len(ids), "authors", len(a.Allow), "relays", n.buzzRelays)
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
	// Our own channel replies come back on our own subscription: a relay fans an
	// event out to the connection that published it too, and go-nostr does not
	// filter self-authored events. In a two-member channel every message counts
	// as addressed to the agent, so without this the agent would answer itself,
	// one answer per answer.
	if ev.PubKey == a.PubKey {
		return
	}
	// The two refusals below are configuration faults, not a message that was
	// simply not addressed to the agent: either the sender's key is not in the
	// allow list, or the channel it was sent in is not one the agent is a member
	// of, and in both cases the agent is deaf until someone changes a file and
	// restarts. So they log at Warn with the fix in the message. The two further
	// down are ordinary group-chat traffic and stay at Debug — see drop.
	if !a.allows(ev.PubKey) {
		n.log.Warn("buzz: inbound message from a pubkey the agent does not allow",
			"agent", a.Name, "from", shortPub(ev.PubKey), "allow", len(a.Allow),
			"hint", "add this pubkey to the agent's allow list in AGENTS_FILE and restart, or the sender is deaf")
		return
	}
	channel := tagValue(ev.Tags, "h")
	members, member := chans[channel]
	if !member {
		n.log.Warn("buzz: inbound message in a channel the agent is not in",
			"agent", a.Name, "channel", channel, "in", strings.Join(slices.Sorted(maps.Keys(chans)), ","),
			"hint", "add the agent to that channel, or send in one it is in: a NIP-29 subscription only receives a channel it names in #h")
		return
	}
	// In a two-member channel every message is addressed to the agent, which is
	// what a DM is; in a group only a p tag for the agent is.
	if !ev.Tags.ContainsAny("p", []string{a.PubKey}) && members > buzzDMMembers {
		n.drop(a, ev, fmt.Sprintf("no p tag for the agent in a channel of %d members", members))
		return
	}
	text := strings.TrimSpace(ev.Content)
	if text == "" {
		n.drop(a, ev, "empty message")
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
	// The one inbound path that used to log nothing. Every refusal above and in
	// drop says why a message did not become a prompt, so "the relay delivered
	// nothing" and "the gateway got it and threw it away" were only separable by
	// the agent's own publish in the relay log — which an unanswered turn never
	// produces. Debug, because this fires once per inbound message.
	n.log.Debug("buzz: inbound channel message",
		"agent", a.Name, "from", shortPub(ev.PubKey), "channel", channel,
		"members", members, "bytes", len(text))
	if _, err := n.hub.Handle(ctx, a, in, ev.PubKey); err != nil {
		// Nowhere to answer on a transport of its own: a rejected message still
		// has to come back, or the sender cannot tell it apart from a lost relay
		// event. It lands in the channel as a kind-9 like any other answer.
		n.log.Warn("buzz inbound", "agent", a.Name, "channel", channel, "err", err)
		n.reply(a, ev.PubKey, Envelope{
			V: protocolVersion, Conversation: buzzConversation(channel), Agent: a.Name,
			Type: TypeError, Timestamp: time.Now().UTC(),
			Payload: Payload{Error: err.Error(), Text: err.Error()},
		})
	}
}

// drop is what an inbound message that never became a prompt logs. Both
// transports refuse messages before they are prompts — buzzReceive two ways
// left, nostr.receive on the allow list — and a refusal is silent by default, so
// a configuration fault looks exactly like an agent that is ignoring its
// senders. The two configuration faults buzzReceive has are Warn with the fix in
// the message; what is left here is a message that was simply not addressed to
// the agent (no p tag in a group) or had nothing in it, which is ordinary
// channel traffic, and a busy channel would drown a Warn in it. Debug, then: at
// LOG_LEVEL=info a group channel stays quiet, and if a sender is being refused
// for one of these reasons the answer is in the channel they can read.
func (n *nostrTransport) drop(a *Agent, ev *nostr.Event, why string) {
	n.log.Debug("inbound message not for this agent",
		"agent", a.Name, "from", shortPub(ev.PubKey), "why", why)
}

// buzzJob folds one protocol event into what the channel will get. A channel
// message is one posted message, so the answer is assembled from the streaming
// deltas and posted once the turn ends — posting each delta would put one
// message per token in front of the reader. Everything a human does not want to
// read in a chat (acks, tool calls, reasoning, progress) contributes nothing, and
// a turn that produced no text posts nothing at all.
//
// It runs on the publish worker goroutine, which is its only writer, so the
// in-flight text needs no lock.
func (n *nostrTransport) buzzJob(ctx context.Context, j job) error {
	post := func(text string) error {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		delete(n.turns, j.env.Conversation)
		return n.buzzPost(ctx, j.agent, j.buzz, j.peer, text)
	}
	switch j.env.Type {
	case TypeMessage:
		t, ok := n.turns[j.env.Conversation]
		if !ok {
			t = &strings.Builder{}
			n.turns[j.env.Conversation] = t
		}
		t.WriteString(j.env.Payload.Text)
		return nil
	case TypeCompleted:
		// The entry is often absent, and *strings.Builder dereferences its own
		// fields, so reading it unguarded kills the process from the one
		// goroutine that publishes for every agent. A turn reaches here with
		// nothing accumulated whenever it produced no text of its own — an
		// error before the first delta, or a permission request, which posts on
		// its own and clears the turn on its way out.
		if t := n.turns[j.env.Conversation]; t != nil {
			return post(t.String())
		}
		return nil
	case TypeError:
		// Whatever the turn said so far, the error is what matters now. Deleted
		// rather than nil'd: an empty error leaves post's own delete unreached,
		// and the nil entry would outlive the turn in the map forever.
		delete(n.turns, j.env.Conversation)
		return post(j.env.Payload.Text)
	case TypePermissionReq:
		// Asked in its own message, because it is its own decision. Answering it
		// from Buzz is not wired: a reply in the channel is a prompt, not a
		// permission_response, so approving still needs the HTTP API.
		return post(j.env.Payload.Text)
	}
	return nil
}

// buzzPost publishes one NIP-29 message into a channel, signed by the agent's own
// key — the agent is a member of the channel, which is how it discovered it, and
// a closed relay accepts nothing else. The p tag names the person who asked, so
// a group client renders the answer as addressed to them.
func (n *nostrTransport) buzzPost(ctx context.Context, a *Agent, channel, peer, text string) error {
	pool, ok := n.buzzPools[a.Name]
	if !ok {
		return fmt.Errorf("agent %s has no buzz pool", a.Name)
	}
	tags := nostr.Tags{{"h", channel}}
	if peer != "" {
		tags = append(tags, nostr.Tag{"p", peer})
	}
	ev := nostr.Event{Kind: buzzChatKind, CreatedAt: nostr.Now(), Tags: tags, Content: text}
	if err := ev.Sign(a.sk); err != nil {
		return err
	}
	var firstErr error
	for res := range pool.PublishMany(ctx, n.buzzRelays, ev) {
		if res.Error != nil && firstErr == nil {
			firstErr = res.Error
		}
	}
	return firstErr
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

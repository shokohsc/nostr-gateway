package main

import "sync"

// conversation is the join between an external conversation (Nostr thread,
// HTTP client) and an OpenCode session. Mapping conversation -> agent ->
// OpenCode session id is the whole point of the gateway.
type conversation struct {
	mu      sync.Mutex
	ID      string
	Agent   string
	session string // OpenCode session id, "" until the first message
	peer    string // hex pubkey of the Nostr participant, "" for pure HTTP
	seen    map[string]bool
	history []Envelope // last few events, replayed to late SSE subscribers
}

const historyDepth = 64

func (c *conversation) sessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

func (c *conversation) peerPub() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peer
}

func (c *conversation) setPeer(hex string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hex != "" {
		c.peer = hex // reply to whoever spoke last
	}
}

// ensureSession returns the conversation's OpenCode session, creating it under
// the conversation lock so two simultaneous first messages cannot produce two
// sessions and orphan one of them.
func (c *conversation) ensureSession(create func() (string, error)) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != "" {
		return c.session, nil
	}
	id, err := create()
	if err != nil {
		return "", err
	}
	c.session = id
	return id, nil
}

// record keeps a bounded replay buffer so a client that opens the SSE stream
// after posting its first message does not miss the beginning of the answer.
func (c *conversation) record(e Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, e)
	if len(c.history) > historyDepth {
		c.history = c.history[len(c.history)-historyDepth:]
	}
}

func (c *conversation) replay() []Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Envelope(nil), c.history...)
}

// sessionMap indexes conversations both ways. ponytail: in-memory for the
// process lifetime; add an eviction sweep if a deployment accumulates
// thousands of conversations.
type sessionMap struct {
	mu        sync.Mutex
	byConv    map[string]*conversation
	bySession map[string]*conversation // key: agent|sessionID
}

func newSessionMap() *sessionMap {
	return &sessionMap{byConv: map[string]*conversation{}, bySession: map[string]*conversation{}}
}

func (m *sessionMap) get(convID string) *conversation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byConv[convID]
}

// getHistory returns the replay buffer of a conversation, or nil if unknown.
func (m *sessionMap) getHistory(convID string) []Envelope {
	if c := m.get(convID); c != nil {
		return c.replay()
	}
	return nil
}

// lookup returns the conversation, creating it for agent if it is new. The peer
// is deliberately not set here: a conversation that belongs to another agent must
// not have its reply target re-pointed by a request that is about to be rejected.
func (m *sessionMap) lookup(convID, agent string) *conversation {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.byConv[convID]
	if !ok {
		c = &conversation{ID: convID, Agent: agent, seen: map[string]bool{}}
		m.byConv[convID] = c
	}
	return c
}

func (m *sessionMap) byOpenCodeSession(agent, sessionID string) *conversation {
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bySession[agent+"|"+sessionID]
}

// index makes a conversation findable by its OpenCode session. The caller owns
// the conversation's session field (ensureSession sets it), so this touches only
// the maps.
func (m *sessionMap) index(c *conversation, agent, sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bySession[agent+"|"+sessionID] = c
}

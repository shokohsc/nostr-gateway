package main

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Envelope is the transport-agnostic agent protocol. Nostr, plain HTTP or
// anything else that can carry JSON speaks this; transports only encrypt and
// route.
type Envelope struct {
	V            int       `json:"v"`
	ID           string    `json:"id"`
	Conversation string    `json:"conversation"`
	Agent        string    `json:"agent"`
	Sender       string    `json:"sender,omitempty"`
	Type         string    `json:"type"`
	Timestamp    time.Time `json:"timestamp"`
	Payload      Payload   `json:"payload"`
}

type Payload struct {
	Text         string `json:"text,omitempty"`
	Tool         string `json:"tool,omitempty"`
	CallID       string `json:"call_id,omitempty"`
	Output       string `json:"output,omitempty"`
	Error        string `json:"error,omitempty"`
	PermissionID string `json:"permission_id,omitempty"`
	Title        string `json:"title,omitempty"`
}

const (
	TypeMessage        = "message"
	TypeAck            = "ack"
	TypeThinking       = "thinking"
	TypeToolStarted    = "tool_started"
	TypeToolFinished   = "tool_finished"
	TypePermissionReq  = "permission_request"
	TypePermissionResp = "permission_response"
	TypeProgress       = "progress"
	TypeCompleted      = "completed"
	TypeError          = "error"
)

const protocolVersion = 1

func newEnvelope(agent, sender, conv, typ string, p Payload) Envelope {
	return Envelope{
		V:            protocolVersion,
		ID:           newID("evt"),
		Conversation: conv,
		Agent:        agent,
		Sender:       sender,
		Type:         typ,
		Timestamp:    time.Now().UTC(),
		Payload:      p,
	}
}

func newID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return prefix + "_" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return prefix + "_" + hex.EncodeToString(b)
}

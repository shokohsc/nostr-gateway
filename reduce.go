package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// reduced is one protocol event, not yet stamped with a conversation/agent.
type reduced struct {
	Type    string
	Payload Payload
}

// sessionOf digs the session id out of an OpenCode event. Session lifecycle
// events carry properties.sessionID; part events carry it on the part.
func sessionOf(ev opencodeEvent) string {
	var p eventProps
	if err := json.Unmarshal(ev.Properties, &p); err != nil {
		return ""
	}
	if p.SessionID != "" {
		return p.SessionID
	}
	if p.Part != nil {
		return p.Part.SessionID
	}
	return ""
}

// reduceEvent collapses OpenCode's event stream onto the agent protocol. `seen`
// is the conversation's dedupe set, and it is also where the reducer remembers
// which messages are the human's — see the message.updated case. A value of 0
// means unseen; for a text part the value is how much of it has been emitted
// already, so a growing snapshot can be diffed against what went out.
//
// ponytail: `seen` is in-memory, so a gateway restart could replay one event
// from a relay. Harmless (duplicates are dropped downstream by event id); move
// it to a shared store only if dedupe across restarts is ever required.
func reduceEvent(ev opencodeEvent, seen map[string]int) []reduced {
	var p eventProps
	if err := json.Unmarshal(ev.Properties, &p); err != nil {
		return nil
	}

	switch ev.Type {
	case "message.updated":
		// Nothing to emit, but the most important thing on the wire. OpenCode
		// streams the prompt back as a message of its own before the model runs,
		// and its text part is the same shape as the assistant's — id, type,
		// text, sessionID — with nothing to say whose it is. A reducer that reads
		// parts alone therefore treats the human's own words as the answer: Buzz
		// posted "@frontend-agent helloon it, one sec", and every subscriber on
		// every transport saw the prompt come back as a message. The role lives on
		// the message, so this is the only place it can be read.
		//
		// Only the user role is recorded. An unknown message id means the role
		// never arrived — a build that does not send this event, or a stream that
		// started mid-turn — and the part is then treated as the agent's, which is
		// how it behaved before: losing an answer is worse than showing a prompt.
		if p.Info != nil && p.Info.Role == "user" && p.Info.ID != "" {
			seen[userKey(p.Info.ID)] = 1
		}
		return nil

	case "message.part.updated":
		if p.Part == nil {
			return nil
		}
		return reducePart(*p.Part, p.Delta, seen)

	case "session.idle":
		return []reduced{{Type: TypeCompleted}}

	case "session.status":
		// Newer OpenCode builds report completion through a status event; only
		// the idle status means the turn is over.
		if strings.EqualFold(statusName(p.Status), "idle") {
			return []reduced{{Type: TypeCompleted}}
		}
		return nil

	case "session.error":
		return []reduced{{Type: TypeError, Payload: Payload{Error: errorText(p.Error), Text: errorText(p.Error)}}}

	case "permission.updated", "permission.asked":
		id, _ := p.permission()
		if id == "" {
			id = p.ID
		}
		if id == "" {
			return nil
		}
		key := "perm:" + id
		if seen[key] != 0 {
			return nil
		}
		seen[key] = 1
		return []reduced{{Type: TypePermissionReq, Payload: Payload{PermissionID: id, Title: p.Title, Text: p.Title}}}

	case "permission.replied":
		id, reply := p.permission()
		return []reduced{{Type: TypeProgress, Payload: Payload{
			PermissionID: id, Text: fmt.Sprintf("permission %s: %s", id, reply),
		}}}

	default:
		return nil
	}
}

func reducePart(pt part, delta string, seen map[string]int) []reduced {
	// The human's own text, streamed back by OpenCode. Nothing downstream wants
	// it: a Buzz answer is one message with the agent's words in it, and a
	// subscriber already has the prompt.
	if pt.MessageID != "" && seen[userKey(pt.MessageID)] != 0 {
		return nil
	}
	switch pt.Type {
	case "text", "reasoning":
		typ := TypeMessage
		if pt.Type == "reasoning" {
			typ = TypeThinking
		}
		text := delta
		if text != "" {
			// Remember that this part streams, so a final full-text snapshot of
			// the same part is not emitted on top of its own deltas.
			seen["stream:"+pt.ID] = 1
		} else {
			text = pt.Text
		}
		if strings.TrimSpace(text) == "" {
			// Nothing to emit and nothing to remember. OpenCode publishes a part
			// before it has any text, and marking it seen here drops every
			// snapshot of it that follows — the whole answer with it, so the turn
			// completes with nothing accumulated and the channel gets no reply.
			return nil
		}
		if delta == "" {
			if seen["stream:"+pt.ID] != 0 {
				return nil // already emitted as deltas; do not repeat the whole thing
			}
			// A snapshot, not a delta: OpenCode repeats the whole part as it
			// grows, so emit only what is new. Keeping just the first snapshot
			// posts the answer's opening tokens and nothing after them.
			key := pt.Type + ":" + pt.ID
			n := seen[key]
			if len(text) <= n {
				return nil
			}
			text = text[n:]
			seen[key] = n + len(text)
		}
		return []reduced{{Type: typ, Payload: Payload{Text: text}}}

	case "tool":
		// OpenCode's ToolState.status: pending | running | completed | error.
		status := pt.State.Status
		if status == "pending" {
			return nil // running and completed are the ones worth a progress event
		}
		key := "tool:" + pt.CallID + ":" + status
		if seen[key] != 0 {
			return nil
		}
		seen[key] = 1
		if status == "running" {
			return []reduced{{Type: TypeToolStarted, Payload: Payload{Tool: pt.Tool, CallID: pt.CallID, Title: pt.State.Title}}}
		}
		p := Payload{Tool: pt.Tool, CallID: pt.CallID, Title: pt.State.Title, Output: pt.State.Output}
		if status == "error" {
			p.Error = pt.State.Error
		}
		return []reduced{{Type: TypeToolFinished, Payload: p}}

	default:
		return nil // step-start/step-finish/retry/... carry no protocol meaning
	}
}

// userKey namespaces the message ids remembered in a conversation's dedupe set,
// next to the "perm:", "stream:", "tool:" keys already in there.
func userKey(messageID string) string { return "user:" + messageID }

// errorText renders OpenCode's error field, which may be a string or an object.
func errorText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var obj struct {
		Name    string `json:"name"`
		Message string `json:"message"`
		Data    struct {
			Message string `json:"message"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return strings.TrimSpace(string(raw))
	}
	for _, c := range []string{obj.Message, obj.Data.Message, obj.Name} {
		if c != "" {
			return c
		}
	}
	return strings.TrimSpace(string(raw))
}

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
// which messages are the human's — see the message.updated case.
//
// ponytail: `seen` is in-memory, so a gateway restart could replay one event
// from a relay. Harmless (duplicates are dropped downstream by event id); move
// it to a shared store only if dedupe across restarts is ever required.
func reduceEvent(ev opencodeEvent, seen map[string]bool) []reduced {
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
			seen[userKey(p.Info.ID)] = true
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
		if strings.EqualFold(p.Status, "idle") {
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
		if seen[key] {
			return nil
		}
		seen[key] = true
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

func reducePart(pt part, delta string, seen map[string]bool) []reduced {
	// The human's own text, streamed back by OpenCode. Nothing downstream wants
	// it: a Buzz answer is one message with the agent's words in it, and a
	// subscriber already has the prompt.
	if pt.MessageID != "" && seen[userKey(pt.MessageID)] {
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
			seen["stream:"+pt.ID] = true
		} else {
			key := pt.Type + ":" + pt.ID
			if seen[key] || seen["stream:"+pt.ID] {
				return nil
			}
			seen[key] = true
			text = pt.Text
		}
		if strings.TrimSpace(text) == "" {
			return nil
		}
		return []reduced{{Type: typ, Payload: Payload{Text: text}}}

	case "tool":
		// OpenCode's ToolState.status: pending | running | completed | error.
		status := pt.State.Status
		if status == "pending" {
			return nil // running and completed are the ones worth a progress event
		}
		key := "tool:" + pt.CallID + ":" + status
		if seen[key] {
			return nil
		}
		seen[key] = true
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

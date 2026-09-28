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
// dedupes per session: OpenCode re-sends the full part on every update, so
// without a delta we would repeat the text once per update.
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

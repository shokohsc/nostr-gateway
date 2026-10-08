package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// callTimeout bounds every non-streaming OpenCode call. The event stream is
// deliberately unbounded — it is supposed to stay open for the life of the
// process.
const callTimeout = 30 * time.Second

// maxEventBytes caps one SSE line. A single event larger than this is dropped
// rather than allowed to kill the whole agent's stream.
const maxEventBytes = 32 << 20

// opencodeClient is the only place that speaks OpenCode's HTTP API.
type opencodeClient struct {
	base string
	hc   *http.Client
	user string
	pass string
}

func newOpencodeClient(base, user, pass string) *opencodeClient {
	return &opencodeClient{base: strings.TrimRight(base, "/"), hc: &http.Client{}, user: user, pass: pass}
}

// opencodeEvent is the subset of OpenCode's Event union the reducer needs.
type opencodeEvent struct {
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
}

type eventProps struct {
	SessionID string `json:"sessionID"`
	Part      *part  `json:"part"`
	Delta     string `json:"delta"`
	ID        string `json:"id"`
	Title     string `json:"title"`
	// message.updated carries the message itself, which is the only thing on the
	// wire that says whether a part belongs to the human or to the agent.
	Info *message `json:"info"`
	// Permission ids were called permissionID with a `response` value, and are
	// now requestID with a `reply` value. Both spellings are in the wild.
	PermissionID string `json:"permissionID"`
	RequestID    string `json:"requestID"`
	Response     string `json:"response"`
	Reply        string `json:"reply"`
	// Status is an object on every current OpenCode — {"type":"idle"} — and a
	// bare string on older ones. It has to stay raw: a typed string fails the
	// unmarshal of the *whole* eventProps, and reduceEvent drops an event it
	// cannot parse without logging, so the completion signal every current build
	// sends was being thrown away.
	Status json.RawMessage `json:"status"`
	Error  json.RawMessage `json:"error"`
}

// statusName reads the status out of either shape: a bare string, or the object
// with a type field that current OpenCode sends.
func statusName(raw json.RawMessage) string {
	var obj struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj.Type
	}
	return strings.Trim(string(raw), `"`)
}

// message is UserMessage | AssistantMessage, discriminated on role.
type message struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionID"`
	Role      string `json:"role"`
}

func (p eventProps) permission() (id, reply string) {
	id, reply = p.PermissionID, p.Response
	if id == "" {
		id = p.RequestID
	}
	if reply == "" {
		reply = p.Reply
	}
	return id, reply
}

type part struct {
	ID string `json:"id"`
	// The message this part belongs to. A text part says nothing about who wrote
	// it, so this is the only way back to the role.
	MessageID string `json:"messageID"`
	Type      string `json:"type"`
	SessionID string `json:"sessionID"`
	CallID    string `json:"callID"`
	Tool      string `json:"tool"`
	Text      string `json:"text"`
	State     struct {
		Status string `json:"status"`
		Title  string `json:"title"`
		Output string `json:"output"`
		Error  string `json:"error"`
	} `json:"state"`
}

func (c *opencodeClient) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *opencodeClient) createSession(ctx context.Context, title string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var s struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http.MethodPost, "/session", map[string]any{"title": title}, &s); err != nil {
		return "", err
	}
	if s.ID == "" {
		return "", fmt.Errorf("session response had no id")
	}
	return s.ID, nil
}

// promptAsync fires and forgets: the reply arrives on the event stream, not on
// this response. That is what makes a conversation asynchronous.
func (c *opencodeClient) promptAsync(ctx context.Context, sessionID, text, model, agent string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	body := map[string]any{
		"parts": []map[string]string{{"type": "text", "text": text}},
	}
	if model != "" {
		// OpenCode's prompt_async takes a model reference object, not the
		// "provider/model" string the config and the docs use. Sending the
		// string is a 400 on every single message:
		// `Expected object | null, got "opencode/big-pickle" at ["model"]`,
		// which lands back in the chat as a gateway error envelope.
		provider, id, _ := strings.Cut(model, "/")
		body["model"] = map[string]string{"providerID": provider, "modelID": id}
	}
	if agent != "" {
		body["agent"] = agent
	}
	return c.do(ctx, http.MethodPost, "/session/"+sessionID+"/prompt_async", body, nil)
}

// replyPermission answers a permission request. OpenCode moved this endpoint
// in v1.1.1, so the documented form is tried first and the new one is the
// fallback — the gateway keeps working across both server generations.
func (c *opencodeClient) replyPermission(ctx context.Context, sessionID, permissionID, response string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	err := c.do(ctx, http.MethodPost, "/session/"+sessionID+"/permissions/"+permissionID,
		map[string]any{"response": response}, nil)
	if err == nil || !isMissingRoute(err) {
		return err
	}
	return c.do(ctx, http.MethodPost, "/session/"+sessionID+"/permission/"+permissionID+"/reply",
		map[string]any{"reply": response}, nil)
}

func isMissingRoute(err error) bool {
	s := err.Error()
	return strings.Contains(s, "404") || strings.Contains(s, "405")
}

// events streams /global/event. The returned channel closes when the stream
// ends; the caller reconnects.
func (c *opencodeClient) events(ctx context.Context) (<-chan opencodeEvent, <-chan error, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/global/event", nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// Keep the body, exactly as do() does. A 401 from OpenCode is an empty
		// body plus `WWW-Authenticate`, and a 403 from whatever is in front of
		// it is usually a proxy's HTML — either way the status alone sends the
		// operator to the wrong component.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, nil, fmt.Errorf("GET /global/event: %s: %s", resp.Status,
			strings.TrimSpace(string(b)))
	}

	out := make(chan opencodeEvent)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		r := bufio.NewReaderSize(resp.Body, 64*1024)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF && ctx.Err() == nil {
					errs <- err
				}
				return
			}
			if len(line) > maxEventBytes {
				continue // a single absurd line must not kill the stream
			}
			data, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data:")
			if !ok {
				continue
			}
			data = strings.TrimSpace(data)
			if data == "" || data == "[DONE]" {
				continue
			}
			var ge struct {
				Payload opencodeEvent `json:"payload"`
			}
			if err := json.Unmarshal([]byte(data), &ge); err != nil || ge.Payload.Type == "" {
				continue // an event shape we don't model is not fatal
			}
			select {
			case out <- ge.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errs, nil
}

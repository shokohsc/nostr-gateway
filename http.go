package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// The plain-HTTP face of the gateway: the same envelope, the same hub, no
// Nostr. Useful for curl, tests, and for any transport added later.
func (h *hub) handler(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /v1/messages", h.postMessage)
	mux.HandleFunc("GET /v1/conversations/{id}/events", h.streamEvents)
	mux.HandleFunc("GET /v1/models", h.listModels)
	mux.HandleFunc("POST /v1/chat/completions", h.chatCompletions)
	if token == "" {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			if !authorized(r.Header.Get("Authorization"), token) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "bad token"})
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (h *hub) postMessage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var in Envelope
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a := h.reg.byName[in.Agent]
	if a == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown agent " + in.Agent})
		return
	}
	ack, err := h.Handle(r.Context(), a, in, "") // no Nostr identity on this path
	if err != nil {
		code := http.StatusBadGateway
		if errors.Is(err, errInvalid) {
			code = http.StatusBadRequest
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, ack)
}

func (h *hub) streamEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	ch, cancel := h.subscribe(r.PathValue("id"))
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case env := <-ch:
			b, err := json.Marshal(env)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", env.Type, b)
			flusher.Flush()
		}
	}
}

// authorized compares the bearer token in constant time.
func authorized(header, token string) bool {
	got, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// The OpenAI-compatible face: the same hub, the same conversations, the same
// allow-list-free bearer token, only the wire format is the one every chat client
// and every agent framework already speaks. It is a translation, not a second
// agent: `model` names a registry agent, `user` scopes the conversation, and
// everything after that is Handle + subscribe.
const oaiModelPrefix = "opencode-nostr-gateway/"

type oaiRequest struct {
	Model    string `json:"model"`
	User     string `json:"user"`
	Stream   bool   `json:"stream"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

// listModels is every registry agent, so a client's model picker is the agent
// list. One entry per agent: the model is chosen by the agent's own config
// (`model` / `opencode_agent`), not per request.
func (h *hub) listModels(w http.ResponseWriter, r *http.Request) {
	data := make([]map[string]any, 0, len(h.reg.byName))
	for _, name := range h.reg.names() {
		data = append(data, map[string]any{
			"id": oaiModelPrefix + name, "object": "model", "owned_by": "opencode",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// chatCompletions turns an OpenAI chat request into a prompt and the answer back
// into chat completions.
//
// The client sends the whole conversation every time, so only the last user
// message is the prompt; everything before it is context the client already holds.
// Which conversation this continues is `user`, since OpenAI has no id for it —
// with no `user` every request is its own conversation, which is what a client
// with no state should get.
func (h *hub) chatCompletions(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var in oaiRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name, ok := strings.CutPrefix(in.Model, oaiModelPrefix)
	a := h.reg.byName[name]
	if !ok || a == nil {
		// An agent name alone is accepted too: the prefix exists to keep a client
		// from mistaking one of these for a hosted model.
		if a = h.reg.byName[in.Model]; a == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown model " + in.Model})
			return
		}
	}
	prompt := ""
	for _, m := range in.Messages {
		if strings.EqualFold(m.Role, "user") && strings.TrimSpace(m.Content) != "" {
			prompt = m.Content
		}
	}
	if prompt == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no user message"})
		return
	}
	conv := "oai-" + newID("c")
	if in.User != "" {
		conv = "oai-" + in.User
	}

	// Subscribed before the prompt and cut at the ack: subscribe replays the
	// conversation's history, which on a continued conversation is the previous
	// turn's answer, and this response must not contain it. Handle emits the ack
	// itself, so its id is an exact cursor with no timestamp to race.
	ch, cancel := h.subscribe(conv)
	defer cancel()
	ack, err := h.Handle(r.Context(), a, Envelope{
		Conversation: conv, Agent: a.Name, Type: TypeMessage, Payload: Payload{Text: prompt},
	}, "")
	if err != nil {
		code := http.StatusBadGateway
		if errors.Is(err, errInvalid) {
			code = http.StatusBadRequest
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}

	if in.Stream {
		h.streamCompletion(w, r, a, conv, ch, ack.ID)
		return
	}
	text, failure := collectTurn(r.Context(), ch, ack.ID)
	if failure != "" {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": failure})
		return
	}
	writeJSON(w, http.StatusOK, completion(a, conv, text))
}

// streamCompletion is the same turn, chunk by chunk, in the SSE shape every
// client already parses: a role delta first, then the deltas as they stream, and
// `data: [DONE]` when the turn ends.
func (h *hub) streamCompletion(w http.ResponseWriter, r *http.Request, a *Agent, conv string, ch <-chan Envelope, after string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	send := func(delta string, done bool) bool {
		chunk := completionChunk(a, conv, delta, done)
		b, err := json.Marshal(chunk)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send("", false) {
		return
	}
	for env := range untilTurnEnd(r.Context(), ch, after) {
		switch env.Type {
		case TypeMessage:
			if !send(env.Payload.Text, false) {
				return
			}
		case TypeError, TypePermissionReq:
			// The turn ended badly, and a stream has no status code left to say
			// so: the error goes in the content, then the stream ends. A
			// permission request ends it too — without this case it fell
			// through, the loop closed on the next terminal event, and the
			// client got an empty successful completion it could not act on.
			send(env.Payload.Text, true)
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
			return
		}
	}
	send("", true)
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// collectTurn joins one turn's text. Everything the protocol emits for a turn —
// acks, tool calls, reasoning, progress — is not something an OpenAI client can
// render, so only the answer text is joined; a permission request is the
// exception, and it arrives as its own error because there is no way to ask for
// one over this surface (see the Known limits in the README).
//
// The failure string must be non-empty in every failure case: chatCompletions
// decides success by `failure != ""`, so a `TypeError` carrying an empty
// `payload.error` would otherwise be reported to the client as a clean 200.
func collectTurn(ctx context.Context, ch <-chan Envelope, after string) (string, string) {
	var text strings.Builder
	for env := range untilTurnEnd(ctx, ch, after) {
		switch env.Type {
		case TypeMessage:
			text.WriteString(env.Payload.Text)
		case TypeError:
			if env.Payload.Error != "" {
				return text.String(), env.Payload.Error
			}
			return text.String(), env.Payload.Text
		case TypePermissionReq:
			// Not the title as a 200 answer: the turn is stalled waiting for a
			// decision nobody on this surface can make, so say that instead.
			return text.String(), "permission " + env.Payload.PermissionID +
				" is pending: it cannot be answered over this surface"
		}
	}
	return text.String(), ""
}

// untilTurnEnd yields one turn's events, dropping the replayed history up to and
// including the ack, and closing on completed, error or the client going away.
// A client that hangs up mid-turn is normal, not an error, so this is where the
// wait ends.
//
// The terminal events other than `completed` are yielded *before* the channel
// closes, never dropped on the way out. Returning first made both callers'
// `case TypeError` branches unreachable: a failed turn came back as a 200 with
// whatever partial text had accumulated, and a permission request — which is
// not terminal here at all — left the request parked until the client gave up.
func untilTurnEnd(ctx context.Context, ch <-chan Envelope, after string) <-chan Envelope {
	out := make(chan Envelope)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case env, ok := <-ch:
				if !ok {
					return
				}
				if after != "" {
					if env.ID == after {
						after = "" // the turn starts here
					}
					continue
				}
				if env.Type == TypeCompleted {
					return // nothing to say; the caller ends the turn here
				}
				select {
				case out <- env:
				case <-ctx.Done():
					return
				}
				if env.Type == TypeError || env.Type == TypePermissionReq {
					return // delivered; the turn is over either way
				}
			}
		}
	}()
	return out
}

func completion(a *Agent, conv, text string) map[string]any {
	return map[string]any{
		"id":      "chatcmpl-" + newID(""),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   oaiModelPrefix + a.Name,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": text},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
		// The gateway's own id for the conversation, so a client that wants the
		// envelope protocol or the SSE event stream can follow it from here.
		"conversation": conv,
	}
}

func completionChunk(a *Agent, conv, delta string, done bool) map[string]any {
	choice := map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": nil}
	if done {
		choice["delta"] = map[string]any{}
		choice["finish_reason"] = "stop"
	} else if delta != "" {
		choice["delta"] = map[string]any{"content": delta}
	} else {
		choice["delta"] = map[string]any{"role": "assistant"}
	}
	return map[string]any{
		"id": "chatcmpl-" + newID(""), "object": "chat.completion.chunk",
		"created": time.Now().Unix(), "model": oaiModelPrefix + a.Name,
		"choices": []any{choice}, "conversation": conv,
	}
}

func (h *hub) serveHTTP(ctx context.Context, addr, token string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h.handler(token),
		ReadHeaderTimeout: 10 * time.Second,
		// No ReadTimeout or WriteTimeout: both would cut a conversation's SSE
		// stream, which is supposed to outlive any request deadline. IdleTimeout
		// only applies between requests on a kept-alive connection, so it reaps
		// sockets that are held open and idle — the one leak a plain timeout
		// would not have fixed.
		IdleTimeout: 120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	h.log.Info("http api", "addr", addr, "auth", token != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

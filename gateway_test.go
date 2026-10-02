package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// ---------- fake OpenCode server ----------

type fakeOC struct {
	*httptest.Server
	// v2Permissions makes the pre-1.1.1 permission endpoint 404, like a current
	// OpenCode build.
	v2Permissions bool
	mu            sync.Mutex
	sessions      []string // session titles requested
	prompts       []map[string]any
	perms         []string // "<sessionID>/<permissionID>"

	events chan string // pre-marshalled SSE payloads
}

func newFakeOC(t *testing.T) *fakeOC {
	t.Helper()
	f := &fakeOC{events: make(chan string, 64)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /session", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Title string `json:"title"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.sessions = append(f.sessions, body.Title)
		id := fmt.Sprintf("ses_%d", len(f.sessions))
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	})
	mux.HandleFunc("POST /session/{id}/prompt_async", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.prompts = append(f.prompts, body)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	record := func(key, response string) {
		f.mu.Lock()
		f.perms = append(f.perms, key+":"+response)
		f.mu.Unlock()
	}
	mux.HandleFunc("POST /session/{id}/permissions/{pid}", func(w http.ResponseWriter, r *http.Request) {
		if f.v2Permissions {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		record(r.PathValue("id")+"/"+r.PathValue("pid"), fmt.Sprint(body["response"]))
	})
	mux.HandleFunc("POST /session/{id}/permission/{pid}/reply", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		record(r.PathValue("id")+"/"+r.PathValue("pid")+"/reply", fmt.Sprint(body["reply"]))
	})
	mux.HandleFunc("GET /global/event", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-f.events:
				fmt.Fprintf(w, "data: {\"directory\":\"/tmp\",\"payload\":%s}\n\n", ev)
				flusher.Flush()
			}
		}
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeOC) push(typ string, props map[string]any) {
	b, _ := json.Marshal(map[string]any{"type": typ, "properties": props})
	f.events <- string(b)
}

func (f *fakeOC) counts() (sessions, prompts, perms int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sessions), len(f.prompts), len(f.perms)
}

// ---------- harness ----------

func testRegistry(t *testing.T, ocURL string) *Registry {
	t.Helper()
	a := &Agent{Name: "frontend-agent", OpenCode: ocURL, PubKey: strings.Repeat("ab", 32), ck: map[string][32]byte{}}
	return &Registry{byName: map[string]*Agent{a.Name: a}}
}

func testHub(t *testing.T, reg *Registry) (*hub, *httptest.Server) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHub(reg, log)
	ctx, cancel := context.WithCancel(context.Background())
	go h.run(ctx)
	t.Cleanup(cancel)
	return h, httptest.NewServer(h.handler("")) // auth is covered by TestHTTPAuthToken
}

func post(t *testing.T, url, body string) (int, Envelope) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env Envelope
	_ = json.NewDecoder(resp.Body).Decode(&env)
	return resp.StatusCode, env
}

// next reads one event or fails, printing what did arrive.
func next(t *testing.T, ch <-chan Envelope) Envelope {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
		return Envelope{}
	}
}

func want(t *testing.T, got Envelope, typ, text string) {
	t.Helper()
	if got.Type != typ || (text != "" && got.Payload.Text != text) {
		t.Fatalf("got %s/%q, want %s/%q", got.Type, got.Payload.Text, typ, text)
	}
	if got.V != protocolVersion || got.ID == "" || got.Conversation == "" || got.Agent != "frontend-agent" {
		t.Fatalf("envelope not stamped: %+v", got)
	}
}

// ---------- tests ----------

func TestPromptFlowOverHTTP(t *testing.T) {
	f := newFakeOC(t)
	h, srv := testHub(t, testRegistry(t, f.URL))

	sub, cancelSub := h.subscribe("conv-1")
	defer cancelSub()

	code, ack := post(t, srv.URL+"/v1/messages",
		`{"conversation":"conv-1","agent":"frontend-agent","type":"message","payload":{"text":"do the thing"}}`)
	if code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}
	want(t, ack, TypeAck, "queued")
	want(t, next(t, sub), TypeAck, "queued") // every outbound event fans out, ack included

	f.push("message.part.updated", map[string]any{
		"part":  map[string]any{"id": "prt_1", "type": "text", "text": "Sure, ", "sessionID": "ses_1"},
		"delta": "Sure, ",
	})
	want(t, next(t, sub), TypeMessage, "Sure, ")

	f.push("message.part.updated", map[string]any{
		"part":  map[string]any{"id": "prt_1", "type": "text", "text": "Sure, on it", "sessionID": "ses_1"},
		"delta": "on it",
	})
	want(t, next(t, sub), TypeMessage, "on it")

	f.push("message.part.updated", map[string]any{
		"part": map[string]any{"id": "prt_2", "type": "tool", "callID": "call_1", "tool": "bash", "sessionID": "ses_1",
			"state": map[string]any{"status": "completed", "title": "ls", "output": "plan.md"}},
	})
	tool := next(t, sub)
	want(t, tool, TypeToolFinished, "")
	if tool.Payload.Tool != "bash" || tool.Payload.Output != "plan.md" || tool.Payload.CallID != "call_1" {
		t.Fatalf("tool payload: %+v", tool.Payload)
	}

	f.push("session.idle", map[string]any{"sessionID": "ses_1"})
	want(t, next(t, sub), TypeCompleted, "")

	sessions, prompts, _ := f.counts()
	if sessions != 1 || prompts != 1 {
		t.Fatalf("sessions=%d prompts=%d, want 1/1", sessions, prompts)
	}
	if got := f.prompts[0]["parts"].([]any)[0].(map[string]any)["text"]; got != "do the thing" {
		t.Fatalf("prompt text %v", got)
	}
}

func TestPromptAsyncSendsModelAsObject(t *testing.T) {
	f := newFakeOC(t)
	c := newOpencodeClient(f.URL, "", "")
	if err := c.promptAsync(context.Background(), "ses_1", "hi", "opencode/big-pickle", "build"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	prompts := append([]map[string]any(nil), f.prompts...)
	f.mu.Unlock()
	if len(prompts) != 1 {
		t.Fatalf("prompts=%d, want 1", len(prompts))
	}
	// prompt_async validates model as an object, so the "provider/model" string
	// the config and the README use has to be split on the way out. Sent as a
	// string it is a 400 on every single message, reported back into the chat as
	// a gateway error envelope.
	ref, ok := prompts[0]["model"].(map[string]any)
	if !ok || ref["providerID"] != "opencode" || ref["modelID"] != "big-pickle" {
		t.Fatalf("model went out as %#v", prompts[0]["model"])
	}
	if prompts[0]["agent"] != "build" {
		t.Fatalf("agent went out as %#v", prompts[0]["agent"])
	}
}

func TestPermissionApprovalLoop(t *testing.T) {
	f := newFakeOC(t)
	h, srv := testHub(t, testRegistry(t, f.URL))
	sub, cancelSub := h.subscribe("conv-2")
	defer cancelSub()

	post(t, srv.URL+"/v1/messages", `{"conversation":"conv-2","agent":"frontend-agent","type":"message","payload":{"text":"delete it"}}`)
	want(t, next(t, sub), TypeAck, "queued")

	f.push("permission.updated", map[string]any{
		"id": "per_1", "sessionID": "ses_1", "type": "bash", "title": "rm -rf build",
	})
	req := next(t, sub)
	want(t, req, TypePermissionReq, "")
	if req.Payload.PermissionID != "per_1" || req.Payload.Title != "rm -rf build" {
		t.Fatalf("permission payload: %+v", req.Payload)
	}

	code, prog := post(t, srv.URL+"/v1/messages",
		`{"conversation":"conv-2","agent":"frontend-agent","type":"permission_response","payload":{"permission_id":"per_1","text":"once"}}`)
	if code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}
	want(t, prog, TypeProgress, "once")
	want(t, next(t, sub), TypeProgress, "once")

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.perms) != 1 || f.perms[0] != "ses_1/per_1:once" {
		t.Fatalf("permissions: %v", f.perms)
	}
}

func TestSessionReusedAcrossMessages(t *testing.T) {
	f := newFakeOC(t)
	h, _ := testHub(t, testRegistry(t, f.URL))
	ctx := context.Background()

	for _, text := range []string{"first", "second"} {
		ack, err := h.Handle(ctx, h.reg.byName["frontend-agent"], Envelope{
			Conversation: "conv-3", Agent: "frontend-agent", Type: TypeMessage, Payload: Payload{Text: text},
		}, "")
		if err != nil {
			t.Fatal(err)
		}
		if ack.Conversation != "conv-3" {
			t.Fatalf("conversation %q", ack.Conversation)
		}
	}
	if s, p, _ := f.counts(); s != 1 || p != 2 {
		t.Fatalf("sessions=%d prompts=%d, want 1/2", s, p)
	}
}

func TestUnknownAgentAndEmptyTextRejected(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))
	if code, _ := post(t, srv.URL+"/v1/messages", `{"agent":"nope","type":"message","payload":{"text":"x"}}`); code != http.StatusBadRequest {
		t.Fatalf("unknown agent status %d", code)
	}
	if code, _ := post(t, srv.URL+"/v1/messages", `{"agent":"frontend-agent","type":"message","payload":{"text":""}}`); code != http.StatusBadRequest {
		t.Fatalf("empty text status %d", code)
	}
	if code, _ := post(t, srv.URL+"/v1/messages", `{"agent":"frontend-agent","type":"question","payload":{"text":"x"}}`); code != http.StatusBadRequest {
		t.Fatalf("bad type status %d", code)
	}
}

func TestHTTPAuthToken(t *testing.T) {
	f := newFakeOC(t)
	h := newHub(testRegistry(t, f.URL), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(h.handler("secret"))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/messages", "application/json",
		strings.NewReader(`{"agent":"frontend-agent","type":"message","payload":{"text":"hi"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status %d", resp.StatusCode)
	}
}

// The allow list is a Nostr-identity gate. An HTTP caller carries no Nostr
// identity and cannot become one by asserting a sender, so the conversation
// must not gain a Nostr peer (see TestNostrAllowList and the exfiltration test
// in nostr_test.go for the Nostr side).
// The OpenAI surface is the same hub behind a wire format every chat client
// already speaks, so the point of the test is that the client gets its answer
// back as a chat completion — and only this turn's, since a continued
// conversation replays the previous answer into the new response otherwise.
func TestOpenAIChatCompletions(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))

	// /v1/models has to name the agent, or a client cannot pick one.
	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	var models struct {
		Data []struct{ ID string } `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&models); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(models.Data) != 1 || models.Data[0].ID != "opencode-nostr-gateway/frontend-agent" {
		t.Fatalf("models %+v", models.Data)
	}

	// The fake names the sessions ses_1, ses_2, ... as it creates them, so the
	// test says which session answers which user.
	ask := func(user, session, question string) string {
		t.Helper()
		go func() {
			time.Sleep(50 * time.Millisecond)
			f.push("message.part.updated", map[string]any{
				"sessionID": session, "delta": "answer to " + question,
				"part": map[string]any{"id": "prt_1", "type": "text", "sessionID": session},
			})
			f.push("session.idle", map[string]any{"sessionID": session})
		}()
		resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"opencode-nostr-gateway/frontend-agent","user":"`+user+
				`","messages":[{"role":"user","content":"`+question+`"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
		var out struct {
			Object       string `json:"object"`
			Conversation string `json:"conversation"`
			Choices      []struct {
				Message struct{ Role, Content string } `json:"message"`
			} `json:"choices"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if out.Object != "chat.completion" || len(out.Choices) != 1 {
			t.Fatalf("not a chat completion: %+v", out)
		}
		return out.Choices[0].Message.Content
	}

	// One OpenCode session for one user: the second turn continues the first.
	if got := ask("alice", "ses_1", "one"); got != "answer to one" {
		t.Fatalf("first turn %q", got)
	}
	if got := ask("alice", "ses_1", "two"); got != "answer to two" {
		t.Fatalf("second turn carried the previous answer: %q", got)
	}
	if sessions, _, _ := f.counts(); sessions != 1 {
		t.Fatalf("one user produced %d sessions", sessions)
	}
	// A different user is a different conversation, and therefore a new session.
	if got := ask("bob", "ses_2", "hello"); got != "answer to hello" {
		t.Fatalf("other user %q", got)
	}
	if sessions, _, _ := f.counts(); sessions != 2 {
		t.Fatalf("two users produced %d sessions", sessions)
	}
}

func TestOpenAIChatCompletionsStream(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))

	go func() {
		time.Sleep(50 * time.Millisecond)
		for _, d := range []string{"str", "eamed"} {
			f.push("message.part.updated", map[string]any{
				"sessionID": "ses_1", "delta": d,
				"part": map[string]any{"id": "prt_1", "type": "text", "sessionID": "ses_1"},
			})
		}
		f.push("session.idle", map[string]any{"sessionID": "ses_1"})
	}()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"opencode-nostr-gateway/frontend-agent","stream":true,`+
			`"messages":[{"role":"user","content":"go"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	var text strings.Builder
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		data, ok := strings.CutPrefix(strings.TrimSpace(line), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Object  string `json:"object"`
			Choices []struct {
				Delta struct{ Content string } `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("chunk %q: %v", data, err)
		}
		if chunk.Object != "chat.completion.chunk" {
			t.Fatalf("object %q", chunk.Object)
		}
		text.WriteString(chunk.Choices[0].Delta.Content)
	}
	if text.String() != "streamed" {
		t.Fatalf("streamed %q", text.String())
	}
	if !strings.HasSuffix(strings.TrimSpace(string(b)), "data: [DONE]") {
		t.Fatal("stream did not end with [DONE]")
	}
}

func TestOpenAIRejectsUnknownModel(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown model got status %d", resp.StatusCode)
	}
	if sessions, _, _ := f.counts(); sessions != 0 {
		t.Fatal("an unknown model reached an agent")
	}
}

func TestHTTPHandleHasNoNostrIdentity(t *testing.T) {
	f := newFakeOC(t)
	reg := testRegistry(t, f.URL)
	reg.byName["frontend-agent"].Allow = []string{strings.Repeat("cd", 32)}
	h, srv := testHub(t, reg)

	code, ack := post(t, srv.URL+"/v1/messages",
		`{"conversation":"c2","agent":"frontend-agent","sender":"`+strings.Repeat("cd", 32)+`","type":"message","payload":{"text":"hi"}}`)
	if code != http.StatusAccepted {
		t.Fatalf("status %d: an authenticated HTTP caller is not a Nostr sender", code)
	}
	if ack.Conversation != "c2" {
		t.Fatalf("conversation %q", ack.Conversation)
	}
	if p := h.convs.get("c2").peerPub(); p != "" {
		t.Fatalf("conversation picked up nostr peer %s from a self-declared sender", p)
	}
}

func TestLoadRegistry(t *testing.T) {
	sk := nostr.GeneratePrivateKey()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "agents.json")
	npub, err := nip19.EncodePublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	// Config is written the way operators write it: bech32. Everything
	// downstream speaks hex, so the registry must normalise it — a p-tag filter
	// built from bech32 never matches a real relay's hex p tag.
	cfg := fmt.Sprintf(`{"agent-a":{"opencode":"http://oc:4096","npub":%q,"nsec_env":"AGENT_A_NSEC","allow":[],"model":"anthropic/claude-sonnet-4"}}`, npub)
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTS_FILE", path)
	t.Setenv("AGENT_A_NSEC", sk) // as mounted from a Kubernetes Secret

	reg, err := loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	a := reg.byName["agent-a"]
	if a == nil || a.PubKey != pk || a.sk != sk || a.Model != "anthropic/claude-sonnet-4" {
		t.Fatalf("registry: %+v", a)
	}
	if !a.allows("") {
		t.Fatal("empty allow list should accept anyone")
	}
	t.Setenv("AGENTS_FILE", "") // fall back to the inline config
	t.Setenv("AGENTS", `{"agent-b":{"opencode":"http://x:4096","npub":"garbage"}}`)
	if _, err := loadRegistry(); err == nil {
		t.Fatal("a malformed npub must be rejected, not silently deaf")
	}
	// A well-formed pair that disagrees is the config error that used to pass
	// every shape check and leave the agent deaf: the relay authenticates the
	// connection as the nsec's key while every p-tag filter asks about the
	// npub, so discovery comes back with no member lists and the log blames the
	// relay. It has to be a startup error that names the key that disagrees.
	t.Setenv("AGENTS", fmt.Sprintf(`{"agent-c":{"opencode":"http://x:4096","npub":%q,"nsec_env":"AGENT_A_NSEC"}}`, strings.Repeat("cd", 32)))
	_, err = loadRegistry()
	if err == nil || !strings.Contains(err.Error(), "AGENT_A_NSEC") {
		t.Fatalf("an npub that is not the nsec's pubkey must be a startup error, got %v", err)
	}
	// prompt_async takes {"providerID","modelID"} and rejects a bare string, so
	// a model without a slash cannot be turned into a reference at all. Say so
	// at startup rather than once per message.
	t.Setenv("AGENTS", `{"agent-d":{"opencode":"http://x:4096","npub":"`+pk+`","model":"big-pickle"}}`)
	if _, err := loadRegistry(); err == nil || !strings.Contains(err.Error(), "provider/model") {
		t.Fatalf("a model with no provider must be a startup error, got %v", err)
	}
}

func TestReduceEvent(t *testing.T) {
	seen := map[string]int{}
	tests := []struct {
		name  string
		ev    map[string]any
		want  []reduced
		after []string // extra calls expected to produce nothing
	}{
		{
			name: "text delta streams",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"delta": "he", "part": map[string]any{"id": "p1", "type": "text"}}},
			want: []reduced{{Type: TypeMessage, Payload: Payload{Text: "he"}}},
		},
		{
			name:  "full text deduplicated by part id",
			ev:    map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p2", "type": "text", "text": "hello"}}},
			want:  []reduced{{Type: TypeMessage, Payload: Payload{Text: "hello"}}},
			after: []string{"message.part.updated"},
		},
		{
			name: "reasoning is thinking",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"delta": "hmm", "part": map[string]any{"id": "p3", "type": "reasoning"}}},
			want: []reduced{{Type: TypeThinking, Payload: Payload{Text: "hmm"}}},
		},
		{
			name: "pending tool is silent",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p4", "type": "tool", "callID": "c1", "state": map[string]any{"status": "pending"}}}},
			want: nil,
		},
		{
			name: "running tool starts",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p5", "type": "tool", "callID": "c2", "tool": "edit", "state": map[string]any{"status": "running", "title": "Edit file"}}}},
			want: []reduced{{Type: TypeToolStarted, Payload: Payload{Tool: "edit", CallID: "c2", Title: "Edit file"}}},
		},
		{
			name: "tool error finishes with error",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p6", "type": "tool", "callID": "c3", "tool": "bash", "state": map[string]any{"status": "error", "error": "exit 1"}}}},
			want: []reduced{{Type: TypeToolFinished, Payload: Payload{Tool: "bash", CallID: "c3", Error: "exit 1"}}},
		},
		{
			name: "step markers ignored",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p7", "type": "step-start"}}},
			want: nil,
		},
		{
			name: "idle completes",
			ev:   map[string]any{"type": "session.idle", "properties": map[string]any{"sessionID": "s"}},
			want: []reduced{{Type: TypeCompleted}},
		},
		{
			name: "session error",
			ev:   map[string]any{"type": "session.error", "properties": map[string]any{"sessionID": "s", "error": map[string]any{"name": "ProviderError", "data": map[string]any{"message": "429"}}}},
			want: []reduced{{Type: TypeError, Payload: Payload{Error: "429", Text: "429"}}},
		},
		{
			// The wire shape, captured from a real opencode 1.18.33 turn: status
			// is an object. As a typed string it failed the unmarshal of the whole
			// properties object, so reduceEvent dropped the event before the
			// switch and this completion signal never fired.
			name: "idle via session.status object",
			ev:   map[string]any{"type": "session.status", "properties": map[string]any{"sessionID": "s", "status": map[string]any{"type": "idle"}}},
			want: []reduced{{Type: TypeCompleted}},
		},
		{
			name: "busy status is not completion",
			ev:   map[string]any{"type": "session.status", "properties": map[string]any{"sessionID": "s", "status": map[string]any{"type": "busy"}}},
			want: nil,
		},
		{
			// Older builds send the bare string, so both shapes have to work.
			name: "idle via session.status string",
			ev:   map[string]any{"type": "session.status", "properties": map[string]any{"sessionID": "s", "status": "idle"}},
			want: []reduced{{Type: TypeCompleted}},
		},
		{
			// OpenCode publishes a text part before it holds any text. Recording
			// it as seen there dropped every later snapshot of the same part, so
			// the turn completed with nothing accumulated and no reply was posted.
			name: "empty first snapshot is silent",
			ev:   map[string]any{"type": "message.part.updated", "properties": map[string]any{"part": map[string]any{"id": "p8", "type": "text", "text": ""}}},
			want: nil,
		},
		{
			name: "permission.asked with requestID",
			ev:   map[string]any{"type": "permission.asked", "properties": map[string]any{"id": "req_1", "requestID": "req_1", "sessionID": "s", "title": "run tests"}},
			want: []reduced{{Type: TypePermissionReq, Payload: Payload{PermissionID: "req_1", Title: "run tests", Text: "run tests"}}},
		},
		{
			name: "permission.replied with reply",
			ev:   map[string]any{"type": "permission.replied", "properties": map[string]any{"sessionID": "s", "requestID": "req_1", "reply": "always"}},
			want: []reduced{{Type: TypeProgress, Payload: Payload{PermissionID: "req_1", Text: "permission req_1: always"}}},
		},
		{
			name: "unknown event ignored",
			ev:   map[string]any{"type": "file.edited", "properties": map[string]any{"file": "a.go"}},
			want: nil,
		},
	}
	for _, tc := range tests {
		raw, _ := json.Marshal(tc.ev)
		var oe opencodeEvent
		_ = json.Unmarshal(raw, &oe)
		got := reduceEvent(oe, seen)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
		for i := range got {
			if got[i].Type != tc.want[i].Type || got[i].Payload != tc.want[i].Payload {
				t.Fatalf("%s: got %+v, want %+v", tc.name, got[i], tc.want[i])
			}
		}
		for _, typ := range tc.after {
			ae := oe
			ae.Type = typ
			if extra := reduceEvent(ae, seen); len(extra) != 0 {
				t.Fatalf("%s: replay produced %+v", tc.name, extra)
			}
		}
	}
}

// A text part arrives empty and fills in over the next few events. The
// regression: marking an empty snapshot as seen dropped every later snapshot of
// that part, so the turn reached completion with nothing accumulated and the
// Buzz channel never got a reply — a whole answer lost to one blank event.
func TestAnEmptyPartDoesNotConsumeTheAnswer(t *testing.T) {
	seen := map[string]int{}
	event := func(text string) opencodeEvent {
		raw, _ := json.Marshal(map[string]any{"type": "message.part.updated", "properties": map[string]any{
			"sessionID": "s",
			"part":      map[string]any{"id": "p1", "type": "text", "messageID": "m1", "text": text},
		}})
		var oe opencodeEvent
		_ = json.Unmarshal(raw, &oe)
		return oe
	}
	if got := reduceEvent(event(""), seen); len(got) != 0 {
		t.Fatalf("an empty part produced %+v, want nothing", got)
	}
	got := reduceEvent(event("the answer"), seen)
	if len(got) != 1 || got[0].Type != TypeMessage || got[0].Payload.Text != "the answer" {
		t.Fatalf("the text after an empty snapshot was lost: got %+v", got)
	}
}

// OpenCode repeats a part in full as it grows, so the reducer emits only the
// new tail of each snapshot. The regression: a bool dedupe set kept the first
// snapshot and dropped the rest, so the channel got the answer's opening tokens
// and then silence.
func TestAGrowingPartEmitsOnlyItsNewText(t *testing.T) {
	seen := map[string]int{}
	event := func(text string, delta string) opencodeEvent {
		raw, _ := json.Marshal(map[string]any{"type": "message.part.updated", "properties": map[string]any{
			"sessionID": "s",
			"delta":     delta,
			"part":      map[string]any{"id": "p1", "type": "text", "messageID": "m1", "text": text},
		}})
		var oe opencodeEvent
		_ = json.Unmarshal(raw, &oe)
		return oe
	}
	var joined string
	for _, snapshot := range []string{"The", "The answer", "The answer is 42."} {
		for _, r := range reduceEvent(event(snapshot, ""), seen) {
			joined += r.Payload.Text
		}
	}
	if joined != "The answer is 42." {
		t.Fatalf("snapshots assembled to %q", joined)
	}
	// A repeated snapshot is not new text.
	if got := reduceEvent(event("The answer is 42.", ""), seen); len(got) != 0 {
		t.Fatalf("a repeated snapshot produced %+v, want nothing", got)
	}
	// Deltas and snapshots are two shapes of the same stream: after deltas, a
	// final full-text snapshot must not repeat what the deltas already sent.
	seen = map[string]int{}
	joined = ""
	for _, d := range []string{"one ", "two ", "three"} {
		for _, r := range reduceEvent(event("one two three", d), seen) {
			joined += r.Payload.Text
		}
	}
	joined += strings.Join(texts(reduceEvent(event("one two three", ""), seen)), "")
	if joined != "one two three" {
		t.Fatalf("a streamed part assembled to %q", joined)
	}
}

func texts(rs []reduced) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Payload.Text)
	}
	return out
}

// Two simultaneous first messages must share one session. The regression: a
// check-then-create outside the lock produced two sessions, and the events of
// the unbound one were dropped forever.
func TestConcurrentFirstMessageCreatesOneSession(t *testing.T) {
	f := newFakeOC(t)
	h, _ := testHub(t, testRegistry(t, f.URL))
	agent := h.reg.byName["frontend-agent"]

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.Handle(context.Background(), agent, Envelope{
				Conversation: "conv-race", Agent: agent.Name, Type: TypeMessage,
				Payload: Payload{Text: fmt.Sprintf("message %d", i)},
			}, "")
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	sessions, prompts, _ := f.counts()
	if sessions != 1 {
		t.Fatalf("created %d sessions for one conversation, want 1", sessions)
	}
	if prompts != 4 {
		t.Fatalf("prompts = %d, want 4", prompts)
	}
	// Every prompt must address the session that is actually bound, or its
	// events land nowhere.
	bound := h.convs.get("conv-race").sessionID()
	if h.convs.byOpenCodeSession(agent.Name, bound) == nil {
		t.Fatalf("session %s is not bound to the conversation", bound)
	}
}

func TestConversationBelongsToOneAgent(t *testing.T) {
	f := newFakeOC(t)
	reg := testRegistry(t, f.URL)
	reg.byName["backend-agent"] = &Agent{Name: "backend-agent", OpenCode: f.URL, PubKey: strings.Repeat("cd", 32), ck: map[string][32]byte{}}
	h, srv := testHub(t, reg)

	if code, _ := post(t, srv.URL+"/v1/messages", `{"conversation":"shared","agent":"frontend-agent","type":"message","payload":{"text":"hi"}}`); code != http.StatusAccepted {
		t.Fatalf("first agent status %d", code)
	}
	code, _ := post(t, srv.URL+"/v1/messages", `{"conversation":"shared","agent":"backend-agent","type":"message","payload":{"text":"hi again"}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("cross-agent conversation status %d, want 400", code)
	}
	if s, p, _ := f.counts(); s != 1 || p != 1 {
		t.Fatalf("sessions=%d prompts=%d, want 1/1", s, p)
	}
	// A rejected request must not re-point the live conversation's replies.
	if c := h.convs.get("shared"); c == nil || c.peerPub() != "" {
		t.Fatalf("rejected cross-agent request changed the reply peer to %q", c.peerPub())
	}
}

func TestPermissionV2Server(t *testing.T) {
	f := newFakeOC(t)
	f.v2Permissions = true
	h, srv := testHub(t, testRegistry(t, f.URL))
	sub, cancelSub := h.subscribe("conv-v2")
	defer cancelSub()

	post(t, srv.URL+"/v1/messages", `{"conversation":"conv-v2","agent":"frontend-agent","type":"message","payload":{"text":"go"}}`)
	want(t, next(t, sub), TypeAck, "queued")

	// A current OpenCode names the event permission.asked with requestID.
	f.push("permission.asked", map[string]any{
		"id": "per_2", "requestID": "req_2", "sessionID": "ses_1", "title": "write /etc/hosts",
	})
	req := next(t, sub)
	want(t, req, TypePermissionReq, "")
	if req.Payload.PermissionID != "req_2" {
		t.Fatalf("permission id %q, want requestID req_2", req.Payload.PermissionID)
	}

	if code, _ := post(t, srv.URL+"/v1/messages",
		`{"conversation":"conv-v2","agent":"frontend-agent","type":"permission_response","payload":{"permission_id":"req_2","text":"always"}}`); code != http.StatusAccepted {
		t.Fatalf("permission_response status %d", code)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.perms) != 1 || f.perms[0] != "ses_1/req_2/reply:always" {
		t.Fatalf("permission reply did not fall back to the v2 endpoint: %v", f.perms)
	}
}

// An event bigger than the old 8MB scanner buffer must not tear down the
// agent's whole stream.
func TestOversizedEventSurvives(t *testing.T) {
	f := newFakeOC(t)
	h, srv := testHub(t, testRegistry(t, f.URL))
	sub, cancelSub := h.subscribe("conv-big")
	defer cancelSub()

	post(t, srv.URL+"/v1/messages", `{"conversation":"conv-big","agent":"frontend-agent","type":"message","payload":{"text":"go"}}`)
	want(t, next(t, sub), TypeAck, "queued")

	huge := strings.Repeat("x", 9<<20)
	f.push("message.part.updated", map[string]any{
		"part": map[string]any{"id": "big", "type": "text", "text": huge, "sessionID": "ses_1"},
	})
	f.push("session.idle", map[string]any{"sessionID": "ses_1"})

	if got := next(t, sub); got.Payload.Text != huge {
		t.Fatalf("oversized event lost: got %d bytes, want %d", len(got.Payload.Text), len(huge))
	}
	want(t, next(t, sub), TypeCompleted, "") // the stream survived
}

func TestSubscribeReplaysHistory(t *testing.T) {
	f := newFakeOC(t)
	h, srv := testHub(t, testRegistry(t, f.URL))

	// The documented flow posts first and opens the stream afterwards.
	code, ack := post(t, srv.URL+"/v1/messages", `{"conversation":"conv-late","agent":"frontend-agent","type":"message","payload":{"text":"go"}}`)
	if code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}
	f.push("message.part.updated", map[string]any{
		"delta": "the answer", "part": map[string]any{"id": "p1", "type": "text", "sessionID": "ses_1"},
	})

	sub, cancelSub := h.subscribe(ack.Conversation)
	defer cancelSub()
	want(t, next(t, sub), TypeAck, "queued")
	want(t, next(t, sub), TypeMessage, "the answer")
}

func TestProtocolVersionRejected(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))
	if code, _ := post(t, srv.URL+"/v1/messages",
		`{"v":99,"agent":"frontend-agent","type":"message","payload":{"text":"hi"}}`); code != http.StatusBadRequest {
		t.Fatalf("future protocol version status %d, want 400", code)
	}
	if s, _, _ := f.counts(); s != 0 {
		t.Fatalf("future version created %d sessions", s)
	}
}

// The backoff has to ratchet while a peer keeps failing and start over once a
// connection outlived the queued wait, or a few restarts in a day pin an agent
// at the 5 minute ceiling forever.
func TestReconnectBackoffRatchetsThenResets(t *testing.T) {
	for _, c := range []struct {
		prev, connected, want time.Duration
		why                   string
	}{
		{reconnectBase, 0, 2 * reconnectBase, "connect refused"},
		{4 * reconnectBase, reconnectBase, 8 * reconnectBase, "stream died instantly"},
		{reconnectMax, time.Minute, reconnectMax, "capped, still flapping"},
		{reconnectMax, 2 * reconnectMax, reconnectBase, "healthy stream ended"},
	} {
		if got := reconnectDelay(c.prev, c.connected); got != c.want {
			t.Errorf("%s: delay %s, want %s", c.why, got, c.want)
		}
	}
}

func TestOversizedRequestBodyRejected(t *testing.T) {
	f := newFakeOC(t)
	_, srv := testHub(t, testRegistry(t, f.URL))
	big := strings.Repeat("y", 2<<20)
	code, _ := post(t, srv.URL+"/v1/messages",
		`{"agent":"frontend-agent","type":"message","payload":{"text":"`+big+`"}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("2MB body status %d, want 400", code)
	}
}

// A part that streamed deltas must not also emit the final full-text snapshot:
// the client would see the whole message twice.
func TestDeltaThenSnapshotDoesNotDouble(t *testing.T) {
	seen := map[string]int{}
	first := opencodeEvent{Type: "message.part.updated", Properties: json.RawMessage(
		`{"delta":"Hel","part":{"id":"p1","type":"text","text":"Hello"}}`)}
	second := opencodeEvent{Type: "message.part.updated", Properties: json.RawMessage(
		`{"delta":"lo","part":{"id":"p1","type":"text","text":"Hello"}}`)}
	snapshot := opencodeEvent{Type: "message.part.updated", Properties: json.RawMessage(
		`{"part":{"id":"p1","type":"text","text":"Hello"}}`)}

	for _, want := range []string{"Hel", "lo"} {
		got := reduceEvent(first, seen)
		if len(got) != 1 || got[0].Payload.Text != want {
			t.Fatalf("delta %q: got %+v", want, got)
		}
		first, second = second, first
	}
	if got := reduceEvent(snapshot, seen); len(got) != 0 {
		t.Fatalf("snapshot after deltas emitted %+v", got)
	}
}

// buzzJob runs on the one goroutine that publishes for every agent, so a turn
// reaching `completed` with nothing accumulated is a process crash, not a failed
// message: *strings.Builder dereferences its own fields. Two real turns get
// there — one that errors before its first delta, and one whose permission
// request posts on its own and clears the turn on the way out. The first job in
// each pair needs a buzz pool to post into, and neither posts anything, so this
// runs without a relay.
func TestBuzzJobCompletesATurnThatNeverAccumulated(t *testing.T) {
	agent := &Agent{Name: "frontend-agent", PubKey: strings.Repeat("ab", 32), sk: nostr.GeneratePrivateKey(), ck: map[string][32]byte{}}
	// A pool with no relays: signing still runs, publishing resolves immediately.
	nt := &nostrTransport{
		log:       slog.New(slog.DiscardHandler),
		turns:     map[string]*strings.Builder{},
		buzzPools: map[string]*nostr.SimplePool{agent.Name: nostr.NewSimplePool(context.Background())},
	}
	conv := buzzConversation("channel-dm")
	job := func(typ, text string) job {
		return job{agent: agent, buzz: "channel-dm", env: Envelope{Conversation: conv, Type: typ, Payload: Payload{Text: text}}}
	}

	// A turn that produced no text of its own.
	if err := nt.buzzJob(context.Background(), job(TypeCompleted, "")); err != nil {
		t.Fatalf("empty completed turn: %v", err)
	}
	// A permission request posts on its own and takes the accumulated turn with
	// it, so the turn that follows also completes with nothing in hand.
	if err := nt.buzzJob(context.Background(), job(TypeMessage, "half an answer")); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := nt.buzzJob(context.Background(), job(TypePermissionReq, "may I?")); err != nil {
		t.Fatalf("permission request: %v", err)
	}
	if err := nt.buzzJob(context.Background(), job(TypeCompleted, "")); err != nil {
		t.Fatalf("completed after a permission request: %v", err)
	}
	// An error discards the partial turn rather than posting it, and leaves no
	// entry behind for the completed that follows to trip over.
	if err := nt.buzzJob(context.Background(), job(TypeMessage, "half an answer")); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := nt.buzzJob(context.Background(), job(TypeError, "")); err != nil {
		t.Fatalf("empty error: %v", err)
	}
	if err := nt.buzzJob(context.Background(), job(TypeCompleted, "")); err != nil {
		t.Fatalf("completed after an error: %v", err)
	}
	if len(nt.turns) != 0 {
		t.Fatalf("in-flight turns left behind: %v", nt.turns)
	}
}

// One list, three variables. RELAYS is the name; the two old names still work,
// their entries merge in, and a relay named in two of them is one relay — the
// same websocket twice is the NIP-42 collision buzzPools exists to avoid.
// Nothing set at all falls back to the public defaults rather than to nothing.
func TestRelayListMergesTheDeprecatedVariables(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	t.Setenv("RELAYS", "wss://one.example, wss://two.example")
	t.Setenv("NOSTR_RELAYS", "wss://two.example")
	t.Setenv("BUZZ_RELAYS", "wss://three.example")
	got := relayList(log)
	want := []string{"wss://one.example", "wss://two.example", "wss://three.example"}
	if !slices.Equal(got, want) {
		t.Fatalf("relay list %v, want %v", got, want)
	}

	t.Setenv("RELAYS", "")
	t.Setenv("NOSTR_RELAYS", "")
	t.Setenv("BUZZ_RELAYS", "")
	if got := relayList(log); !slices.Equal(got, splitCSV(defaultRelays)) {
		t.Fatalf("unset relays gave %v, want the defaults %v", got, splitCSV(defaultRelays))
	}
}

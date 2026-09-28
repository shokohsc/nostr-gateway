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

func (h *hub) serveHTTP(ctx context.Context, addr, token string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h.handler(token),
		ReadHeaderTimeout: 10 * time.Second,
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

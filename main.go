package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const defaultRelays = "wss://nos.lol,wss://relay.damus.io"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))

	reg, err := loadRegistry()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	h := newHub(reg, log)

	token := os.Getenv("GATEWAY_TOKEN")
	relays := relayList(log)
	log.Info("relays", "relays", strings.Join(relays, ","),
		"roles", "encrypted kind-30078 envelopes in and out, Buzz channels read and answered, agent profile and presence published")
	nt := newNostrTransport(ctx, relays, reg, h, log)
	h.nostr = nt
	go h.run(ctx)
	go nt.run(ctx)

	log.Info("agents", "names", strings.Join(reg.names(), ","))
	if token == "" {
		log.Warn("GATEWAY_TOKEN is unset: the HTTP API is open to anything that can reach it")
	}
	if err := h.serveHTTP(ctx, envOr("GATEWAY_ADDR", ":8080"), token); err != nil {
		log.Error("http", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// relayList is every relay the gateway talks to, as one list, because there is
// no such thing here as a nostr relay and a Buzz relay: the gateway runs both
// roles on every relay it is given, and a relay that answers NIP-29 differs from
// one that does not only in what comes back.
//
// RELAYS is the name. NOSTR_RELAYS and BUZZ_RELAYS are still read and their
// entries merged in, because a ConfigMap still carrying only those two would
// otherwise be read as "none set" and fall back to the public defaults — an agent
// on a private Buzz relay going deaf with a clean log. Deprecated, warned about,
// and never the only way to say it.
func relayList(log *slog.Logger) []string {
	var out []string
	seen := map[string]bool{}
	for _, env := range []string{"RELAYS", "NOSTR_RELAYS", "BUZZ_RELAYS"} {
		if env != "RELAYS" && os.Getenv(env) != "" {
			log.Warn("relay variable is deprecated: every relay goes in RELAYS now, both roles included",
				"var", env)
		}
		for _, r := range splitCSV(os.Getenv(env)) {
			if seen[r] { // the same relay in two of the three variables is still one relay
				continue
			}
			seen[r] = true
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return splitCSV(defaultRelays)
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func logLevel() slog.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

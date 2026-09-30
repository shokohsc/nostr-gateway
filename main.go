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
	relays := splitCSV(envOr("NOSTR_RELAYS", defaultRelays))
	log.Info("nostr relays", "relays", strings.Join(relays, ","))
	nt := newNostrTransport(ctx, relays, splitCSV(os.Getenv("BUZZ_RELAYS")), reg, h, log)
	if len(nt.buzzRelays) > 0 {
		log.Info("buzz relays (channels read, channel answers written, agent profile published)", "relays", strings.Join(nt.buzzRelays, ","))
	}
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

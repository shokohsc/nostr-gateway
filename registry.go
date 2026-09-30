package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/nbd-wtf/go-nostr/nip44"
)

// Agent is one OpenCode server plus the Nostr identity that fronts it.
type Agent struct {
	Name string `json:"-"`

	// OpenCode is the cluster-internal base URL of `opencode serve`.
	OpenCode string `json:"opencode"`
	// PubKey is the agent's Nostr public key: hex, or npub1... in the config.
	PubKey string `json:"npub"`
	// NSecEnv names the environment variable holding the agent's private key,
	// so secrets stay in Kubernetes Secrets and out of this config.
	NSecEnv string `json:"nsec_env"`
	// Allow lists the hex pubkeys allowed to talk to this agent. Empty = anyone.
	Allow []string `json:"allow"`
	// Model and OpencodeAgent are optional overrides passed to prompt_async.
	Model         string `json:"model"`
	OpencodeAgent string `json:"opencode_agent"`

	sk string
	mu sync.Mutex // guards ck; the inbound and publish paths both derive keys
	ck map[string][32]byte
}

// Registry routes by agent name.
type Registry struct {
	byName map[string]*Agent
}

func (r *Registry) names() []string {
	out := make([]string, 0, len(r.byName))
	for n := range r.byName {
		out = append(out, n)
	}
	return out
}

// loadRegistry reads AGENTS_FILE (path to JSON) or AGENTS (inline JSON):
//
//	{"frontend-agent": {"opencode": "http://frontend-agent:4096",
//	                    "npub": "npub1...", "nsec_env": "FRONTEND_NSEC",
//	                    "allow": ["<hex pubkey>"], "model": "...", "opencode_agent": "build"}}
//
// Keys are normalised to hex here and only here: everything downstream — p-tag
// filters, the NIP-44 key, the allow list — speaks hex.
//
// ponytail: static config, no CRD watcher. If agents must be added or removed
// at runtime, replace loadRegistry with a KubeOpenCode Agent/AgentTemplate
// watcher that populates byName under a lock.
func loadRegistry() (*Registry, error) {
	raw := os.Getenv("AGENTS")
	if path := os.Getenv("AGENTS_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		raw = string(b)
	}
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("set AGENTS_FILE or AGENTS")
	}

	var cfg map[string]*Agent
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, fmt.Errorf("parse agents: %w", err)
	}

	r := &Registry{byName: map[string]*Agent{}}
	for name, a := range cfg {
		if a.OpenCode == "" {
			return nil, fmt.Errorf("agent %q: missing opencode url", name)
		}
		pub, err := decodeKey(a.PubKey)
		if err != nil {
			return nil, fmt.Errorf("agent %q: npub: %w", name, err)
		}
		if err := validPubKey(pub); err != nil {
			return nil, fmt.Errorf("agent %q: npub: %w", name, err)
		}
		a.PubKey = pub
		if a.NSecEnv != "" {
			sk, err := decodeKey(os.Getenv(a.NSecEnv))
			if err != nil {
				return nil, fmt.Errorf("agent %q: %s: %w", name, a.NSecEnv, err)
			}
			if sk == "" {
				return nil, fmt.Errorf("agent %q: %s is empty", name, a.NSecEnv)
			}
			// One identity, not two keys: every p-tag filter — the kind-30078
			// answers and the Buzz member lists — asks the relay about
			// a.PubKey, while the connection authenticates as whatever a.sk
			// derives. A pair that disagrees is well formed and matches nothing
			// the relay holds for us, so the agent hears nothing and the only
			// log line blames the relay. Name both keys and stop.
			derived, err := nostr.GetPublicKey(sk)
			if err != nil {
				return nil, fmt.Errorf("agent %q: %s: not a private key: %w", name, a.NSecEnv, err)
			}
			if derived != a.PubKey {
				return nil, fmt.Errorf("agent %q: npub %s is not the pubkey of %s (that key is %s): every p-tag filter would ask about a key this agent does not own",
					name, a.PubKey[:8], a.NSecEnv, derived[:8])
			}
			a.sk = sk
		}
		for i, p := range a.Allow {
			h, err := decodeKey(p)
			if err != nil {
				return nil, fmt.Errorf("agent %q: allow[%d]: %w", name, i, err)
			}
			if err := validPubKey(h); err != nil {
				return nil, fmt.Errorf("agent %q: allow[%d]: %w", name, i, err)
			}
			a.Allow[i] = h
		}
		a.Name = name
		a.ck = map[string][32]byte{}
		r.byName[name] = a
	}
	return r, nil
}

// conversationKey caches the NIP-44 key per peer: deriving one is a scalar
// multiply, and the pair is stable for the life of the process. Inbound and
// publish goroutines share this, hence the mutex.
func (a *Agent) conversationKey(peerPub string) ([32]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ck, ok := a.ck[peerPub]; ok {
		return ck, nil
	}
	ck, err := nip44.GenerateConversationKey(peerPub, a.sk)
	if err != nil {
		return ck, err
	}
	a.ck[peerPub] = ck
	return ck, nil
}

func (a *Agent) allows(hexPub string) bool {
	if len(a.Allow) == 0 {
		return true
	}
	for _, p := range a.Allow {
		if p == hexPub {
			return true
		}
	}
	return false
}

func (a *Agent) npub() string {
	s, err := nip19.EncodePublicKey(a.PubKey)
	if err != nil {
		return a.PubKey
	}
	return s
}

func validPubKey(h string) error {
	if len(h) != 64 {
		return fmt.Errorf("not a 32-byte hex key")
	}
	if _, err := hex.DecodeString(h); err != nil {
		return fmt.Errorf("not hex: %w", err)
	}
	return nil
}

// decodeKey turns npub1/nsec1/note1 into hex and passes hex through.
func decodeKey(s string) (string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "npub1") && !strings.HasPrefix(s, "nsec1") && !strings.HasPrefix(s, "note1") {
		return s, nil
	}
	_, v, err := nip19.Decode(s)
	if err != nil {
		return "", err
	}
	h, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unexpected key encoding")
	}
	return h, nil
}

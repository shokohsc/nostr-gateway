package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// The relay side of the gateway is one subscriber per agent key, and an agent key
// is one identity. Two pods holding a REQ for the same `p` tag turn every inbound
// message into two prompts in two OpenCode sessions and put the answer on the wire
// twice, which is why `deploy/k8s.yaml` used to run Recreate at `replicas: 1` — the
// only strategy that keeps one pod at a time, and also the reason one node's
// eviction takes the whole gateway with it.
//
// A Lease is what lets the second pod exist. Exactly one replica subscribes to the
// relays; every replica serves the HTTP API, and that needs no coordination at
// all: a chat completion is prompted, waited for and answered inside its own
// request, and `hub.emit` only reaches Nostr for a conversation that has a Nostr
// peer, which only a relay event ever sets. So a follower is not a degraded
// gateway — it is a complete gateway on the HTTP face and idle on the relay face.
const (
	// leaseDuration is how long a claim is honoured after its last renewal and
	// leaseRenew how often the holder refreshes it. The gap is the tolerance for
	// clock skew between nodes, which is what stops one pod judging another's
	// fresh write stale.
	// ponytail: constants, not configuration. A deployment that needs a different
	// failover time should change this, not every pod's environment.
	leaseDuration = 15 * time.Second
	leaseRenew    = 5 * time.Second
	// leaseRetry is the pause between two looks at the lease: after losing one,
	// after failing to reach the API server, and after losing it deliberately.
	leaseRetry = 2 * time.Second

	leaseAPIVersion = "coordination.k8s.io/v1"
	saTokenPath     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saCAPath        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// election is this process's claim on the relay side of the gateway, kept in a
// coordination.k8s.io Lease so exactly one replica of the gateway holds it.
// Kubernetes is used for this and nothing else: it is the one place two replicas
// of this program can be told apart from each other, and the Lease API is four
// verbs of JSON over HTTPS — not worth a client-go dependency for.
type election struct {
	log    *slog.Logger
	client *http.Client
	base   string // the API server, e.g. https://10.0.0.1:443
	token  string
	holder string // this pod's identity: POD_NAME, or the hostname
	ns     string
	name   string
}

// leaderElection returns nil when there is no lease to hold: `LEASE_NAME` unset
// means one replica, which is what a developer running the binary locally has,
// and nothing has to be coordinated with anything. It is an error rather than a
// nil when `LEASE_NAME` is set and the pod cannot reach the API server, because
// the alternative is a replica that believes it is the only one, subscribes to
// the relays and is deaf by accident — with every other replica doing the same
// and no log that says why. A pod that cannot do its job should say so loudly
// enough to be noticed.
func leaderElection(log *slog.Logger) (*election, error) {
	name := os.Getenv("LEASE_NAME")
	if name == "" {
		return nil, nil
	}
	token, err := os.ReadFile(saTokenPath)
	if err != nil {
		return nil, fmt.Errorf("LEASE_NAME is set but this pod has no service account token: %w", err)
	}
	ca, err := os.ReadFile(saCAPath)
	if err != nil {
		return nil, fmt.Errorf("cluster CA: %w", err)
	}
	// The cluster CA is the whole trust store for this client: the API server is
	// the only thing it ever talks to, and it is not in the image's CA bundle.
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	ns := envOr("POD_NAMESPACE", "default")
	holder := envOr("POD_NAME", mustHostname())
	log.Info("relay lease", "lease", ns+"/"+name, "holder", holder)
	return &election{
		log:    log,
		client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		base:   apiServer(),
		token:  string(token),
		holder: holder,
		ns:     ns,
		name:   name,
	}, nil
}

// apiServer is the in-cluster address, from the service env vars every pod is
// given. The port defaults to 443 because KUBERNETES_SERVICE_PORT_HTTPS is
// present on every pod that has the kubernetes Service in view and there is
// nothing to fall back to if it is not.
func apiServer() string {
	host := envOr("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	port := envOr("KUBERNETES_SERVICE_PORT_HTTPS", "443")
	return "https://" + host + ":" + port
}

func mustHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "nostr-gateway"
	}
	return h
}

func (e *election) path() string {
	return fmt.Sprintf("%s/apis/%s/namespaces/%s/leases/%s", e.base, leaseAPIVersion, e.ns, e.name)
}

// lease is the coordination.k8s.io/v1 Lease with only the fields this mechanism
// reads and writes. The API server fills in the rest.
type lease struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity       string `json:"holderIdentity,omitempty"`
		LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
		AcquireTime          string `json:"acquireTime,omitempty"`
		RenewTime            string `json:"renewTime,omitempty"`
	} `json:"spec"`
}

// hold runs duty for as long as this process holds the lease, then stands by until
// it gets it back. It returns only when the process is shutting down.
//
// Two things follow from being the only thing that starts the listeners. A pod
// that cannot renew gives the duty up rather than carrying on with a claim it can
// no longer prove, because another pod may hold it by then — and two subscribers
// is a second prompt for every message. And it releases the lease on the way out,
// so the pod taking over does not sit out leaseDuration doing nothing.
func (e *election) hold(ctx context.Context, duty func(context.Context)) {
	for ctx.Err() == nil {
		switch won, err := e.campaign(ctx); {
		case err != nil:
			e.log.Warn("relay lease: could not claim it", "err", err)
		case won:
			e.log.Info("relay lease held: this replica subscribes to the relays",
				"lease", e.ns+"/"+e.name, "holder", e.holder)
			term, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer cancel()
				e.renew(term)
			}()
			duty(term)
			cancel()
			// The renewer is gone before the next claim, so one process never has
			// two writers on its own lease.
			<-done
			if ctx.Err() == nil { // not a shutdown: the lease went away under us
				e.log.Warn("relay lease lost: this replica stops subscribing to the relays",
					"holder", e.holder)
			}
		}
		if !wait(ctx, leaseRetry) {
			e.release()
			return
		}
	}
}

// renew keeps the claim alive and returns as soon as this pod can no longer prove
// it holds it. The proof runs out halfway to expiry, not at expiry: by then
// another pod is entitled to look at the lease, and a pod that keeps listening
// after it has lost the right to is exactly the double-subscriber case.
func (e *election) renew(ctx context.Context) {
	provable := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaseRenew):
		}
		won, err := e.campaign(ctx)
		switch {
		case err == nil && won:
			provable = time.Now().Add(leaseDuration / 2)
		case err != nil && time.Now().Before(provable):
			// One unreachable API server is a blip, not a lost lease, and the
			// claim is still good for the rest of its duration.
		default:
			if err != nil {
				e.log.Warn("relay lease: renewal failed and the claim is running out", "err", err)
			}
			return
		}
	}
}

// campaign claims the lease if it can, and reports whether it did. It is one
// compare-and-swap: the write carries the resourceVersion it was read at, so of
// two pods racing for an expired lease exactly one lands and the other gets a 409
// and comes back in two seconds.
func (e *election) campaign(ctx context.Context) (bool, error) {
	code, body, err := e.do(ctx, http.MethodGet, nil)
	switch {
	case err != nil:
		return false, err
	case code == http.StatusNotFound:
		// Nothing has claimed it yet. A create is atomic, so this is the one
		// race the API server settles for us.
		return e.claim(ctx, nil, leaseDuration)
	case code != http.StatusOK:
		return false, fmt.Errorf("get lease: %d %s", code, body)
	}
	var cur lease
	if err := json.Unmarshal(body, &cur); err != nil {
		return false, fmt.Errorf("read lease: %w", err)
	}
	// Our own lease is renewed with the same write, which is also what a pod that
	// reconnects to its own claim (a new network connection, an API server
	// restart) needs.
	if cur.Spec.HolderIdentity == e.holder || expired(&cur, time.Now()) {
		return e.claim(ctx, &cur, leaseDuration)
	}
	return false, nil // a live claim by another pod
}

// claim writes the lease with this process as the holder. A nil cur is a create,
// which only wins when there is no lease at all; otherwise it is a replace of
// exactly that revision of it. A 409 means another pod won.
func (e *election) claim(ctx context.Context, cur *lease, duration time.Duration) (bool, error) {
	now := time.Now().UTC()
	next := lease{APIVersion: leaseAPIVersion, Kind: "Lease"}
	next.Metadata.Name, next.Metadata.Namespace = e.name, e.ns
	next.Spec.HolderIdentity = e.holder
	next.Spec.LeaseDurationSeconds = int(duration / time.Second)
	next.Spec.AcquireTime = now.Format(time.RFC3339)
	next.Spec.RenewTime = next.Spec.AcquireTime
	method := http.MethodPost
	if cur != nil {
		next.Metadata.ResourceVersion = cur.Metadata.ResourceVersion
		next.Spec.AcquireTime = cur.Spec.AcquireTime
		method = http.MethodPut
	}
	body, err := json.Marshal(next)
	if err != nil {
		return false, err
	}
	code, out, err := e.do(ctx, method, body)
	switch {
	case err != nil:
		return false, err
	case code == http.StatusConflict:
		return false, nil
	case code < 300:
		return true, nil
	}
	return false, fmt.Errorf("write lease: %d %s", code, out)
}

// release shortens the lease on the way out, so the next pod does not have to wait
// leaseDuration for a claim nobody is using. It re-reads the lease and only writes
// while it is still ours: a stale write here would take the lease away from the pod
// that has just been given it, and a holder that loses its own lease stops
// listening. If the read or the write fails the claim simply expires.
func (e *election) release() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), callTimeout)
	defer cancel()
	_, body, err := e.do(ctx, http.MethodGet, nil)
	var cur lease
	if err != nil || json.Unmarshal(body, &cur) != nil {
		return
	}
	if cur.Spec.HolderIdentity != e.holder {
		return
	}
	if _, err := e.claim(ctx, &cur, time.Second); err != nil {
		e.log.Warn("relay lease: could not hand it back", "err", err)
	}
}

// expired is the whole rule Kubernetes applies to a Lease: the claim is good until
// renewTime + leaseDurationSeconds. An unreadable renewTime is not a claim anybody
// should keep honouring.
func expired(l *lease, now time.Time) bool {
	renew, err := time.Parse(time.RFC3339, l.Spec.RenewTime)
	if err != nil {
		return true
	}
	return now.After(renew.Add(time.Duration(l.Spec.LeaseDurationSeconds) * time.Second))
}

func (e *election) do(ctx context.Context, method string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var in io.Reader
	if body != nil {
		in = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, e.path(), in)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := e.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	return res.StatusCode, out, err
}

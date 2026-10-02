package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeLeaseAPI is the coordination.k8s.io/v1 Lease endpoint with the two
// properties the election depends on and nothing else: a create only wins when
// there is no lease, and a write only wins against the exact revision it was read
// at. Everything else — auth, admission, the rest of the API — is the cluster's
// problem, and a mechanism that cannot be exercised without one is a mechanism
// nobody runs.
type fakeLeaseAPI struct {
	mu     sync.Mutex
	stored lease
	rev    int // 0 means no lease exists yet
}

func (f *fakeLeaseAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method != http.MethodGet {
		var next lease
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// A create only wins against an absent lease, a write only against the
		// exact revision it was read at. Those two 409s are the whole election.
		if r.Method == http.MethodPost && f.rev != 0 {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if r.Method == http.MethodPut && next.Metadata.ResourceVersion != strconv.Itoa(f.rev) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.rev++
		next.Metadata.ResourceVersion = strconv.Itoa(f.rev)
		f.stored = next
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(f.stored)
		return
	}
	if f.rev == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(f.stored)
}

// expire moves the lease's renewTime into the past, which is what a pod being
// evicted looks like from here: it stops renewing and nobody has to tell anyone.
func (f *fakeLeaseAPI) expire(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rev == 0 {
		t.Fatal("no lease to expire")
	}
	f.stored.Spec.RenewTime = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
}

func (f *fakeLeaseAPI) holder(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stored.Spec.HolderIdentity
}

func (f *fakeLeaseAPI) duration(t *testing.T) int {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stored.Spec.LeaseDurationSeconds
}

// pod is one replica of the gateway, wired to a fake API server instead of the
// one in the cluster.
func pod(t *testing.T, srv *httptest.Server, name string) *election {
	t.Helper()
	return &election{
		log:    slog.New(slog.DiscardHandler),
		client: srv.Client(),
		base:   srv.URL,
		token:  "test-token",
		holder: name,
		ns:     "kubeopencode",
		name:   "nostr-gateway",
	}
}

// TestOneReplicaHoldsTheRelayLease is the whole safety argument of running more
// than one pod: two of them ask for the same claim at the same time and exactly
// one gets it, the loser stays out until the holder stops renewing, and then it
// takes over without the holder's help. Two holders is a second prompt for every
// message the agents receive.
func TestOneReplicaHoldsTheRelayLease(t *testing.T) {
	api := &fakeLeaseAPI{}
	srv := httptest.NewServer(api)
	defer srv.Close()
	ctx := context.Background()
	a, b := pod(t, srv, "pod-a"), pod(t, srv, "pod-b")

	assertClaim := func(e *election, want bool, what string) {
		t.Helper()
		got, err := e.campaign(ctx)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if got != want {
			t.Fatalf("%s: claimed=%v, want %v", what, got, want)
		}
	}

	assertClaim(a, true, "the first pod to arrive takes the empty lease")
	if h := api.holder(t); h != "pod-a" {
		t.Fatalf("lease holder is %q, want pod-a", h)
	}
	assertClaim(b, false, "a second pod must not take a live lease")
	assertClaim(a, true, "the holder renews its own lease")
	assertClaim(b, false, "and the other pod is still locked out")

	// The holder goes away without saying anything, as an evicted pod does.
	api.expire(t)
	assertClaim(b, true, "an expired lease is claimable by anyone")
	if h := api.holder(t); h != "pod-b" {
		t.Fatalf("lease holder is %q, want pod-b", h)
	}
	assertClaim(a, false, "the pod that stopped renewing must not take it back")

	// A pod handing the lease back on its way out must not take it from whoever
	// was given it: the holder watches its own claim and would stop listening.
	a.release()
	if h := api.holder(t); h != "pod-b" {
		t.Fatalf("release by a pod that no longer holds the lease changed it to %q", h)
	}
	b.release()
	if h := api.holder(t); h != "pod-b" {
		t.Fatalf("release by the holder changed it to %q", h)
	}
	if d := api.duration(t); d != 1 {
		t.Fatalf("a released lease lasts %ds, want 1s so the next pod does not wait leaseDuration", d)
	}
}

// TestRelayDutyRunsUnderTheLeaseAndStopsWithTheProcess is the other half: holding
// the lease starts the listeners, and a terminating process ends them rather than
// leaving a subscription behind on a relay the next pod is about to subscribe to.
func TestRelayDutyRunsUnderTheLeaseAndStopsWithTheProcess(t *testing.T) {
	api := &fakeLeaseAPI{}
	srv := httptest.NewServer(api)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pod(t, srv, "pod-a").hold(ctx, func(ctx context.Context) {
			close(entered)
			<-ctx.Done()
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the replica holding the lease never started its relay listeners")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("hold did not return after the process was told to stop")
	}
}

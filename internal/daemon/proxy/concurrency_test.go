package proxy

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// TestConcurrencyCap: pick skips endpoints already at max_concurrency and
// resumes using them once capacity frees.
func TestConcurrencyCap(t *testing.T) {
	b := newBalancer("n1")
	eps := []*clusterv1.Endpoint{endpoint("a:1", "n1"), endpoint("b:2", "n1")}
	first := func(int) int { return 0 }

	// No cap → picks normally.
	if b.pick(eps, first, 0) == nil {
		t.Fatal("uncapped pick returned nil")
	}

	// Fill a:1 to the cap of 1 → pick must avoid it.
	relA := b.acquire("a:1")
	if got := b.pick(eps, first, 1); got == nil || got.GetAddr() != "b:2" {
		t.Fatalf("capped pick should skip the full endpoint, got %v", got)
	}

	// Fill b:2 too → nothing available under the cap.
	relB := b.acquire("b:2")
	if got := b.pick(eps, first, 1); got != nil {
		t.Fatalf("all endpoints at cap should yield nil, got %v", got.GetAddr())
	}

	// Free a:1 → it becomes available again.
	relA()
	if got := b.pick(eps, first, 1); got == nil || got.GetAddr() != "a:1" {
		t.Fatalf("freed endpoint should be pickable, got %v", got)
	}
	relB()
}

// TestL7ConcurrencyBackpressure: with max_concurrency=1 and a single replica
// busy, a second request parks and is served once the first frees the slot.
func TestL7ConcurrencyBackpressure(t *testing.T) {
	release := make(chan struct{})
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		<-release // hold the slot until the test releases it
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	route := &clusterv1.HTTPRoute{
		Hostname: "app", EnvironmentId: "e1", MaxConcurrency: 1,
		Endpoints: []*clusterv1.Endpoint{endpoint(addr, "n1")},
	}
	src := newMutableSource(&clusterv1.RouteSnapshot{Version: 1, HttpRoutes: []*clusterv1.HTTPRoute{route}})
	p := NewL7(src, "n1", clock.NewFake())
	p.SetActivator(NewActivator(src, func(context.Context, string) error { return nil }, clock.NewFake()))

	// First request occupies the single concurrency slot (blocks in the backend).
	go func() {
		req := httptest.NewRequest("GET", "http://app/", nil)
		req.TLS = &tls.ConnectionState{}
		p.ServeHTTP(httptest.NewRecorder(), req)
	}()
	waitFor(t, func() bool { return atomic.LoadInt32(&hits) == 1 }, 2*time.Second)

	// Free the slot shortly; the second request must park then proxy.
	go func() { time.Sleep(40 * time.Millisecond); close(release) }()
	rec := doReq(t, p, "GET", "http://app/", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("backpressured request not served: code=%d body=%q", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Fatalf("expected 2 backend hits, got %d", hits)
	}
}

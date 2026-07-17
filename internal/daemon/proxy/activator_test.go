package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// mutableSource is a RouteSource whose snapshot can be updated at runtime,
// broadcasting each change to Updates subscribers.
type mutableSource struct {
	mu   sync.Mutex
	snap *clusterv1.RouteSnapshot
	subs map[chan *clusterv1.RouteSnapshot]struct{}
}

func newMutableSource(snap *clusterv1.RouteSnapshot) *mutableSource {
	return &mutableSource{snap: snap, subs: map[chan *clusterv1.RouteSnapshot]struct{}{}}
}

func (m *mutableSource) Current() *clusterv1.RouteSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap
}

func (m *mutableSource) Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot {
	ch := make(chan *clusterv1.RouteSnapshot, 1)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	ch <- m.snap
	m.mu.Unlock()
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		delete(m.subs, ch)
		close(ch)
		m.mu.Unlock()
	}()
	return ch
}

func (m *mutableSource) push(snap *clusterv1.RouteSnapshot) {
	m.mu.Lock()
	m.snap = snap
	for ch := range m.subs {
		select {
		case ch <- snap:
		default:
		}
	}
	m.mu.Unlock()
}

func coldSnap(version uint64) *clusterv1.RouteSnapshot {
	return &clusterv1.RouteSnapshot{Version: version, HttpRoutes: []*clusterv1.HTTPRoute{
		{Hostname: "app", EnvironmentId: "e1", ScaleToZero: true},
	}}
}

func warmSnap(version uint64, addr string) *clusterv1.RouteSnapshot {
	return &clusterv1.RouteSnapshot{Version: version, HttpRoutes: []*clusterv1.HTTPRoute{
		{Hostname: "app", EnvironmentId: "e1", ScaleToZero: true, Endpoints: []*clusterv1.Endpoint{endpoint(addr, "n1")}},
	}}
}

func TestActivator(t *testing.T) {
	t.Run("park_activate_flush", testParkActivateFlush)
	t.Run("queue_full_sheds_503", testQueueFullSheds)
	t.Run("deadline_504", testDeadline504)
	t.Run("oversized_body_503", testOversizedBody)
	t.Run("l7_wakes_and_proxies", testL7WakesAndProxies)
}

// testParkActivateFlush: a held request triggers activation and returns true
// once an endpoint appears.
func testParkActivateFlush(t *testing.T) {
	src := newMutableSource(coldSnap(1))
	var activated int32
	a := NewActivator(src, func(context.Context, string) error {
		atomic.AddInt32(&activated, 1)
		return nil
	}, clock.NewFake())

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://app/", nil)
	done := make(chan bool, 1)
	go func() { done <- a.Hold(w, r, "e1", coldReady(src, r)) }()

	// Activation should be requested promptly.
	waitFor(t, func() bool { return atomic.LoadInt32(&activated) == 1 }, time.Second)
	// The endpoint comes up → the hold releases with "retry".
	src.push(warmSnap(2, "127.0.0.1:9"))
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Hold returned false after an endpoint appeared")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Hold did not release after the endpoint appeared")
	}
	if n, _, _ := a.ColdStart(); n != 1 {
		t.Fatalf("cold-start not recorded: count=%d", n)
	}
}

// testQueueFullSheds: with the per-env slot full, a further request is shed with
// 503 + Retry-After.
func testQueueFullSheds(t *testing.T) {
	src := newMutableSource(coldSnap(1))
	a := NewActivator(src, func(context.Context, string) error { return nil }, clock.NewFake())
	a.maxParked = 1

	// Occupy the single slot with a request that blocks (endpoint never appears).
	blocked := httptest.NewRecorder()
	br := httptest.NewRequest("GET", "http://app/", nil)
	go func() { _ = a.Hold(blocked, br, "e1", coldReady(src, br)) }()
	waitFor(t, func() bool { return a.parkedCount("e1") == 1 }, time.Second)

	// The next one is shed immediately.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://app/", nil)
	if a.Hold(w, r, "e1", coldReady(src, r)) {
		t.Fatal("Hold should shed when the queue is full")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("shed status = %d, want 503", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("shed response missing Retry-After")
	}
}

// testDeadline504: a hold that never sees an endpoint expires with 504.
func testDeadline504(t *testing.T) {
	src := newMutableSource(coldSnap(1))
	a := NewActivator(src, func(context.Context, string) error { return nil }, clock.NewFake())
	a.holdTimeout = 150 * time.Millisecond

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://app/", nil)
	if a.Hold(w, r, "e1", coldReady(src, r)) {
		t.Fatal("Hold should not succeed without an endpoint")
	}
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("timeout status = %d, want 504", w.Code)
	}
}

// testOversizedBody: a request larger than the cold-start body cap is shed.
func testOversizedBody(t *testing.T) {
	src := newMutableSource(coldSnap(1))
	a := NewActivator(src, func(context.Context, string) error { return nil }, clock.NewFake())

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://app/", nil)
	r.ContentLength = maxColdBodyBytes + 1
	if a.Hold(w, r, "e1", coldReady(src, r)) {
		t.Fatal("oversized body should be shed")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("oversized status = %d, want 503", w.Code)
	}
}

// testL7WakesAndProxies: an end-to-end request to a cold scale-to-zero route
// parks, wakes, and proxies to the endpoint that comes up.
func testL7WakesAndProxies(t *testing.T) {
	addr, _ := backend(t, "awake")
	src := newMutableSource(coldSnap(1))
	p := NewL7(src, "n1", clock.NewFake())

	var activated int32
	p.SetActivator(NewActivator(src, func(context.Context, string) error {
		atomic.AddInt32(&activated, 1)
		// Simulate the scheduler placing a replica: the endpoint appears.
		src.push(warmSnap(2, addr))
		return nil
	}, clock.NewFake()))

	rec := doReq(t, p, "GET", "http://app/", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%q)", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "awake" {
		t.Fatalf("body = %q, want awake", rec.Body.String())
	}
	if atomic.LoadInt32(&activated) != 1 {
		t.Fatalf("activation not triggered: %d", activated)
	}
}

// coldReady is the scale-to-zero readiness predicate: the route has a healthy
// endpoint (mirrors L7's cold-start hold).
func coldReady(src RouteSource, r *http.Request) func() bool {
	return func() bool { return anyHealthy(matchHTTP(src.Current(), r).GetEndpoints()) }
}

// parkedCount reads the current parked count for an env (test helper).
func (a *Activator) parkedCount(envID string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.parked[envID]
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

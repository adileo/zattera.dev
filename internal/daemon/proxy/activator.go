package proxy

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

const (
	// defaultMaxParked bounds how many requests may wait for one env's cold start
	// at once. Beyond this the proxy sheds load with 503 Retry-After.
	defaultMaxParked = 100
	// defaultHoldTimeout bounds how long a parked request waits for an endpoint
	// before giving up with 504.
	defaultHoldTimeout = 60 * time.Second
	// holdPoll re-checks readiness while parked. Route updates wake the wait for
	// a new endpoint (cold start); the poll catches capacity freeing up as
	// in-flight requests drain (serverless backpressure), which is not a route
	// event.
	holdPoll = 25 * time.Millisecond
	// maxColdBodyBytes caps the request body accepted during a cold start: a large
	// upload must not tie up a scarce park slot while the app spins up.
	maxColdBodyBytes = 10 << 20 // 10 MiB
	// retryAfterSeconds is sent when the park queue is full.
	retryAfterSeconds = "2"
)

// ActivateFunc asks the control plane to wake a scaled-to-zero env. It returns
// when the activation has been requested (not when the endpoint is ready).
type ActivateFunc func(ctx context.Context, envID string) error

// Activator holds a request for a scaled-to-zero env: it triggers activation,
// waits (bounded) for the env to gain a healthy endpoint via route updates, then
// signals the caller to retry — or sheds/expires the request (T-70).
type Activator struct {
	source   RouteSource
	activate ActivateFunc
	clk      clock.Clock

	maxParked   int
	holdTimeout time.Duration

	mu     sync.Mutex
	parked map[string]int  // env → currently parked count
	waking map[string]bool // env → an activation RPC is in flight
	cold   coldStartStats  // cold-start latency accounting
}

// NewActivator builds an activator over a route source and an activation hook.
func NewActivator(source RouteSource, activate ActivateFunc, clk clock.Clock) *Activator {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Activator{
		source:      source,
		activate:    activate,
		clk:         clk,
		maxParked:   defaultMaxParked,
		holdTimeout: defaultHoldTimeout,
		parked:      map[string]int{},
		waking:      map[string]bool{},
	}
}

// Hold parks a request for an env until ready() reports the caller may proceed
// — a healthy endpoint appearing (scale-to-zero cold start) or a replica
// freeing capacity (serverless backpressure). It triggers activation (which
// nudges the scheduler to add replicas) and waits on route updates plus a short
// poll. Returns true when the caller should re-select an endpoint and proxy;
// false when the activator has already written a response (503 shed, 504
// timeout, or oversized body).
func (a *Activator) Hold(w http.ResponseWriter, r *http.Request, envID string, ready func() bool) bool {
	// A large body must not occupy a park slot during cold start.
	if r.ContentLength > maxColdBodyBytes {
		writeProxyError(w, http.StatusServiceUnavailable, "request too large during cold start")
		return false
	}

	if !a.reserve(envID) {
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeProxyError(w, http.StatusServiceUnavailable, "activating; queue full, retry shortly")
		return false
	}
	defer a.release(envID)

	start := a.clk.Now()
	a.triggerActivate(r.Context(), envID)

	// May already be ready (raced with another request's activation / a drain).
	if ready() {
		a.cold.record(a.clk.Now().Sub(start))
		return true
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.holdTimeout)
	defer cancel()
	updates := a.source.Updates(ctx)
	poll := time.NewTicker(holdPoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			// Deadline (or client hangup): give up.
			if r.Context().Err() != nil {
				return false // client went away; nothing to write
			}
			writeProxyError(w, http.StatusGatewayTimeout, "activation timed out")
			return false
		case _, ok := <-updates:
			if !ok {
				writeProxyError(w, http.StatusGatewayTimeout, "activation stream closed")
				return false
			}
			if ready() {
				a.cold.record(a.clk.Now().Sub(start))
				return true
			}
		case <-poll.C:
			if ready() {
				a.cold.record(a.clk.Now().Sub(start))
				return true
			}
		}
	}
}

// reserve takes a park slot for env, or reports false when the env is at
// capacity.
func (a *Activator) reserve(envID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.parked[envID] >= a.maxParked {
		return false
	}
	a.parked[envID]++
	return true
}

func (a *Activator) release(envID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.parked[envID] > 0 {
		a.parked[envID]--
	}
	if a.parked[envID] == 0 {
		delete(a.parked, envID)
	}
}

// triggerActivate issues one activation RPC per env at a time (proxy-side
// singleflight; control also debounces). Fire-and-forget: the wait loop watches
// for the endpoint.
func (a *Activator) triggerActivate(ctx context.Context, envID string) {
	if a.activate == nil {
		return
	}
	a.mu.Lock()
	if a.waking[envID] {
		a.mu.Unlock()
		return
	}
	a.waking[envID] = true
	a.mu.Unlock()

	// Detach from the request context so a fast client cancel doesn't abort the
	// activation the other parked requests are waiting on.
	go func() {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = a.activate(actx, envID)
		a.mu.Lock()
		delete(a.waking, envID)
		a.mu.Unlock()
	}()
}

// ColdStart returns cold-start latency stats for metrics/introspection.
func (a *Activator) ColdStart() (count uint64, avg time.Duration, max time.Duration) {
	return a.cold.snapshot()
}

// coldStartStats accumulates cold-start latencies (thread-safe).
type coldStartStats struct {
	mu    sync.Mutex
	count uint64
	total time.Duration
	max   time.Duration
}

func (c *coldStartStats) record(d time.Duration) {
	c.mu.Lock()
	c.count++
	c.total += d
	if d > c.max {
		c.max = d
	}
	c.mu.Unlock()
}

func (c *coldStartStats) snapshot() (count uint64, avg time.Duration, max time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count == 0 {
		return 0, 0, 0
	}
	return c.count, c.total / time.Duration(c.count), c.max
}

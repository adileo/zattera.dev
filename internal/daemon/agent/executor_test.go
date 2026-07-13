package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

func TestExecutor(t *testing.T) {
	t.Run("converge from empty to two assignments", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)

		e.reconcile(ctx(), buildSet(1,
			pair(assign("a1", "h1", run), rtp("nginx:alpine", port("http", 8080))),
			pair(assign("a2", "h1", run), rtp("redis:7")),
		))

		if got := runningCount(rt); got != 2 {
			t.Fatalf("want 2 running containers, got %d", got)
		}
		if rec.state("a1") != running || rec.state("a2") != running {
			t.Fatalf("both should be RUNNING: a1=%v a2=%v", rec.state("a1"), rec.state("a2"))
		}
		// The http port was bound and reported back.
		if p := rec.latest["a1"].GetMeshPortBindings()["http"]; p == 0 {
			t.Fatalf("expected a bound http port for a1, got %d", p)
		}
		// Labels carry identity for later adoption.
		for _, c := range rt.Snapshot() {
			if c.Spec.Labels[runtime.LabelAssignmentID] == "" || c.Spec.Labels[runtime.ManagedLabel] != "true" {
				t.Fatalf("container missing identity labels: %+v", c.Spec.Labels)
			}
		}
	})

	t.Run("remove an assignment stops and removes its container", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)

		e.reconcile(ctx(), buildSet(1,
			pair(assign("a1", "h1", run), rtp("nginx:alpine")),
			pair(assign("a2", "h1", run), rtp("redis:7")),
		))
		// a2 flips to STOP; a1 stays.
		e.reconcile(ctx(), buildSet(2,
			pair(assign("a1", "h1", run), rtp("nginx:alpine")),
			pair(assign("a2", "h1", stop), rtp("redis:7")),
		))

		if got := len(rt.Snapshot()); got != 1 {
			t.Fatalf("want 1 container after stop, got %d", got)
		}
		if rec.state("a2") != stopped {
			t.Fatalf("a2 should report STOPPED, got %v", rec.state("a2"))
		}
	})

	t.Run("config hash change replaces the container", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)

		e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), rtp("app:v1"))))
		before := onlyContainer(t, rt)

		e.reconcile(ctx(), buildSet(2, pair(assign("a1", "h2", run), rtp("app:v2"))))
		after := onlyContainer(t, rt)

		if before.ID == after.ID {
			t.Fatal("container should have been replaced (new id)")
		}
		if after.Spec.Labels[labelConfigHash] != "h2" || after.Spec.Image != "app:v2" {
			t.Fatalf("replacement should carry the new hash/image: %+v", after.Spec)
		}
		if !after.Running {
			t.Fatal("replacement should be running")
		}
	})

	t.Run("adoption after agent restart creates nothing new", func(t *testing.T) {
		rt := fakeruntime.New()
		set := buildSet(1,
			pair(assign("a1", "h1", run), rtp("nginx:alpine")),
			pair(assign("a2", "h1", run), rtp("redis:7")),
		)
		newExec(rt, discardRec()).reconcile(ctx(), set)
		idsBefore := containerIDs(rt)

		// A fresh executor (agent restarted) converges the same set.
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		newExec(rt, rec).reconcile(ctx(), set)

		if got := containerIDs(rt); !sameSet(idsBefore, got) {
			t.Fatalf("adoption should not recreate: before=%v after=%v", idsBefore, got)
		}
	})

	t.Run("pull failure reports FAILED, retries, then parks", func(t *testing.T) {
		rt := fakeruntime.New()
		rt.Hooks.FailPull = func(ref string) error {
			if ref == "bad:image" {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)

		badSet := buildSet(1, pair(assign("a1", "h1", run), rtp("bad:image")))
		// Attempt it maxAttempts+2 times; only maxAttempts should actually try.
		for i := 0; i < maxAttempts+2; i++ {
			e.reconcile(ctx(), badSet)
		}
		if rec.state("a1") != failed {
			t.Fatalf("a1 should be FAILED, got %v", rec.state("a1"))
		}
		if got := rec.count("a1", pulling); got != maxAttempts {
			t.Fatalf("expected %d pull attempts before parking, got %d", maxAttempts, got)
		}
		if len(rt.Snapshot()) != 0 {
			t.Fatal("no container should exist after failed pulls")
		}

		// A config change unparks it; with the image now pullable it comes up.
		goodSet := buildSet(2, pair(assign("a1", "h2", run), rtp("good:image")))
		e.reconcile(ctx(), goodSet)
		if rec.state("a1") != running {
			t.Fatalf("a1 should recover to RUNNING after config change, got %v", rec.state("a1"))
		}
	})
}

// --- helpers --------------------------------------------------------------

const (
	run     = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN
	stop    = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
	running = zatterav1.InstanceState_INSTANCE_STATE_RUNNING
	stopped = zatterav1.InstanceState_INSTANCE_STATE_STOPPED
	failed  = zatterav1.InstanceState_INSTANCE_STATE_FAILED
	pulling = zatterav1.InstanceState_INSTANCE_STATE_PULLING
)

func ctx() context.Context { return context.Background() }

func newExec(rt runtime.ContainerRuntime, rec *statusRec) *Executor {
	return NewExecutor(ExecutorConfig{
		NodeID:  "n1",
		HostIP:  "10.90.0.1",
		Runtime: rt,
		Clock:   clock.NewFake(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Report:  rec.sink,
	})
}

func discardRec() *statusRec { return &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}} }

// statusRec captures every status report the executor emits.
type statusRec struct {
	mu     sync.Mutex
	latest map[string]*zatterav1.AssignmentObserved
	events []event
}

type event struct {
	id    string
	state zatterav1.InstanceState
}

func (r *statusRec) sink(observed map[string]*zatterav1.AssignmentObserved) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, obs := range observed {
		r.latest[id] = obs
		r.events = append(r.events, event{id: id, state: obs.GetState()})
	}
}

func (r *statusRec) state(id string) zatterav1.InstanceState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if obs, ok := r.latest[id]; ok {
		return obs.GetState()
	}
	return zatterav1.InstanceState_INSTANCE_STATE_UNSPECIFIED
}

func (r *statusRec) count(id string, state zatterav1.InstanceState) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.id == id && e.state == state {
			n++
		}
	}
	return n
}

func assign(id, hash string, desired zatterav1.AssignmentDesired) *zatterav1.Assignment {
	return &zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: id},
		NodeId:        "n1",
		ProjectId:     "proj1",
		AppId:         "app1",
		EnvironmentId: "env1",
		ReleaseId:     "rel1",
		Desired:       desired,
		ConfigHash:    hash,
	}
}

func rtp(image string, ports ...*zatterav1.PortSpec) *clusterv1.AssignmentRuntime {
	return &clusterv1.AssignmentRuntime{
		ImageRef: image,
		Spec:     &zatterav1.ServiceSpec{Ports: ports},
		Env:      map[string]string{"FOO": "bar"},
	}
}

func port(name string, cport uint32) *zatterav1.PortSpec {
	return &zatterav1.PortSpec{Name: name, ContainerPort: cport, Protocol: zatterav1.Protocol_PROTOCOL_TCP}
}

type assignPair struct {
	a  *zatterav1.Assignment
	rt *clusterv1.AssignmentRuntime
}

func pair(a *zatterav1.Assignment, rt *clusterv1.AssignmentRuntime) assignPair {
	return assignPair{a: a, rt: rt}
}

func buildSet(version uint64, pairs ...assignPair) *clusterv1.AssignmentSet {
	s := &clusterv1.AssignmentSet{Version: version, Runtime: map[string]*clusterv1.AssignmentRuntime{}}
	for _, p := range pairs {
		s.Assignments = append(s.Assignments, p.a)
		if p.rt != nil {
			s.Runtime[p.a.GetMeta().GetId()] = p.rt
		}
	}
	return s
}

func runningCount(rt *fakeruntime.Fake) int {
	n := 0
	for _, c := range rt.Snapshot() {
		if c.Running {
			n++
		}
	}
	return n
}

func onlyContainer(t *testing.T, rt *fakeruntime.Fake) fakeruntime.Container {
	t.Helper()
	snap := rt.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected exactly 1 container, got %d", len(snap))
	}
	return snap[0]
}

func containerIDs(rt *fakeruntime.Fake) []string {
	var ids []string
	for _, c := range rt.Snapshot() {
		ids = append(ids, c.ID)
	}
	return ids
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

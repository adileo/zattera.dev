package agent

import (
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

// statefulRt builds a stateful runtime declaring one volume, with the given
// fencing lease (nil = no lease yet).
func statefulRt(lease *zatterav1.VolumeLease) *clusterv1.AssignmentRuntime {
	return &clusterv1.AssignmentRuntime{
		ImageRef: "postgres:16",
		Spec: &zatterav1.ServiceSpec{
			Stateful: true,
			Volumes:  []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/var/lib/postgresql"}},
		},
		VolumeLease: lease,
	}
}

func TestExecutorVolumeFencing(t *testing.T) {
	t.Run("starts when the lease names this node and assignment", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)
		lease := &zatterav1.VolumeLease{NodeId: "n1", AssignmentId: "a1", ExpiresAt: timestamppb.Now()}

		e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), statefulRt(lease))))

		if got := runningCount(rt); got != 1 {
			t.Fatalf("want 1 running container, got %d", got)
		}
		if rec.state("a1") != running {
			t.Fatalf("a1 = %v, want RUNNING", rec.state("a1"))
		}
	})

	t.Run("withholds when the lease names another node", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)
		lease := &zatterav1.VolumeLease{NodeId: "n2", AssignmentId: "a1", ExpiresAt: timestamppb.Now()}

		e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), statefulRt(lease))))

		if got := runningCount(rt); got != 0 {
			t.Fatalf("container started despite foreign lease: %d running", got)
		}
		if s := rec.state("a1"); s != zatterav1.InstanceState_INSTANCE_STATE_PENDING {
			t.Fatalf("a1 = %v, want PENDING", s)
		}
	})

	t.Run("withholds when no lease has arrived yet", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)

		e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), statefulRt(nil))))

		if got := runningCount(rt); got != 0 {
			t.Fatalf("container started without a lease: %d running", got)
		}
		if s := rec.state("a1"); s != zatterav1.InstanceState_INSTANCE_STATE_PENDING {
			t.Fatalf("a1 = %v, want PENDING", s)
		}
	})

	t.Run("withholds when the lease names another instance on this node", func(t *testing.T) {
		rt := fakeruntime.New()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		e := newExec(rt, rec)
		lease := &zatterav1.VolumeLease{NodeId: "n1", AssignmentId: "other", ExpiresAt: timestamppb.Now()}

		e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), statefulRt(lease))))

		if got := runningCount(rt); got != 0 {
			t.Fatalf("second instance started on the same volume: %d running", got)
		}
	})
}

func TestLeaseWithholds(t *testing.T) {
	stateful := statefulRt(&zatterav1.VolumeLease{NodeId: "n1", AssignmentId: "a1"})
	if r := leaseWithholds("a1", "n1", stateful); r != "" {
		t.Errorf("matching lease withheld: %q", r)
	}
	// A stateless assignment is never fenced, even without a lease.
	stateless := &clusterv1.AssignmentRuntime{Spec: &zatterav1.ServiceSpec{}}
	if r := leaseWithholds("a1", "n1", stateless); r != "" {
		t.Errorf("stateless assignment withheld: %q", r)
	}
	// Stateful with no lease waits.
	if r := leaseWithholds("a1", "n1", statefulRt(nil)); r == "" {
		t.Error("stateful without lease should be withheld")
	}
}

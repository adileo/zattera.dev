package scheduler

import (
	"context"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

func envWithPorts(st *state.Store, id string, ports int) {
	var ps []*zatterav1.PortSpec
	for i := 0; i < ports; i++ {
		ps = append(ps, &zatterav1.PortSpec{Name: "http", ContainerPort: 8080})
	}
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: id}, Name: id,
		Service: &zatterav1.ServiceSpec{Ports: ps},
	})
}

func TestVIPsAllocateAndFree(t *testing.T) {
	s, rs := newSched(t)
	st := rs.State()
	ctx := context.Background()

	envWithPorts(st, "e1", 1)
	envWithPorts(st, "e2", 2)
	envWithPorts(st, "e3", 0) // no ports → no VIP

	if err := s.reconcileVIPs(ctx, st); err != nil {
		t.Fatal(err)
	}
	v1, ok1 := st.ServiceVIP("e1")
	v2, ok2 := st.ServiceVIP("e2")
	if !ok1 || !ok2 || v1 == "" || v2 == "" {
		t.Fatalf("VIPs missing: %q/%v %q/%v", v1, ok1, v2, ok2)
	}
	if v1 == v2 {
		t.Fatalf("VIPs must be unique, both %q", v1)
	}
	if _, ok := st.ServiceVIP("e3"); ok {
		t.Fatal("env without ports should have no VIP")
	}

	// Idempotent.
	if err := s.reconcileVIPs(ctx, st); err != nil {
		t.Fatal(err)
	}
	if v, _ := st.ServiceVIP("e1"); v != v1 {
		t.Fatalf("VIP changed on re-run: %q → %q", v1, v)
	}

	// Drop e2's ports → its VIP is freed.
	envWithPorts(st, "e2", 0)
	if err := s.reconcileVIPs(ctx, st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.ServiceVIP("e2"); ok {
		t.Fatal("e2 VIP should have been freed")
	}
	if _, ok := st.ServiceVIP("e1"); !ok {
		t.Fatal("e1 VIP must survive")
	}
}

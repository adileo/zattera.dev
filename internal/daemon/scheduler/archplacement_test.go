package scheduler

import (
	"sort"
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// archNode is an ALIVE, schedulable node reporting the given os_arch.
func archNode(id, osArch string) *zatterav1.Node {
	return &zatterav1.Node{
		Meta:        &zatterav1.Meta{Id: id},
		Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true,
		OsArch:      osArch,
		Capacity:    &zatterav1.ResourceLimits{CpuMillis: 4000, MemoryMb: 8192},
	}
}

func platRel(platforms ...string) *zatterav1.Release {
	return &zatterav1.Release{Service: &zatterav1.ServiceSpec{}, Platforms: platforms}
}

func TestArchPlacement(t *testing.T) {
	mixed := func() *state.Store {
		st := state.New()
		st.PutNode(archNode("amd1", "linux/amd64"))
		st.PutNode(archNode("arm1", "linux/arm64"))
		return st
	}

	t.Run("amd64-only release skips arm64 nodes", func(t *testing.T) {
		got, err := Place(mixed(), platRel("linux/amd64"), "env1", 1, nil)
		if err != nil || len(got) != 1 || got[0] != "amd1" {
			t.Fatalf("want [amd1], got %v err=%v", got, err)
		}
	})

	t.Run("arm64-only release skips amd64 nodes", func(t *testing.T) {
		got, err := Place(mixed(), platRel("linux/arm64"), "env1", 1, nil)
		if err != nil || len(got) != 1 || got[0] != "arm1" {
			t.Fatalf("want [arm1], got %v err=%v", got, err)
		}
	})

	t.Run("mixed-arch release spreads across both", func(t *testing.T) {
		got, err := Place(mixed(), platRel("linux/amd64", "linux/arm64"), "env1", 2, nil)
		if err != nil {
			t.Fatalf("place: %v", err)
		}
		sort.Strings(got)
		if len(got) != 2 || got[0] != "amd1" || got[1] != "arm1" {
			t.Fatalf("want both nodes, got %v", got)
		}
	})

	t.Run("empty platforms places on any node (regression)", func(t *testing.T) {
		got, err := Place(mixed(), platRel(), "env1", 2, nil)
		if err != nil || len(got) != 2 {
			t.Fatalf("unconstrained release must use both nodes, got %v err=%v", got, err)
		}
	})

	t.Run("node without a reported arch still runs unconstrained releases", func(t *testing.T) {
		st := state.New()
		st.PutNode(archNode("legacy", ""))
		got, err := Place(st, platRel(), "env1", 1, nil)
		if err != nil || len(got) != 1 {
			t.Fatalf("legacy node must accept unconstrained release, got %v err=%v", got, err)
		}
	})

	t.Run("no supported arch yields distinct error and zero placements", func(t *testing.T) {
		st := state.New()
		st.PutNode(archNode("arm1", "linux/arm64"))
		got, err := Place(st, platRel("linux/amd64"), "env1", 1, nil)
		if len(got) != 0 {
			t.Fatalf("nothing should place, got %v", got)
		}
		if err == nil || !strings.Contains(err.Error(), "supported architecture") || !strings.Contains(err.Error(), "linux/amd64") {
			t.Fatalf("want distinct arch error naming the platforms, got %v", err)
		}
	})

	t.Run("stateful pin to wrong-arch node surfaces the arch conflict", func(t *testing.T) {
		st := state.New()
		st.PutNode(archNode("arm1", "linux/arm64"))
		st.PutNode(archNode("amd1", "linux/amd64"))
		st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env1"}, ProjectId: "p1"})
		st.PutVolume(&zatterav1.Volume{
			Meta: &zatterav1.Meta{Id: "vol1"}, ProjectId: "p1", EnvironmentId: "env1",
			Name: "data", NodeId: "arm1",
		})
		rel := &zatterav1.Release{
			Service: &zatterav1.ServiceSpec{
				Stateful: true,
				Volumes:  []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/data"}},
			},
			Platforms: []string{"linux/amd64"},
		}
		got, err := Place(st, rel, "env1", 1, nil)
		if len(got) != 0 || err == nil {
			t.Fatalf("pinned node has the wrong arch: want zero placements + error, got %v err=%v", got, err)
		}
	})

	t.Run("alias platforms still match", func(t *testing.T) {
		st := state.New()
		st.PutNode(archNode("arm1", "linux/arm64"))
		got, err := Place(st, platRel("linux/aarch64"), "env1", 1, nil)
		if err != nil || len(got) != 1 {
			t.Fatalf("aarch64 alias should match arm64 node, got %v err=%v", got, err)
		}
	})
}

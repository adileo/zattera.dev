package proxy

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// fakeRouteStream replays snapshots then errClosed.
type fakeRouteStream struct {
	snaps []*clusterv1.RouteSnapshot
	i     int
}

func (f *fakeRouteStream) Recv() (*clusterv1.RouteSnapshot, error) {
	if f.i >= len(f.snaps) {
		return nil, errClosed
	}
	s := f.snaps[f.i]
	f.i++
	return s, nil
}

type fakeRouteDialer struct {
	snaps    []*clusterv1.RouteSnapshot
	haveSeen uint64
}

func (d *fakeRouteDialer) WatchRoutes(_ context.Context, have uint64) (RouteStream, error) {
	d.haveSeen = have
	return &fakeRouteStream{snaps: d.snaps}, nil
}

func snapshot(v uint64, host string) *clusterv1.RouteSnapshot {
	return &clusterv1.RouteSnapshot{
		Version:    v,
		HttpRoutes: []*clusterv1.HTTPRoute{{Hostname: host}},
	}
}

func TestRouteClientDiskRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy", "routes.pb")

	// First client receives a snapshot and persists it.
	d := &fakeRouteDialer{snaps: []*clusterv1.RouteSnapshot{snapshot(7, "api.example.com")}}
	c := NewRouteClient(d, "n1", path, nil)
	c.apply(d.snaps[0])
	if c.Current().GetVersion() != 7 {
		t.Fatalf("current version = %d, want 7", c.Current().GetVersion())
	}

	// A fresh client loads the persisted snapshot at startup (before any sync),
	// so the node keeps serving during quorum loss.
	c2 := NewRouteClient(&fakeRouteDialer{}, "n1", path, nil)
	got := c2.Current()
	if got.GetVersion() != 7 || len(got.GetHttpRoutes()) != 1 || got.GetHttpRoutes()[0].GetHostname() != "api.example.com" {
		t.Fatalf("loaded snapshot mismatch: %+v", got)
	}
}

func TestRouteClientStreamApplies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.pb")
	d := &fakeRouteDialer{snaps: []*clusterv1.RouteSnapshot{snapshot(1, "a"), snapshot(2, "b")}}
	c := NewRouteClient(d, "n1", path, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := c.Updates(ctx)
	<-updates // initial (empty) snapshot

	go c.Run(ctx)

	// Drain updates until we observe version 2.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case snap := <-updates:
			if snap.GetVersion() == 2 {
				if c.Current().GetVersion() != 2 {
					t.Fatalf("current not updated to 2")
				}
				return
			}
		case <-deadline:
			t.Fatalf("did not receive version 2 (current=%d)", c.Current().GetVersion())
		}
	}
}

func TestRouteClientSendsHaveVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.pb")
	// Pre-persist a snapshot so the client reconnects with have_version set.
	seed := &fakeRouteDialer{snaps: []*clusterv1.RouteSnapshot{snapshot(5, "x")}}
	c := NewRouteClient(seed, "n1", path, nil)
	c.apply(seed.snaps[0])

	d := &fakeRouteDialer{}
	c2 := NewRouteClient(d, "n1", path, nil)
	ctx, cancel := context.WithCancel(context.Background())
	_ = c2.stream(ctx) // one round; fake returns errClosed immediately
	cancel()
	if d.haveSeen != 5 {
		t.Fatalf("reconnect have_version = %d, want 5", d.haveSeen)
	}
}

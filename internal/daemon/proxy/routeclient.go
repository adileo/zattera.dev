package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// RouteDialer opens a WatchRoutes stream to a control node (over node mTLS). The
// daemon supplies the concrete transport; tests inject a fake.
type RouteDialer interface {
	WatchRoutes(ctx context.Context, haveVersion uint64) (RouteStream, error)
}

// RouteStream is the receive side of a WatchRoutes stream.
type RouteStream interface {
	Recv() (*clusterv1.RouteSnapshot, error)
}

// RouteClient keeps a WatchRoutes stream, persists each snapshot to disk, and
// implements proxy.RouteSource. It loads the last snapshot at startup so a node
// keeps serving traffic while the control plane is unreachable (spec §7).
type RouteClient struct {
	dialer RouteDialer
	nodeID string
	path   string // <data-dir>/proxy/routes.pb
	log    *slog.Logger

	mu      sync.Mutex
	current *clusterv1.RouteSnapshot
	subs    []chan *clusterv1.RouteSnapshot
}

// NewRouteClient constructs the client. path is where snapshots are cached.
func NewRouteClient(dialer RouteDialer, nodeID, path string, log *slog.Logger) *RouteClient {
	if log == nil {
		log = slog.Default()
	}
	c := &RouteClient{dialer: dialer, nodeID: nodeID, path: path, log: log, current: &clusterv1.RouteSnapshot{}}
	c.load()
	return c
}

// load reads the persisted snapshot at startup (before the first sync).
func (c *RouteClient) load() {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return // no cache yet
	}
	var snap clusterv1.RouteSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		c.log.Warn("route cache corrupt; ignoring", "err", err)
		return
	}
	c.current = &snap
}

// Current returns the latest snapshot (never nil).
func (c *RouteClient) Current() *clusterv1.RouteSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// Updates returns a channel receiving each new snapshot until ctx is canceled.
func (c *RouteClient) Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot {
	ch := make(chan *clusterv1.RouteSnapshot, 1)
	c.mu.Lock()
	c.subs = append(c.subs, ch)
	ch <- c.current
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		for i, s := range c.subs {
			if s == ch {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
				break
			}
		}
		close(ch)
		c.mu.Unlock()
	}()
	return ch
}

// Run keeps the WatchRoutes stream alive, applying each snapshot and reconnecting
// with backoff. It returns when ctx is canceled.
func (c *RouteClient) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.stream(ctx); err != nil && ctx.Err() == nil {
			c.log.Debug("route stream dropped; retrying", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (c *RouteClient) stream(ctx context.Context) error {
	stream, err := c.dialer.WatchRoutes(ctx, c.Current().GetVersion())
	if err != nil {
		return err
	}
	for {
		snap, err := stream.Recv()
		if err != nil {
			return err
		}
		c.apply(snap)
	}
}

// apply stores a snapshot, persists it to disk, and notifies subscribers.
func (c *RouteClient) apply(snap *clusterv1.RouteSnapshot) {
	if err := c.persist(snap); err != nil {
		c.log.Warn("persist route snapshot", "err", err)
	}
	c.mu.Lock()
	c.current = snap
	for _, ch := range c.subs {
		pushLatest(ch, snap)
	}
	c.mu.Unlock()
}

// persist atomically writes the snapshot to disk (temp file + rename).
func (c *RouteClient) persist(snap *clusterv1.RouteSnapshot) error {
	data, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("proxy: persist routes: %w", err)
	}
	return nil
}

func pushLatest(ch chan *clusterv1.RouteSnapshot, snap *clusterv1.RouteSnapshot) {
	for {
		select {
		case ch <- snap:
			return
		default:
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

// ensure RouteClient satisfies RouteSource.
var _ RouteSource = (*RouteClient)(nil)

// errClosed is returned by fake dialers in tests when the stream ends.
var errClosed = errors.New("proxy: route stream closed")

package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/builder"
)

// SourceFetcher fetches a source tarball from a control-plane blob URL. The
// production implementation dials over node mTLS; tests inject a fake.
type SourceFetcher interface {
	Fetch(ctx context.Context, url string) (io.ReadCloser, error)
}

// BuildServer implements the build methods of AgentLocalService on builder
// nodes (T-35). Other AgentLocalService methods are added by later tasks
// (T-41 logs, T-49 exec, T-65 volumes) via the same embedded server.
type BuildServer struct {
	clusterv1.UnimplementedAgentLocalServiceServer
	builder   *builder.Builder
	fetch     SourceFetcher
	regAuth   builder.RegistryAuth
	insecure  bool
	loadLocal bool
	log       *slog.Logger

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// BuildServerConfig configures the build server.
type BuildServerConfig struct {
	Builder          *builder.Builder
	Fetch            SourceFetcher
	RegistryAuth     builder.RegistryAuth
	RegistryInsecure bool
	// LocalLoad loads the built image into the local Docker store instead of
	// pushing to a registry (single-node/dev, T-54).
	LocalLoad bool
	Logger    *slog.Logger
}

// NewBuildServer constructs the build server.
func NewBuildServer(cfg BuildServerConfig) *BuildServer {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &BuildServer{
		builder: cfg.Builder, fetch: cfg.Fetch,
		regAuth: cfg.RegistryAuth, insecure: cfg.RegistryInsecure,
		loadLocal: cfg.LocalLoad,
		log:       log,
		cancels:   map[string]context.CancelFunc{},
	}
}

// RunBuild fetches the source, builds it (Dockerfile or nixpacks), pushes the
// image to the registry, and streams progress back to the control plane.
func (s *BuildServer) RunBuild(req *clusterv1.RunBuildRequest, stream grpc.ServerStreamingServer[clusterv1.BuildEvent]) error {
	b := req.GetBuild()
	buildID := b.GetMeta().GetId()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	s.track(buildID, cancel)
	defer s.untrack(buildID)

	src, err := s.fetch.Fetch(ctx, req.GetSourceUrl())
	if err != nil {
		return stream.Send(failEvent("fetch source: " + err.Error()))
	}
	defer func() { _ = src.Close() }()

	breq := builder.RunBuildRequest{
		BuildID:          buildID,
		Project:          b.GetProjectId(),
		App:              b.GetAppId(),
		Registry:         registryHost(req.GetPushImageRef()),
		ImageRef:         req.GetPushImageRef(),
		BuildArgs:        req.GetBuildArgs(),
		Platforms:        b.GetPlatforms(),
		Auth:             s.regAuth,
		RegistryInsecure: s.insecure,
		LoadLocally:      s.loadLocal,
	}

	events := make(chan builder.BuildEvent, 128)
	go func() {
		_, _ = s.builder.BuildFromSource(ctx, breq, src, events)
		close(events)
	}()

	for ev := range events {
		if err := stream.Send(toProtoEvent(ev)); err != nil {
			cancel() // client gone; stop the build
			return err
		}
	}
	return nil
}

// CancelBuild aborts an in-flight build on this node.
func (s *BuildServer) CancelBuild(_ context.Context, req *clusterv1.CancelBuildRequest) (*clusterv1.CancelBuildResponse, error) {
	s.mu.Lock()
	cancel, ok := s.cancels[req.GetBuildId()]
	s.mu.Unlock()
	if ok {
		cancel()
	}
	return &clusterv1.CancelBuildResponse{Canceled: ok}, nil
}

func (s *BuildServer) track(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	s.cancels[id] = cancel
	s.mu.Unlock()
}

func (s *BuildServer) untrack(id string) {
	s.mu.Lock()
	delete(s.cancels, id)
	s.mu.Unlock()
}

// toProtoEvent maps a builder event onto the wire BuildEvent.
func toProtoEvent(ev builder.BuildEvent) *clusterv1.BuildEvent {
	if ev.Done {
		if ev.Success {
			return &clusterv1.BuildEvent{Status: zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED, ImageDigest: ev.ImageDigest, Platforms: ev.Platforms}
		}
		return failEvent(ev.Err)
	}
	return &clusterv1.BuildEvent{Log: &zatterav1.LogLine{Line: ev.Log}}
}

func failEvent(msg string) *clusterv1.BuildEvent {
	return &clusterv1.BuildEvent{Status: zatterav1.BuildStatus_BUILD_STATUS_FAILED, Error: msg}
}

// registryHost returns the host:port prefix of a push reference.
func registryHost(ref string) string {
	if i := strings.IndexByte(ref, '/'); i >= 0 {
		return ref[:i]
	}
	return ref
}

// HTTPSourceFetcher fetches source tarballs over HTTP(S) with the node's mTLS
// client (the control blob endpoint is node-cert-authenticated).
type HTTPSourceFetcher struct {
	Client *http.Client
}

// Fetch GETs url and returns the body stream.
func (h HTTPSourceFetcher) Fetch(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("source fetch: HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

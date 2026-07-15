package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// maxSourceBytes caps an uploaded source tarball (matches the build context
// cap; larger uploads are rejected before they fill the disk).
const maxSourceBytes = 512 << 20

// UploadSource streams a tar.gz of the source directory to the control node,
// spools it under <uploads>/<digest>, and creates the QUEUED Build plus a
// PENDING Deployment that gates on it (T-35). The build dispatcher picks the
// build up and the orchestrator advances the deployment once it succeeds.
func (s *DeployServer) UploadSource(stream grpc.ClientStreamingServer[zatterav1.UploadSourceChunk, zatterav1.UploadSourceResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "empty upload stream")
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first chunk must carry the header")
	}
	env, err := s.resolveEnv(hdr.GetEnvironmentId())
	if err != nil {
		return err
	}
	if s.hasActiveDeployment(env.GetMeta().GetId()) {
		return status.Error(codes.FailedPrecondition, "a deployment is already in progress for this environment")
	}

	if err := os.MkdirAll(s.uploadsDir, 0o755); err != nil {
		return status.Errorf(codes.Internal, "prepare uploads dir: %v", err)
	}
	tmp, err := os.CreateTemp(s.uploadsDir, "upload-*")
	if err != nil {
		return status.Errorf(codes.Internal, "spool source: %v", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op after a successful rename

	h := sha256.New()
	w := io.MultiWriter(tmp, h)
	var total int64
	writeChunk := func(b []byte) error {
		total += int64(len(b))
		if total > maxSourceBytes {
			return status.Error(codes.InvalidArgument, "source exceeds size limit")
		}
		_, werr := w.Write(b)
		return werr
	}
	if err := writeChunk(first.GetData()); err != nil {
		_ = tmp.Close()
		return err
	}
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = tmp.Close()
			return rerr
		}
		if err := writeChunk(chunk.GetData()); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return status.Errorf(codes.Internal, "close upload: %v", err)
	}

	hexDigest := hex.EncodeToString(h.Sum(nil))
	digest := "sha256:" + hexDigest
	dest := filepath.Join(s.uploadsDir, hexDigest)
	// Content-addressed: identical re-uploads dedupe to the same file.
	if _, statErr := os.Stat(dest); errors.Is(statErr, os.ErrNotExist) {
		if err := os.Rename(tmpName, dest); err != nil {
			return status.Errorf(codes.Internal, "store source: %v", err)
		}
	}

	app, _ := s.store.App(env.GetAppId())
	build := &zatterav1.Build{
		Meta:          newMeta(ids.New(), s.clock.Now()),
		AppId:         env.GetAppId(),
		ProjectId:     env.GetProjectId(),
		EnvironmentId: env.GetMeta().GetId(),
		Type:          hdr.GetBuildType(),
		Status:        zatterav1.BuildStatus_BUILD_STATUS_QUEUED,
		TarballDigest: digest,
		GitSha:        hdr.GetGitSha(),
		Platforms:     app.GetBuild().GetPlatforms(),
	}
	rel := s.buildRelease(env, "") // image_ref filled in once the build succeeds
	dep := s.buildDeployment(env, rel.GetMeta().GetId(), false)
	dep.BuildId = build.GetMeta().GetId()

	ctx := stream.Context()
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutBuild{PutBuild: &clusterv1.PutBuild{Build: build}}}); err != nil {
		return err
	}
	if err := s.commit(ctx, rel, dep); err != nil {
		return err
	}
	return stream.SendAndClose(&zatterav1.UploadSourceResponse{Build: build, Deployment: dep})
}

// SourceBlobHandler serves spooled source tarballs by digest to builder nodes.
// It is mounted on the node-mTLS API surface (the client cert is the auth); the
// builder fetches source_url = "<control>/internal/blobs/<digest>".
func SourceBlobHandler(uploadsDir string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		digest := filepath.Base(r.URL.Path)
		hexDigest := digest
		if len(digest) > 7 && digest[:7] == "sha256:" {
			hexDigest = digest[7:]
		}
		// Guard against path traversal: a digest is 64 lowercase hex chars.
		if !isHexDigest(hexDigest) {
			http.Error(w, "bad digest", http.StatusBadRequest)
			return
		}
		f, err := os.Open(filepath.Join(uploadsDir, hexDigest))
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer func() { _ = f.Close() }()
		w.Header().Set("Content-Type", "application/gzip")
		http.ServeContent(w, r, hexDigest, time.Time{}, f)
	})
}

func isHexDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

package daemon

import (
	"context"
	"log/slog"
	"strings"

	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/registry"
)

// platformResolver builds the deploy-time image platform resolver (T-88).
// References into the embedded registry (reg non-nil, ref prefixed with
// localAddr) are read straight from its manifest store; everything else is
// best-effort inspected over the distribution API. Failures resolve to nil
// (unconstrained) with a debug log — a deploy must never fail over manifest
// inspection.
func platformResolver(reg *registry.Registry, localAddr string, log *slog.Logger) api.PlatformResolver {
	return func(ctx context.Context, imageRef string) []string {
		if reg != nil && localAddr != "" && strings.HasPrefix(imageRef, localAddr+"/") {
			repo, ref := registry.SplitRepoRef(strings.TrimPrefix(imageRef, localAddr+"/"))
			plats, err := reg.Manifests.Platforms(repo, ref)
			if err != nil {
				log.Debug("platform inspect: embedded registry lookup failed", "image", imageRef, "err", err)
				return nil
			}
			return plats
		}
		plats, err := registry.RemotePlatforms(ctx, imageRef)
		if err != nil {
			log.Debug("platform inspect: remote manifest lookup failed", "image", imageRef, "err", err)
			return nil
		}
		return plats
	}
}

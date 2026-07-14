package scheduler

import (
	"context"
	"fmt"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// VIP pool: a cluster-unique /32 in 10.97.0.0/16 per environment that exposes a
// service port (spec §2.7). The per-node VIP proxy binds these and splices to
// the env's endpoints; the internal DNS resolver answers with them.
const (
	vipOctet0 = 10
	vipOctet1 = 97
)

// reconcileVIPs allocates a service VIP for each environment that has at least
// one service port and frees VIPs for environments that no longer do.
func (s *Scheduler) reconcileVIPs(ctx context.Context, st *state.Store) error {
	needed := map[string]bool{}
	for _, env := range st.ListEnvironments("", "") {
		if len(env.GetService().GetPorts()) > 0 {
			needed[env.GetMeta().GetId()] = true
		}
	}

	current := st.ListServiceVIPs() // envID → vip
	used := map[string]bool{}
	for envID, vip := range current {
		if needed[envID] {
			used[vip] = true
			continue
		}
		if err := s.setVIP(ctx, envID, ""); err != nil { // free
			return err
		}
	}
	for envID := range needed {
		if _, ok := current[envID]; ok {
			continue
		}
		vip, err := nextFreeVIP(used)
		if err != nil {
			s.log.Warn("service VIP pool exhausted", "err", err)
			continue
		}
		if err := s.setVIP(ctx, envID, vip); err != nil {
			return err
		}
		used[vip] = true
	}
	return nil
}

// nextFreeVIP returns the lowest free 10.97.a.b (b in 2..254) not in used.
func nextFreeVIP(used map[string]bool) (string, error) {
	for a := 0; a < 256; a++ {
		for b := 2; b < 255; b++ {
			ip := fmt.Sprintf("%d.%d.%d.%d", vipOctet0, vipOctet1, a, b)
			if !used[ip] {
				return ip, nil
			}
		}
	}
	return "", fmt.Errorf("service VIP pool %d.%d.0.0/16 exhausted", vipOctet0, vipOctet1)
}

func (s *Scheduler) setVIP(ctx context.Context, envID, vip string) error {
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutServiceVip{
		PutServiceVip: &clusterv1.PutServiceVIP{EnvironmentId: envID, Vip: vip},
	}})
}

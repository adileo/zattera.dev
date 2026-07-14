package daemon

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/tlsmgr"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// raftCertKV adapts the replicated KV to tlsmgr.KV so the ACME cert storage and
// its distributed locks live in raft (T-89). Reads hit local state; writes go
// through the raft log (leader-only — cert issuance runs on the leader).
type raftCertKV struct {
	rs *raftstore.Store
}

// newRaftCertKV builds the cert KV over the raft store.
func newRaftCertKV(rs *raftstore.Store) *raftCertKV { return &raftCertKV{rs: rs} }

var _ tlsmgr.KV = (*raftCertKV)(nil)

func (k *raftCertKV) Get(key string) (value []byte, version int64, expiresAtMs int64, ok bool) {
	return k.rs.State().KV(key)
}

func (k *raftCertKV) Put(ctx context.Context, key string, value []byte, expectedVersion, expiresAtMs int64) (int64, error) {
	var exp *timestamppb.Timestamp
	if expiresAtMs > 0 {
		exp = timestamppb.New(time.UnixMilli(expiresAtMs))
	}
	err := k.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
		Key:             key,
		Value:           value,
		ExpectedVersion: expectedVersion,
		ExpiresAt:       exp,
	}}})
	if errors.Is(err, state.ErrKVConflict) {
		return 0, tlsmgr.ErrConflict
	}
	if err != nil {
		return 0, err
	}
	_, v, _, _ := k.rs.State().KV(key)
	return v, nil
}

func (k *raftCertKV) Delete(ctx context.Context, key string, expectedVersion int64) error {
	err := k.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteKv{DeleteKv: &clusterv1.DeleteKV{
		Key:             key,
		ExpectedVersion: expectedVersion,
	}}})
	if errors.Is(err, state.ErrKVConflict) {
		return tlsmgr.ErrConflict
	}
	return err
}

func (k *raftCertKV) ListPrefix(prefix string) []string {
	return k.rs.State().ListKVPrefix(prefix)
}

func (k *raftCertKV) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:tlsmgr"
	cmd.Time = timestamppb.Now()
	return k.rs.Apply(ctx, cmd)
}

package api

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/state"
)

// clone deep-copies a proto message, preserving its concrete type.
func clone[T proto.Message](m T) T {
	return proto.Clone(m).(T)
}

// toStatus maps an Apply/business error to a gRPC status. Errors already
// carrying a status code pass through; known store errors get mapped; anything
// else is Internal.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, raftstore.ErrNotLeader):
		// T-08's leader-forward interceptor normally handles this transparently.
		return status.Error(codes.Unavailable, "not the leader; retry against the leader")
	case errors.Is(err, state.ErrKVConflict):
		return status.Error(codes.Aborted, "conflict; retry")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

package api

import (
	"testing"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestPublicAPIMethodTableComplete asserts every method served on the public
// API gRPC server has an auth-policy entry. The server fail-closes at startup
// (ValidateMethodTable) on a missing entry, so a new RPC without a policy line
// crash-loops EVERY control node — this test catches it in CI instead of on
// real infra. Keep the registered set in sync with server.go's grpc block.
func TestPublicAPIMethodTableComplete(t *testing.T) {
	s := grpc.NewServer()
	// Registering with a nil impl only records the service descriptor, which is
	// all GetServiceInfo (and thus the policy check) inspects.
	clusterv1.RegisterAgentSyncServiceServer(s, nil)
	clusterv1.RegisterJoinServiceServer(s, nil)
	clusterv1.RegisterMeshServiceServer(s, nil)
	clusterv1.RegisterRouteServiceServer(s, nil)
	zatterav1.RegisterAuthServiceServer(s, nil)
	zatterav1.RegisterProjectServiceServer(s, nil)
	zatterav1.RegisterAppServiceServer(s, nil)
	zatterav1.RegisterDeployServiceServer(s, nil)
	zatterav1.RegisterNodeServiceServer(s, nil)
	zatterav1.RegisterStateServiceServer(s, nil)
	zatterav1.RegisterAuditServiceServer(s, nil)
	zatterav1.RegisterLogServiceServer(s, nil)
	zatterav1.RegisterDomainServiceServer(s, nil)
	zatterav1.RegisterExecServiceServer(s, nil)
	zatterav1.RegisterMetricsServiceServer(s, nil)
	zatterav1.RegisterJobServiceServer(s, nil)

	if err := ValidateMethodTable(s.GetServiceInfo()); err != nil {
		t.Fatalf("public API method missing an auth policy entry: %v", err)
	}
}

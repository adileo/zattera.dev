package api

import (
	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// RouteProvider is the route-table source the RouteService streams from. The
// scheduler's RouteBuilder implements it.
type RouteProvider interface {
	Current() *clusterv1.RouteSnapshot
	Subscribe() (int, <-chan *clusterv1.RouteSnapshot)
	Unsubscribe(id int)
}

// RouteServer implements RouteService.WatchRoutes: it streams the current route
// snapshot on connect (skipping the resend when the client already has that
// version) and every rebuild thereafter. Node-mTLS only (policy table).
type RouteServer struct {
	clusterv1.UnimplementedRouteServiceServer
	routes RouteProvider
}

// NewRouteServer wraps a route provider.
func NewRouteServer(routes RouteProvider) *RouteServer {
	return &RouteServer{routes: routes}
}

// WatchRoutes streams route snapshots to a node's proxy/DNS.
func (s *RouteServer) WatchRoutes(req *clusterv1.WatchRoutesRequest, stream grpc.ServerStreamingServer[clusterv1.RouteSnapshot]) error {
	// Subscribe first so no rebuild is missed between the initial send and the
	// stream loop.
	id, ch := s.routes.Subscribe()
	defer s.routes.Unsubscribe(id)

	cur := s.routes.Current()
	if cur.GetVersion() != req.GetHaveVersion() {
		if err := stream.Send(cur); err != nil {
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case snap := <-ch:
			if err := stream.Send(snap); err != nil {
				return err
			}
		}
	}
}

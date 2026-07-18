package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newDomainServer(t *testing.T, clusterDomain string) (*DomainServer, *raftstore.Store) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app"}, ProjectId: "proj", Name: "api"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env"}, ProjectId: "proj", AppId: "app", Name: "production"})
	return NewDomainServer(st, rs, clock.NewFake(), clusterDomain), rs
}

func addReq(host string) *zatterav1.AddDomainRequest {
	return &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env", Hostname: host}
}

func TestDomainsCRUD(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	dom, err := s.AddDomain(ctx, addReq("API.Example.com")) // mixed case → normalized
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if dom.GetHostname() != "api.example.com" {
		t.Fatalf("hostname not normalized: %q", dom.GetHostname())
	}
	if dom.GetAppId() != "app" || dom.GetEnvironmentId() != "env" {
		t.Fatalf("domain not linked to env/app: %+v", dom)
	}
	if dom.GetCertStatus() != zatterav1.CertStatus_CERT_STATUS_PENDING {
		t.Fatalf("cert status = %v, want PENDING", dom.GetCertStatus())
	}

	list, _ := s.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: "proj"})
	if len(list.GetDomains()) != 1 {
		t.Fatalf("list = %d domains", len(list.GetDomains()))
	}

	if _, err := s.RemoveDomain(ctx, &zatterav1.RemoveDomainRequest{ProjectId: "proj", DomainId: dom.GetMeta().GetId()}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	list, _ = s.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: "proj"})
	if len(list.GetDomains()) != 0 {
		t.Fatal("domain not removed")
	}
}

func TestDomainsValidationAndUniqueness(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	// Invalid hostname.
	if _, err := s.AddDomain(ctx, addReq("not a host!")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid host code = %v, want InvalidArgument", status.Code(err))
	}
	// Unknown environment.
	if _, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "ghost", Hostname: "x.example.com"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown env code = %v, want NotFound", status.Code(err))
	}
	// Duplicate hostname.
	if _, err := s.AddDomain(ctx, addReq("dup.example.com")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddDomain(ctx, addReq("dup.example.com")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestDomainsClusterSubdomainCollision(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	// A hostname under the reserved cluster domain is rejected.
	if _, err := s.AddDomain(ctx, addReq("api-production.apps.example.com")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cluster-subdomain collision code = %v, want InvalidArgument", status.Code(err))
	}
	// The cluster apex itself is rejected too.
	if _, err := s.AddDomain(ctx, addReq("apps.example.com")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("apex collision code = %v, want InvalidArgument", status.Code(err))
	}
	// A hostname outside the cluster domain is fine.
	if _, err := s.AddDomain(ctx, addReq("api.example.com")); err != nil {
		t.Fatalf("external hostname rejected: %v", err)
	}
}

func TestDomainsSetMiddlewareAndCertStatus(t *testing.T) {
	s, _ := newDomainServer(t, "")
	ctx := context.Background()

	dom, err := s.AddDomain(ctx, addReq("api.example.com"))
	if err != nil {
		t.Fatal(err)
	}
	mw := &zatterav1.Middleware{Compress: true, MaxBodyBytes: 1024}
	updated, err := s.SetMiddleware(ctx, &zatterav1.SetMiddlewareRequest{ProjectId: "proj", DomainId: dom.GetMeta().GetId(), Middleware: mw})
	if err != nil {
		t.Fatal(err)
	}
	if !updated.GetMiddleware().GetCompress() || updated.GetMiddleware().GetMaxBodyBytes() != 1024 {
		t.Fatalf("middleware not applied: %+v", updated.GetMiddleware())
	}

	// The cert-status callback promotes PENDING → ISSUED.
	s.SetCertStatus(ctx, "api.example.com", zatterav1.CertStatus_CERT_STATUS_ISSUED)
	got, _ := s.store.Domain(dom.GetMeta().GetId())
	if got.GetCertStatus() != zatterav1.CertStatus_CERT_STATUS_ISSUED {
		t.Fatalf("cert status = %v, want ISSUED", got.GetCertStatus())
	}
}

// TestDomainsSharedHostnameByPath: one hostname, several apps, split by path
// prefix — "/" to the web app and "/api" to the API is the whole point of
// T-104. Identity is (hostname, prefix), so only an exact repeat collides.
func TestDomainsSharedHostnameByPath(t *testing.T) {
	s, rs := newDomainServer(t, "apps.example.com")
	st := rs.State()
	// A second app in the SAME project.
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app2"}, ProjectId: "proj", Name: "web"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env2"}, ProjectId: "proj", AppId: "app2", Name: "production"})
	ctx := context.Background()

	root, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env2", Hostname: "shop.example.com"})
	if err != nil {
		t.Fatalf("root route: %v", err)
	}
	api, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env", Hostname: "shop.example.com", PathPrefix: "/api"})
	if err != nil {
		t.Fatalf("prefixed route on the same host must be allowed: %v", err)
	}
	if root.GetEnvironmentId() == api.GetEnvironmentId() {
		t.Fatal("the two routes should point at different environments")
	}

	// The exact pair is still unique.
	if _, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env", Hostname: "shop.example.com", PathPrefix: "/api"}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate (host,prefix) code = %v, want AlreadyExists", status.Code(err))
	}

	// Prefix normalization: "/api/", "api" and "/api" are the same route.
	for _, variant := range []string{"/api/", "api"} {
		if _, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env", Hostname: "shop.example.com", PathPrefix: variant}); status.Code(err) != codes.AlreadyExists {
			t.Errorf("prefix %q should normalize onto /api, got %v", variant, status.Code(err))
		}
	}

	// Both routes survive independently; removing one leaves the other.
	if _, err := s.RemoveDomain(ctx, &zatterav1.RemoveDomainRequest{ProjectId: "proj", DomainId: api.GetMeta().GetId()}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := st.DomainsByHostname("shop.example.com"); len(got) != 1 || got[0].GetMeta().GetId() != root.GetMeta().GetId() {
		t.Fatalf("after removing /api the root route must remain, got %d", len(got))
	}
}

// TestDomainsHostnameNotSharedAcrossProjects: the certificate is per-hostname,
// so a second project claiming a path on someone else's host would ride on
// their certificate. Refused.
func TestDomainsHostnameNotSharedAcrossProjects(t *testing.T) {
	s, rs := newDomainServer(t, "apps.example.com")
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app3"}, ProjectId: "other", Name: "evil"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env3"}, ProjectId: "other", AppId: "app3", Name: "production"})
	ctx := context.Background()

	if _, err := s.AddDomain(ctx, addReq("shared.example.com")); err != nil {
		t.Fatal(err)
	}
	_, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "other", EnvironmentId: "env3", Hostname: "shared.example.com", PathPrefix: "/steal"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-project claim code = %v, want PermissionDenied", status.Code(err))
	}
}

// TestDomainsCertStatusCoversEveryRoute: one certificate serves a hostname, so
// issuing it must mark every route of that hostname issued — otherwise
// `domains ls` shows siblings stuck pending forever.
func TestDomainsCertStatusCoversEveryRoute(t *testing.T) {
	s, rs := newDomainServer(t, "apps.example.com")
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app2"}, ProjectId: "proj", Name: "web"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env2"}, ProjectId: "proj", AppId: "app2", Name: "production"})
	ctx := context.Background()

	if _, err := s.AddDomain(ctx, addReq("multi.example.com")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env2", Hostname: "multi.example.com", PathPrefix: "/admin"}); err != nil {
		t.Fatal(err)
	}

	s.SetCertStatus(ctx, "multi.example.com", zatterav1.CertStatus_CERT_STATUS_ISSUED)
	for _, d := range st.DomainsByHostname("multi.example.com") {
		if d.GetCertStatus() != zatterav1.CertStatus_CERT_STATUS_ISSUED {
			t.Errorf("route %q%s still %v", d.GetHostname(), d.GetPathPrefix(), d.GetCertStatus())
		}
	}
}

package api

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// eventsFixture seeds two projects with events, an org admin, a member of only
// the first project, and an outsider.
func eventsFixture(t *testing.T) (a *Auditor, st *state.Store, projA, projB, admin, member, outsider string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st = rs.State()

	projA, projB = ids.New(), ids.New()
	admin, member, outsider = ids.New(), ids.New(), ids.New()
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: admin}, Email: "admin@x", OrgRole: zatterav1.Role_ROLE_ADMIN})
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: member}, Email: "dev@x", OrgRole: zatterav1.Role_ROLE_DEVELOPER})
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: outsider}, Email: "nobody@x", OrgRole: zatterav1.Role_ROLE_DEVELOPER})
	st.PutProjectMember(&zatterav1.ProjectMember{ProjectId: projA, UserId: member, Role: zatterav1.Role_ROLE_DEVELOPER})

	now := time.Now()
	st.AppendEvents([]*zatterav1.Event{
		{Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(now.Add(-time.Hour))}, ProjectId: projA, Kind: "deploy.succeeded", Severity: "info", Message: "a1"},
		{Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(now.Add(-time.Minute))}, ProjectId: projA, Kind: "deploy.failed", Severity: "error", Message: "a2"},
		{Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(now)}, ProjectId: projB, Kind: "node.down", Severity: "warning", Message: "b1"},
	})
	return NewAuditor(st, rs, nil, 0), st, projA, projB, admin, member, outsider
}

func callerCtx(userID string) context.Context {
	return withIdentity(context.Background(), Identity{UserID: userID})
}

// TestListEventsScoping is the access-control contract: ListEvents is open to
// any authenticated user (unlike admin-only QueryAudit), so the handler must
// scope non-admins to projects they belong to.
func TestListEventsScoping(t *testing.T) {
	a, _, projA, projB, admin, member, outsider := eventsFixture(t)

	t.Run("admin sees the cluster", func(t *testing.T) {
		resp, err := a.ListEvents(callerCtx(admin), &zatterav1.ListEventsRequest{})
		if err != nil {
			t.Fatalf("admin query: %v", err)
		}
		if len(resp.GetEvents()) != 3 {
			t.Fatalf("admin saw %d events, want all 3", len(resp.GetEvents()))
		}
	})

	t.Run("member must name a project", func(t *testing.T) {
		_, err := a.ListEvents(callerCtx(member), &zatterav1.ListEventsRequest{})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unscoped member query = %v, want InvalidArgument", err)
		}
	})

	t.Run("member sees their own project", func(t *testing.T) {
		resp, err := a.ListEvents(callerCtx(member), &zatterav1.ListEventsRequest{ProjectId: projA})
		if err != nil {
			t.Fatalf("member query: %v", err)
		}
		if len(resp.GetEvents()) != 2 {
			t.Fatalf("member saw %d events, want 2", len(resp.GetEvents()))
		}
		for _, e := range resp.GetEvents() {
			if e.GetProjectId() != projA {
				t.Errorf("leaked an event from project %s", e.GetProjectId())
			}
		}
	})

	t.Run("non-member is told the project does not exist", func(t *testing.T) {
		_, err := a.ListEvents(callerCtx(outsider), &zatterav1.ListEventsRequest{ProjectId: projA})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("outsider query = %v, want NotFound", err)
		}
		// A member of nothing must not reach another project either.
		if _, err := a.ListEvents(callerCtx(member), &zatterav1.ListEventsRequest{ProjectId: projB}); status.Code(err) != codes.NotFound {
			t.Fatalf("cross-project query = %v, want NotFound", err)
		}
	})

	t.Run("unauthenticated is rejected", func(t *testing.T) {
		if _, err := a.ListEvents(context.Background(), &zatterav1.ListEventsRequest{}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("anonymous query = %v, want Unauthenticated", err)
		}
	})
}

// TestListEventsFilters covers kind/severity/since filtering and the
// newest-first ordering that matches QueryAudit.
func TestListEventsFilters(t *testing.T) {
	a, _, _, _, admin, _, _ := eventsFixture(t)
	ctx := callerCtx(admin)

	resp, err := a.ListEvents(ctx, &zatterav1.ListEventsRequest{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := resp.GetEvents()[0].GetMessage(); got != "b1" {
		t.Fatalf("first event = %q, want the newest (b1)", got)
	}

	resp, err = a.ListEvents(ctx, &zatterav1.ListEventsRequest{KindPrefix: "deploy."})
	if err != nil {
		t.Fatalf("kind query: %v", err)
	}
	if len(resp.GetEvents()) != 2 {
		t.Fatalf("kind prefix returned %d events, want 2", len(resp.GetEvents()))
	}

	resp, err = a.ListEvents(ctx, &zatterav1.ListEventsRequest{Severity: "error"})
	if err != nil {
		t.Fatalf("severity query: %v", err)
	}
	if len(resp.GetEvents()) != 1 || resp.GetEvents()[0].GetMessage() != "a2" {
		t.Fatalf("severity filter = %+v", resp.GetEvents())
	}

	resp, err = a.ListEvents(ctx, &zatterav1.ListEventsRequest{SinceUnixMs: time.Now().Add(-30 * time.Minute).UnixMilli()})
	if err != nil {
		t.Fatalf("since query: %v", err)
	}
	if len(resp.GetEvents()) != 2 {
		t.Fatalf("since filter returned %d events, want the 2 recent ones", len(resp.GetEvents()))
	}

	resp, err = a.ListEvents(ctx, &zatterav1.ListEventsRequest{Limit: 1})
	if err != nil {
		t.Fatalf("limit query: %v", err)
	}
	if len(resp.GetEvents()) != 1 {
		t.Fatalf("limit=1 returned %d events", len(resp.GetEvents()))
	}
}

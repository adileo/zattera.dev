package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// T-76 covers both `zattera audit` and `zattera events`; every test here is
// named TestAudit* so the task's acceptance command exercises the whole thing.

// auditFixture stands up the API server, logs the CLI in, and creates a project
// so audit/event rows can be attributed to it.
func auditFixture(t *testing.T) (rs *raftstore.Store, projectID string) {
	t.Helper()
	addr, caPEM, token, rs := testServer(t)

	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv("ZATTERA_CONFIG", cfgPath)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, "login", "--server", "https://"+addr, "--token", token, "--ca-cert", caPath); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, _, err := run(t, "projects", "create", "demo"); err != nil {
		t.Fatalf("create project: %v", err)
	}
	projects := rs.State().ListProjects()
	if len(projects) != 1 {
		t.Fatalf("expected 1 project, got %d", len(projects))
	}
	return rs, projects[0].GetMeta().GetId()
}

// applyCmd pushes a command through raft. The oneof wrapper interface is
// unexported, so callers build the whole Command.
func applyCmd(t *testing.T, rs *raftstore.Store, cmd *clusterv1.Command) {
	t.Helper()
	cmd.RequestId = ids.New()
	cmd.Time = timestamppb.Now()
	if err := rs.Apply(context.Background(), cmd); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func seedAudit(t *testing.T, rs *raftstore.Store, entries ...*zatterav1.AuditEntry) {
	t.Helper()
	applyCmd(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_AppendAudit{AppendAudit: &clusterv1.AppendAudit{Entries: entries}}})
}

func seedEvents(t *testing.T, rs *raftstore.Store, events ...*zatterav1.Event) {
	t.Helper()
	applyCmd(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: events}}})
}

func auditEntry(projectID, method, outcome string, age time.Duration) *zatterav1.AuditEntry {
	return &zatterav1.AuditEntry{
		Meta:        &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(time.Now().Add(-age))},
		ProjectId:   projectID,
		Method:      method,
		Outcome:     outcome,
		RemoteAddr:  "10.0.0.5:4242",
		ActorUserId: "usr_alice",
	}
}

func event(projectID, kind, severity, message string, age time.Duration) *zatterav1.Event {
	return &zatterav1.Event{
		Meta:      &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(time.Now().Add(-age))},
		ProjectId: projectID,
		Kind:      kind,
		Severity:  severity,
		Message:   message,
	}
}

// TestAuditQuery covers `zattera audit`: table rendering, --json, and the
// --method and --since filters.
func TestAuditQuery(t *testing.T) {
	rs, projectID := auditFixture(t)
	seedAudit(t, rs,
		auditEntry(projectID, "/zattera.v1.AppService/CreateApp", "ok", 10*time.Minute),
		auditEntry(projectID, "/zattera.v1.DeployService/Deploy", "PermissionDenied", 5*time.Minute),
		auditEntry(projectID, "/zattera.v1.DeployService/Rollback", "ok", 3*time.Hour),
	)

	out, _, err := run(t, "audit")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	for _, want := range []string{"METHOD", "OUTCOME", "AppService/CreateApp", "DeployService/Deploy", "PermissionDenied", "10.0.0.5:4242"} {
		if !strings.Contains(out, want) {
			t.Errorf("audit output missing %q:\n%s", want, out)
		}
	}

	// Newest first.
	if i, j := strings.Index(out, "Deploy\n"), strings.Index(out, "CreateApp"); i > j && j >= 0 {
		t.Errorf("entries not newest-first:\n%s", out)
	}

	t.Run("json", func(t *testing.T) {
		out, _, err := run(t, "audit", "--json")
		if err != nil {
			t.Fatalf("audit --json: %v", err)
		}
		if !strings.Contains(out, `"method"`) || !strings.Contains(out, "CreateApp") {
			t.Errorf("json output unexpected:\n%s", out)
		}
		// The table header must not leak into JSON mode.
		if strings.Contains(out, "OUTCOME") {
			t.Errorf("json output contains table header:\n%s", out)
		}
	})

	t.Run("method filter", func(t *testing.T) {
		out, _, err := run(t, "audit", "--method", "/zattera.v1.DeployService/")
		if err != nil {
			t.Fatalf("audit --method: %v", err)
		}
		if strings.Contains(out, "CreateApp") {
			t.Errorf("--method did not filter:\n%s", out)
		}
		if !strings.Contains(out, "DeployService/Deploy") {
			t.Errorf("--method dropped a match:\n%s", out)
		}
	})

	t.Run("since filter", func(t *testing.T) {
		out, _, err := run(t, "audit", "--since", "30m")
		if err != nil {
			t.Fatalf("audit --since: %v", err)
		}
		if strings.Contains(out, "Rollback") {
			t.Errorf("--since 30m returned a 3h-old entry:\n%s", out)
		}
		if !strings.Contains(out, "CreateApp") {
			t.Errorf("--since 30m dropped a recent entry:\n%s", out)
		}
	})

	t.Run("no matches", func(t *testing.T) {
		out, errOut, err := run(t, "audit", "--method", "/zattera.v1.NoSuchService/")
		if err != nil {
			t.Fatalf("audit: %v", err)
		}
		if strings.Contains(out, "METHOD") {
			t.Errorf("empty result rendered a table:\n%s", out)
		}
		if !strings.Contains(errOut, "no audit entries match") {
			t.Errorf("missing empty-result notice: %q", errOut)
		}
	})
}

// TestAuditProjectScope pins the --project behaviour. QueryAudit is not in the
// RBAC project table (an empty project legitimately means cluster-wide), so the
// CLI must resolve the project NAME to its id itself — passing the name through
// would silently match nothing.
func TestAuditProjectScope(t *testing.T) {
	rs, projectID := auditFixture(t)
	if _, _, err := run(t, "projects", "create", "other"); err != nil {
		t.Fatalf("create other: %v", err)
	}
	var otherID string
	for _, p := range rs.State().ListProjects() {
		if p.GetName() == "other" {
			otherID = p.GetMeta().GetId()
		}
	}
	seedAudit(t, rs,
		auditEntry(projectID, "/zattera.v1.AppService/CreateApp", "ok", time.Minute),
		auditEntry(otherID, "/zattera.v1.AppService/DeleteApp", "ok", time.Minute),
	)

	out, _, err := run(t, "audit", "--project", "demo")
	if err != nil {
		t.Fatalf("audit --project: %v", err)
	}
	if !strings.Contains(out, "CreateApp") {
		t.Fatalf("--project demo returned nothing — the name was not resolved to an id:\n%s", out)
	}
	if strings.Contains(out, "DeleteApp") {
		t.Errorf("--project demo leaked another project's entries:\n%s", out)
	}

	// Cluster-wide (no --project) sees both.
	out, _, err = run(t, "audit")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !strings.Contains(out, "CreateApp") || !strings.Contains(out, "DeleteApp") {
		t.Errorf("cluster-wide query missing entries:\n%s", out)
	}

	if _, _, err := run(t, "audit", "--project", "nope"); err == nil {
		t.Error("expected an error for an unknown project")
	}
}

// TestAuditEvents covers `zattera events` one-shot output and its filters.
func TestAuditEvents(t *testing.T) {
	rs, projectID := auditFixture(t)
	seedEvents(t, rs,
		event(projectID, "deploy.succeeded", "info", "web v3 deployed", 10*time.Minute),
		event(projectID, "deploy.failed", "error", "web v4 build failed", 5*time.Minute),
		event(projectID, "node.down", "warning", "node-2 stopped heartbeating", time.Minute),
	)

	out, _, err := run(t, "events")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, want := range []string{"SEVERITY", "KIND", "deploy.succeeded", "node.down", "web v4 build failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("events output missing %q:\n%s", want, out)
		}
	}

	t.Run("kind filter", func(t *testing.T) {
		out, _, err := run(t, "events", "--kind", "deploy.")
		if err != nil {
			t.Fatalf("events --kind: %v", err)
		}
		if strings.Contains(out, "node.down") {
			t.Errorf("--kind did not filter:\n%s", out)
		}
		if !strings.Contains(out, "deploy.failed") || !strings.Contains(out, "deploy.succeeded") {
			t.Errorf("--kind dropped matches:\n%s", out)
		}
	})

	t.Run("severity filter", func(t *testing.T) {
		out, _, err := run(t, "events", "--severity", "error")
		if err != nil {
			t.Fatalf("events --severity: %v", err)
		}
		if !strings.Contains(out, "deploy.failed") {
			t.Errorf("--severity error dropped the error event:\n%s", out)
		}
		if strings.Contains(out, "deploy.succeeded") || strings.Contains(out, "node.down") {
			t.Errorf("--severity error returned other severities:\n%s", out)
		}
	})

	t.Run("invalid severity is rejected before dialing", func(t *testing.T) {
		_, _, err := run(t, "events", "--severity", "critical")
		if err == nil {
			t.Fatal("expected an error for an unknown severity")
		}
		if !strings.Contains(err.Error(), "info, warning or error") {
			t.Errorf("unhelpful error: %v", err)
		}
	})

	t.Run("json", func(t *testing.T) {
		out, _, err := run(t, "events", "--json")
		if err != nil {
			t.Fatalf("events --json: %v", err)
		}
		if !strings.Contains(out, `"kind"`) || strings.Contains(out, "SEVERITY") {
			t.Errorf("json output unexpected:\n%s", out)
		}
	})
}

// TestAuditEventsFollow drives `zattera events -f`: it prints the backlog
// oldest-first, picks up an event appended mid-poll exactly once, and returns
// cleanly when the context is canceled.
func TestAuditEventsFollow(t *testing.T) {
	rs, projectID := auditFixture(t)
	seedEvents(t, rs, event(projectID, "deploy.succeeded", "info", "first event", time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jsonFlag = false
	projectFlag = ""
	root := &cobra.Command{Use: "zattera", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(Commands()...)
	// follow writes from cobra's goroutine while this test polls from its own,
	// so the buffer must be synchronized (a plain bytes.Buffer is not).
	out := &syncBuffer{}
	root.SetOut(out)
	root.SetErr(&syncBuffer{})
	root.SetArgs([]string{"events", "-f"})

	done := make(chan error, 1)
	go func() { done <- root.ExecuteContext(ctx) }()

	waitFor(t, out, "first event")

	// An event appended while following must show up on the next poll.
	seedEvents(t, rs, event(projectID, "node.down", "warning", "second event", 0))
	waitFor(t, out, "second event")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("follow returned an error on cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follow did not return after cancel")
	}

	got := out.String()
	if strings.Count(got, "first event") != 1 {
		t.Errorf("event printed more than once across polls:\n%s", got)
	}
	if strings.Index(got, "first event") > strings.Index(got, "second event") {
		t.Errorf("follow output is not oldest-first:\n%s", got)
	}
}

// syncBuffer is a bytes.Buffer safe for one writer and one reader on different
// goroutines — the shape of a follow test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// waitFor blocks until buf contains want, polling the shared buffer.
func waitFor(t *testing.T, buf *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q; got:\n%s", want, buf.String())
}

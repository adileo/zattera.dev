package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/cli/cliconfig"
	"github.com/zattera-dev/zattera/internal/cli/ui"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// projectFlag is the shared --project selector.
var projectFlag string

// printerFor builds a printer bound to the command's output streams (so tests
// can capture them via cmd.SetOut/SetErr).
func printerFor(cmd *cobra.Command) *ui.Printer {
	return &ui.Printer{Out: cmd.OutOrStdout(), Err: cmd.ErrOrStderr(), JSON: jsonFlag}
}

// clientFromContext dials the API using the active CLI context.
func clientFromContext() (*apiclient.Client, cliconfig.Context, error) {
	cfg, err := cliconfig.Load()
	if err != nil {
		return nil, cliconfig.Context{}, err
	}
	ctx, ok := cfg.Current()
	if !ok {
		return nil, cliconfig.Context{}, errors.New("no active context; run 'zattera login' first")
	}
	client, err := apiclient.New(apiclient.Config{
		Address:            ctx.Server,
		Token:              ctx.Token,
		CACertPEM:          []byte(ctx.CACertPEM),
		InsecureSkipVerify: ctx.Insecure,
	})
	if err != nil {
		return nil, ctx, err
	}
	return client, ctx, nil
}

// projectName resolves the project from --project or the context default.
func projectName(ctx cliconfig.Context) (string, error) {
	if projectFlag != "" {
		return projectFlag, nil
	}
	if ctx.DefaultProject != "" {
		return ctx.DefaultProject, nil
	}
	return "", errors.New("no project selected: pass --project or set a default in your context")
}

// resolveProjectID turns a project name into its canonical id. Most RPCs are
// project-scoped and let the server's RBAC interceptor do this rewrite, but
// audit and event queries treat an empty project as "cluster-wide", so they are
// not in that table and need the id resolved here (T-76).
func resolveProjectID(ctx context.Context, client *apiclient.Client, name string) (string, error) {
	if name == "" {
		return "", nil
	}
	proj, err := client.Projects.GetProject(ctx, &zatterav1.GetProjectRequest{ProjectId: name})
	if err != nil {
		return "", apiError(err)
	}
	return proj.GetMeta().GetId(), nil
}

// apiError strips the "rpc error: code = ... desc = " noise so users see a
// plain message ("project demo not found") and the command exits non-zero.
func apiError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return fmt.Errorf("%s", st.Message())
	}
	return err
}

// addProjectFlag registers --project on a command.
func addProjectFlag(cmd *cobra.Command) {
	cmd.Flags().StringVar(&projectFlag, "project", "", "project name (defaults to the context's project)")
}

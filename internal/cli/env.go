package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newEnvCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Manage environment variables",
	}
	cmd.AddCommand(newEnvSetCmd(), newEnvPullCmd(), newEnvUnsetCmd())
	return cmd
}

// envTargetFlags adds --app and --env to a command.
func envTargetFlags(cmd *cobra.Command) {
	addProjectFlag(cmd)
	cmd.Flags().String("app", "", "app name")
	cmd.Flags().String("env", "production", "environment name")
}

// resolveEnv looks up the environment id for (project, app, envName) via GetApp.
func resolveEnv(ctx context.Context, client *apiclient.Client, project, app, envName string) (string, error) {
	if app == "" {
		return "", fmt.Errorf("--app is required")
	}
	resp, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: project, AppId: app})
	if err != nil {
		return "", apiError(err)
	}
	for _, e := range resp.GetEnvironments() {
		if e.GetName() == envName {
			return e.GetMeta().GetId(), nil
		}
	}
	return "", fmt.Errorf("environment %q not found on app %q", envName, app)
}

func newEnvSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set KEY=VALUE [KEY=VALUE...]",
		Short: "Set environment variables",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			set := map[string]string{}
			for _, a := range args {
				k, v, ok := strings.Cut(a, "=")
				if !ok || k == "" {
					return fmt.Errorf("invalid KEY=VALUE: %q", a)
				}
				set[k] = v
			}
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			app, _ := cmd.Flags().GetString("app")
			envName, _ := cmd.Flags().GetString("env")
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			envID, err := resolveEnv(ctx, client, proj, app, envName)
			if err != nil {
				return err
			}
			if _, err := client.Apps.SetEnvVars(ctx, &zatterav1.SetEnvVarsRequest{ProjectId: proj, EnvironmentId: envID, Set: set}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Set %d variable(s) on %s/%s", len(set), app, envName)
			return nil
		},
	}
	envTargetFlags(cmd)
	return cmd
}

func newEnvPullCmd() *cobra.Command {
	var reveal bool
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Print environment variables as KEY=value lines",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			app, _ := cmd.Flags().GetString("app")
			envName, _ := cmd.Flags().GetString("env")
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			envID, err := resolveEnv(ctx, client, proj, app, envName)
			if err != nil {
				return err
			}
			resp, err := client.Apps.GetEnvVars(ctx, &zatterav1.GetEnvVarsRequest{ProjectId: proj, EnvironmentId: envID, Reveal: reveal})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetVars())
			}
			keys := make([]string, 0, len(resp.GetVars()))
			for k := range resp.GetVars() {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(cmd.OutOrStdout(), "%s=%s\n", k, resp.GetVars()[k])
			}
			return nil
		},
	}
	envTargetFlags(cmd)
	cmd.Flags().BoolVar(&reveal, "reveal", false, "reveal secret values (developer+)")
	return cmd
}

func newEnvUnsetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unset KEY [KEY...]",
		Short: "Remove environment variables",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			app, _ := cmd.Flags().GetString("app")
			envName, _ := cmd.Flags().GetString("env")
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			envID, err := resolveEnv(ctx, client, proj, app, envName)
			if err != nil {
				return err
			}
			if _, err := client.Apps.SetEnvVars(ctx, &zatterav1.SetEnvVarsRequest{ProjectId: proj, EnvironmentId: envID, Unset: args}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Unset %d variable(s) on %s/%s", len(args), app, envName)
			return nil
		},
	}
	envTargetFlags(cmd)
	return cmd
}

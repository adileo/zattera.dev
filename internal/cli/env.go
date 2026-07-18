package cli

import (
	"context"
	"fmt"
	"os"
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
	var fromFile string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "set [KEY=VALUE...]",
		Short: "Set environment variables",
		Long: "Set environment variables from arguments, a .env file, or both.\n\n" +
			"--from-file parses a .env properly (comments, `export ` prefixes, quoted\n" +
			"values, \\n escapes) instead of leaving it to the shell, which cannot do it\n" +
			"without silently mangling quoted or multi-line values. Use \"-\" to read\n" +
			"stdin. Arguments win over file entries for the same key.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && fromFile == "" {
				return fmt.Errorf("nothing to set: pass KEY=VALUE arguments or --from-file")
			}
			set := map[string]string{}
			if fromFile != "" {
				fileVars, err := readDotenv(cmd, fromFile)
				if err != nil {
					return err
				}
				set = fileVars
			}
			// Explicit arguments are applied last: an argument beats the file.
			for _, a := range args {
				k, v, ok := strings.Cut(a, "=")
				if !ok || k == "" {
					return fmt.Errorf("invalid KEY=VALUE: %q", a)
				}
				set[k] = v
			}
			if dryRun {
				// Names only, never values — a dry run must be safe to paste
				// into a ticket or a CI log.
				p := printerFor(cmd)
				keys := sortedKeys(set)
				if p.JSON {
					return p.EmitJSON(keys)
				}
				p.Infof("would set %d variable(s):", len(keys))
				for _, k := range keys {
					fmt.Fprintln(cmd.OutOrStdout(), k)
				}
				return nil
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
	cmd.Flags().StringVar(&fromFile, "from-file", "", `read KEY=VALUE lines from a .env file ("-" for stdin)`)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the keys that would be set (never the values) and exit")
	return cmd
}

// readDotenv loads a .env from a path or stdin.
func readDotenv(cmd *cobra.Command, path string) (map[string]string, error) {
	if path == "-" {
		return parseDotenv(cmd.InOrStdin(), "-")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return parseDotenv(f, path)
}

// sortedKeys returns a map's keys in sorted order.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
				// Quote so `env pull --reveal > .env` feeds straight back into
				// `env set --from-file` without losing spaces, quotes or
				// newlines (T-103).
				fmt.Fprintf(cmd.OutOrStdout(), "%s=%s\n", k, quoteEnvValue(resp.GetVars()[k]))
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

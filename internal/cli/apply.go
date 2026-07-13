package cli

import (
	"os"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
)

func newApplyCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a zattera.toml to an app (build config + per-env service specs)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			data, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			ac, err := appconfig.Parse(data)
			if err != nil {
				return err
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
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			// Ensure the app exists (create if missing), then apply the config.
			if _, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: ac.Name}); err != nil {
				if _, cerr := client.Apps.CreateApp(ctx, &zatterav1.CreateAppRequest{ProjectId: proj, Name: ac.Name, Build: ac.Build}); cerr != nil {
					return apiError(cerr)
				}
			}
			resp, err := client.Apps.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
				ProjectId:    proj,
				AppId:        ac.Name,
				Build:        ac.Build,
				Github:       ac.GitHub,
				Environments: ac.Services,
			})
			if err != nil {
				return apiError(err)
			}
			// TODO(T-40): apply ac.Domains via DomainService.
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp)
			}
			p.Successf("Applied %s (%d environment(s))", ac.Name, len(resp.GetEnvironments()))
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "zattera.toml", "path to zattera.toml")
	addProjectFlag(cmd)
	return cmd
}

package cli

import (
	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newAppsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "apps",
		Aliases: []string{"app"},
		Short:   "Manage apps",
	}
	cmd.AddCommand(newAppsCreateCmd(), newAppsLsCmd(), newAppsRmCmd())
	return cmd
}

func newAppsCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create an app (auto-creates production + staging)",
		Args:  cobra.ExactArgs(1),
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
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			app, err := client.Apps.CreateApp(ctx, &zatterav1.CreateAppRequest{ProjectId: proj, Name: args[0]})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(app)
			}
			p.Successf("Created app %s", app.GetName())
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newAppsLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List apps",
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
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Apps.ListApps(ctx, &zatterav1.ListAppsRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetApps())
			}
			rows := make([][]string, 0, len(resp.GetApps()))
			for _, a := range resp.GetApps() {
				rows = append(rows, []string{a.GetName(), a.GetMeta().GetId()})
			}
			p.Table([]string{"NAME", "ID"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newAppsRmCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rm <name>",
		Aliases: []string{"delete"},
		Short:   "Delete an app",
		Args:    cobra.ExactArgs(1),
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
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			if _, err := client.Apps.DeleteApp(ctx, &zatterav1.DeleteAppRequest{ProjectId: proj, AppId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Deleted app %s", args[0])
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

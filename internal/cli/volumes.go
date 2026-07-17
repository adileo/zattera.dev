package cli

import (
	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newVolumesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "volume",
		Aliases: []string{"volumes", "vol"},
		Short:   "Manage node-pinned persistent volumes",
	}
	cmd.AddCommand(newVolumeLsCmd(), newVolumeCreateCmd(), newVolumeRmCmd())
	return cmd
}

func newVolumeLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List volumes in the project",
		Args:  cobra.NoArgs,
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

			resp, err := client.Volumes.ListVolumes(ctx, &zatterav1.ListVolumesRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetVolumes())
			}
			rows := make([][]string, 0, len(resp.GetVolumes()))
			for _, v := range resp.GetVolumes() {
				rows = append(rows, []string{
					shortID(v.GetMeta().GetId()),
					v.GetName(),
					shortID(v.GetEnvironmentId()),
					v.GetNodeId(),
					volumeStatus(v.GetStatus()),
				})
			}
			p.Table([]string{"ID", "NAME", "ENV", "NODE", "STATUS"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newVolumeCreateCmd() *cobra.Command {
	var app, env, node string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a volume for a stateful service's environment",
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
			appName, err := resolveAppName(app)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			envID, err := resolveEnv(ctx, client, proj, appName, env)
			if err != nil {
				return err
			}
			v, err := client.Volumes.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{
				ProjectId:     proj,
				EnvironmentId: envID,
				Name:          args[0],
				NodeId:        node,
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(v)
			}
			p.Successf("Volume %q created on node %s", v.GetName(), v.GetNodeId())
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "production", "environment")
	cmd.Flags().StringVar(&node, "node", "", "pin to this node id (default: least-used healthy node)")
	addProjectFlag(cmd)
	return cmd
}

func newVolumeRmCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rm <volume-id>",
		Aliases: []string{"delete"},
		Short:   "Delete a volume (refused while its service is running)",
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

			if _, err := client.Volumes.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: proj, VolumeId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Volume %s deleted", shortID(args[0]))
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func volumeStatus(s zatterav1.VolumeStatus) string {
	switch s {
	case zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return "active"
	case zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST:
		return "node-lost"
	case zatterav1.VolumeStatus_VOLUME_STATUS_RESTORING:
		return "restoring"
	default:
		return "unknown"
	}
}

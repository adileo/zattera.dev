package cli

import (
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newNodesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect cluster nodes and manage join tokens",
	}
	cmd.AddCommand(newNodesLsCmd(), newJoinTokenCmd())
	return cmd
}

func newNodesLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List cluster nodes",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Nodes.ListNodes(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetNodes())
			}
			rows := make([][]string, 0, len(resp.GetNodes()))
			for _, n := range resp.GetNodes() {
				rows = append(rows, []string{
					n.GetName(),
					nodeRoles(n.GetRoles()),
					strings.TrimPrefix(n.GetStatus().String(), "NODE_STATUS_"),
					n.GetMeshIp(),
					nodeLabels(n.GetLabels()),
				})
			}
			p.Table([]string{"NAME", "ROLES", "STATUS", "MESH IP", "LABELS"}, rows)
			return nil
		},
	}
}

func newJoinTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join-token",
		Short: "Manage node join tokens",
	}
	var (
		singleUse bool
		worker    bool
		control   bool
	)
	create := &cobra.Command{
		Use:   "create",
		Short: "Create a join token",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			var roles []zatterav1.NodeRole
			if control {
				roles = append(roles, zatterav1.NodeRole_NODE_ROLE_CONTROL)
			}
			if worker || !control {
				roles = append(roles, zatterav1.NodeRole_NODE_ROLE_WORKER)
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{SingleUse: singleUse, Roles: roles})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(map[string]string{"token": resp.GetToken()})
			}
			// The token is a credential; print it plainly on stdout so it can be
			// piped to the joining node.
			p.Successf("Join token (store securely, shown once):")
			cmd.Println(resp.GetToken())
			return nil
		},
	}
	create.Flags().BoolVar(&singleUse, "single-use", true, "token can be used only once")
	create.Flags().BoolVar(&worker, "worker", false, "allow joining as a worker (default)")
	create.Flags().BoolVar(&control, "control", false, "allow joining as a control node")
	cmd.AddCommand(create)
	return cmd
}

func nodeRoles(roles []zatterav1.NodeRole) string {
	parts := make([]string, 0, len(roles))
	for _, r := range roles {
		parts = append(parts, strings.ToLower(strings.TrimPrefix(r.String(), "NODE_ROLE_")))
	}
	return strings.Join(parts, ",")
}

func nodeLabels(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

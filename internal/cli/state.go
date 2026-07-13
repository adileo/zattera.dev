package cli

import (
	"io"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// dryRunMDKey mirrors the server's metadata key for validate-only applies.
const dryRunMDKey = "x-zattera-dry-run"

func newStateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Export and apply desired cluster state (YAML)",
	}
	cmd.AddCommand(newStateExportCmd(), newStateApplyCmd())
	return cmd
}

func newStateExportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export desired state as YAML to stdout",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			// --project is optional here: empty exports the whole cluster.
			proj := projectFlag
			if proj == "" {
				proj = cctx.DefaultProject
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			stream, err := client.State.Export(ctx, &zatterav1.ExportRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			w := cmd.OutOrStdout()
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return apiError(err)
				}
				if _, werr := w.Write(chunk.GetData()); werr != nil {
					return werr
				}
			}
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newStateApplyCmd() *cobra.Command {
	var (
		file   string
		dryRun bool
	)
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a YAML desired-state document",
		RunE: func(cmd *cobra.Command, _ []string) error {
			data, err := readFileOrStdin(cmd, file)
			if err != nil {
				return err
			}
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			if dryRun {
				ctx = metadata.AppendToOutgoingContext(ctx, dryRunMDKey, "true")
			}
			stream, err := client.State.Apply(ctx)
			if err != nil {
				return apiError(err)
			}
			for len(data) > 0 {
				n := min(len(data), 64<<10)
				if err := stream.Send(&zatterav1.StateChunk{Data: data[:n]}); err != nil {
					return apiError(err)
				}
				data = data[n:]
			}
			resp, err := stream.CloseAndRecv()
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp)
			}
			verb := "Applied"
			if dryRun {
				verb = "Validated (dry-run)"
			}
			p.Successf("%s: %d created, %d updated, %d unchanged", verb, resp.GetCreated(), resp.GetUpdated(), resp.GetUnchanged())
			for _, w := range resp.GetWarnings() {
				p.Infof("warning: %s", w)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "path to the YAML document ('-' or empty = stdin)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate and count without writing")
	addProjectFlag(cmd)
	return cmd
}

func readFileOrStdin(cmd *cobra.Command, file string) ([]byte, error) {
	if file == "" || file == "-" {
		return io.ReadAll(cmd.InOrStdin())
	}
	return os.ReadFile(file)
}

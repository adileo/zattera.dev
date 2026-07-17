package daemon

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zattera-dev/zattera/internal/daemon/backup"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// restoreCommand is `zatterad restore` — disaster recovery from an S3 backup
// into a fresh data dir. RPO is the age of the latest backup (see --help).
func restoreCommand() *cobra.Command {
	var (
		from      string
		dataDir   string
		passFile  string
		endpoint  string
		region    string
		accessKey string
		secretKey string
		nodeID    string
	)
	cmd := &cobra.Command{
		Use:   "restore --from s3://BUCKET/PREFIX --passphrase-file FILE --data-dir DIR",
		Short: "Rebuild a fresh single-node cluster from a backup (disaster recovery)",
		Long: "Restore reads the latest full backup, unseals the cluster data key with\n" +
			"the recovery passphrase, and writes the restored state + CA into a FRESH\n" +
			"data dir. Then start the node with `zatterad server --data-dir DIR`; as\n" +
			"workers rejoin they reclaim their volumes.\n\n" +
			"RPO = the age of the latest backup. The data dir MUST be empty.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			bucket, prefix, err := parseS3URL(from)
			if err != nil {
				return err
			}
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required (must be empty)")
			}
			if err := ensureEmptyDir(dataDir); err != nil {
				return err
			}
			pass, err := readPassphrase(passFile)
			if err != nil {
				return err
			}
			ak, sk := credOr(accessKey, "AWS_ACCESS_KEY_ID"), credOr(secretKey, "AWS_SECRET_ACCESS_KEY")

			ep, ssl := splitEndpoint(endpoint)
			store, err := volumes.NewS3Store(volumes.S3Config{
				Endpoint: ep, Region: region, Bucket: bucket, Prefix: prefix,
				AccessKey: ak, SecretKey: sk, UseSSL: ssl,
			})
			if err != nil {
				return err
			}
			if nodeID == "" {
				nodeID = ids.New()
			}
			idx, err := backup.Restore(cmd.Context(), backup.RestoreInput{
				ObjectStore: store, Passphrase: pass, DataDir: dataDir, NodeID: nodeID,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"Restored backup from %s into %s (%d volumes to reclaim as workers rejoin).\n"+
					"Start the node: zatterad server --data-dir %s\n",
				from, dataDir, len(idx.Volumes), dataDir)
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "backup location, e.g. s3://my-bucket/zattera")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "fresh (empty) data dir to restore into")
	cmd.Flags().StringVar(&passFile, "passphrase-file", "", "file holding the recovery passphrase")
	cmd.Flags().StringVar(&endpoint, "s3-endpoint", "", "S3 endpoint (default AWS; use http://host:port for MinIO)")
	cmd.Flags().StringVar(&region, "s3-region", "us-east-1", "S3 region")
	cmd.Flags().StringVar(&accessKey, "s3-access-key", "", "S3 access key (or AWS_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&secretKey, "s3-secret-key", "", "S3 secret key (or AWS_SECRET_ACCESS_KEY)")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "id for the restored node (default: generated)")
	return cmd
}

func parseS3URL(u string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(u, "s3://") {
		return "", "", fmt.Errorf("--from must be an s3:// URL, got %q", u)
	}
	rest := strings.TrimPrefix(u, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("--from is missing a bucket")
	}
	return bucket, prefix, nil
}

func splitEndpoint(ep string) (host string, useSSL bool) {
	switch {
	case ep == "":
		return "s3.amazonaws.com", true
	case strings.HasPrefix(ep, "https://"):
		return strings.TrimPrefix(ep, "https://"), true
	case strings.HasPrefix(ep, "http://"):
		return strings.TrimPrefix(ep, "http://"), false
	default:
		return ep, true
	}
}

func credOr(flag, env string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv(env)
}

func readPassphrase(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("--passphrase-file is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read passphrase: %w", err)
	}
	pass := strings.TrimRight(string(b), "\r\n")
	if pass == "" {
		return "", fmt.Errorf("passphrase file is empty")
	}
	return pass, nil
}

func ensureEmptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0o700)
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("data dir %s is not empty; restore needs a fresh dir", dir)
	}
	return nil
}

package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Detect the app type and write a zattera.toml",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			if name == "" {
				name = filepath.Base(cwd)
			}
			target := filepath.Join(cwd, "zattera.toml")
			if _, err := os.Stat(target); err == nil {
				return fmt.Errorf("zattera.toml already exists")
			}
			buildType := detectBuildType(cwd)
			content := renderInitTOML(name, buildType)
			if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
				return err
			}
			p := printerFor(cmd)
			p.Successf("Wrote zattera.toml (app %q, build %q)", name, buildType)
			p.Infof("Next: zattera apply --project <project>   then   zattera deploy")
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "app name (defaults to the directory name)")
	return cmd
}

// detectBuildType inspects the working directory: a Dockerfile wins, otherwise
// package.json / go.mod imply nixpacks.
func detectBuildType(dir string) string {
	if exists(filepath.Join(dir, "Dockerfile")) {
		return "dockerfile"
	}
	if exists(filepath.Join(dir, "package.json")) || exists(filepath.Join(dir, "go.mod")) {
		return "nixpacks"
	}
	return "nixpacks"
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func renderInitTOML(name, buildType string) string {
	return fmt.Sprintf(`[app]
name = "%s"

[build]
type = "%s"

[env.production]
replicas = 1

[env.staging]
replicas = 1
`, name, buildType)
}

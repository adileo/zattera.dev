package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newDomainsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "domains",
		Aliases: []string{"domain"},
		Short:   "Manage custom domains for an app environment",
	}
	cmd.AddCommand(newDomainsAddCmd(), newDomainsListCmd(), newDomainsRemoveCmd())
	return cmd
}

func newDomainsAddCmd() *cobra.Command {
	var app, env, pathPrefix, portName string
	var prod bool
	cmd := &cobra.Command{
		Use:   "add <hostname>",
		Short: "Attach a hostname to an app environment",
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

			envID, err := resolveEnv(ctx, client, proj, appName, deployEnvName(env, prod))
			if err != nil {
				return err
			}
			dom, err := client.Domains.AddDomain(ctx, &zatterav1.AddDomainRequest{
				ProjectId: proj, EnvironmentId: envID, Hostname: args[0], PathPrefix: pathPrefix, PortName: portName,
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(dom)
			}
			p.Successf("Added %s → %s (%s)", dom.GetHostname(), appName, deployEnvName(env, prod))
			p.Infof("  certificate: %s", certStatusLabel(dom.GetCertStatus()))
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment (default: staging)")
	cmd.Flags().BoolVar(&prod, "prod", false, "shortcut for --env production")
	cmd.Flags().StringVar(&pathPrefix, "path", "", "route only this path prefix")
	cmd.Flags().StringVar(&portName, "port", "", "target service port name (default: first HTTP port)")
	addProjectFlag(cmd)
	return cmd
}

func newDomainsListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List domains in the project",
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

			resp, err := client.Domains.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp)
			}
			rows := make([][]string, 0, len(resp.GetDomains()))
			for _, d := range resp.GetDomains() {
				host := d.GetHostname()
				if pp := d.GetPathPrefix(); pp != "" {
					host += pp
				}
				rows = append(rows, []string{host, certStatusLabel(d.GetCertStatus())})
			}
			p.Table([]string{"HOSTNAME", "CERT"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newDomainsRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rm <hostname>[<path-prefix>]",
		Aliases: []string{"remove"},
		Short:   "Remove a domain route",
		Long: "Remove a domain route.\n\n" +
			"A hostname can carry several routes that differ by path prefix, so pass\n" +
			"the same form `domains ls` prints — e.g. \"shop.example.com/admin\". A bare\n" +
			"hostname works when it has exactly one route; with more, the command lists\n" +
			"them instead of guessing.",
		Args: cobra.ExactArgs(1),
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

			resp, err := client.Domains.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			id, err := matchDomainRoute(resp.GetDomains(), args[0])
			if err != nil {
				return err
			}
			if _, err := client.Domains.RemoveDomain(ctx, &zatterav1.RemoveDomainRequest{ProjectId: proj, DomainId: id}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Removed %s", args[0])
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

// matchDomainRoute resolves a `rm` argument to exactly one domain id. The
// argument may be a domain id, a "host/prefix" route (as `domains ls` prints
// it), or a bare hostname. A bare hostname that carries several routes is
// ambiguous: list them rather than deleting whichever happened to come first
// (T-104).
func matchDomainRoute(domains []*zatterav1.Domain, arg string) (string, error) {
	var byHost []*zatterav1.Domain
	for _, d := range domains {
		if d.GetMeta().GetId() == arg {
			return arg, nil
		}
		if d.GetHostname()+d.GetPathPrefix() == arg {
			return d.GetMeta().GetId(), nil // exact route match wins
		}
		if d.GetHostname() == arg {
			byHost = append(byHost, d)
		}
	}
	switch len(byHost) {
	case 0:
		return "", fmt.Errorf("domain %q not found", arg)
	case 1:
		return byHost[0].GetMeta().GetId(), nil
	default:
		var routes []string
		for _, d := range byHost {
			routes = append(routes, d.GetHostname()+d.GetPathPrefix())
		}
		sort.Strings(routes)
		return "", fmt.Errorf("%q has %d routes; pass the one to remove: %s",
			arg, len(byHost), strings.Join(routes, ", "))
	}
}

func certStatusLabel(s zatterav1.CertStatus) string {
	switch s {
	case zatterav1.CertStatus_CERT_STATUS_PENDING:
		return "pending"
	case zatterav1.CertStatus_CERT_STATUS_ISSUED:
		return "issued"
	case zatterav1.CertStatus_CERT_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

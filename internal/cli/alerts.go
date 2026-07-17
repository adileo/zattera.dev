package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newAlertsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alerts",
		Short: "Manage alert rules and notification channels",
	}
	cmd.AddCommand(newAlertRulesCmd(), newAlertChannelsCmd())
	return cmd
}

// --- rules -----------------------------------------------------------------

func newAlertRulesCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "rules", Short: "Alert rules"}
	cmd.AddCommand(newAlertRulesLsCmd(), newAlertRulesAddCmd(), newAlertRulesRmCmd())
	return cmd
}

func newAlertRulesLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List alert rules",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Alerts.ListRules(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetRules())
			}
			rows := make([][]string, 0, len(resp.GetRules()))
			for _, r := range resp.GetRules() {
				rows = append(rows, []string{
					shortID(r.GetMeta().GetId()), r.GetName(), ruleCondition(r),
					fmt.Sprintf("%d", len(r.GetChannelIds())), disabledLabel(r.GetDisabled()),
				})
			}
			p.Table([]string{"ID", "NAME", "CONDITION", "CHANNELS", "STATE"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newAlertRulesAddCmd() *cobra.Command {
	var name, metric, scope, op, event string
	var threshold float64
	var sustained time.Duration
	var channels []string
	cmd := &cobra.Command{
		Use:   "add NAME",
		Short: "Add an alert rule (metric threshold or event kind)",
		Long: "Metric rule:  alerts rules add hot --metric cpu_percent --scope node:<id> --op '>' --threshold 90 --for 5m\n" +
			"Event rule:   alerts rules add deploys --event deploy.failed --channel <id>",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("a rule name is required")
			}
			if (metric == "") == (event == "") {
				return fmt.Errorf("set exactly one of --metric or --event")
			}
			rule := &zatterav1.AlertRule{Name: name, ChannelIds: channels}
			if metric != "" {
				if op == "" {
					op = ">"
				}
				if scope == "" {
					scope = "cluster"
				}
				rule.Metric = &zatterav1.MetricCondition{
					Metric: metric, Scope: scope, Op: op, Threshold: threshold,
					Sustained: durationProtoOrNil(sustained),
				}
			} else {
				rule.EventKind = event
			}

			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			out, err := client.Alerts.PutRule(ctx, &zatterav1.PutRuleRequest{Rule: rule})
			if err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Rule %s (%s) created", out.GetName(), shortID(out.GetMeta().GetId()))
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "rule name")
	cmd.Flags().StringVar(&metric, "metric", "", "metric name (cpu_percent, memory_percent, disk_percent, error_rate, ...)")
	cmd.Flags().StringVar(&scope, "scope", "", "metric scope: node:<id> | env:<id> | cluster (default cluster)")
	cmd.Flags().StringVar(&op, "op", "", "comparison: > >= < <= (default >)")
	cmd.Flags().Float64Var(&threshold, "threshold", 0, "metric threshold")
	cmd.Flags().DurationVar(&sustained, "for", 0, "condition must hold for this long before firing")
	cmd.Flags().StringVar(&event, "event", "", "event kind to alert on (e.g. deploy.failed)")
	cmd.Flags().StringArrayVar(&channels, "channel", nil, "notification channel id (repeatable)")
	addProjectFlag(cmd)
	return cmd
}

func newAlertRulesRmCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm ID",
		Short: "Delete an alert rule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			if _, err := client.Alerts.DeleteRule(ctx, &zatterav1.DeleteRuleRequest{RuleId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Rule %s deleted", args[0])
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

// --- channels --------------------------------------------------------------

func newAlertChannelsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "channels", Short: "Notification channels (admin)"}
	cmd.AddCommand(newAlertChannelsLsCmd(), newAlertChannelsAddCmd(), newAlertChannelsRmCmd())
	return cmd
}

func newAlertChannelsLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List notification channels (secrets redacted)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Alerts.ListChannels(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetChannels())
			}
			rows := make([][]string, 0, len(resp.GetChannels()))
			for _, c := range resp.GetChannels() {
				rows = append(rows, []string{shortID(c.GetMeta().GetId()), c.GetName(), c.GetType(), channelTarget(c)})
			}
			p.Table([]string{"ID", "NAME", "TYPE", "TARGET"}, rows)
			return nil
		},
	}
	return cmd
}

func newAlertChannelsAddCmd() *cobra.Command {
	var webhookURL, webhookSecret, slackURL string
	var emailTo, smtpHost, smtpUser, smtpPass, smtpFrom string
	var smtpPort uint32
	var smtpStartTLS bool
	cmd := &cobra.Command{
		Use:   "add TYPE NAME",
		Short: "Add a notification channel: webhook | slack | email",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			typ, chName := args[0], args[1]
			req := &zatterav1.PutChannelRequest{Channel: &zatterav1.NotificationChannel{Name: chName, Type: typ}}
			switch typ {
			case "webhook":
				if webhookURL == "" {
					return fmt.Errorf("--url is required for a webhook channel")
				}
				req.Channel.WebhookUrlPlain = webhookURL
				req.WebhookSecretPlain = webhookSecret
			case "slack":
				if slackURL == "" {
					return fmt.Errorf("--slack-url is required for a slack channel")
				}
				req.SlackWebhookUrlPlain = slackURL
			case "email":
				if emailTo == "" || smtpHost == "" || smtpFrom == "" {
					return fmt.Errorf("--to, --smtp-host and --from are required for an email channel")
				}
				req.Channel.EmailTo = emailTo
				req.Channel.Smtp = &zatterav1.SMTPConfig{
					Host: smtpHost, Port: smtpPort, Username: smtpUser, From: smtpFrom, Starttls: smtpStartTLS,
				}
				req.SmtpPasswordPlain = smtpPass
			default:
				return fmt.Errorf("unknown channel type %q (want webhook|slack|email)", typ)
			}

			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			out, err := client.Alerts.PutChannel(ctx, req)
			if err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Channel %s (%s) created — attach it to a rule with --channel %s",
				out.GetName(), out.GetType(), shortID(out.GetMeta().GetId()))
			return nil
		},
	}
	cmd.Flags().StringVar(&webhookURL, "url", "", "webhook: POST URL")
	cmd.Flags().StringVar(&webhookSecret, "secret", "", "webhook: HMAC signing key (sealed server-side)")
	cmd.Flags().StringVar(&slackURL, "slack-url", "", "slack: incoming-webhook URL (sealed server-side)")
	cmd.Flags().StringVar(&emailTo, "to", "", "email: recipient")
	cmd.Flags().StringVar(&smtpHost, "smtp-host", "", "email: SMTP host")
	cmd.Flags().Uint32Var(&smtpPort, "smtp-port", 587, "email: SMTP port")
	cmd.Flags().StringVar(&smtpUser, "smtp-user", "", "email: SMTP username")
	cmd.Flags().StringVar(&smtpPass, "smtp-pass", "", "email: SMTP password (sealed server-side)")
	cmd.Flags().StringVar(&smtpFrom, "from", "", "email: From address")
	cmd.Flags().BoolVar(&smtpStartTLS, "starttls", true, "email: use STARTTLS")
	return cmd
}

func newAlertChannelsRmCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm ID",
		Short: "Delete a notification channel",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			if _, err := client.Alerts.DeleteChannel(ctx, &zatterav1.DeleteChannelRequest{ChannelId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Channel %s deleted", args[0])
			return nil
		},
	}
	return cmd
}

// --- helpers ---------------------------------------------------------------

func ruleCondition(r *zatterav1.AlertRule) string {
	if mc := r.GetMetric(); mc.GetMetric() != "" {
		s := fmt.Sprintf("%s %s %.0f @%s", mc.GetMetric(), mc.GetOp(), mc.GetThreshold(), mc.GetScope())
		if d := mc.GetSustained().AsDuration(); d > 0 {
			s += " for " + d.String()
		}
		return s
	}
	return "event:" + r.GetEventKind()
}

func channelTarget(c *zatterav1.NotificationChannel) string {
	switch c.GetType() {
	case "webhook":
		return c.GetWebhookUrlPlain()
	case "email":
		return c.GetEmailTo()
	case "slack":
		return "(webhook)"
	default:
		return ""
	}
}

func disabledLabel(disabled bool) string {
	if disabled {
		return "disabled"
	}
	return "enabled"
}

func durationProtoOrNil(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}

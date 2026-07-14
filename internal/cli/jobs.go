package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Run and inspect one-shot jobs",
	}
	cmd.AddCommand(newJobsRunCmd(), newJobsLsCmd())
	return cmd
}

func newJobsRunCmd() *cobra.Command {
	var app, env string
	var maxRetries uint32
	var noWait bool
	cmd := &cobra.Command{
		Use:   "run [app] -- <command...>",
		Short: "Run a one-shot job in an environment's active release image",
		Long: "Enqueue a one-shot job and (by default) wait for it, streaming logs\n" +
			"and exiting with the job's exit code. Example:\n" +
			"  zattera jobs run api --env production -- rails db:migrate",
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			command := argsAfterDashDash(cmd, args)
			if len(command) == 0 {
				return errors.New("a command is required after --, e.g. jobs run -- echo hi")
			}
			pos := args[:len(args)-len(command)]
			appName := app
			if len(pos) >= 1 {
				appName = pos[0]
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
			appName, err = resolveAppName(appName)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			envID, err := resolveEnv(ctx, client, proj, appName, env)
			if err != nil {
				return err
			}

			job, err := client.Jobs.RunJob(ctx, &zatterav1.RunJobRequest{
				ProjectId:     proj,
				EnvironmentId: envID,
				Command:       shellJoin(command),
				MaxRetries:    maxRetries,
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			p.Successf("Job %s queued", shortID(job.GetMeta().GetId()))
			if noWait {
				if jsonFlag {
					return p.EmitJSON(job)
				}
				return nil
			}
			return waitForJob(ctx, client, proj, job.GetMeta().GetId(), cmd)
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "production", "environment")
	cmd.Flags().Uint32Var(&maxRetries, "max-retries", 0, "retry the job up to N times on failure")
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "return immediately instead of waiting for completion")
	addProjectFlag(cmd)
	return cmd
}

// waitForJob streams the job's logs and polls until it reaches a terminal state,
// then exits with the job's recorded exit code.
func waitForJob(ctx context.Context, client *apiclient.Client, proj, jobID string, cmd *cobra.Command) error {
	// Stream logs in the background (best-effort; ends when the job finishes).
	go streamJobLogs(ctx, client, proj, jobID, cmd)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			job, err := client.Jobs.GetJob(ctx, &zatterav1.GetJobRequest{ProjectId: proj, JobId: jobID})
			if err != nil {
				return apiError(err)
			}
			switch job.GetStatus() {
			case zatterav1.JobStatus_JOB_STATUS_SUCCEEDED:
				printerFor(cmd).Successf("Job %s succeeded", shortID(jobID))
				return nil
			case zatterav1.JobStatus_JOB_STATUS_FAILED:
				printerFor(cmd).Errorf("Job %s failed (exit %d): %s", shortID(jobID), job.GetExitCode(), job.GetError())
				return exitError{code: exitCodeOf(job)}
			case zatterav1.JobStatus_JOB_STATUS_CANCELED:
				printerFor(cmd).Errorf("Job %s canceled", shortID(jobID))
				return exitError{code: 1}
			}
		}
	}
}

func streamJobLogs(ctx context.Context, client *apiclient.Client, proj, jobID string, cmd *cobra.Command) {
	stream, err := client.Logs.Query(ctx, &zatterav1.LogQuery{
		Selector: &zatterav1.LogSelector{ProjectId: proj, JobId: jobID},
		Follow:   true,
	})
	if err != nil {
		return
	}
	for {
		line, err := stream.Recv()
		if err != nil {
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), line.GetLine())
	}
}

func newJobsLsCmd() *cobra.Command {
	var app, env string
	cmd := &cobra.Command{
		Use:   "ls [app]",
		Short: "List recent jobs",
		Args:  cobra.MaximumNArgs(1),
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

			var envID string
			appName := app
			if len(args) == 1 {
				appName = args[0]
			}
			if env != "" && appName != "" {
				if appName, err = resolveAppName(appName); err == nil {
					envID, _ = resolveEnv(ctx, client, proj, appName, env)
				}
			}

			resp, err := client.Jobs.ListJobs(ctx, &zatterav1.ListJobsRequest{ProjectId: proj, EnvironmentId: envID})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetJobs())
			}
			rows := make([][]string, 0, len(resp.GetJobs()))
			for _, j := range resp.GetJobs() {
				rows = append(rows, []string{
					shortID(j.GetMeta().GetId()),
					jobStatus(j.GetStatus()),
					fmt.Sprintf("%d", j.GetExitCode()),
					fmt.Sprintf("%d", j.GetAttempt()),
					j.GetCommand(),
				})
			}
			p.Table([]string{"JOB", "STATUS", "EXIT", "ATTEMPT", "COMMAND"}, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name filter")
	cmd.Flags().StringVar(&env, "env", "", "environment filter (requires app)")
	addProjectFlag(cmd)
	return cmd
}

// exitCodeOf maps a job's recorded exit code to a process code (non-zero even
// when the job failed without a captured code).
func exitCodeOf(job *zatterav1.Job) int {
	if c := int(job.GetExitCode()); c != 0 {
		return c
	}
	return 1
}

func jobStatus(s zatterav1.JobStatus) string {
	switch s {
	case zatterav1.JobStatus_JOB_STATUS_QUEUED:
		return "queued"
	case zatterav1.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case zatterav1.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case zatterav1.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case zatterav1.JobStatus_JOB_STATUS_RETRYING:
		return "retrying"
	case zatterav1.JobStatus_JOB_STATUS_CANCELED:
		return "canceled"
	default:
		return "unknown"
	}
}

// shellJoin renders argv as a single shell command string (the Job model stores
// a shell command; the agent runs it via /bin/sh -c).
func shellJoin(args []string) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = shellQuote(a)
	}
	return strings.Join(parts, " ")
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	for _, r := range s {
		if !shellSafe(r) {
			return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
		}
	}
	return s
}

// shellSafe reports whether r can appear unquoted in a shell word.
func shellSafe(r rune) bool {
	switch r {
	case '-', '_', '.', '/', ':', '=':
		return true
	}
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newStatsCmd() *cobra.Command {
	var nodesFlag bool
	var app string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show live resource stats (current values from heartbeats)",
		Long: "Live stats sampled from node heartbeats. By default (or with --nodes)\n" +
			"shows per-node CPU/memory/disk; with --app shows per-environment traffic.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			q := &zatterav1.StatsQuery{}
			appMode := app != ""
			if appMode {
				proj, err := projectName(cctx)
				if err != nil {
					return err
				}
				got, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: app})
				if err != nil {
					return apiError(err)
				}
				q.AppId = got.GetApp().GetMeta().GetId()
			}

			resp, err := client.Metrics.Stats(ctx, q)
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetSeries())
			}
			_ = nodesFlag // --nodes is the default view
			if appMode {
				renderStatsTable(p.Table, resp.GetSeries(), "env", envMetricCols)
			} else {
				renderStatsTable(p.Table, resp.GetSeries(), "node", nodeMetricCols)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&nodesFlag, "nodes", false, "show per-node stats (default)")
	cmd.Flags().StringVar(&app, "app", "", "show per-environment traffic stats for this app")
	addProjectFlag(cmd)
	return cmd
}

// statsColumn pairs a metric key with its column header and a value formatter.
type statsColumn struct {
	metric string
	header string
	format func(float64) string
}

var nodeMetricCols = []statsColumn{
	{"cpu_percent", "CPU%", fmtPercent},
	{"memory_bytes", "MEM", fmtBytes},
	{"memory_percent", "MEM%", fmtPercent},
	{"disk_bytes", "DISK", fmtBytes},
	{"disk_percent", "DISK%", fmtPercent},
}

var envMetricCols = []statsColumn{
	{"rps", "RPS", fmtFloat},
	{"inflight", "INFLIGHT", fmtFloat},
	{"error_rate", "ERR%", fmtPercent},
	{"latency_p50_ms", "P50ms", fmtFloat},
	{"latency_p99_ms", "P99ms", fmtFloat},
}

// renderStatsTable pivots the flat series list into one row per scope entity
// (node or env), one column per metric.
func renderStatsTable(table func([]string, [][]string), series []*zatterav1.Series, scopeLabel string, cols []statsColumn) {
	// entity id → metric → value
	rowsByID := map[string]map[string]float64{}
	for _, s := range series {
		id := s.GetLabels()[scopeLabel]
		if id == "" {
			continue
		}
		vals := rowsByID[id]
		if vals == nil {
			vals = map[string]float64{}
			rowsByID[id] = vals
		}
		if pts := s.GetPoints(); len(pts) > 0 {
			vals[s.GetMetric()] = pts[len(pts)-1].GetValue()
		}
	}

	ids := make([]string, 0, len(rowsByID))
	for id := range rowsByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	headers := make([]string, 0, len(cols)+1)
	headers = append(headers, strings.ToUpper(scopeLabel)) // "NODE" / "ENV"
	for _, c := range cols {
		headers = append(headers, c.header)
	}

	rows := make([][]string, 0, len(ids))
	for _, id := range ids {
		vals := rowsByID[id]
		row := make([]string, 0, len(cols)+1)
		row = append(row, shortID(id))
		for _, c := range cols {
			if v, ok := vals[c.metric]; ok {
				row = append(row, c.format(v))
			} else {
				row = append(row, "-")
			}
		}
		rows = append(rows, row)
	}
	table(headers, rows)
}

func fmtPercent(v float64) string { return fmt.Sprintf("%.1f%%", v) }
func fmtFloat(v float64) string   { return fmt.Sprintf("%.1f", v) }

func fmtBytes(v float64) string {
	const unit = 1024.0
	if v < unit {
		return fmt.Sprintf("%.0fB", v)
	}
	div, exp := unit, 0
	for n := v / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", v/div, "KMGT"[exp])
}

// Package notify is the alert engine (T-74): the leader evaluates AlertRules
// against metrics (TSDB/livestate) and cluster events, and delivers firing and
// resolved notifications to NotificationChannels (webhook, Slack, email) with
// per-rule dedupe. Notifier failures never stall evaluation.
package notify

import (
	"context"
	"time"
)

const (
	// dedupeWindow silences a repeat notification for the same rule+scope.
	dedupeWindow = 15 * time.Minute
	// channelTimeout bounds a single notifier delivery so one slow channel can't
	// stall the engine.
	channelTimeout = 10 * time.Second
	// eventScan bounds how many recent events an evaluation tick inspects.
	eventScan = 500
)

// Notification is one alert delivery — a rule firing or resolving.
type Notification struct {
	Rule     string    // rule name
	Firing   bool      // true = firing, false = resolved
	Severity string    // "info" | "warning" | "error"
	Scope    string    // "node:<id>" | "env:<id>" | "cluster" | event scope
	Summary  string    // human-readable one-liner
	At       time.Time // when the engine decided this

	// Metric rules only:
	Metric    string
	Value     float64
	Threshold float64
	Op        string

	// Event rules only:
	EventKind string
}

// Notifier delivers a Notification to one channel type.
type Notifier interface {
	Send(ctx context.Context, n Notification) error
}

// compare evaluates `value op threshold`.
func compare(value float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	default:
		return false
	}
}

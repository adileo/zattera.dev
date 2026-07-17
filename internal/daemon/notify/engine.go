package notify

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Store is the read side the engine needs (the leader's replicated state).
type Store interface {
	ListAlertRules() []*zatterav1.AlertRule
	ListNotificationChannels() []*zatterav1.NotificationChannel
	ListEvents(limit int) []*zatterav1.Event
}

// MetricSource resolves the current value of a metric for a scope
// ("node:<id>" | "env:<id>" | "cluster"). ok is false when no data is available.
type MetricSource interface {
	Value(metric, scope string) (value float64, ok bool)
}

// Opener decrypts a channel's sealed secret values.
type Opener interface {
	Open(*zatterav1.EncryptedValue) ([]byte, error)
}

// Engine evaluates alert rules and delivers notifications (T-74).
type Engine struct {
	store   Store
	metrics MetricSource
	opener  Opener
	clock   clock.Clock
	log     *slog.Logger
	http    *http.Client

	// emitEvent records an event (e.g. a notifier failure) through raft;
	// best-effort and may be nil.
	emitEvent func(kind, severity, message string)

	// send delivers a notification to one channel. Overridable in tests; the
	// default resolves the channel's notifier and calls it.
	send func(ctx context.Context, ch *zatterav1.NotificationChannel, n Notification) error

	mu     sync.Mutex
	since  map[string]time.Time // rule|scope → first time the condition held
	firing map[string]time.Time // rule|scope → last delivered while firing (dedupe + resolve)
	seen   map[string]bool      // event ids already processed this term
}

// Config wires an Engine.
type Config struct {
	Store     Store
	Metrics   MetricSource
	Opener    Opener
	Clock     clock.Clock
	Logger    *slog.Logger
	HTTP      *http.Client
	EmitEvent func(kind, severity, message string)
}

// NewEngine builds the engine.
func NewEngine(cfg Config) *Engine {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	httpc := cfg.HTTP
	if httpc == nil {
		httpc = &http.Client{Timeout: channelTimeout}
	}
	e := &Engine{
		store: cfg.Store, metrics: cfg.Metrics, opener: cfg.Opener, clock: clk, log: log,
		http: httpc, emitEvent: cfg.EmitEvent,
		since: map[string]time.Time{}, firing: map[string]time.Time{}, seen: map[string]bool{},
	}
	e.send = e.dispatch
	return e
}

// Reset clears per-term state and marks all existing events as already seen, so
// a freshly elected leader does not replay history as fresh alerts.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.since = map[string]time.Time{}
	e.firing = map[string]time.Time{}
	e.seen = map[string]bool{}
	for _, ev := range e.store.ListEvents(eventScan) {
		e.seen[ev.GetMeta().GetId()] = true
	}
}

// Tick performs one evaluation pass: metric rules (with sustained + dedupe) and
// event rules (new events since the last pass).
func (e *Engine) Tick(ctx context.Context) {
	now := e.clock.Now()
	rules := e.store.ListAlertRules()
	channels := indexChannels(e.store.ListNotificationChannels())
	for _, r := range rules {
		if r.GetDisabled() {
			continue
		}
		if mc := r.GetMetric(); mc.GetMetric() != "" {
			e.evalMetric(ctx, r, mc, channels, now)
		}
	}
	e.evalEvents(ctx, rules, channels, now)
}

// evalMetric handles one metric rule: it fires once the condition has held for
// `sustained`, re-alerts past the dedupe window, and resolves when it clears.
func (e *Engine) evalMetric(ctx context.Context, r *zatterav1.AlertRule, mc *zatterav1.MetricCondition, channels map[string]*zatterav1.NotificationChannel, now time.Time) {
	key := r.GetMeta().GetId() + "|" + mc.GetScope()
	v, ok := e.metrics.Value(mc.GetMetric(), mc.GetScope())
	if !ok {
		return // no data: freeze (neither fire nor resolve)
	}
	cond := compare(v, mc.GetOp(), mc.GetThreshold())

	e.mu.Lock()
	if cond {
		if e.since[key].IsZero() {
			e.since[key] = now
		}
		if now.Sub(e.since[key]) < mc.GetSustained().AsDuration() {
			e.mu.Unlock()
			return // not held long enough
		}
		if last, firing := e.firing[key]; firing && now.Sub(last) < dedupeWindow {
			e.mu.Unlock()
			return // still firing, silenced
		}
		e.firing[key] = now
		e.mu.Unlock()
		e.deliver(ctx, r, channels, Notification{
			Rule: r.GetName(), Firing: true, Severity: severityFor(mc), Scope: mc.GetScope(),
			Metric: mc.GetMetric(), Value: v, Threshold: mc.GetThreshold(), Op: mc.GetOp(),
			Summary: fmt.Sprintf("%s is %.1f (%s %.1f) for %s", mc.GetMetric(), v, mc.GetOp(), mc.GetThreshold(), mc.GetScope()),
			At:      now,
		})
		return
	}
	delete(e.since, key)
	_, wasFiring := e.firing[key]
	delete(e.firing, key)
	e.mu.Unlock()
	if wasFiring {
		e.deliver(ctx, r, channels, Notification{
			Rule: r.GetName(), Firing: false, Severity: "info", Scope: mc.GetScope(),
			Metric: mc.GetMetric(), Value: v, Threshold: mc.GetThreshold(), Op: mc.GetOp(),
			Summary: fmt.Sprintf("%s recovered: %.1f", mc.GetMetric(), v),
			At:      now,
		})
	}
}

// evalEvents fires event rules for cluster events not yet processed.
func (e *Engine) evalEvents(ctx context.Context, rules []*zatterav1.AlertRule, channels map[string]*zatterav1.NotificationChannel, now time.Time) {
	for _, ev := range e.store.ListEvents(eventScan) {
		id := ev.GetMeta().GetId()
		e.mu.Lock()
		if e.seen[id] {
			e.mu.Unlock()
			continue
		}
		e.seen[id] = true
		e.mu.Unlock()

		for _, r := range rules {
			if r.GetDisabled() || r.GetEventKind() == "" || r.GetEventKind() != ev.GetKind() {
				continue
			}
			sev := ev.GetSeverity()
			if sev == "" {
				sev = "warning"
			}
			e.deliver(ctx, r, channels, Notification{
				Rule: r.GetName(), Firing: true, Severity: sev, Scope: eventScope(ev),
				EventKind: ev.GetKind(), Summary: ev.GetMessage(), At: now,
			})
		}
	}
}

// deliver sends a notification to every channel the rule targets, isolating each
// with a timeout so one slow/broken channel cannot stall evaluation.
func (e *Engine) deliver(ctx context.Context, r *zatterav1.AlertRule, channels map[string]*zatterav1.NotificationChannel, n Notification) {
	for _, cid := range r.GetChannelIds() {
		ch, ok := channels[cid]
		if !ok {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, channelTimeout)
		err := e.send(cctx, ch, n)
		cancel()
		if err != nil {
			e.log.Warn("notify: channel delivery failed", "rule", r.GetName(), "channel", ch.GetName(), "type", ch.GetType(), "err", err)
			if e.emitEvent != nil {
				e.emitEvent("alert.notify_failed", "warning", fmt.Sprintf("channel %q (%s) failed for rule %q: %v", ch.GetName(), ch.GetType(), r.GetName(), err))
			}
		}
	}
}

// dispatch resolves a channel's notifier (unsealing secrets) and delivers.
func (e *Engine) dispatch(ctx context.Context, ch *zatterav1.NotificationChannel, n Notification) error {
	switch ch.GetType() {
	case "webhook":
		var secret []byte
		if ch.GetWebhookSecret() != nil {
			s, err := e.opener.Open(ch.GetWebhookSecret())
			if err != nil {
				return fmt.Errorf("notify: open webhook secret: %w", err)
			}
			secret = s
		}
		return NewWebhook(ch.GetWebhookUrlPlain(), secret, e.http).Send(ctx, n)
	case "slack":
		url, err := e.openString(ch.GetSlackWebhookUrl())
		if err != nil {
			return err
		}
		return NewSlack(url, e.http).Send(ctx, n)
	case "email":
		sm := ch.GetSmtp()
		pw, err := e.openString(sm.GetPassword())
		if err != nil {
			return err
		}
		return NewEmail(EmailConfig{
			Host: sm.GetHost(), Port: sm.GetPort(), Username: sm.GetUsername(), Password: pw,
			From: sm.GetFrom(), To: ch.GetEmailTo(), StartTLS: sm.GetStarttls(),
		}).Send(ctx, n)
	default:
		return fmt.Errorf("notify: unknown channel type %q", ch.GetType())
	}
}

func (e *Engine) openString(v *zatterav1.EncryptedValue) (string, error) {
	if v == nil {
		return "", nil
	}
	b, err := e.opener.Open(v)
	if err != nil {
		return "", fmt.Errorf("notify: open secret: %w", err)
	}
	return string(b), nil
}

func indexChannels(chs []*zatterav1.NotificationChannel) map[string]*zatterav1.NotificationChannel {
	m := make(map[string]*zatterav1.NotificationChannel, len(chs))
	for _, c := range chs {
		m[c.GetMeta().GetId()] = c
	}
	return m
}

// eventScope renders the most specific scope carried by an event.
func eventScope(ev *zatterav1.Event) string {
	switch {
	case ev.GetNodeId() != "":
		return "node:" + ev.GetNodeId()
	case ev.GetEnvironmentId() != "":
		return "env:" + ev.GetEnvironmentId()
	default:
		return "cluster"
	}
}

// severityFor maps a metric condition to a severity (disk/cert-ish stay warning;
// error_rate escalates).
func severityFor(mc *zatterav1.MetricCondition) string {
	if mc.GetMetric() == "error_rate" {
		return "error"
	}
	return "warning"
}

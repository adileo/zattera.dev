package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// --- fakes -----------------------------------------------------------------

type fakeStore struct {
	rules    []*zatterav1.AlertRule
	channels []*zatterav1.NotificationChannel
	events   []*zatterav1.Event
}

func (f *fakeStore) ListAlertRules() []*zatterav1.AlertRule                     { return f.rules }
func (f *fakeStore) ListNotificationChannels() []*zatterav1.NotificationChannel { return f.channels }
func (f *fakeStore) ListEvents(limit int) []*zatterav1.Event                    { return f.events }

type fakeMetrics map[string]float64 // "metric|scope" → value

func (m fakeMetrics) Value(metric, scope string) (float64, bool) {
	v, ok := m[metric+"|"+scope]
	return v, ok
}

func (m fakeMetrics) set(metric, scope string, v float64) { m[metric+"|"+scope] = v }

// plainOpener treats ciphertext as plaintext (test double for the sealer).
type plainOpener struct{}

func (plainOpener) Open(v *zatterav1.EncryptedValue) ([]byte, error) { return v.GetCiphertext(), nil }

// capture records delivered notifications instead of sending them.
type capture struct {
	mu   sync.Mutex
	sent []Notification
}

func (c *capture) send(_ context.Context, _ *zatterav1.NotificationChannel, n Notification) error {
	c.mu.Lock()
	c.sent = append(c.sent, n)
	c.mu.Unlock()
	return nil
}

func (c *capture) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.sent) }
func (c *capture) last() Notification {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent[len(c.sent)-1]
}

func metricRule(id, name, metric, scope, op string, threshold float64, sustained time.Duration, channels ...string) *zatterav1.AlertRule {
	return &zatterav1.AlertRule{
		Meta: &zatterav1.Meta{Id: id}, Name: name, ChannelIds: channels,
		Metric: &zatterav1.MetricCondition{
			Metric: metric, Scope: scope, Op: op, Threshold: threshold,
			Sustained: durationpb.New(sustained),
		},
	}
}

func newEngine(t *testing.T, store *fakeStore, metrics fakeMetrics, clk *clock.Fake) (*Engine, *capture) {
	t.Helper()
	e := NewEngine(Config{Store: store, Metrics: metrics, Opener: plainOpener{}, Clock: clk})
	cap := &capture{}
	e.send = cap.send
	e.Reset()
	return e, cap
}

// --- engine tests ----------------------------------------------------------

// TestMetricSustainedAndDedupe: a rule fires only after the condition holds for
// `sustained`, then is silenced for the dedupe window, then re-alerts.
func TestMetricSustainedAndDedupe(t *testing.T) {
	clk := clock.NewFake()
	store := &fakeStore{
		channels: []*zatterav1.NotificationChannel{{Meta: &zatterav1.Meta{Id: "c1"}, Type: "webhook"}},
		rules:    []*zatterav1.AlertRule{metricRule("r1", "disk-full", "disk_percent", "node:n1", ">", 90, 5*time.Minute, "c1")},
	}
	metrics := fakeMetrics{}
	e, cap := newEngine(t, store, metrics, clk)

	// Over threshold but not yet sustained → no fire.
	metrics.set("disk_percent", "node:n1", 95)
	e.Tick(context.Background())
	if cap.count() != 0 {
		t.Fatalf("fired before sustained: %d", cap.count())
	}

	// Cross the sustained window → fires once.
	clk.Advance(6 * time.Minute)
	e.Tick(context.Background())
	if cap.count() != 1 {
		t.Fatalf("did not fire after sustained: %d", cap.count())
	}
	if n := cap.last(); !n.Firing || n.Metric != "disk_percent" || n.Value != 95 {
		t.Fatalf("bad firing notification: %+v", n)
	}

	// Still firing, within dedupe window → silenced.
	clk.Advance(2 * time.Minute)
	e.Tick(context.Background())
	if cap.count() != 1 {
		t.Fatalf("re-alerted inside dedupe window: %d", cap.count())
	}

	// Past the dedupe window → re-alerts.
	clk.Advance(dedupeWindow)
	e.Tick(context.Background())
	if cap.count() != 2 {
		t.Fatalf("did not re-alert past dedupe window: %d", cap.count())
	}
}

// TestMetricResolves: when the condition clears, a resolved notification is sent
// once.
func TestMetricResolves(t *testing.T) {
	clk := clock.NewFake()
	store := &fakeStore{
		channels: []*zatterav1.NotificationChannel{{Meta: &zatterav1.Meta{Id: "c1"}, Type: "webhook"}},
		rules:    []*zatterav1.AlertRule{metricRule("r1", "cpu-hot", "cpu_percent", "node:n1", ">", 80, 0, "c1")},
	}
	metrics := fakeMetrics{}
	e, cap := newEngine(t, store, metrics, clk)

	metrics.set("cpu_percent", "node:n1", 90)
	e.Tick(context.Background())
	if cap.count() != 1 || !cap.last().Firing {
		t.Fatalf("expected a firing notification, got %d", cap.count())
	}

	// Drops below threshold → one resolved notification.
	metrics.set("cpu_percent", "node:n1", 10)
	clk.Advance(time.Minute)
	e.Tick(context.Background())
	if cap.count() != 2 || cap.last().Firing {
		t.Fatalf("expected a resolved notification, got count=%d firing=%v", cap.count(), cap.last().Firing)
	}
	// Staying resolved does not re-notify.
	e.Tick(context.Background())
	if cap.count() != 2 {
		t.Fatalf("resolved re-notified: %d", cap.count())
	}
}

// TestMetricFreezesWithoutData: a rule with no metric data neither fires nor
// resolves.
func TestMetricFreezesWithoutData(t *testing.T) {
	clk := clock.NewFake()
	store := &fakeStore{
		channels: []*zatterav1.NotificationChannel{{Meta: &zatterav1.Meta{Id: "c1"}, Type: "webhook"}},
		rules:    []*zatterav1.AlertRule{metricRule("r1", "cpu", "cpu_percent", "node:gone", ">", 1, 0, "c1")},
	}
	e, cap := newEngine(t, store, fakeMetrics{}, clk)
	e.Tick(context.Background())
	if cap.count() != 0 {
		t.Fatalf("fired without data: %d", cap.count())
	}
}

// TestEventRuleMatchesNewEvents: an event rule fires for a new matching event,
// once, and ignores pre-existing events after Reset.
func TestEventRuleMatchesNewEvents(t *testing.T) {
	clk := clock.NewFake()
	store := &fakeStore{
		channels: []*zatterav1.NotificationChannel{{Meta: &zatterav1.Meta{Id: "c1"}, Type: "webhook"}},
		rules: []*zatterav1.AlertRule{
			{Meta: &zatterav1.Meta{Id: "r1"}, Name: "deploy-failed", EventKind: "deploy.failed", ChannelIds: []string{"c1"}},
		},
		events: []*zatterav1.Event{
			{Meta: &zatterav1.Meta{Id: "old"}, Kind: "deploy.failed", Severity: "error", Message: "old"},
		},
	}
	e, cap := newEngine(t, store, fakeMetrics{}, clk) // Reset marks "old" as seen

	// Existing event was seen at Reset → not replayed.
	e.Tick(context.Background())
	if cap.count() != 0 {
		t.Fatalf("replayed pre-existing event: %d", cap.count())
	}

	// A new matching event fires once; a non-matching one is ignored.
	store.events = append(store.events,
		&zatterav1.Event{Meta: &zatterav1.Meta{Id: "new"}, Kind: "deploy.failed", Severity: "error", Message: "boom", EnvironmentId: "e1"},
		&zatterav1.Event{Meta: &zatterav1.Meta{Id: "other"}, Kind: "autoscale.scaled", Message: "meh"},
	)
	e.Tick(context.Background())
	if cap.count() != 1 {
		t.Fatalf("event rule fired %d times, want 1", cap.count())
	}
	if n := cap.last(); n.EventKind != "deploy.failed" || n.Scope != "env:e1" || n.Summary != "boom" {
		t.Fatalf("bad event notification: %+v", n)
	}

	// Re-ticking does not re-fire for the same event.
	e.Tick(context.Background())
	if cap.count() != 1 {
		t.Fatalf("re-fired for a seen event: %d", cap.count())
	}
}

// --- notifier tests --------------------------------------------------------

// TestWebhookPayloadAndHMAC: the webhook posts the golden JSON and signs it.
func TestWebhookPayloadAndHMAC(t *testing.T) {
	var gotBody []byte
	var gotSig, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(signatureHeader)
		gotCT = r.Header.Get("Content-Type")
	}))
	defer srv.Close()

	n := Notification{
		Rule: "disk-full", Firing: true, Severity: "warning", Scope: "node:n1",
		Summary: "disk_percent is 95.0 (> 90.0) for node:n1",
		Metric:  "disk_percent", Value: 95, Threshold: 90, Op: ">",
		At: time.Unix(1700000000, 0).UTC(),
	}
	if err := NewWebhook(srv.URL, []byte("shh"), srv.Client()).Send(context.Background(), n); err != nil {
		t.Fatalf("send: %v", err)
	}
	if gotCT != "application/json" {
		t.Fatalf("content-type = %q", gotCT)
	}

	var got webhookPayload
	if err := json.Unmarshal(gotBody, &got); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, gotBody)
	}
	want := webhookPayload{
		Rule: "disk-full", Status: "firing", Severity: "warning", Scope: "node:n1",
		Summary: "disk_percent is 95.0 (> 90.0) for node:n1",
		Metric:  "disk_percent", Value: 95, Threshold: 90, Op: ">",
		At: "2023-11-14T22:13:20Z",
	}
	if got != want {
		t.Fatalf("payload mismatch:\n got %+v\nwant %+v", got, want)
	}
	if gotSig == "" || gotSig[:7] != "sha256=" {
		t.Fatalf("missing/invalid HMAC signature: %q", gotSig)
	}
	// A different secret must not verify.
	if wrong := signPayload([]byte("nope"), gotBody); wrong == gotSig {
		t.Fatal("signature did not depend on the secret")
	}
	if right := signPayload([]byte("shh"), gotBody); right != gotSig {
		t.Fatalf("signature mismatch: got %q want %q", gotSig, right)
	}
}

// TestWebhookUnsignedOmitsSignature: no secret → no signature header.
func TestWebhookUnsignedOmitsSignature(t *testing.T) {
	var hasSig bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasSig = r.Header[signatureHeader]
	}))
	defer srv.Close()
	if err := NewWebhook(srv.URL, nil, srv.Client()).Send(context.Background(), Notification{Rule: "x", At: time.Unix(0, 0)}); err != nil {
		t.Fatal(err)
	}
	if hasSig {
		t.Fatal("unsigned webhook should not send a signature header")
	}
}

// TestSlackPayload: the Slack notifier posts a {"text": ...} body.
func TestSlackPayload(t *testing.T) {
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
	}))
	defer srv.Close()
	n := Notification{Rule: "node-down", Firing: true, Severity: "error", Scope: "node:n1", Summary: "node is DOWN"}
	if err := NewSlack(srv.URL, srv.Client()).Send(context.Background(), n); err != nil {
		t.Fatalf("send: %v", err)
	}
	text := got["text"]
	if text == "" || !contains(text, "FIRING") || !contains(text, "node-down") || !contains(text, "node:n1") {
		t.Fatalf("unexpected slack text: %q", text)
	}
}

// TestTelegramPayload: the Telegram notifier posts sendMessage with the chat id
// and text, and puts the bot token in the URL path (never the body).
func TestTelegramPayload(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
	}))
	defer srv.Close()

	tg := NewTelegram("123:secret-token", "-1001", srv.Client())
	tg.baseURL = srv.URL // redirect to the test server

	n := Notification{Rule: "node-down", Firing: true, Severity: "error", Scope: "node:n1", Summary: "node is DOWN"}
	if err := tg.Send(context.Background(), n); err != nil {
		t.Fatalf("send: %v", err)
	}
	if gotBody["chat_id"] != "-1001" {
		t.Fatalf("chat_id = %q", gotBody["chat_id"])
	}
	if text := gotBody["text"]; !contains(text, "FIRING") || !contains(text, "node-down") {
		t.Fatalf("unexpected telegram text: %q", text)
	}
	if !contains(gotPath, "sendMessage") || contains(gotBody["text"], "secret-token") {
		t.Fatalf("bot token leaked or wrong endpoint: path=%q", gotPath)
	}
}

// TestTelegramMissingConfig: an incomplete telegram channel errors rather than
// sending.
func TestTelegramMissingConfig(t *testing.T) {
	if err := NewTelegram("", "chat", nil).Send(context.Background(), Notification{}); err == nil {
		t.Fatal("expected an error for a telegram channel without a token")
	}
}

// TestDeliverIsolatesChannelFailure: a failing channel does not stop delivery to
// others, and records a failure event.
func TestDeliverIsolatesChannelFailure(t *testing.T) {
	clk := clock.NewFake()
	store := &fakeStore{
		channels: []*zatterav1.NotificationChannel{
			{Meta: &zatterav1.Meta{Id: "bad"}, Type: "webhook", Name: "bad"},
			{Meta: &zatterav1.Meta{Id: "good"}, Type: "webhook", Name: "good"},
		},
		rules: []*zatterav1.AlertRule{metricRule("r1", "cpu", "cpu_percent", "node:n1", ">", 1, 0, "bad", "good")},
	}
	metrics := fakeMetrics{}
	metrics.set("cpu_percent", "node:n1", 50)

	var failures int
	e := NewEngine(Config{
		Store: store, Metrics: metrics, Opener: plainOpener{}, Clock: clk,
		EmitEvent: func(kind, sev, msg string) {
			if kind == "alert.notify_failed" {
				failures++
			}
		},
	})
	var delivered []string
	e.send = func(_ context.Context, ch *zatterav1.NotificationChannel, _ Notification) error {
		if ch.GetName() == "bad" {
			return io.ErrUnexpectedEOF
		}
		delivered = append(delivered, ch.GetName())
		return nil
	}
	e.Reset()
	e.Tick(context.Background())

	if len(delivered) != 1 || delivered[0] != "good" {
		t.Fatalf("good channel not delivered despite bad one: %v", delivered)
	}
	if failures != 1 {
		t.Fatalf("expected 1 failure event, got %d", failures)
	}
}

func signPayload(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

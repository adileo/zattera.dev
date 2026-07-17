package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// signatureHeader carries the HMAC-SHA256 of the payload when the channel has a
// signing key, so the receiver can authenticate the notification.
const signatureHeader = "X-Zattera-Signature"

// Webhook posts a JSON notification to a URL, optionally HMAC-signing the body.
// The payload never contains secret values.
type Webhook struct {
	url    string
	secret []byte // optional HMAC key
	client *http.Client
}

// NewWebhook builds a webhook notifier. secret may be nil (unsigned).
func NewWebhook(url string, secret []byte, client *http.Client) *Webhook {
	if client == nil {
		client = http.DefaultClient
	}
	return &Webhook{url: url, secret: secret, client: client}
}

// webhookPayload is the stable JSON schema receivers can rely on.
type webhookPayload struct {
	Rule      string  `json:"rule"`
	Status    string  `json:"status"` // "firing" | "resolved"
	Severity  string  `json:"severity"`
	Scope     string  `json:"scope"`
	Summary   string  `json:"summary"`
	Metric    string  `json:"metric,omitempty"`
	Value     float64 `json:"value,omitempty"`
	Threshold float64 `json:"threshold,omitempty"`
	Op        string  `json:"op,omitempty"`
	EventKind string  `json:"event_kind,omitempty"`
	At        string  `json:"at"` // RFC3339
}

func (w *Webhook) Send(ctx context.Context, n Notification) error {
	body, err := json.Marshal(toPayload(n))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(w.secret) > 0 {
		mac := hmac.New(sha256.New, w.secret)
		mac.Write(body)
		req.Header.Set(signatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("notify: webhook returned %s", resp.Status)
	}
	return nil
}

func toPayload(n Notification) webhookPayload {
	status := "resolved"
	if n.Firing {
		status = "firing"
	}
	return webhookPayload{
		Rule: n.Rule, Status: status, Severity: n.Severity, Scope: n.Scope,
		Summary: n.Summary, Metric: n.Metric, Value: n.Value, Threshold: n.Threshold,
		Op: n.Op, EventKind: n.EventKind, At: n.At.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

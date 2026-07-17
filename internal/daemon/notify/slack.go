package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Slack posts a notification to a Slack Incoming Webhook URL as a simple text
// message. The URL itself is the secret and is never echoed into the payload.
type Slack struct {
	url    string
	client *http.Client
}

// NewSlack builds a Slack notifier over an incoming-webhook URL.
func NewSlack(url string, client *http.Client) *Slack {
	if client == nil {
		client = http.DefaultClient
	}
	return &Slack{url: url, client: client}
}

func (s *Slack) Send(ctx context.Context, n Notification) error {
	body, err := json.Marshal(map[string]string{"text": slackText(n)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("notify: slack returned %s", resp.Status)
	}
	return nil
}

// slackText renders a notification as a one-line Slack message with a status
// emoji.
func slackText(n Notification) string {
	icon := ":white_check_mark:"
	status := "RESOLVED"
	if n.Firing {
		status = "FIRING"
		icon = ":rotating_light:"
		if n.Severity == "warning" {
			icon = ":warning:"
		}
	}
	return fmt.Sprintf("%s *[%s]* %s — %s (%s)", icon, status, n.Rule, n.Summary, n.Scope)
}

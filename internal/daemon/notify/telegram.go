package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// telegramAPI is the Bot API base; the bot token is appended per request.
const telegramAPI = "https://api.telegram.org"

// Telegram delivers notifications via the Telegram Bot API sendMessage method.
// The bot token is the secret and is never echoed into the message body.
type Telegram struct {
	token   string
	chatID  string
	baseURL string // overridable for tests; defaults to telegramAPI
	client  *http.Client
}

// NewTelegram builds a Telegram notifier for a bot token + target chat id.
func NewTelegram(token, chatID string, client *http.Client) *Telegram {
	if client == nil {
		client = http.DefaultClient
	}
	return &Telegram{token: token, chatID: chatID, baseURL: telegramAPI, client: client}
}

func (t *Telegram) Send(ctx context.Context, n Notification) error {
	if t.token == "" || t.chatID == "" {
		return fmt.Errorf("notify: telegram channel is missing bot token or chat id")
	}
	body, err := json.Marshal(map[string]string{"chat_id": t.chatID, "text": telegramText(n)})
	if err != nil {
		return err
	}
	endpoint := t.baseURL + "/bot" + url.PathEscape(t.token) + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("notify: telegram returned %s", resp.Status)
	}
	return nil
}

// telegramText renders a notification as a one-line message with a status emoji.
func telegramText(n Notification) string {
	icon := "✅" // ✅
	status := "RESOLVED"
	if n.Firing {
		status = "FIRING"
		icon = "\U0001F6A8" // 🚨
		if n.Severity == "warning" {
			icon = "⚠️" // ⚠️
		}
	}
	return fmt.Sprintf("%s [%s] %s — %s (%s)", icon, status, n.Rule, n.Summary, n.Scope)
}

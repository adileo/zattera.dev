package notify

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
)

// EmailConfig is a resolved (secrets already unsealed) SMTP target.
type EmailConfig struct {
	Host     string
	Port     uint32
	Username string
	Password string
	From     string
	To       string
	StartTLS bool
}

// Email delivers notifications over SMTP. It is intentionally best-effort —
// SMTP is the flakiest channel — and honors the caller's context deadline for
// the initial connection.
type Email struct{ cfg EmailConfig }

// NewEmail builds an SMTP notifier from a resolved config.
func NewEmail(cfg EmailConfig) *Email { return &Email{cfg: cfg} }

func (e *Email) Send(ctx context.Context, n Notification) error {
	cfg := e.cfg
	if cfg.Host == "" || cfg.To == "" || cfg.From == "" {
		return fmt.Errorf("notify: email channel is missing host/from/to")
	}
	port := cfg.Port
	if port == 0 {
		port = 587
	}
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", port))

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	c, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() { _ = c.Close() }()

	if cfg.StartTLS {
		if err := c.StartTLS(&tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if cfg.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)); err != nil {
			return err
		}
	}
	if err := c.Mail(cfg.From); err != nil {
		return err
	}
	if err := c.Rcpt(cfg.To); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(emailMessage(cfg.From, cfg.To, n)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

// emailMessage renders an RFC-5322 message for a notification.
func emailMessage(from, to string, n Notification) []byte {
	status := "RESOLVED"
	if n.Firing {
		status = "FIRING"
	}
	subject := fmt.Sprintf("[Zattera %s] %s", status, n.Rule)
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n")
	fmt.Fprintf(&b, "%s\n\nRule:     %s\nScope:    %s\nSeverity: %s\nWhen:     %s\n",
		n.Summary, n.Rule, n.Scope, n.Severity, n.At.UTC().Format("2006-01-02 15:04:05 UTC"))
	if n.Metric != "" {
		fmt.Fprintf(&b, "Metric:   %s %s %.2f (observed %.2f)\n", n.Metric, n.Op, n.Threshold, n.Value)
	}
	if n.EventKind != "" {
		fmt.Fprintf(&b, "Event:    %s\n", n.EventKind)
	}
	return []byte(b.String())
}

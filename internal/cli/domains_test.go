package cli

import (
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func dom(id, host, prefix string) *zatterav1.Domain {
	return &zatterav1.Domain{Meta: &zatterav1.Meta{Id: id}, Hostname: host, PathPrefix: prefix}
}

// TestMatchDomainRoute: with several routes on one hostname, `rm <host>` must
// not silently delete whichever came first (T-104).
func TestMatchDomainRoute(t *testing.T) {
	single := []*zatterav1.Domain{dom("d1", "solo.example.com", "")}
	multi := []*zatterav1.Domain{
		dom("d1", "shop.example.com", ""),
		dom("d2", "shop.example.com", "/api"),
		dom("d3", "other.example.com", ""),
	}

	t.Run("bare hostname with one route", func(t *testing.T) {
		got, err := matchDomainRoute(single, "solo.example.com")
		if err != nil || got != "d1" {
			t.Fatalf("got %q err=%v, want d1", got, err)
		}
	})

	t.Run("host/prefix selects the exact route", func(t *testing.T) {
		got, err := matchDomainRoute(multi, "shop.example.com/api")
		if err != nil || got != "d2" {
			t.Fatalf("got %q err=%v, want d2", got, err)
		}
	})

	t.Run("bare hostname is the root route, not a guess", func(t *testing.T) {
		// `ls` prints the root route as a bare hostname, so removing by that
		// exact string is unambiguous even when prefixed siblings exist.
		got, err := matchDomainRoute(multi, "shop.example.com")
		if err != nil || got != "d1" {
			t.Fatalf("got %q err=%v, want d1 (the / route)", got, err)
		}
	})

	t.Run("domain id works", func(t *testing.T) {
		if got, err := matchDomainRoute(multi, "d3"); err != nil || got != "d3" {
			t.Fatalf("got %q err=%v, want d3", got, err)
		}
	})

	t.Run("ambiguous when every route is prefixed", func(t *testing.T) {
		// No root route: a bare hostname could mean either, so list them
		// instead of deleting whichever came first.
		prefixed := []*zatterav1.Domain{
			dom("d1", "shop.example.com", "/api"),
			dom("d2", "shop.example.com", "/admin"),
		}
		_, err := matchDomainRoute(prefixed, "shop.example.com")
		if err == nil {
			t.Fatal("an ambiguous hostname must not resolve to an arbitrary route")
		}
		for _, want := range []string{"shop.example.com/admin", "shop.example.com/api", "2 routes"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should mention %q", err, want)
			}
		}
	})

	t.Run("unknown", func(t *testing.T) {
		if _, err := matchDomainRoute(multi, "nope.example.com"); err == nil {
			t.Fatal("want an error for an unknown domain")
		}
	})
}

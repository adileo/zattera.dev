package cli

import (
	"reflect"
	"strings"
	"testing"
)

func TestEnvFileParse(t *testing.T) {
	t.Run("the cases the shell got wrong", func(t *testing.T) {
		// Every line here is one that a shell splitter mangles silently:
		// quotes kept literally, `export ` glued onto the key, comments in
		// values (T-103).
		in := strings.Join([]string{
			`# a comment`,
			``,
			`   # indented comment`,
			`DATABASE_URL=postgres://user:pw@host:5432/db?sslmode=require`,
			`QUOTED="hello world"`,
			`SINGLE='raw $NOT_EXPANDED value'`,
			`export EXPORTED=yes`,
			`  export SPACED=indented`,
			`TRAILING=value # a note`,
			`EMPTY=`,
			`EQUALS=a=b=c`,
			`HASH_IN_URL=https://example.com/x#frag`,
		}, "\n")

		got, err := parseDotenv(strings.NewReader(in), ".env")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		want := map[string]string{
			"DATABASE_URL": "postgres://user:pw@host:5432/db?sslmode=require",
			"QUOTED":       "hello world",             // quotes stripped
			"SINGLE":       "raw $NOT_EXPANDED value", // no interpolation
			"EXPORTED":     "yes",                     // `export ` stripped from the KEY
			"SPACED":       "indented",
			"TRAILING":     "value", // trailing comment removed
			"EMPTY":        "",
			"EQUALS":       "a=b=c", // only the first = splits
			"HASH_IN_URL":  "https://example.com/x#frag",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %#v\nwant %#v", got, want)
		}
	})

	t.Run("escapes and multi-line values", func(t *testing.T) {
		got, err := parseDotenv(strings.NewReader(
			`TLS_KEY="-----BEGIN-----\nline2\n-----END-----"`+"\n"+
				`ESCAPED="say \"hi\""`+"\n"+
				`TABBED="a\tb"`+"\n"+
				`BACKSLASH="C:\\path"`+"\n"), ".env")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if want := "-----BEGIN-----\nline2\n-----END-----"; got["TLS_KEY"] != want {
			t.Errorf("TLS_KEY = %q, want %q", got["TLS_KEY"], want)
		}
		if want := `say "hi"`; got["ESCAPED"] != want {
			t.Errorf("ESCAPED = %q, want %q", got["ESCAPED"], want)
		}
		if want := "a\tb"; got["TABBED"] != want {
			t.Errorf("TABBED = %q, want %q", got["TABBED"], want)
		}
		if want := `C:\path`; got["BACKSLASH"] != want {
			t.Errorf("BACKSLASH = %q, want %q", got["BACKSLASH"], want)
		}
	})

	t.Run("CRLF leaves no carriage return in values", func(t *testing.T) {
		got, err := parseDotenv(strings.NewReader("A=1\r\nB=two\r\n"), ".env")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if got["A"] != "1" || got["B"] != "two" {
			t.Fatalf("CRLF file produced %#v", got)
		}
	})

	t.Run("errors name the file and line", func(t *testing.T) {
		for _, tc := range []struct{ name, in, want string }{
			{"no equals", "A=1\nGARBAGE\n", ".env:2"},
			{"empty key", "=value\n", ".env:1"},
			{"whitespace in key", "BAD KEY=1\n", ".env:1"},
			{"unterminated quote", `A="oops` + "\n", ".env:1"},
		} {
			_, err := parseDotenv(strings.NewReader(tc.in), ".env")
			if err == nil {
				t.Errorf("%s: want an error", tc.name)
				continue
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: error %q should locate %s", tc.name, err, tc.want)
			}
		}
	})

	t.Run("no variable interpolation", func(t *testing.T) {
		got, err := parseDotenv(strings.NewReader(`A=${OTHER}/x`+"\n"), ".env")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if got["A"] != "${OTHER}/x" {
			t.Errorf("interpolation must not happen, got %q", got["A"])
		}
	})

	t.Run("later duplicate wins", func(t *testing.T) {
		got, _ := parseDotenv(strings.NewReader("A=first\nA=second\n"), ".env")
		if got["A"] != "second" {
			t.Errorf("A = %q, want second", got["A"])
		}
	})
}

// TestEnvFileRoundTrip is the property that makes `env pull --reveal > .env`
// followed by `env set --from-file .env` safe: values survive the trip exactly,
// including the ones a shell would destroy.
func TestEnvFileRoundTrip(t *testing.T) {
	original := map[string]string{
		"SIMPLE":    "plain",
		"SPACES":    "hello world",
		"QUOTES":    `say "hi"`,
		"SINGLE":    "it's here",
		"HASH":      "value # not a comment",
		"NEWLINES":  "-----BEGIN-----\nbody\n-----END-----",
		"TABS":      "a\tb",
		"BACKSLASH": `C:\path\to`,
		"EMPTY":     "",
		"EQUALS":    "a=b=c",
		"DOLLAR":    "${NOT_EXPANDED}",
	}

	var rendered strings.Builder
	for _, k := range sortedKeys(original) {
		rendered.WriteString(k + "=" + quoteEnvValue(original[k]) + "\n")
	}

	got, err := parseDotenv(strings.NewReader(rendered.String()), ".env")
	if err != nil {
		t.Fatalf("re-parsing our own output failed: %v\n---\n%s", err, rendered.String())
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatalf("round-trip lost data:\ngot  %#v\nwant %#v\n---\n%s", got, original, rendered.String())
	}
}

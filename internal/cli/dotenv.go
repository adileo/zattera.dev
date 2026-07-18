package cli

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// dotenv parsing for `zt env set --from-file` (T-103).
//
// The shell cannot do this: splitting a .env on whitespace breaks quoted
// values, and splitting on newlines stores them literally — quotes included,
// `export ` glued onto the key. Both fail silently, which for secrets is worse
// than an error. So the file is parsed here, once, with an explicit contract:
//
//   - blank lines and whole-line `#` comments are skipped
//   - an optional leading `export ` is stripped
//   - surrounding single or double quotes are removed
//   - inside DOUBLE quotes, \n \r \t \\ \" are unescaped; single quotes are
//     literal (POSIX-shell semantics, matching common dotenv tooling)
//   - an unquoted value may carry a trailing ` # comment`; a quoted one may not
//     (the `#` is part of the value)
//   - `=` inside a value is preserved (only the first `=` splits)
//   - CRLF endings are handled: no stray \r at the end of every value
//   - `${VAR}` is NOT interpolated — expanding one secret into another
//     silently is a footgun. The text is kept verbatim.
//
// A line that is not blank, not a comment and has no `=` is an error naming
// the file and line number, rather than a silently dropped variable.

// parseDotenv reads KEY=VALUE pairs from r. name is used in error messages
// (a path, or "-" for stdin). Later duplicate keys win, matching how a shell
// would source the file.
func parseDotenv(r io.Reader, name string) (map[string]string, error) {
	out := map[string]string{}
	sc := bufio.NewScanner(r)
	// Values like PEM bodies (escaped into one line) can exceed the default
	// 64KB token limit.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for line := 1; sc.Scan(); line++ {
		raw := strings.TrimRight(sc.Text(), "\r") // tolerate CRLF files
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		trimmed = strings.TrimPrefix(trimmed, "export ")
		trimmed = strings.TrimSpace(trimmed)

		key, rest, ok := strings.Cut(trimmed, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: not a KEY=VALUE line: %q", name, line, raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%s:%d: empty key", name, line)
		}
		if strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("%s:%d: invalid key %q (contains whitespace)", name, line, key)
		}
		value, err := dotenvValue(rest)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", name, line, err)
		}
		out[key] = value
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return out, nil
}

// dotenvValue resolves one right-hand side: quote handling, escapes and
// trailing comments.
func dotenvValue(rest string) (string, error) {
	v := strings.TrimSpace(rest)
	if v == "" {
		return "", nil
	}
	switch v[0] {
	case '"':
		body, err := quotedBody(v, '"')
		if err != nil {
			return "", err
		}
		return unescapeDouble(body), nil
	case '\'':
		body, err := quotedBody(v, '\'')
		if err != nil {
			return "", err
		}
		return body, nil // single quotes are literal
	}
	// Unquoted: a trailing " #" starts a comment. A bare "#" mid-token (as in
	// a URL fragment or a password) is kept, which is why the space matters.
	if i := strings.Index(v, " #"); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v), nil
}

// quotedBody returns the contents of a quoted value, honouring backslash
// escapes so an escaped quote does not terminate it early.
func quotedBody(v string, quote byte) (string, error) {
	var b strings.Builder
	for i := 1; i < len(v); i++ {
		c := v[i]
		if c == '\\' && quote == '"' && i+1 < len(v) {
			b.WriteByte(c)
			i++
			b.WriteByte(v[i])
			continue
		}
		if c == quote {
			return b.String(), nil // trailing text after the closing quote is ignored
		}
		b.WriteByte(c)
	}
	return "", fmt.Errorf("unterminated %c-quoted value", quote)
}

// unescapeDouble expands the escapes recognized inside double quotes. This is
// what lets a single line carry a multi-line value (PEM keys), which no
// shell-splitting approach can express.
func unescapeDouble(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case '\\':
			b.WriteByte('\\')
		case '"':
			b.WriteByte('"')
		default:
			// Unknown escape: keep both bytes rather than eat the backslash.
			b.WriteByte('\\')
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// quoteEnvValue renders a value so `zt env pull --reveal` output can be fed
// back through `--from-file` unchanged. Values needing no quoting are printed
// bare, so simple output stays readable.
func quoteEnvValue(v string) string {
	if v == "" {
		return ""
	}
	if !strings.ContainsAny(v, " \t\n\r\"'#\\") {
		return v
	}
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(v[i])
		}
	}
	b.WriteByte('"')
	return b.String()
}

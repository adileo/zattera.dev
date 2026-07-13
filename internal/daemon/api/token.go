package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// TokenPrefix marks a Zattera personal/session token string.
const TokenPrefix = "zpat_"

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// MintToken returns a fresh token string (zpat_<base62>) and the hex SHA-256 of
// the whole string, which is what gets persisted in Token.SecretHash. The
// plaintext is shown to the user exactly once and never stored.
func MintToken() (token string, secretHash string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("api: token entropy: %w", err)
	}
	token = TokenPrefix + base62Encode(raw)
	return token, HashToken(token), nil
}

// HashToken returns the hex SHA-256 of a full token string. The whole string
// (prefix included) is hashed, so callers must not strip the prefix first.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// LooksLikeToken reports whether s carries the token prefix.
func LooksLikeToken(s string) bool { return strings.HasPrefix(s, TokenPrefix) }

func base62Encode(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(int64(len(base62Alphabet)))
	if n.Sign() == 0 {
		return "0"
	}
	var out []byte
	mod := new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, base62Alphabet[mod.Int64()])
	}
	// Reverse (least-significant digit was appended first).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

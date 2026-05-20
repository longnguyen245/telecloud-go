package utils

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// SecretKey holds the HMAC key used to sign direct-download tokens.
// It is derived from the master key (TELECLOUD_MASTER_KEY) via HKDF so it is
// independent of the admin password — rotating the password does NOT
// invalidate previously issued direct-download links.
var SecretKey []byte

// GenerateRandomString returns a cryptographically random alphanumeric string
// of length n. Uses rejection sampling so the distribution across the 62-char
// alphabet is uniform (no modulo bias).
func GenerateRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const alphabetLen = byte(len(letters))
	// Largest multiple of alphabetLen that fits in a byte. Bytes >= this are
	// rejected to keep the distribution uniform.
	maxAcceptable := byte(256 - (256 % int(alphabetLen)))

	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			return ""
		}
		for _, b := range buf {
			if b >= maxAcceptable {
				continue
			}
			out = append(out, letters[b%alphabetLen])
			if len(out) == n {
				break
			}
		}
	}
	return string(out)
}

// InitCrypto derives the HMAC key for direct-download tokens from the master
// key. Must be called after the master key has been loaded (LoadMasterKey).
func InitCrypto() error {
	k, err := DeriveSubKey("telecloud/direct-token/v1")
	if err != nil {
		return fmt.Errorf("derive direct-token HMAC key: %w", err)
	}
	SecretKey = k
	return nil
}

func GenerateDirectToken(shareToken string) string {
	h := hmac.New(sha256.New, SecretKey)
	h.Write([]byte(shareToken))
	sig := hex.EncodeToString(h.Sum(nil))
	if len(sig) > 32 {
		sig = sig[:32]
	}
	return shareToken + "_" + sig
}

func VerifyDirectToken(directToken string) *string {
	parts := strings.SplitN(directToken, "_", 2)
	if len(parts) != 2 {
		return nil
	}
	shareToken := parts[0]
	signature := parts[1]

	expected := GenerateDirectToken(shareToken)
	expectedSig := strings.SplitN(expected, "_", 2)[1]

	if hmac.Equal([]byte(signature), []byte(expectedSig)) {
		return &shareToken
	}
	return nil
}

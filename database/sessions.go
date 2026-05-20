package database

import (
	"time"

	"github.com/google/uuid"
)

// SessionTTL is how long a freshly-issued web session is valid for.
// Clients also receive a 30-day cookie; the DB expiry is the authoritative
// limit so revoking the row instantly invalidates the session.
const SessionTTL = 30 * 24 * time.Hour

// CreateSession inserts a fresh row in the sessions table and returns the
// opaque token to put in the cookie.
func CreateSession(username string) (string, error) {
	token := uuid.New().String()
	expiresAt := time.Now().Add(SessionTTL)
	_, err := DB.Exec(
		"INSERT INTO sessions (token, username, expires_at) VALUES (?, ?, ?)",
		token, username, expiresAt,
	)
	if err != nil {
		return "", err
	}
	return token, nil
}

// LookupSessionUser returns the username bound to a session token, or "" if
// the token is unknown or expired. Replaces the bare
// `SELECT username FROM sessions WHERE token = ?` pattern so every callsite
// honors the expiry column without each one having to remember.
func LookupSessionUser(token string) string {
	if token == "" {
		return ""
	}
	var row struct {
		Username  string     `db:"username"`
		ExpiresAt *time.Time `db:"expires_at"`
	}
	err := RODB.Get(&row, "SELECT username, expires_at FROM sessions WHERE token = ?", token)
	if err != nil {
		return ""
	}
	if row.ExpiresAt != nil && time.Now().After(*row.ExpiresAt) {
		DB.Exec("DELETE FROM sessions WHERE token = ?", token)
		return ""
	}
	return row.Username
}

// DeleteOtherSessions wipes every session row for `username` except `keepToken`.
// Used after a password change so old devices are forced to re-authenticate.
func DeleteOtherSessions(username, keepToken string) error {
	_, err := DB.Exec(
		"DELETE FROM sessions WHERE username = ? AND token != ?",
		username, keepToken,
	)
	return err
}

// CleanupExpiredSessions deletes rows whose expires_at is in the past. Also
// trims share_sessions for tidy housekeeping. Safe to call on a schedule.
func CleanupExpiredSessions() (sessions int64, shares int64) {
	now := time.Now()
	if res, err := DB.Exec("DELETE FROM sessions WHERE expires_at IS NOT NULL AND expires_at < ?", now); err == nil {
		sessions, _ = res.RowsAffected()
	}
	if res, err := DB.Exec("DELETE FROM share_sessions WHERE expires_at < ?", now); err == nil {
		shares, _ = res.RowsAffected()
	}
	return
}

package database

// sensitiveKeys lists settings whose values must be encrypted at rest
// using the master key. GetSetting and SetSetting auto-encrypt/decrypt
// these transparently, so callers do not need to change.
//
// Adding a key here will cause new writes to be encrypted. Existing rows
// are migrated on boot by MigrateEncryptV1.
var sensitiveKeys = map[string]struct{}{
	"api_id":       {},
	"api_hash":     {},
	"log_group_id": {},
	"bot_tokens":   {},
}

// IsSensitiveSetting reports whether a settings key is auto-encrypted.
func IsSensitiveSetting(key string) bool {
	_, ok := sensitiveKeys[key]
	return ok
}

// SensitiveSettingKeys returns the keys that should be encrypted at rest.
// Used by the migration to find rows that need re-encryption.
func SensitiveSettingKeys() []string {
	out := make([]string, 0, len(sensitiveKeys))
	for k := range sensitiveKeys {
		out = append(out, k)
	}
	return out
}

package durablestream

import (
	"strings"
	"time"
)

// Matches checks if two StreamConfigs are equivalent for idempotent create.
// Content-Type is compared case-insensitively (per RFC 2045).
// ExpiresAt is only compared when TTL is zero (i.e., explicit Stream-Expires-At header).
func (c StreamConfig) Matches(other StreamConfig) bool {
	// Content-Type media types are case-insensitive per RFC 2045
	if !contentTypesMatch(c.ContentType, other.ContentType) {
		return false
	}
	if c.TTL != other.TTL {
		return false
	}
	// Only compare ExpiresAt directly when TTL isn't set (i.e., explicit Stream-Expires-At header).
	// When TTL is set, ExpiresAt is derived from TTL at request time and will differ between requests.
	if c.TTL == 0 && other.TTL == 0 && !c.ExpiresAt.Equal(other.ExpiresAt) {
		return false
	}
	if c.IsPrivate != other.IsPrivate {
		return false
	}
	return true
}

// IsExpired checks if the config has expired based on its ExpiresAt field.
// Returns false if ExpiresAt is zero (no expiry).
func (c StreamConfig) IsExpired() bool {
	return !c.ExpiresAt.IsZero() && time.Now().After(c.ExpiresAt)
}

// SlideExpiry restarts the sliding TTL window as of now and reports whether
// ExpiresAt moved. Storage implementations call it from [Storage.Touch]; the
// bool lets a backend skip a write only when the exact requested deadline is
// already stored.
//
// A config with a zero TTL has no sliding window — it either never expires or
// carries an absolute Stream-Expires-At deadline, and neither may be moved by
// activity — so it is returned unchanged.
//
// The new expiry is exactly now+TTL, as required by the protocol's definition
// of resetting the inactivity countdown to the configured TTL.
func (c StreamConfig) SlideExpiry(now time.Time) (StreamConfig, bool) {
	if c.TTL <= 0 {
		return c, false
	}
	next := now.Add(c.TTL)
	if c.ExpiresAt.Equal(next) {
		return c, false
	}
	c.ExpiresAt = next
	return c, true
}

// contentTypesMatch compares two content types case-insensitively for the base media type.
// Parameters after the semicolon are ignored.
func contentTypesMatch(a, b string) bool {
	partsA := strings.Split(a, ";")
	partsB := strings.Split(b, ";")
	mediaTypeA := strings.TrimSpace(partsA[0])
	mediaTypeB := strings.TrimSpace(partsB[0])
	return strings.EqualFold(mediaTypeA, mediaTypeB)
}

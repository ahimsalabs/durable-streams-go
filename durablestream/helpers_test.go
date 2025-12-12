package durablestream

import "time"

// Test helper functions shared across test files

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

func timePtr(t time.Time) *time.Time {
	return &t
}

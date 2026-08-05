package durablestream

import (
	"testing"
	"time"
)

func TestStreamConfigSlideExpiryUsesExactTTL(t *testing.T) {
	now := time.Date(2026, time.August, 4, 12, 0, 0, 123, time.UTC)
	cfg := StreamConfig{
		TTL:       time.Hour,
		ExpiresAt: now.Add(2 * time.Hour),
	}

	got, moved := cfg.SlideExpiry(now)
	if !moved {
		t.Fatal("SlideExpiry reported moved=false for a different deadline")
	}
	want := now.Add(time.Hour)
	if !got.ExpiresAt.Equal(want) {
		t.Fatalf("SlideExpiry deadline = %v, want exact now+TTL %v", got.ExpiresAt, want)
	}

	again, moved := got.SlideExpiry(now)
	if moved {
		t.Fatal("SlideExpiry reported moved=true when the exact deadline was already stored")
	}
	if !again.ExpiresAt.Equal(want) {
		t.Fatalf("unchanged SlideExpiry deadline = %v, want %v", again.ExpiresAt, want)
	}
}

func TestStreamConfigSlideExpiryLeavesAbsoluteDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	want := now.Add(time.Hour)
	cfg := StreamConfig{ExpiresAt: want}

	got, moved := cfg.SlideExpiry(now.Add(30 * time.Minute))
	if moved {
		t.Fatal("SlideExpiry moved an absolute deadline with zero TTL")
	}
	if !got.ExpiresAt.Equal(want) {
		t.Fatalf("absolute deadline = %v after SlideExpiry, want %v", got.ExpiresAt, want)
	}
}

func TestStreamConfigMatchesIncludesClosedState(t *testing.T) {
	open := StreamConfig{ContentType: "text/plain"}
	closed := StreamConfig{ContentType: "text/plain", Closed: true}

	if open.Matches(closed) || closed.Matches(open) {
		t.Fatal("open and closed stream configurations matched")
	}
	if !closed.Matches(closed) {
		t.Fatal("identical closed stream configurations did not match")
	}
}

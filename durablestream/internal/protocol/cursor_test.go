package protocol

import (
	"strconv"
	"testing"
	"time"
)

func TestGenerateCursorAt(t *testing.T) {
	// Use a fixed time for deterministic testing
	// October 9, 2024 00:00:40 UTC = 2 intervals (40 seconds / 20 seconds per interval)
	testTime := cursorEpoch.Add(40 * time.Second)

	t.Run("generates cursor without client cursor", func(t *testing.T) {
		cursor := GenerateCursorAt(testTime, "")
		if cursor != "2" {
			t.Errorf("expected cursor '2', got '%s'", cursor)
		}
	})

	t.Run("generates cursor at epoch", func(t *testing.T) {
		cursor := GenerateCursorAt(cursorEpoch, "")
		if cursor != "0" {
			t.Errorf("expected cursor '0', got '%s'", cursor)
		}
	})

	t.Run("generates cursor for large time offset", func(t *testing.T) {
		// 1 hour = 3600 seconds = 180 intervals
		farFuture := cursorEpoch.Add(1 * time.Hour)
		cursor := GenerateCursorAt(farFuture, "")
		expected := "180" // 3600 / 20
		if cursor != expected {
			t.Errorf("expected cursor '%s', got '%s'", expected, cursor)
		}
	})

	t.Run("returns client cursor as-is when less than current", func(t *testing.T) {
		// Client cursor "0" < current interval "2" - no jitter needed
		cursor := GenerateCursorAt(testTime, "0")
		if cursor != "2" {
			t.Errorf("expected cursor '2', got '%s'", cursor)
		}
	})

	t.Run("adds jitter when client cursor equals current interval", func(t *testing.T) {
		cursor := GenerateCursorAt(testTime, "2")
		cursorInt, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			t.Fatalf("cursor should be numeric: %v", err)
		}
		// With jitter, cursor must be > 2
		if cursorInt <= 2 {
			t.Errorf("expected cursor > 2 when client cursor equals current, got %d", cursorInt)
		}
	})

	t.Run("adds jitter when client cursor exceeds current interval", func(t *testing.T) {
		// Client cursor "100" > current interval "2" - must add jitter
		cursor := GenerateCursorAt(testTime, "100")
		cursorInt, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			t.Fatalf("cursor should be numeric: %v", err)
		}
		// With jitter, cursor must be > 100
		if cursorInt <= 100 {
			t.Errorf("expected cursor > 100 when client cursor exceeds current, got %d", cursorInt)
		}
	})

	t.Run("handles invalid client cursor gracefully", func(t *testing.T) {
		// Invalid cursor should be ignored, returning current interval
		cursor := GenerateCursorAt(testTime, "not-a-number")
		if cursor != "2" {
			t.Errorf("expected cursor '2' for invalid client cursor, got '%s'", cursor)
		}
	})

	t.Run("handles negative client cursor", func(t *testing.T) {
		// Negative cursor is less than current, so no jitter
		cursor := GenerateCursorAt(testTime, "-5")
		if cursor != "2" {
			t.Errorf("expected cursor '2' for negative client cursor, got '%s'", cursor)
		}
	})
}

func TestGenerateCursor(t *testing.T) {
	// Test that GenerateCursor returns a valid numeric string
	cursor := GenerateCursor("")
	_, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		t.Errorf("GenerateCursor should return numeric string, got '%s': %v", cursor, err)
	}
}

func TestCursorJitterRange(t *testing.T) {
	// Verify that jitter produces values within expected range
	// Run multiple times to check jitter distribution
	testTime := cursorEpoch.Add(40 * time.Second) // interval 2

	minJitter := int64(cursorMinJitter) * int64(time.Second) / int64(cursorInterval)
	maxJitter := int64(cursorMaxJitter) * int64(time.Second) / int64(cursorInterval)

	for i := 0; i < 100; i++ {
		cursor := GenerateCursorAt(testTime, "2")
		cursorInt, _ := strconv.ParseInt(cursor, 10, 64)

		// Cursor should be clientCursor + jitter where jitter >= minJitter
		if cursorInt < 2+minJitter {
			t.Errorf("cursor %d is below minimum expected %d", cursorInt, 2+minJitter)
		}
		if cursorInt > 2+maxJitter {
			t.Errorf("cursor %d exceeds maximum expected %d", cursorInt, 2+maxJitter)
		}
	}
}

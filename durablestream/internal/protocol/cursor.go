// Package protocol contains internal HTTP protocol constants and utilities.
package protocol

import (
	"math/rand"
	"strconv"
	"time"
)

// Cursor generation constants per protocol spec Section 8.1.
const (
	// cursorInterval is the duration of each cursor interval (20 seconds).
	cursorInterval = 20 * time.Second

	// cursorMinJitter is the minimum jitter added when client cursor >= current (1 second).
	cursorMinJitter = 1

	// cursorMaxJitter is the maximum jitter added when client cursor >= current (3600 seconds).
	cursorMaxJitter = 3600
)

// cursorEpoch is October 9, 2024 00:00:00 UTC (protocol-defined epoch for cursor calculation).
var cursorEpoch = time.Date(2024, 10, 9, 0, 0, 0, 0, time.UTC)

// GenerateCursor generates a cursor value for CDN collapsing.
// Per spec Section 8.1:
//   - Cursors are based on 20-second intervals from October 9, 2024 00:00:00 UTC
//   - When clientCursor >= current interval, jitter is added to ensure monotonic progression
//   - Returns the cursor as a decimal string
func GenerateCursor(clientCursor string) string {
	return GenerateCursorAt(time.Now(), clientCursor)
}

// GenerateCursorAt generates a cursor at a specific time (for testing).
func GenerateCursorAt(now time.Time, clientCursor string) string {
	// Calculate current interval number
	elapsed := now.Sub(cursorEpoch)
	currentInterval := int64(elapsed / cursorInterval)

	// If client provided a cursor, check for collision
	if clientCursor != "" {
		clientInterval, err := strconv.ParseInt(clientCursor, 10, 64)
		if err == nil && clientInterval >= currentInterval {
			// Client cursor >= current interval: add jitter to ensure monotonic progression
			// Jitter is 1-3600 seconds worth of intervals
			jitterSeconds := cursorMinJitter + rand.Intn(cursorMaxJitter-cursorMinJitter+1)
			jitterIntervals := int64(jitterSeconds) * int64(time.Second) / int64(cursorInterval)
			currentInterval = clientInterval + jitterIntervals
		}
	}

	return strconv.FormatInt(currentInterval, 10)
}

package seglog

import (
	"fmt"
	"testing"
)

// TestStreamHashGoldenValues pins streamHash to seed-zero XXH64 over the raw
// stream-ID bytes. It enforces invariant I4: the hash is persisted routing
// state, so if this test fails, the hash changed — that is a new on-disk
// format, and formatHashLine in storage.go MUST change with it (never ship a
// silent rehash; existing directories would misroute streams). Dependency
// upgrades must not change these outputs either.
func TestStreamHashGoldenValues(t *testing.T) {
	golden := map[string]uint64{
		"a":                      0xd24ec4f1a98c6e5b,
		"stream-1":               0xf5df26f5951d89f5,
		"s-000042":               0xed7b6851256c7076,
		"source-cross-partition": 0x73a4198977453484,
		"日本語-stream-識別子":         0xa6682f72f4e7edb8,
		"the-quick-brown-fox-jumps-over-the-lazy-dog-stream-identifier": 0xb20948ec56b213bb,
	}
	for id, want := range golden {
		if got := streamHash(id); got != want {
			t.Errorf("streamHash(%q) = %#016x, want %#016x", id, got, want)
		}
	}

	// Pin the two derived routing projections for one representative ID so a
	// change to the projection code (not just the hash) also fails here.
	if got := streamHash("stream-1") % 32; got != 21 {
		t.Errorf("partition projection changed: streamHash(stream-1) %% 32 = %d, want 21", got)
	}
	if got := streamShard("stream-1"); got != "f5" {
		t.Errorf("shard projection changed: streamShard(stream-1) = %q, want \"f5\"", got)
	}
}

// crossPartitionID derives a stream ID that provably routes to a different
// partition than base, so tests exercising cross-partition topology never
// depend on how any particular hash scatters fixed literals.
func crossPartitionID(t *testing.T, base string, partitions int) string {
	t.Helper()
	want := streamHash(base) % uint64(partitions)
	for i := range 1024 {
		id := fmt.Sprintf("%s-peer-%d", base, i)
		if streamHash(id)%uint64(partitions) != want {
			return id
		}
	}
	t.Fatalf("no cross-partition ID found near %q with %d partitions", base, partitions)
	return ""
}

// TestStreamHashDistributionSmoke is a gross-bias check, not a statistical
// test: over generated IDs every one of 32 partitions must be populated and
// none may exceed 3x the mean.
func TestStreamHashDistributionSmoke(t *testing.T) {
	const (
		n          = 8192
		partitions = 32
	)
	var counts [partitions]int
	for i := range n {
		counts[streamHash(fmt.Sprintf("stream-%d", i))%partitions]++
	}
	mean := n / partitions
	for p, c := range counts {
		if c == 0 {
			t.Errorf("partition %d received no streams", p)
		}
		if c > 3*mean {
			t.Errorf("partition %d received %d streams, more than 3x the mean %d", p, c, mean)
		}
	}
}

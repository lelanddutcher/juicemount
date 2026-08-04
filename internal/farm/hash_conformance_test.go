package farm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// keyPatternByte is the deterministic fill used by the shared conformance
// vectors: byte at offset i == (i*37 + 11) mod 256. Chosen to be trivially
// reproducible in any language (the consumer implements this in Swift).
func keyPatternByte(i int64) byte { return byte((i*37 + 11) % 256) }

// TestSampleHashConformanceVectors pins the DERIVATIVE KEY recipe.
//
// These exact values are published in spec/DERIVATIVE_KEY.md and
// fixtures/derivative-key/vectors.json as the cross-language conformance set
// (KEY-FORMAT, LOOP.md). The consumer implements xxh3 in Swift against them, so
// ANY change here is a wire-breaking change to every derivative directory name
// on every volume — if this test fails, do not "update the expected values".
//
// Boundary cases worth understanding before touching sampleWindow:
//   - size <= 1 MiB      : head is the whole file, no tail.
//   - 1 MiB < size <= 2 MiB : head is capped at 1 MiB and there is STILL no
//     tail, so the bytes between 1 MiB and the end are NOT hashed. This is the
//     one genuinely surprising property of the recipe and it is deliberate.
//   - size > 2 MiB       : head 1 MiB + tail 1 MiB, non-overlapping.
func TestSampleHashConformanceVectors(t *testing.T) {
	const MiB = 1 << 20
	vectors := []struct {
		size int64
		want string
	}{
		{0, "c77b3abb6f87acd9"},           // empty: hash of the length prefix alone
		{11, "68fd8b5a4aefd666"},          // tiny: whole file is the head
		{1 * MiB, "0acccca480537429"},     // exactly one window
		{1*MiB + 1, "9ab1d1e5f05a2b91"},   // head capped, still no tail
		{1536 * 1024, "4b0648b67fce051a"}, // 1.5 MiB — middle bytes NOT hashed
		{2 * MiB, "556ec8137c8c575e"},     // boundary: NOT > 2 windows, so no tail
		{2*MiB + 1, "1c72362ca4a8a97a"},   // first size where the tail engages
		{3 * MiB, "acd8772d176ec7ee"},     // head + tail, clear gap between
	}
	dir := t.TempDir()
	for _, v := range vectors {
		t.Run(fmt.Sprintf("size_%d", v.size), func(t *testing.T) {
			p := filepath.Join(dir, fmt.Sprintf("f_%d.bin", v.size))
			buf := make([]byte, v.size)
			for i := int64(0); i < v.size; i++ {
				buf[i] = keyPatternByte(i)
			}
			if err := os.WriteFile(p, buf, 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := SampleHash(p, v.size)
			if err != nil {
				t.Fatalf("SampleHash: %v", err)
			}
			if got != v.want {
				t.Errorf("size=%d: got %s, want %s — this is a WIRE BREAK, see the doc comment", v.size, got, v.want)
			}
		})
	}
}

// The recipe must be stable against how the caller obtained the size, and must
// surface a size that disagrees with the file rather than hashing silently.
func TestSampleHashRejectsShortFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "short.bin")
	if err := os.WriteFile(p, make([]byte, 16), 0o644); err != nil {
		t.Fatal(err)
	}
	// Claim the file is far larger than it is: the head read must fail rather
	// than hash a zero-padded buffer, which would produce a key that a clean
	// re-read could never reproduce.
	if _, err := SampleHash(p, 4<<20); err == nil {
		t.Error("expected an error when the claimed size exceeds the file, got nil")
	}
}

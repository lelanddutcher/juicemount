package nfs

import "testing"

// The Redis proxy accepts nextinode only as a bare INCRBY of exactly 1024
// from a Mac principal (internal/respproxy/guard.go, "nextinode is allocated
// by a bare INCRBY of exactly 1024"). A different reservation size is refused
// for every enrolled Mac, which turned each spooled MKDIR into a 6 s
// synchronous fallback on a far link.
func TestDirInodeBatchMatchesProxyRule(t *testing.T) {
	if dirInodeBatch != 1024 {
		t.Fatalf("dirInodeBatch = %d; the proxy accepts only 1024 for a Mac", dirInodeBatch)
	}
	if dirInodeLow >= dirInodeBatch {
		t.Fatalf("dirInodeLow %d must be below the batch %d", dirInodeLow, dirInodeBatch)
	}
}

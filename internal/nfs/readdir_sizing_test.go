package nfs

import (
	"bytes"
	"strings"
	"testing"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// TestReadDirEntryMaxBytesIsUpperBound is the C11 correctness gate: the accurate
// per-entry size estimate must NEVER be below the actual XDR-encoded size of a
// readDirEntity, or a packed READDIR reply could exceed the client's requested
// Count and overflow its buffer (a walk-correctness bug). Encode a real entity
// and assert estimate >= actual for a spread of name lengths incl. unicode and
// the 255-byte max.
func TestReadDirEntryMaxBytesIsUpperBound(t *testing.T) {
	t.Setenv("JM_READDIR_ACCURATE_SIZING", "1")
	names := []string{
		"", "a", "ab", "abc", "abcd", "abcde",
		"IMG_1234.CR2", ".DS_Store", "._sidecar.mov",
		"café.mov", "🎬 clip 01.mov", "  leading spaces  ",
		strings.Repeat("x", 255), strings.Repeat("é", 63), // 63*2=126 bytes
	}
	for _, name := range names {
		est := readDirEntryMaxBytes(name)
		var buf bytes.Buffer
		e := readDirEntity{FileID: 0xDEADBEEF, Name: []byte(name), Cookie: 42, Next: true}
		if err := xdr.Write(&buf, e); err != nil {
			t.Fatalf("xdr encode %q: %v", name, err)
		}
		if uint32(buf.Len()) > est {
			t.Errorf("name %q: actual XDR %d bytes > estimate %d — UNDER-count, client buffer overflow risk",
				name, buf.Len(), est)
		}
	}
}

// TestReadDirEntryMaxBytesDefaultUnchanged: with the flag OFF (default), the
// estimate is the historical flat 512, so READDIR paging behaviour is
// byte-for-byte identical to before C11.
func TestReadDirEntryMaxBytesDefaultUnchanged(t *testing.T) {
	// flag unset by default in the test env
	if got := readDirEntryMaxBytes("anything.mov"); got != 512 {
		t.Fatalf("default (accurate sizing OFF) = %d, want the historical 512", got)
	}
	if got := readDirEntryMaxBytes(strings.Repeat("x", 255)); got != 512 {
		t.Fatalf("default OFF for a long name = %d, want 512", got)
	}
}

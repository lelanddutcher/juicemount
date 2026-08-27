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

func TestReadDirEntryAccurateSizingDefaultsOnWithRollbackSwitch(t *testing.T) {
	t.Setenv("JM_READDIR_ACCURATE_SIZING", "")
	if got := readDirEntryMaxBytes("anything.mov"); got >= 512 {
		t.Fatalf("default accurate sizing = %d, want a packed short-name estimate below the legacy 512", got)
	}
	t.Setenv("JM_READDIR_ACCURATE_SIZING", "0")
	if got := readDirEntryMaxBytes("anything.mov"); got != 512 {
		t.Fatalf("rollback switch = %d, want historical 512", got)
	}
}

func TestReadDirStartIndexJumpsDirectlyFromCookie(t *testing.T) {
	cases := []struct {
		cookie uint64
		length int
		want   int
	}{
		{0, 5000, 0}, // first page includes dot entries, then content[0]
		{1, 5000, 0}, // dotdot continuation starts at content[0]
		{2, 5000, 1}, // content[0] was the last acknowledged entry
		{100, 5000, 99},
		{5001, 5000, 5000},
		{9000, 5000, 5000},
		{2, 0, 0},
	}
	for _, tc := range cases {
		if got := readDirStartIndex(tc.cookie, tc.length); got != tc.want {
			t.Errorf("cookie=%d len=%d start=%d, want %d", tc.cookie, tc.length, got, tc.want)
		}
	}
}

func TestFSInfoDirectoryPreferenceMatchesBulkTransferWindow(t *testing.T) {
	got := defaultFSInfoResponse()
	if got.Dtpref != 1<<20 || got.Rtpref != got.Dtpref || got.Wtpref != got.Dtpref {
		t.Fatalf("FSINFO transfer preferences = read:%d write:%d dir:%d, want one shared 1 MiB window", got.Rtpref, got.Wtpref, got.Dtpref)
	}
}

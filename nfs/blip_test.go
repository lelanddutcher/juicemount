package nfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// TestClassifyBlipError pins #9's scope: ONLY transport-class errnos inside a
// blip window re-map to the retryable sentinel; the torn-read guards, EOF,
// offline errors, and healthy-backend failures pass through untouched.
func TestClassifyBlipError(t *testing.T) {
	inBlip := &JuiceMountHandler{blipHook: func() bool { return true }}
	healthy := &JuiceMountHandler{blipHook: func() bool { return false }}

	pathEIO := &os.PathError{Op: "read", Path: "x", Err: syscall.EIO}

	cases := []struct {
		name   string
		h      *JuiceMountHandler
		err    error
		parked bool
	}{
		{"nil error", inBlip, nil, false},
		{"EIO in blip", inBlip, syscall.EIO, true},
		{"wrapped PathError EIO in blip", inBlip, pathEIO, true},
		{"ETIMEDOUT in blip", inBlip, syscall.ETIMEDOUT, true},
		{"ENOTCONN in blip", inBlip, syscall.ENOTCONN, true},
		{"EIO healthy backend", healthy, syscall.EIO, false},
		{"EOF in blip (never parked)", inBlip, io.EOF, false},
		{"torn-read guard in blip (never parked)", inBlip, io.ErrUnexpectedEOF, false},
		{"ENOENT in blip (not transport)", inBlip, syscall.ENOENT, false},
		{"no redis client wired", &JuiceMountHandler{}, syscall.EIO, false},
	}
	for _, c := range cases {
		got := c.h.classifyBlipError(c.err)
		if c.parked != errors.Is(got, nfslib.ErrBackendBlip) {
			t.Fatalf("%s: parked=%v, want %v (got err: %v)",
				c.name, errors.Is(got, nfslib.ErrBackendBlip), c.parked, got)
		}
		if !c.parked && !errors.Is(got, c.err) && got != nil && c.err != nil {
			t.Fatalf("%s: non-parked error was altered: %v -> %v", c.name, c.err, got)
		}
	}

	// The wrap must keep the original detail in its message for logs.
	wrapped := inBlip.classifyBlipError(pathEIO)
	if wrapped == nil || !errors.Is(wrapped, nfslib.ErrBackendBlip) {
		t.Fatal("wrap lost the sentinel")
	}
	if want := pathEIO.Error(); !containsStr(wrapped.Error(), want) {
		t.Fatalf("wrap dropped original detail: %q missing from %q", want, wrapped.Error())
	}
}

// TestClassifyBlipErrorKillSwitch: JM_BLIP_PARK=0 disables the re-map even
// inside a blip. blipParkEnabled is init-time; toggle the var directly the
// way the process env would have set it.
func TestClassifyBlipErrorKillSwitch(t *testing.T) {
	old := blipParkEnabled
	blipParkEnabled = false
	defer func() { blipParkEnabled = old }()
	h := &JuiceMountHandler{blipHook: func() bool { return true }}
	if got := h.classifyBlipError(syscall.EIO); errors.Is(got, nfslib.ErrBackendBlip) {
		t.Fatal("kill switch did not disable blip parking")
	}
}

// TestBlipSentinelSurvivesWrapChains: the %w chain conn.handle inspects.
func TestBlipSentinelSurvivesWrapChains(t *testing.T) {
	h := &JuiceMountHandler{blipHook: func() bool { return true }}
	inner := h.classifyBlipError(syscall.EIO)
	rewrapped := fmt.Errorf("handler layer: %w", inner)
	if !errors.Is(rewrapped, nfslib.ErrBackendBlip) {
		t.Fatal("sentinel lost through a %w re-wrap — conn.handle would miss it")
	}
}

func containsStr(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && stringsIndex(haystack, needle) >= 0)
}

func stringsIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

package farm

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadSidecarBoundedRetriesTransientTransportFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte(`{"inode":42}`), 0o600); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	raw, err := readSidecarBoundedWith("mount", "manifest.json", func(_, _ string) (*os.File, error) {
		attempts++
		if attempts < sidecarReadAttempts {
			return nil, syscall.ENOSYS
		}
		return os.Open(path)
	})
	if err != nil {
		t.Fatalf("read after transient failures: %v", err)
	}
	if attempts != sidecarReadAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, sidecarReadAttempts)
	}
	if string(raw) != `{"inode":42}` {
		t.Fatalf("raw = %q", raw)
	}
}

func TestReadSidecarBoundedTimesOutBehindWedgedFuse(t *testing.T) {
	gate := make(chan struct{}, 1)
	blocked := make(chan struct{})
	started := make(chan struct{})
	defer close(blocked)

	start := time.Now()
	_, err := readSidecarBoundedWithin("mount", "manifest.json", 75*time.Millisecond, gate, func(_, _ string) (*os.File, error) {
		close(started)
		<-blocked
		return nil, syscall.ENXIO
	})
	if !errors.Is(err, errSidecarReadTimeout) {
		t.Fatalf("error = %v, want bounded timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("wedged FUSE read blocked caller for %v", elapsed)
	}
	select {
	case <-started:
	default:
		t.Fatal("test opener never started")
	}

	// The stuck worker still owns the one-slot gate, so a second caller must
	// time out at admission instead of spawning another stuck syscall.
	_, err = readSidecarBoundedWithin("mount", "second.json", 50*time.Millisecond, gate, func(_, _ string) (*os.File, error) {
		t.Fatal("second opener ran while the first FUSE syscall was stuck")
		return nil, nil
	})
	if !errors.Is(err, errSidecarReadTimeout) {
		t.Fatalf("second error = %v, want bounded admission timeout", err)
	}
}

func TestReadSidecarBoundedDoesNotRetryPermanentRefusal(t *testing.T) {
	attempts := 0
	_, err := readSidecarBoundedWith("mount", "manifest.json", func(_, _ string) (*os.File, error) {
		attempts++
		return nil, os.ErrPermission
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission", err)
	}
	if attempts != 1 {
		t.Fatalf("permanent refusal attempts = %d, want 1", attempts)
	}
}

func TestReadSidecarBoundedStopsAfterTransportRetryBudget(t *testing.T) {
	attempts := 0
	_, err := readSidecarBoundedWith("mount", "manifest.json", func(_, _ string) (*os.File, error) {
		attempts++
		return nil, syscall.EIO
	})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("error = %v, want EIO", err)
	}
	if attempts != sidecarReadAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, sidecarReadAttempts)
	}
}

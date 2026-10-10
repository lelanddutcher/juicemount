package pin

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type reasonProvider struct {
	store  *Store
	root   string
	mode   string
	err    error
	cancel context.CancelFunc
}

type reasonJob struct {
	provider *reasonProvider
	want     ExpectedFile
}

func (p *reasonProvider) Capture(_ context.Context, e Entry) (OwnedAuthorityJob, error) {
	info, err := os.Stat(filepath.Join(p.root, filepath.Base(e.Path)))
	if err != nil {
		return nil, err
	}
	return &reasonJob{provider: p, want: ExpectedFile{
		Inode: info.Sys().(*syscall.Stat_t).Ino, Size: info.Size(), MtimeNS: info.ModTime().UnixNano(),
	}}, nil
}
func (p *reasonProvider) ValidateReady(_ context.Context, e Entry) bool { return len(e.Authority) != 0 }
func (p *reasonProvider) Current(e Entry) bool                          { return len(e.Authority) != 0 }
func (p *reasonProvider) DiscardReady(Entry)                            {}
func (j *reasonJob) Release()                                           {}
func (j *reasonJob) Expected() ExpectedFile                             { return j.want }
func (j *reasonJob) SourceCurrent() bool                                { return j.provider.mode != "source" }
func (j *reasonJob) Complete(_ context.Context, _ [sha256.Size]byte) ([]byte, error) {
	switch j.provider.mode {
	case "cancel":
		j.provider.cancel() // Cancel the actual retained prefetcher lifetime.
		return nil, j.provider.err
	case "complete-error", "deadline":
		return nil, j.provider.err
	case "complete-empty":
		return nil, nil
	default:
		return []byte("unit-owned-witness"), nil
	}
}
func (j *reasonJob) Finalize(_ context.Context, e Entry) (bool, error) {
	switch j.provider.mode {
	case "finalize-error":
		return false, j.provider.err
	case "finalize-refuse":
		return false, nil
	default:
		return j.provider.store.AdmitReady(e)
	}
}

func reasonAttempt(t *testing.T, mode string, stageErr error) (Entry, error) {
	t.Helper()
	s, p, path := pinnedFixture(t, "clip.mov", []byte("actual verified source bytes"))
	p.provider = &reasonProvider{store: s, root: p.fusePath, mode: mode, err: stageErr, cancel: p.cancel}
	initial := pinEntry(t, s, path)
	claimed, ok, err := s.ClaimPrefetch(initial)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	result := p.prefetch(claimed)
	return pinEntry(t, s, path), result
}

func TestPrefetchFailureReasonSurvivesDurablePublication(t *testing.T) {
	completeErr := errors.New("complete sentinel")
	finalizeErr := errors.New("finalize sentinel")
	for _, tc := range []struct {
		name, mode, durableStage string
		stageErr                 error
		wantIs                   error
		wantStatus               Status
	}{
		{"source", "source", "source changed or did not end at expected EOF", nil, ErrAuthorityUnavailable, StatusFailed},
		{"complete-error", "complete-error", "Complete rejected", completeErr, completeErr, StatusFailed},
		{"complete-empty", "complete-empty", "Complete returned empty witness", nil, ErrAuthorityUnavailable, StatusFailed},
		{"finalize-error", "finalize-error", "Finalize rejected", finalizeErr, finalizeErr, StatusFailed},
		{"finalize-refuse", "finalize-refuse", "Finalize refused admission", nil, ErrAuthorityUnavailable, StatusFailed},
		{"cancel", "cancel", "", context.Canceled, context.Canceled, StatusPending},
		{"provider-deadline-live-context", "deadline", "Complete rejected", context.DeadlineExceeded, context.DeadlineExceeded, StatusFailed},
		{"success", "success", "", nil, nil, StatusReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := reasonAttempt(t, tc.mode, tc.stageErr)
			if tc.wantIs == nil {
				if err != nil {
					t.Fatalf("successful prefetch: %v", err)
				}
			} else if !errors.Is(err, tc.wantIs) {
				t.Fatalf("return lost sentinel %v: %v", tc.wantIs, err)
			}
			if entry.Status != tc.wantStatus || entry.BytesCached != entry.Size {
				t.Fatalf("durable result: %+v", entry)
			}
			if tc.durableStage == "" {
				if entry.LastError != "" {
					t.Fatalf("pending/ready retained failure: %+v", entry)
				}
			} else if !strings.Contains(entry.LastError, tc.durableStage) {
				t.Fatalf("durable reason lost after return: %q", entry.LastError)
			}
		})
	}
}

func TestPrefetchFailureCannotChangeSuccessorGeneration(t *testing.T) {
	s, p, path := pinnedFixture(t, "clip.mov", []byte("verified source"))
	p.provider = &reasonProvider{store: s, root: p.fusePath}
	old := pinEntry(t, s, path)
	claimed, ok, err := s.ClaimPrefetch(old)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	var mutationErr error
	p.testBeforeReady = func() {
		mutationErr = s.UnpinPath(path)
		if mutationErr == nil {
			mutationErr = s.Pin(path, old.Size, "/Volumes/zpool")
		}
	}
	if err := p.prefetch(claimed); !errors.Is(err, ErrPinSuperseded) {
		t.Fatalf("stale completion: %v", err)
	}
	if mutationErr != nil {
		t.Fatal(mutationErr)
	}
	successor := pinEntry(t, s, path)
	if successor.Generation == claimed.Generation || successor.Status != StatusPending ||
		successor.BytesCached != 0 || successor.LastError != "" {
		t.Fatalf("stale failure changed successor: %+v", successor)
	}
}

package nfs

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

var errInjectedSync = errors.New("injected sync failure")

type recordingSyncWriter struct {
	bytes.Buffer
	syncOffsets []int
	syncErrAt   int
}

func (w *recordingSyncWriter) Sync() error {
	w.syncOffsets = append(w.syncOffsets, w.Len())
	if w.syncErrAt > 0 && len(w.syncOffsets) == w.syncErrAt {
		return errInjectedSync
	}
	return nil
}

func TestCopyWithDurableCheckpointsBoundsUnsyncedBytes(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 131) // 2,096 bytes
	w := &recordingSyncWriter{}

	got, err := copyWithDurableCheckpoints(w, bytes.NewReader(payload), 512, make([]byte, 777))
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	wantOffsets := []int{512, 1024, 1536, 2048, len(payload)}
	if !equalIntSlices(w.syncOffsets, wantOffsets) {
		t.Fatalf("sync offsets=%v, want %v", w.syncOffsets, wantOffsets)
	}
	if got.Bytes != int64(len(payload)) {
		t.Fatalf("bytes=%d, want %d", got.Bytes, len(payload))
	}
	if got.SyncCalls != len(wantOffsets) {
		t.Fatalf("sync calls=%d, want %d", got.SyncCalls, len(wantOffsets))
	}
	if got.SHA256 != sha256.Sum256(payload) {
		t.Fatal("streaming SHA does not match source")
	}
	if !bytes.Equal(w.Bytes(), payload) {
		t.Fatal("destination bytes do not match source")
	}
}

func TestCopyWithDurableCheckpointsDoesNotRedundantlySyncExactBoundary(t *testing.T) {
	payload := bytes.Repeat([]byte{0xa5}, 2048)
	w := &recordingSyncWriter{}

	got, err := copyWithDurableCheckpoints(w, bytes.NewReader(payload), 512, make([]byte, 1024))
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if want := []int{512, 1024, 1536, 2048}; !equalIntSlices(w.syncOffsets, want) {
		t.Fatalf("sync offsets=%v, want %v", w.syncOffsets, want)
	}
	if got.SyncCalls != 4 {
		t.Fatalf("sync calls=%d, want 4", got.SyncCalls)
	}
}

func TestCopyWithDurableCheckpointsSyncsEmptyFile(t *testing.T) {
	w := &recordingSyncWriter{}
	got, err := copyWithDurableCheckpoints(w, bytes.NewReader(nil), 512, make([]byte, 1024))
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if got.Bytes != 0 || got.SyncCalls != 1 || !equalIntSlices(w.syncOffsets, []int{0}) {
		t.Fatalf("empty copy result=%+v offsets=%v, want one sync at zero", got, w.syncOffsets)
	}
}

func TestCopyWithDurableCheckpointsStopsAtSyncFailure(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, 4096)
	w := &recordingSyncWriter{syncErrAt: 2}

	got, err := copyWithDurableCheckpoints(w, bytes.NewReader(payload), 512, make([]byte, 1024))
	if err == nil || !errors.Is(err, errInjectedSync) {
		t.Fatalf("error=%v, want injected sync failure", err)
	}
	if got.Bytes != 1024 {
		t.Fatalf("bytes accepted=%d, want 1024", got.Bytes)
	}
	if w.Len() != 1024 {
		t.Fatalf("destination length=%d, want 1024 (copy must stop immediately)", w.Len())
	}
	if want := []int{512, 1024}; !equalIntSlices(w.syncOffsets, want) {
		t.Fatalf("sync offsets=%v, want %v", w.syncOffsets, want)
	}
}

func TestDurableSyncIntervalAdaptsToLiveLinkClass(t *testing.T) {
	d := &Drainer{}
	for _, tc := range []struct {
		class netprofile.LinkClass
		want  int64
	}{
		{netprofile.ClassMetered, drainSyncMeteredBytes},
		{netprofile.ClassSlow, drainSyncSlowBytes},
		{netprofile.ClassMedium, drainSyncDefaultBytes},
		{netprofile.ClassFast, drainSyncDefaultBytes},
	} {
		restore := forceNetClassForTest(t, tc.class)
		if got := d.durableSyncInterval(); got != tc.want {
			restore()
			t.Fatalf("class %s interval=%d, want %d", tc.class, got, tc.want)
		}
		restore()
	}

	d.syncIntervalBytes = 512 << 10
	if got := d.durableSyncInterval(); got != 512<<10 {
		t.Fatalf("configured interval=%d, want %d", got, 512<<10)
	}
	d.syncIntervalBytes = 1
	if got := d.durableSyncInterval(); got != drainSyncMinBytes {
		t.Fatalf("too-small configured interval=%d, want clamp %d", got, drainSyncMinBytes)
	}
	d.syncIntervalBytes = 1 << 30
	if got := d.durableSyncInterval(); got != drainSyncDefaultBytes {
		t.Fatalf("too-large configured interval=%d, want clamp %d", got, drainSyncDefaultBytes)
	}
}

func TestDrainFUSESlotPreservesForegroundReservation(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()

	d := &Drainer{stop: make(chan struct{})}
	releaseDrain, ok := d.acquireDrainFUSESlot()
	if !ok {
		t.Fatal("first background drain was not admitted")
	}
	defer releaseDrain()

	if inUse, width := fuseDataGateDepth(); inUse != 1 || width != 2 {
		t.Fatalf("gate depth=%d/%d, want 1/2 with one metered drain", inUse, width)
	}
	if release, admitted := tryAcquireFUSEDataBackground(); admitted {
		release()
		t.Fatal("a second background operation consumed the foreground reservation")
	}
	releaseForeground, admitted := acquireFUSEData()
	if !admitted {
		t.Fatal("foreground operation was refused while only the background half was occupied")
	}
	releaseForeground()
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

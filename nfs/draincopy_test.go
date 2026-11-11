package nfs

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

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
	if got := d.durableSyncInterval(); got != drainSyncMaxBytes {
		t.Fatalf("too-large configured interval=%d, want clamp %d", got, drainSyncMaxBytes)
	}
	// A slow link checkpoints every four blocks, so the app's four
	// parallel uploads stay busy between checkpoints.
	if drainSyncSlowBytes%(4<<20) != 0 || drainSyncSlowBytes < 4*(4<<20) {
		t.Fatalf("slow interval %d is not a multiple of four 4 MiB blocks", drainSyncSlowBytes)
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

// While a media-lane drain waits for the background gate, a small-lane drain
// does not take the slot that frees: the large file gets it.
func TestDrainFUSESlotGivesWaitingMediaDrainTheNextSlot(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassSlow)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}
	defer close(d.stop)

	// bg small drains fill the shared share and one more takes the small
	// lane's reserved slot, so a further small drain has no slot of its own.
	bg := int(backgroundFUSEDataWidth())
	var held []func()
	for i := 0; i < bg+1; i++ {
		release, ok := d.acquireDrainFUSESlotFor(true)
		if !ok {
			t.Fatalf("small drain %d not admitted", i)
		}
		held = append(held, release)
	}
	media := make(chan func())
	go func() {
		release, _ := d.acquireDrainFUSESlotFor(false)
		media <- release
	}()
	deadline := time.Now().Add(5 * time.Second)
	for d.mediaGateWaiters.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("media drain never waited")
		}
		time.Sleep(time.Millisecond)
	}
	small := make(chan func())
	go func() {
		release, _ := d.acquireDrainFUSESlotFor(true)
		small <- release
	}()
	held[0]() // one slot frees
	var mediaRelease func()
	select {
	case mediaRelease = <-media:
	case r := <-small:
		r()
		t.Fatal("a small drain took the slot a waiting media drain needed")
	case <-time.After(5 * time.Second):
		t.Fatal("media drain not admitted after a slot freed")
	}
	select {
	case r := <-small:
		r()
		t.Fatal("small drain admitted with the gate full")
	case <-time.After(50 * time.Millisecond):
	}
	mediaRelease()
	select {
	case r := <-small:
		r()
	case <-time.After(5 * time.Second):
		t.Fatal("small drain not admitted once the media drain had its slot")
	}
	for _, r := range held[1:] {
		r()
	}
}

// M3: small drains yield to a waiting media drain only while media holds
// fewer than half the background slots. Small saves keep moving.
func TestDrainFUSESlotSmallKeepsHalfWhileMediaWaits(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassSlow)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}
	defer close(d.stop)
	bg := int(backgroundFUSEDataWidth())
	if bg < 2 {
		t.Skipf("background width %d", bg)
	}
	var held []func()
	for i := 0; i < bg/2; i++ { // media takes its half
		release, ok := d.acquireDrainFUSESlotFor(false)
		if !ok {
			t.Fatal("media drain not admitted")
		}
		held = append(held, release)
	}
	d.mediaGateWaiters.Add(1) // one more media drain waits
	defer d.mediaGateWaiters.Add(-1)
	got := make(chan func(), 1)
	go func() {
		release, _ := d.acquireDrainFUSESlotFor(true)
		got <- release
	}()
	select {
	case r := <-got:
		r()
	case <-time.After(5 * time.Second):
		t.Fatal("small drain starved while media already held half the slots")
	}
	for _, r := range held {
		r()
	}
	if n := d.mediaGateHeld.Load(); n != 0 {
		t.Fatalf("media slots still counted: %d", n)
	}
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

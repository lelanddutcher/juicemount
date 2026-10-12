package nfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// These local-filesystem controls qualify the diagnostic, not macFUSE causality.
type metadataTraceCapture struct {
	mu   sync.Mutex
	rows []metadataMutationRecord
}

func (c *metadataTraceCapture) emit(r metadataMutationRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, r)
}

func (c *metadataTraceCapture) snapshot() []metadataMutationRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]metadataMutationRecord(nil), c.rows...)
}

func metadataTraceTestOnline(t *testing.T) {
	t.Helper()
	offline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(offline) })
}

func metadataTraceTestInstall(t *testing.T, h *JuiceMountHandler) (*metadataMutationTrace, *metadataTraceCapture) {
	t.Helper()
	trace := newMetadataMutationTrace("Trace", "1")
	if trace == nil {
		t.Fatal("trace admission failed")
	}
	capture := &metadataTraceCapture{}
	trace.emit = capture.emit
	h.metadataTrace = trace // Immutable before any operation using this fixture.
	return trace, capture
}

func metadataTraceTestEntry(t *testing.T, h *JuiceMountHandler, root, name string, directory bool) {
	t.Helper()
	if err := h.store.Insert(metadata.MakeEntry(name, directory, 4, time.Now(), 9876)); err != nil {
		t.Fatal(err)
	}
	full := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if directory {
		if err := os.Mkdir(full, 0700); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(full, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
}

func metadataTraceFind(t *testing.T, rows []metadataMutationRecord, operation, stage string) metadataMutationRecord {
	t.Helper()
	var found []metadataMutationRecord
	for _, r := range rows {
		if r.Operation == operation && r.Stage == stage {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s/%s records=%d", operation, stage, len(found))
	}
	return found[0]
}

func TestMetadataMutationTraceConstructorAdmission(t *testing.T) {
	cases := []struct {
		prefix, isolation string
		enabled           bool
	}{
		{"", "1", false}, {"Trace", "", false}, {"Trace", "true", false},
		{".", "1", false}, {"/Trace", "1", false}, {"Trace/../other", "1", false},
		{"Trace/", "1", false}, {"Trace//child", "1", false},
		{strings.Repeat("x", metadataTracePathLimit+1), "1", false},
		{"Trace", "1", true}, {"Trace/child", "1", true},
	}
	generations := map[string]bool{}
	for _, tc := range cases {
		t.Setenv("JM_DIAGNOSTIC_METADATA_TRACE_PREFIX", tc.prefix)
		t.Setenv("JM_APP_ISOLATION", tc.isolation)
		h := NewHandler(nil, "")
		if (h.metadataTrace != nil) != tc.enabled {
			t.Errorf("prefix=%q isolation=%q enabled=%v", tc.prefix, tc.isolation, h.metadataTrace != nil)
		}
		if h.metadataTrace != nil {
			tr := h.metadataTrace
			if tr.prefix != tc.prefix || tr.next.Load() != 0 || len(tr.generation) != 32 || generations[tr.generation] {
				t.Error("constructor did not establish a fresh, unused generation")
			}
			generations[tr.generation] = true
			// Changing the environment does not retarget an existing handler.
			t.Setenv("JM_DIAGNOSTIC_METADATA_TRACE_PREFIX", "Different")
			if tr.prefix != tc.prefix {
				t.Error("live handler followed an environment change")
			}
		}
		if !h.StopHandler() {
			t.Fatal("constructor fixture did not stop")
		}
	}
}

func TestMetadataMutationTraceDefaultOffAndNonmatchingChmod(t *testing.T) {
	metadataTraceTestOnline(t)
	t.Setenv("JM_DIAGNOSTIC_METADATA_TRACE_PREFIX", "")
	t.Setenv("JM_APP_ISOLATION", "1")
	jfs, _, _, root := newSpoolWiredHandlerNoDrain(t)
	if jfs.handler.metadataTrace != nil {
		t.Fatal("default path allocated a tracer")
	}
	const name = "TraceSibling/file"
	metadataTraceTestEntry(t, jfs.handler, root, name, false)
	if err := (&juiceChange{handler: jfs.handler}).Chmod(name, 0600); err != nil {
		t.Fatal(err)
	}
	tr, capture := metadataTraceTestInstall(t, jfs.handler)
	var observations atomic.Int64
	tr.lstat = func(string, *unix.Stat_t) error { observations.Add(1); return unix.EIO }
	tr.fstat = func(int, *unix.Stat_t) error { observations.Add(1); return unix.EIO }
	if err := (&juiceChange{handler: jfs.handler}).Chmod(name, 0644); err != nil {
		t.Fatal(err)
	}
	if observations.Load() != 0 || tr.next.Load() != 0 || len(capture.snapshot()) != 0 {
		t.Fatal("sibling-prefix name admitted observations, IDs or records")
	}
	if tr.beginChmod("Trace//invalid", 0600, false, false) != nil {
		t.Fatal("noncanonical target admitted")
	}
	if observations.Load() != 0 || tr.next.Load() != 0 {
		t.Fatal("invalid path was observed")
	}
	if rows := capture.snapshot(); len(rows) != 1 || !rows[0].Incomplete {
		t.Fatal("invalid target did not disclose incomplete coverage")
	}
}

func TestMetadataMutationTraceFixedCountAndOutputBounds(t *testing.T) {
	tr := newMetadataMutationTrace("Trace", "1")
	var calls, records, incomplete, starts, terminals int
	tr.lstat = func(_ string, st *unix.Stat_t) error {
		calls++
		st.Dev, st.Ino, st.Size, st.Mode = 7, 8, 4, unix.S_IFREG|0600
		return nil
	}
	tr.emit = func(r metadataMutationRecord) {
		records++
		data, err := json.Marshal(r)
		if err != nil || len(data) > metadataTraceRecordLimit {
			t.Errorf("unbounded record: bytes=%d err=%v", len(data), err)
		}
		if r.Stage == "incomplete" {
			incomplete++
		}
		if r.Stage == "start" {
			starts++
			if r.EventID != uint64(starts) {
				t.Error("IDs not monotonic")
			}
		}
		if r.Stage == "terminal" {
			terminals++
		}
	}
	for n := 0; n < metadataTraceOperationLimit+3; n++ {
		op := tr.beginChmod("Trace/file", 0600, true, false)
		if n >= metadataTraceOperationLimit {
			if op != nil {
				t.Fatal("operation cap admitted more work")
			}
			continue
		}
		if op == nil {
			t.Fatal("operation refused before cap")
		}
		op.before("/unused", nil)
		op.after("/unused", nil, nil)
	}
	if records != metadataTraceOperationLimit*metadataTraceRecordsPerOperation+1 || incomplete != 1 ||
		calls != 4*metadataTraceOperationLimit || starts != metadataTraceOperationLimit || terminals != starts ||
		tr.next.Load() != metadataTraceOperationLimit {
		t.Fatalf("bound mismatch: records=%d incomplete=%d calls=%d starts=%d terminals=%d", records, incomplete, calls, starts, terminals)
	}
	other := newMetadataMutationTrace("Trace", "1")
	capture := &metadataTraceCapture{}
	other.emit = capture.emit
	other.write(metadataMutationRecord{Target: strings.Repeat("x", metadataTraceRecordLimit+1)})
	other.write(metadataMutationRecord{Target: strings.Repeat("x", metadataTraceRecordLimit+1)})
	if rows := capture.snapshot(); len(rows) != 1 || rows[0].Reason != "record_bound" || !rows[0].Incomplete {
		t.Fatal("oversized payload emitted or repeated incomplete signal")
	}
}

func TestMetadataMutationTraceChmodPreservesResultAndObservationalUncertainty(t *testing.T) {
	for _, scenario := range []string{"success", "missing", "before_error", "after_error", "changed_identity", "sibling_symlink"} {
		t.Run(scenario, func(t *testing.T) {
			metadataTraceTestOnline(t)
			jfs, _, _, root := newSpoolWiredHandlerNoDrain(t)
			const name = "Trace/file"
			metadataTraceTestEntry(t, jfs.handler, root, name, false)
			sibling := filepath.Join(root, "Trace/._file")
			if scenario == "sibling_symlink" {
				if err := os.Symlink("file", sibling); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(sibling, []byte("sidecar"), 0600); err != nil {
				t.Fatal(err)
			}
			if scenario == "missing" {
				if err := os.Remove(filepath.Join(root, name)); err != nil {
					t.Fatal(err)
				}
			}
			tr, capture := metadataTraceTestInstall(t, jfs.handler)
			targetCalls := 0
			tr.lstat = func(p string, st *unix.Stat_t) error {
				if p == filepath.Join(root, name) {
					targetCalls++
					if (scenario == "before_error" && targetCalls == 1) || (scenario == "after_error" && targetCalls == 2) {
						return unix.EIO
					}
				}
				err := unix.Lstat(p, st)
				if err == nil && scenario == "changed_identity" && p == filepath.Join(root, name) && targetCalls == 2 {
					st.Ino++
				}
				return err
			}
			// Original Chmod is metadata-authoritative even on raw ENOENT.
			if err := (&juiceChange{handler: jfs.handler}).Chmod(name, 0600); err != nil {
				t.Fatalf("diagnostic changed caller result: %v", err)
			}
			rows := capture.snapshot()
			start := metadataTraceFind(t, rows, "namespace_chmod", "start")
			syscall := metadataTraceFind(t, rows, "namespace_chmod", "syscall")
			terminal := metadataTraceFind(t, rows, "namespace_chmod", "terminal")
			if start.EventID != terminal.EventID || start.Generation != terminal.Generation || start.Target != name ||
				start.Sibling != "Trace/._file" || start.RequestedMode != 0600 || !start.SpoolChecked || start.SpoolPending ||
				syscall.SyscallOK == nil || *syscall.SyscallOK != (scenario != "missing") || !terminal.ObserverComplete || terminal.WorkerJoined {
				t.Fatalf("incorrect context/result/ownership record: start=%+v syscall=%+v terminal=%+v", start, syscall, terminal)
			}
			if scenario == "missing" && (syscall.SyscallError == nil || syscall.SyscallError.Errno == nil || *syscall.SyscallError.Errno != int(unix.ENOENT)) {
				t.Fatal("raw syscall errno lost")
			}
			if terminal.Incomplete != (scenario != "success") {
				t.Fatalf("observational uncertainty=%v scenario=%s", terminal.Incomplete, scenario)
			}
			for _, r := range rows {
				if r.EndNS < r.StartNS || r.StartNS < r.OperationStartNS {
					t.Error("nonmonotonic interval")
				}
				if r.Observation != nil && !r.Observation.OK && r.Observation.Stat != nil {
					t.Error("fabricated zero stat on observer error")
				}
			}
		})
	}
}

func TestMetadataMutationTracePrefixRootDoesNotObserveOutsideSibling(t *testing.T) {
	tr := newMetadataMutationTrace("Trace", "1")
	capture := &metadataTraceCapture{}
	tr.emit = capture.emit
	var paths []string
	tr.lstat = func(p string, st *unix.Stat_t) error {
		paths = append(paths, p)
		st.Dev, st.Ino, st.Mode = 1, 2, unix.S_IFDIR|0700
		return nil
	}
	op := tr.beginChmod("Trace", 0700, false, false)
	if op == nil {
		t.Fatal("exact prefix equality refused")
	}
	op.before("/fixture", nil)
	op.after("/fixture", nil, nil)
	if len(paths) != 2 || paths[0] != "/fixture/Trace" || paths[1] != paths[0] {
		t.Fatalf("observed outside prefix: %v", paths)
	}
	terminal := metadataTraceFind(t, capture.snapshot(), "namespace_chmod", "terminal")
	if !terminal.Incomplete || terminal.SiblingIdentity != "unavailable_inconclusive" {
		t.Fatal("excluded sibling implied coverage")
	}
}

func TestMetadataMutationTraceCompletionUsesRetainedFDAndExactReceipt(t *testing.T) {
	jfs, spool, drainer, root := newSpoolWiredHandlerNoDrain(t)
	const name = "Trace/._clip"
	metadataTraceTestEntry(t, jfs.handler, root, "Trace", true)
	tr, capture := metadataTraceTestInstall(t, jfs.handler)
	var expectedFD int
	var expectedStat unix.Stat_t
	var gotReceipt metadata.DrainCommitResult
	var gotItem metadata.DrainCommitItem
	fstatCalls, lstatCalls := 0, 0
	tr.fstat = func(fd int, st *unix.Stat_t) error {
		fstatCalls++
		if fd != expectedFD {
			return unix.EBADF
		}
		return unix.Fstat(fd, st)
	}
	tr.lstat = func(string, *unix.Stat_t) error { lstatCalls++; return unix.EIO }
	retained := filepath.Join(root, "Trace/retained-original")
	destination := filepath.Join(root, name)
	replacement := []byte("unrelated replacement")
	effects := drainer.onDrainEffects
	drainer.SetOnDrainEffects(func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
		gotItem, gotReceipt = item, receipt
		expectedFD = int(fd.Fd())
		if err := unix.Fstat(expectedFD, &expectedStat); err != nil {
			return err
		}
		// Changing the pathname cannot retarget the retained completion FD.
		if err := os.Rename(destination, retained); err != nil {
			return err
		}
		if err := os.WriteFile(destination, replacement, 0600); err != nil {
			return err
		}
		return effects(item, receipt, fd, body)
	})
	simulateNFSCreateThenWrite(t, jfs, name, []byte("original body"), 1024)
	spool.sweepOnce(0)
	drainAllForTest(t, drainer)
	if drainer.Metrics().DrainsSucceeded.Load() != 1 || fstatCalls != 2 || lstatCalls != 0 {
		t.Fatalf("completion did not use only retained FD: successes=%d fstat=%d lstat=%d", drainer.Metrics().DrainsSucceeded.Load(), fstatCalls, lstatCalls)
	}
	rows := capture.snapshot()
	before := metadataTraceFind(t, rows, "completion_fd_chmod", "before_target")
	after := metadataTraceFind(t, rows, "completion_fd_chmod", "after_target")
	terminal := metadataTraceFind(t, rows, "completion_fd_chmod", "terminal")
	for _, r := range []metadataMutationRecord{before, after} {
		if r.SpoolID != gotItem.SpoolID || r.ReceiptSpoolID != gotReceipt.SpoolID || r.CompletionID != gotReceipt.CompletionID ||
			r.PublishedIncarnation != gotReceipt.Published.Incarnation || r.PublishedRevision != gotReceipt.Published.Revision ||
			r.ReceiptBackingIdentity != gotReceipt.BackingIdentity || r.ItemBackingIdentity != gotItem.BackingIdentity ||
			r.Observation == nil || !r.Observation.OK || r.Observation.Stat == nil ||
			r.Observation.Stat.Dev != uint64(expectedStat.Dev) || r.Observation.Stat.Ino != uint64(expectedStat.Ino) {
			t.Fatalf("completion binding lost: %+v", r)
		}
	}
	if terminal.Incomplete || !terminal.ObserverComplete || terminal.WorkerJoined {
		t.Fatalf("bad terminal: %+v", terminal)
	}
	if body, err := os.ReadFile(destination); err != nil || !bytes.Equal(body, replacement) {
		t.Fatalf("trace retargeted replacement: %q %v", body, err)
	}
	if st, err := os.Stat(destination); err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("replacement mode changed: %v %v", st, err)
	}
	if st, err := os.Stat(retained); err != nil || st.Mode().Perm() != gotReceipt.Mode.Perm() {
		t.Fatalf("retained mode missing: %v %v", st, err)
	}
}

func TestMetadataMutationTraceTimeoutRetainsNamespaceAndGate(t *testing.T) {
	metadataTraceTestOnline(t)
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "40")
	jfs, _, _, root := newSpoolWiredHandlerNoDrain(t)
	const name = "Trace/directory"
	metadataTraceTestEntry(t, jfs.handler, root, name, true)
	if err := os.WriteFile(filepath.Join(root, "Trace/._directory"), []byte("sidecar"), 0600); err != nil {
		t.Fatal(err)
	}
	tr, capture := metadataTraceTestInstall(t, jfs.handler)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int64
	tr.lstat = func(p string, st *unix.Stat_t) error {
		if p == filepath.Join(root, name) && calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return unix.Lstat(p, st)
	}
	result := make(chan error, 1)
	go func() { result <- (&juiceChange{handler: jfs.handler}).Chmod(name, 0750) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("observer did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("caller result=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("caller failed to time out")
	}
	if rows := capture.snapshot(); len(rows) != 1 || rows[0].Stage != "start" {
		t.Fatalf("blocked observer fabricated completion: %+v", rows)
	}
	if len(nfsLstatGate) == 0 {
		t.Error("observer released existing syscall gate")
	}
	if op := jfs.handler.namespace.begin([]string{name}); op != nil {
		op.finish(nil)
		t.Fatal("timed-out observer released namespace")
	}
	// Sibling observation adds no namespace scope or mutation serialization.
	for _, other := range []string{"Trace/._directory", "unrelated"} {
		op := jfs.handler.namespace.begin([]string{other})
		if op == nil {
			t.Fatalf("diagnostic added scope/serialization at %s", other)
		}
		op.finish(nil)
	}
	if jfs.handler.namespace.stop(10 * time.Millisecond) {
		t.Fatal("namespace reported joined while observer blocked")
	}
	unblock()
	if !jfs.handler.namespace.stop(5 * time.Second) {
		t.Fatal("original worker did not settle")
	}
	terminal := metadataTraceFind(t, capture.snapshot(), "namespace_chmod", "terminal")
	if !terminal.ObserverComplete || terminal.WorkerJoined || terminal.Incomplete {
		t.Fatalf("bad observer terminal: %+v", terminal)
	}
}

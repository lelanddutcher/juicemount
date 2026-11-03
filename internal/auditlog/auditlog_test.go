package auditlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quiet(string, ...any) {}

func mustOpen(t *testing.T, dir string, o Options) *Writer {
	t.Helper()
	if o.Logf == nil {
		o.Logf = quiet
	}
	w, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func appendN(t *testing.T, w *Writer, n int, person string) []uint64 {
	t.Helper()
	var seqs []uint64
	for i := 0; i < n; i++ {
		seq, err := w.Append(&Record{Type: TypeIntent, Person: person, Principal: "mac-1",
			Projects: []Project{{Ino: 10, Path: "/Clients/A"}},
			Ops:      []Op{{Op: "add", Parent: 10, Name: "f" + string(rune('a'+i%26)), Path: "/Clients/A/f"}}})
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

func mustVerify(t *testing.T, dir string) Report {
	t.Helper()
	rep, err := Verify(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("chain does not verify: %s", rep.Problem)
	}
	return rep
}

func TestChainVerifiesAndDetectsTampering(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{})
	appendN(t, w, 50, "p-john")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	rep := mustVerify(t, dir)
	if rep.Records != 51 { // the open record + 50
		t.Fatalf("records = %d, want 51", rep.Records)
	}
	segs, _ := segments(dir)
	orig, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(orig, []byte("\n")), []byte("\n"))

	for name, tamper := range map[string]func() []byte{
		// A changed field (same length, still valid JSON).
		"edit": func() []byte {
			l := append([][]byte(nil), lines...)
			l[20] = bytes.Replace(l[20], []byte(`"p-john"`), []byte(`"p-jane"`), 1)
			return append(bytes.Join(l, []byte("\n")), '\n')
		},
		// A removed line.
		"delete": func() []byte {
			l := append(append([][]byte(nil), lines[:20]...), lines[21:]...)
			return append(bytes.Join(l, []byte("\n")), '\n')
		},
		// Two lines swapped.
		"reorder": func() []byte {
			l := append([][]byte(nil), lines...)
			l[20], l[21] = l[21], l[20]
			return append(bytes.Join(l, []byte("\n")), '\n')
		},
	} {
		if err := os.WriteFile(segs[0], tamper(), 0o600); err != nil {
			t.Fatal(err)
		}
		rep, err := Verify(dir)
		if err != nil {
			t.Fatal(err)
		}
		if rep.OK || !strings.Contains(rep.Problem, "line 2") {
			t.Fatalf("%s: tampering not detected near line 21: %+v", name, rep)
		}
	}
	if err := os.WriteFile(segs[0], orig, 0o600); err != nil {
		t.Fatal(err)
	}
	mustVerify(t, dir)
}

// A crash in the middle of a write leaves a torn last line: reopening cuts
// it, says so, and continues the chain.
func TestReopenCutsTornTailAndContinuesChain(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{})
	appendN(t, w, 3, "p-1")
	w.Close()
	segs, _ := segments(dir)
	f, err := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"seq":99,"ts_ms":1,"type":"inte`)
	f.Close()
	if rep, _ := Verify(dir); !rep.OK || rep.Records != 4 {
		t.Fatalf("torn tail before reopen: %+v (a torn line is not a record)", rep)
	}
	w2 := mustOpen(t, dir, Options{})
	seq, err := w2.Append(&Record{Type: TypeIntent})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 { // 1 open, 2-4 intents, 5 the second open
		t.Fatalf("seq after reopen = %d, want 6", seq)
	}
	w2.Close()
	rep := mustVerify(t, dir)
	if rep.Records != 6 || rep.LastSeq != 6 {
		t.Fatalf("after reopen: %+v", rep)
	}
	p, _ := Query(dir, Filter{Types: []string{TypeOpen}}, 0, 10)
	if len(p.Records) != 2 || !strings.Contains(p.Records[0].Detail, "torn") {
		t.Fatalf("open records = %+v, want the reopen to report the cut", p.Records)
	}
}

// A sink failure refuses appends (ErrUnavailable) until a retry succeeds;
// then a gap record counts what was lost and the chain still verifies.
func TestSinkFailureRefusesThenRecordsGap(t *testing.T) {
	dir := t.TempDir()
	var broken atomic.Bool
	w := mustOpen(t, dir, Options{RetryEvery: 20 * time.Millisecond, BeforeWrite: func() error {
		if broken.Load() {
			return errors.New("disk full (injected)")
		}
		return nil
	}})
	appendN(t, w, 2, "p-1")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	broken.Store(true)
	if _, err := w.Append(&Record{Type: TypeIntent}); err != nil {
		t.Fatal(err) // queued before the writer saw the failure
	}
	if err := w.Sync(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Sync during failure = %v, want ErrUnavailable", err)
	}
	if w.Healthy() {
		t.Fatal("writer healthy after a failed write")
	}
	if _, err := w.Append(&Record{Type: TypeIntent}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Append while failed = %v, want ErrUnavailable", err)
	}
	broken.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !w.Healthy() {
		t.Fatal("writer did not recover")
	}
	appendN(t, w, 1, "p-1")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	mustVerify(t, dir)
	p, _ := Query(dir, Filter{Types: []string{TypeGap}}, 0, 10)
	if len(p.Records) != 1 || p.Records[0].Dropped != 2 {
		t.Fatalf("gap records = %+v, want one gap of 2 lost records", p.Records)
	}
}

// Open on a directory that cannot be created still returns a Writer: it
// starts failed and recovers once the directory can be made.
func TestOpenFailureStartsFailedAndRecovers(t *testing.T) {
	base := t.TempDir()
	blocker := filepath.Join(base, "audit")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := Open(blocker, Options{Logf: quiet, RetryEvery: 20 * time.Millisecond})
	if err == nil || w == nil {
		t.Fatalf("Open = %v, %v; want an error and a usable writer", w, err)
	}
	defer w.Close()
	if _, err := w.Append(&Record{Type: TypeIntent}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Append = %v, want ErrUnavailable", err)
	}
	os.Remove(blocker)
	deadline := time.Now().Add(5 * time.Second)
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !w.Healthy() {
		t.Fatal("writer did not recover")
	}
	w.Close()
	mustVerify(t, blocker)
}

func TestSegmentsRotateAndChainAcrossThem(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 2048})
	for i := 0; i < 40; i++ {
		appendN(t, w, 1, "p-1")
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	rep := mustVerify(t, dir)
	if rep.Segments < 3 || rep.Records != 41 {
		t.Fatalf("rotation: %+v", rep)
	}
	// A reopen continues in the newest segment.
	w2 := mustOpen(t, dir, Options{SegmentBytes: 2048})
	appendN(t, w2, 1, "p-1")
	w2.Close()
	if rep := mustVerify(t, dir); rep.Records != 43 {
		t.Fatalf("after reopen: %+v", rep)
	}
	// Paging across segments returns every intent exactly once, newest first.
	var got []uint64
	var before uint64
	for {
		p, err := Query(dir, Filter{Types: []string{TypeIntent}}, before, 7)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Records {
			got = append(got, r.Seq)
		}
		if p.Next == 0 {
			break
		}
		before = p.Next
	}
	if len(got) != 41 {
		t.Fatalf("paged %d intents, want 41: %v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] >= got[i-1] {
			t.Fatalf("not newest first: %v", got)
		}
	}
}

func TestQueryFilters(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{})
	w.Append(&Record{Type: TypeIntent, TS: 1000, Person: "p-a", Principal: "mac-1",
		Projects: []Project{{Ino: 5, Path: "/Clients/A"}}})
	w.Append(&Record{Type: TypeRefused, TS: 2000, Person: "p-b", Principal: "mac-2",
		Projects: []Project{{Ino: 6, Path: "/Clients/B"}}})
	w.Append(&Record{Type: TypeIntent, TS: 3000, Person: "p-a", Principal: "mac-1",
		Ops: []Op{{Op: "add", Parent: 1, Name: "Top", Path: "/Top"}}})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	count := func(f Filter) int {
		t.Helper()
		p, err := Query(dir, f, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		return len(p.Records)
	}
	for name, c := range map[string]struct {
		f    Filter
		want int
	}{
		"person":        {Filter{Person: "p-a"}, 2},
		"principal":     {Filter{Principal: "mac-2"}, 1},
		"project":       {Filter{Project: "/Clients"}, 2},
		"project exact": {Filter{Project: "/Clients/A"}, 1},
		"not a prefix":  {Filter{Project: "/Clients/AB"}, 0},
		"entry path":    {Filter{Project: "/Top"}, 1},
		"since":         {Filter{Since: 2000, Types: []string{TypeIntent, TypeRefused}}, 2},
		"until":         {Filter{Until: 2000, Types: []string{TypeIntent}}, 1},
	} {
		if got := count(c.f); got != c.want {
			t.Errorf("%s: %d records, want %d", name, got, c.want)
		}
	}
}

// Append never waits for the disk: with the writer stuck in a write, many
// goroutines still append at full speed until the queue is full, and then
// get ErrQueueFull instead of blocking.
func TestAppendDoesNotBlockOnDisk(t *testing.T) {
	dir := t.TempDir()
	release := make(chan struct{})
	var once sync.Once
	stuck := make(chan struct{})
	first := true
	w := mustOpen(t, dir, Options{QueueLen: 1000, BeforeWrite: func() error {
		if first {
			first = false // the open record
			return nil
		}
		once.Do(func() { close(stuck) })
		<-release
		return nil
	}})
	w.Append(&Record{Type: TypeIntent})
	<-stuck
	start := time.Now()
	var wg sync.WaitGroup
	var full atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := w.Append(&Record{Type: TypeIntent}); errors.Is(err, ErrQueueFull) {
					full.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("appends took %v with the disk stuck", d)
	}
	if full.Load() != 600 {
		t.Fatalf("ErrQueueFull %d times, want 600 (1600 appends, queue 1000)", full.Load())
	}
	close(release)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

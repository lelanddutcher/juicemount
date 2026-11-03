package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

// A log the Manager cannot read (a line longer than the readers accept,
// written by an older proxy) is a problem: the minute check only logged
// the error, so the anchors froze and the People page said the chain was
// fine (review round 3, H2).
func TestAuditWatchReportsAnUnreadableLog(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, HealthPath: dir + ".health", MaintainEvery: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x"})
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	aw := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
	aw.tick()
	if st := aw.status(false); !st.ChainOK || len(aw.st.Anchors) == 0 {
		t.Fatalf("before: %+v", st)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "audit-*.jsonl"))
	f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte(`{"seq":99,"type":"intent","detail":"` + strings.Repeat("x", 17<<20) + `"}` + "\n"))
	f.Close()
	aw.tick()
	st := aw.status(false)
	if st.ChainOK || !strings.Contains(st.Problem, "cannot be read") {
		t.Errorf("after an unreadable line: chain_ok=%v problem=%q", st.ChainOK, st.Problem)
	}
}

// The minute check's whole-log read (a start no retention record the
// Manager read names) runs without the lock: on a 2 GB log it took about a
// minute, and the status and the People page waited for it (review round
// 3, M2).
func TestAuditWatchFullReadDoesNotBlockStatus(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, SegmentBytes: 2048, RetainBytes: 8 << 10, RetainAge: -1,
		HealthPath: dir + ".health", MaintainEvery: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x", Action: "remove"})
		if i%50 == 49 {
			w.Sync()
			time.Sleep(20 * time.Millisecond) // retention runs
		}
	}
	w.Sync()
	w.Close()
	start, ok, _ := auditlog.FirstRecord(dir)
	if !ok || auditlog.ZeroPrev(start.Prev) {
		t.Fatalf("retention removed nothing: %+v", start)
	}
	// A Manager that anchored the head but never read the retention
	// record: the next tick reads the whole log.
	aw := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
	_, head, err := auditlog.VerifyAnchors(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	aw.st.Anchors = []auditlog.Anchor{head}
	entered, release := make(chan struct{}), make(chan struct{})
	auditFullRead = func(dir string, a []auditlog.Anchor) (auditlog.Report, auditlog.Anchor, error) {
		close(entered)
		<-release
		return auditlog.VerifyAnchors(dir, a)
	}
	defer func() { auditFullRead = auditlog.VerifyAnchors }()
	ticked := make(chan struct{})
	go func() { aw.tick(); close(ticked) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the tick did not read the whole log")
	}
	got := make(chan *AuditStatus)
	go func() { got <- aw.status(false) }()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Error("status waited for the whole-log read")
	}
	close(release)
	<-ticked
	aw.mu.Lock()
	n := len(aw.st.Retentions)
	aw.mu.Unlock()
	if n == 0 {
		t.Error("the retention records the whole-log read found were not merged")
	}
}

// Before the size limit cuts any history, the status warns when the rate
// the log grows at would leave it less than a week (review round 3).
func TestAuditRetentionForecast(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "audit")
	then := time.Now().Add(-2 * time.Hour)
	w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, Now: func() time.Time { return then }})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x"})
	w.Sync()
	w.Close()
	h := auditlog.Health{RetainBytes: 2 << 30, Bytes: 1 << 30}
	if f := retentionForecast(dir, h, time.Now()); !strings.Contains(f, "keeps only about 4h0m0s") {
		t.Errorf("forecast at 1 GB in 2 h with a 2 GB limit: %q", f)
	}
	h.Bytes = 1 << 20
	if f := retentionForecast(dir, h, time.Now()); f != "" {
		t.Errorf("forecast with the log far below its limit: %q", f)
	}
}

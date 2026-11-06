package manager

// Review round 3: a removed audit directory, a prefix cut with a forged
// retention record, retention that removes recent history, and a log that
// stays failed.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

func quietLog(string, ...any) {}

// TestAuditWatchReportsARemovedDirectory: the audit directory is removed
// while the proxy runs. Once the Manager has anchored the log, a missing
// directory is a removed log (it read as "logging off" and the status
// said healthy and verified), and the writer itself stops trusting a
// segment that is no longer at its path.
func TestAuditWatchReportsARemovedDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, HealthPath: dir + ".health", MaintainEvery: 10 * time.Millisecond,
		RetryEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 10; i++ {
		w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x"})
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	aw := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
	aw.tick()
	if len(aw.st.Anchors) == 0 {
		t.Fatal("no anchor")
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x"})
	}
	syncErr := w.Sync()
	time.Sleep(100 * time.Millisecond)
	aw.tick()
	st := aw.status(true)
	if syncErr == nil {
		t.Errorf("the writer kept writing into the removed directory: sync %v, healthy %v", syncErr, w.Healthy())
	}
	if st.ChainOK || st.Problem == "" {
		t.Errorf("status after the directory was removed: chain_ok=%v healthy=%v problem=%q", st.ChainOK, st.Healthy, st.Problem)
	}
	// The same with the writer stopped (nothing recreates the directory).
	w.Close()
	os.RemoveAll(dir)
	aw2 := newAuditWatch(dir, filepath.Join(base, "audit-anchors-2.json"))
	aw2.st.Anchors = append([]auditlog.Anchor(nil), aw.st.Anchors...)
	aw2.tick()
	if st := aw2.status(false); st.ChainOK || !strings.Contains(st.Problem, "was removed") {
		t.Errorf("status with the directory gone: chain_ok=%v problem=%q", st.ChainOK, st.Problem)
	}
	// Before the first anchor a missing directory is "logging off".
	fresh := newAuditWatch(filepath.Join(base, "never"), filepath.Join(base, "other-anchors.json"))
	fresh.tick()
	if st := fresh.status(false); !st.ChainOK {
		t.Errorf("no directory and no anchor: %+v", st)
	}
}

// TestAuditWatchJudgesRetention: someone who can write the audit directory
// removes the oldest segments, with records the Manager anchored, and
// appends a retention record naming the new start, chained to the head.
// The chain verifies; the Manager's minute check and its full check report
// it because no retention policy explains the removal. The writer's own
// retention under a size limit is accepted, and since it removed records
// written minutes ago the status warns that the log lost recent history.
func TestAuditWatchJudgesRetention(t *testing.T) {
	t.Run("forged", func(t *testing.T) {
		base := t.TempDir()
		dir := filepath.Join(base, "audit")
		aw := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
		w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, SegmentBytes: 2048, RetainBytes: -1, RetainAge: -1,
			HealthPath: dir + ".health", MaintainEvery: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x", Action: "remove", Detail: "evidence"})
			if i%40 == 39 {
				w.Sync()
				time.Sleep(30 * time.Millisecond) // the health file's durable seq
				aw.tick()
			}
		}
		w.Close()
		aw.tick()
		if st := aw.status(false); !st.ChainOK || len(aw.st.Anchors) < 4 {
			t.Fatalf("before the cut: %+v, %d anchors", st, len(aw.st.Anchors))
		}
		segs, _ := filepath.Glob(filepath.Join(dir, "audit-*.jsonl"))
		cut := len(segs) / 2
		for _, s := range segs[:cut] {
			os.Remove(s)
		}
		start, _, _ := auditlog.FirstRecord(dir)
		_, head, _ := auditlog.VerifyAnchors(dir, nil)
		forged := auditlog.Record{Seq: head.Seq + 1, TS: time.Now().UnixMilli(), Type: auditlog.TypeRetention, StartSeq: start.Seq,
			StartPrev: start.Prev, Detail: "removing old segments (size limit)", Prev: head.Hash}
		line, _ := json.Marshal(forged)
		f, _ := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
		f.Write(append(line, '\n'))
		f.Close()

		aw.tick()
		if st := aw.status(false); st.ChainOK || !strings.Contains(st.Problem, "retention policy does not explain") {
			t.Errorf("minute check after a forged retention: chain_ok=%v problem=%q", st.ChainOK, st.Problem)
		}
		again := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
		if rep, err := again.fullVerify(); err != nil || rep.OK {
			t.Errorf("full check after a forged retention: %+v, %v", rep, err)
		}
		// A Manager that never read the record (it was down) still
		// reports the anchors the cut removed.
		blind := newAuditWatch(dir, filepath.Join(base, "blind-anchors.json"))
		blind.st.Anchors = append([]auditlog.Anchor(nil), aw.st.Anchors...)
		blind.checkStart(auditlog.Policy{}, time.Now().UnixMilli())
		if blind.st.Problem == "" {
			t.Error("start check of a cut log with no retention record seen: no problem")
		}
	})
	t.Run("own", func(t *testing.T) {
		base := t.TempDir()
		dir := filepath.Join(base, "audit")
		aw := newAuditWatch(dir, filepath.Join(base, "audit-anchors.json"))
		w, err := auditlog.Open(dir, auditlog.Options{Logf: quietLog, SegmentBytes: 2048, RetainBytes: 8 << 10, RetainAge: -1,
			HealthPath: dir + ".health", MaintainEvery: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 300; i++ {
			w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Principal: "mac-x", Action: "remove", Detail: "evidence"})
			w.Sync()
			if i%40 == 39 {
				time.Sleep(30 * time.Millisecond)
				aw.tick()
			}
		}
		w.Close()
		aw.tick()
		st := aw.status(false)
		if !st.ChainOK || st.Removed == 0 {
			t.Fatalf("the writer's own retention: %+v", st)
		}
		if !strings.Contains(st.RetentionWarning, "left only") || !strings.Contains(st.Problem, "lost recent history") {
			t.Errorf("retention of minutes-old records raises no warning: %+v", st)
		}
		if rep, err := aw.fullVerify(); err != nil || !rep.OK {
			t.Fatalf("full check of the writer's own retention: %+v, %v", rep, err)
		}
		if st := aw.status(false); st.RetentionWarning != "" {
			t.Errorf("a passing full check keeps the warning: %+v", st)
		}
	})
}

// TestAuditStatusExplainsAStuckLog: the health note of a failed writer
// (retention has nothing more to remove) is in the status.
func TestAuditStatusExplainsAStuckLog(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	os.MkdirAll(dir, 0o700)
	h := auditlog.Health{TS: time.Now().UnixMilli(), Active: true, Error: "no space left on device",
		Note: "still cannot write; retention has nothing more to remove: free disk space or lower the limit"}
	b, _ := json.Marshal(h)
	os.WriteFile(dir+".health", b, 0o600)
	st := newAuditWatch(dir, filepath.Join(base, "a.json")).status(true)
	if !st.WritesRefused || !strings.Contains(st.Problem, "nothing more to remove") {
		t.Fatalf("status of a stuck log: %+v", st)
	}
}

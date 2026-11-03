package manager

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

const rotatedInstanceID = "9A8B7C6D-5E4F-4A3B-8C2D-1E0F9A8B7C6D"

// TestRotatedMacGetsItsPersonOnApproval: a Mac enrolled as a rotated
// identity of John's Mac gets John when an admin approves it; the
// auto-approve window (an unproven claim, nobody looked) leaves it with no
// person, so read-only while grants are in force, until an admin confirms
// it.
func TestRotatedMacGetsItsPersonOnApproval(t *testing.T) {
	for _, auto := range []bool{false, true} {
		name := "approved by an admin"
		if auto {
			name = "auto-approved, then confirmed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			startRealProxyReloader(t, dir)
			old := approvedMac(t, s, testInstanceID)
			john, _ := s.CreatePerson("John")
			if err := s.AssignDevice(old, john.ID); err != nil {
				t.Fatal(err)
			}
			if err := s.AddGrant(john.ID, "/Clients/A"); err != nil {
				t.Fatal(err)
			}
			if auto {
				if _, err := s.SetAutoApproveWindow(time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Enroll(EnrollRequest{InstanceID: rotatedInstanceID, Host: "edit-bay", Secret: testMacSecret + "2",
				PreviousInstanceID: testInstanceID}, "192.0.2.8"); err != nil {
				t.Fatal(err)
			}
			mac := macPrincipalName(rotatedInstanceID)
			device := func() DeviceView {
				for _, d := range s.PeopleSnapshot().Devices {
					if d.Name == mac {
						return d
					}
				}
				t.Fatalf("%s not in the snapshot", mac)
				return DeviceView{}
			}
			if d := device(); d.Person != "" || d.PersonFromPrevious != john.ID {
				t.Fatalf("before approval: %+v", d)
			}
			if auto {
				d := device()
				if d.Status != PrincipalApproved || !d.ReadOnly {
					t.Fatalf("auto-approved rotated Mac: %+v, want approved, no person, read-only", d)
				}
				if _, ps := proxyFile(t, dir); ps.PersonOf(mac) != nil {
					t.Fatal("the auto-approve window gave the rotated Mac a person")
				}
			}
			if err := s.Approve(mac); err != nil {
				t.Fatal(err)
			}
			if d := device(); d.Person != john.ID || d.ReadOnly {
				t.Fatalf("after approval: %+v", d)
			}
			if _, ps := proxyFile(t, dir); ps.PersonOf(mac) == nil || ps.PersonOf(mac).ID != john.ID {
				t.Fatal("the approved rotated Mac is not John's in the proxy file")
			}
		})
	}
}

// TestAuditStatusSaysWhyWritesAreRefused: the status reads the proxy's
// health file; a failed log or a stale health file while grants are in
// force is reported as refusing Mac writes.
func TestAuditStatusSaysWhyWritesAreRefused(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	var broken atomic.Bool
	w, err := auditlog.Open(dir, auditlog.Options{Logf: func(string, ...any) {}, HealthPath: dir + ".health",
		RetryEvery: time.Hour, BeforeWrite: func() error {
			if broken.Load() {
				return errors.New("no space left on device (injected)")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	aw := newAuditWatch(dir, filepath.Join(base, "anchors.json"))
	if st := aw.status(true); !st.Enabled || !st.Healthy || st.WritesRefused || st.Problem != "" || st.Segments != 1 {
		t.Fatalf("healthy status: %+v", st)
	}
	broken.Store(true)
	w.Append(&auditlog.Record{Type: auditlog.TypeIntent})
	deadline := time.Now().Add(5 * time.Second)
	for w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // the health file follows the state
	st := aw.status(true)
	if st.Healthy || !st.WritesRefused || !strings.Contains(st.Problem, "Mac writes are refused") || !strings.Contains(st.Problem, "no space") {
		t.Fatalf("failed log with grants in force: %+v", st)
	}
	if st := aw.status(false); st.WritesRefused || !strings.Contains(st.Problem, "not recorded") {
		t.Fatalf("failed log without grants: %+v", st)
	}
	// A stale health file (the proxy stopped refreshing it).
	h, _ := auditlog.ReadHealth(dir + ".health")
	h.Healthy, h.Error, h.TS = true, "", time.Now().Add(-time.Hour).UnixMilli()
	b, _ := json.Marshal(h)
	os.WriteFile(dir+".health", b, 0o600)
	if st := aw.status(true); !st.Stale || !st.WritesRefused || !strings.Contains(st.Problem, "not refreshed") {
		t.Fatalf("stale health: %+v", st)
	}
}

// TestAuditAnchorsCatchATruncatedLog: the Manager anchors the head every
// tick; a log cut back to before the anchor is reported, the report
// survives a Manager restart, and only a passing full check clears it.
func TestAuditAnchorsCatchATruncatedLog(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	anchors := filepath.Join(base, "state", "audit-anchors.json")
	os.MkdirAll(filepath.Dir(anchors), 0o700)
	w, err := auditlog.Open(dir, auditlog.Options{Logf: func(string, ...any) {}, HealthPath: dir + ".health"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Person: "p-john"})
	}
	w.Sync()
	w.Close() // writes the final health (durable seq)
	aw := newAuditWatch(dir, anchors)
	aw.tick()
	st := aw.status(false)
	if !st.ChainOK || st.AnchorSeq < 20 {
		t.Fatalf("first anchor: %+v", st)
	}
	if b, err := os.ReadFile(anchors); err != nil || !strings.Contains(string(b), `"seq"`) {
		t.Fatalf("anchors not saved: %v %s", err, b)
	}
	aw.tick() // nothing new: still fine
	if st := aw.status(false); !st.ChainOK {
		t.Fatalf("second tick: %+v", st)
	}
	// Cut the log back to before the anchored record.
	segs, _ := filepath.Glob(filepath.Join(dir, "audit-*.jsonl"))
	fi, _ := os.Stat(segs[0])
	if err := os.Truncate(segs[0], fi.Size()/2); err != nil {
		t.Fatal(err)
	}
	aw.tick()
	if st := aw.status(false); st.ChainOK || !strings.Contains(st.Problem, "anchors") {
		t.Fatalf("truncated log: %+v", st)
	}
	again := newAuditWatch(dir, anchors)
	if st := again.status(false); st.ChainOK {
		t.Fatal("the problem did not survive a restart")
	}
	if rep, err := again.fullVerify(); err != nil || rep.OK {
		t.Fatalf("full verify of the truncated log: %+v %v", rep, err)
	}
	if st := again.status(false); st.ChainOK {
		t.Fatal("a failed full check cleared the problem")
	}
}

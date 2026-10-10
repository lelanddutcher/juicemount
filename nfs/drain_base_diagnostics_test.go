package nfs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

type exactBaseLog struct {
	Message            string `json:"msg"`
	RowID              int64  `json:"row_id"`
	Path               string `json:"path"`
	BaseKind           string `json:"base_kind"`
	MismatchFields     string `json:"mismatch_fields"`
	SavedInode         uint64 `json:"saved_inode"`
	SavedMtimeNS       int64  `json:"saved_mtime_ns"`
	SavedCtimeNS       int64  `json:"saved_ctime_ns"`
	SavedLength        int64  `json:"saved_length"`
	SavedMode          string `json:"saved_mode"`
	CurrentInode       uint64 `json:"current_inode"`
	CurrentMtimeNS     int64  `json:"current_mtime_ns"`
	CurrentCtimeNS     int64  `json:"current_ctime_ns"`
	CurrentLength      int64  `json:"current_length"`
	CurrentMode        string `json:"current_mode"`
	AcceptedEpoch      uint64 `json:"accepted_epoch"`
	ForeignBeforeEpoch uint64 `json:"foreign_before_epoch"`
	LeaseTainted       bool   `json:"lease_tainted"`
}

// This exercises the actual Chmod caller, including its raw syscall result.
// The log is emitted only after runNamespaceOperation joins the worker.
func TestDrainBaseDiagnosticSidecarChmodResult(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	t.Cleanup(func() { _ = jmlog.Init(jmlog.Config{Stderr: os.Stderr}) })
	jfs, _, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	for _, tc := range []struct {
		name      string
		exists    bool
		wantErrno int
	}{
		{name: "present", exists: true},
		{name: "missing", wantErrno: int(unix.ENOENT)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			if err := jmlog.Init(jmlog.Config{Stderr: &logs}); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join("Film", "._"+tc.name)
			if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, 4, time.Now(), 9001)); err != nil {
				t.Fatal(err)
			}
			if tc.exists {
				full := filepath.Join(fuseRoot, name)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte("data"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := (&juiceChange{handler: jfs.handler}).Chmod(name, 0o600); err != nil {
				t.Fatalf("metadata Chmod: %v", err)
			}
			var found []struct {
				Message          string `json:"msg"`
				Path             string `json:"path"`
				SpoolChecked     bool   `json:"spool_checked"`
				BackendAttempted bool   `json:"backend_attempted"`
				BackendApplied   bool   `json:"backend_applied"`
				BackendErrno     *int   `json:"backend_errno"`
			}
			s := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
			for s.Scan() {
				var record struct {
					Message          string `json:"msg"`
					Path             string `json:"path"`
					SpoolChecked     bool   `json:"spool_checked"`
					BackendAttempted bool   `json:"backend_attempted"`
					BackendApplied   bool   `json:"backend_applied"`
					BackendErrno     *int   `json:"backend_errno"`
				}
				if err := json.Unmarshal(s.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				if record.Message == "nfs: sidecar chmod result" {
					found = append(found, record)
				}
			}
			if err := s.Err(); err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 || found[0].Path != name || !found[0].SpoolChecked || !found[0].BackendAttempted ||
				found[0].BackendApplied != tc.exists || found[0].BackendErrno == nil ||
				*found[0].BackendErrno != tc.wantErrno {
				t.Fatalf("unexpected sidecar Chmod provenance: %+v; logs: %s", found, logs.Bytes())
			}
		})
	}
}

func findExactBaseLog(t *testing.T, output []byte) exactBaseLog {
	t.Helper()
	var found []exactBaseLog
	s := bufio.NewScanner(bytes.NewReader(output))
	for s.Scan() {
		var r exactBaseLog
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			t.Fatalf("malformed structured log: %v: %q", err, s.Bytes())
		}
		if r.Message == "drain: exact base refused in-place write" {
			found = append(found, r)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("want one exact-base refusal log, got %d: %s", len(found), output)
	}
	return found[0]
}

// The real openDrainTarget caller must still choose a conflict stage. These
// cases distinguish ctime-only metadata drift from a changed body with a
// restored mtime and from a replaced inode. A tainted lease stays visible in
// the same refusal record; it never grants a bypass.
func TestDrainBaseDiagnosticRefusalPreservesConflict(t *testing.T) {
	t.Cleanup(func() { _ = jmlog.Init(jmlog.Config{Stderr: os.Stderr}) })
	for _, tc := range []struct {
		name           string
		mutate         func(t *testing.T, dest string, base metadata.SpoolBase) []byte
		foreignBefore  uint64
		wantMask       string
		wantMaskPrefix string
	}{
		{
			name: "ctime_only",
			mutate: func(t *testing.T, dest string, _ metadata.SpoolBase) []byte {
				t.Helper()
				time.Sleep(5 * time.Millisecond)
				if err := os.Chmod(dest, 0o600); err != nil {
					t.Fatal(err)
				}
				return []byte("old-body")
			},
			wantMask: "ctime_ns",
		},
		{
			name: "changed_bytes_restored_mtime",
			mutate: func(t *testing.T, dest string, base metadata.SpoolBase) []byte {
				t.Helper()
				time.Sleep(5 * time.Millisecond)
				body := []byte("new-body") // same length as the base
				if err := os.WriteFile(dest, body, 0o644); err != nil {
					t.Fatal(err)
				}
				mt := time.Unix(0, base.MtimeNs)
				if err := os.Chtimes(dest, mt, mt); err != nil {
					t.Fatal(err)
				}
				return body
			},
			wantMask: "ctime_ns",
		},
		{
			name: "replaced_inode_identical_body",
			mutate: func(t *testing.T, dest string, _ metadata.SpoolBase) []byte {
				t.Helper()
				if err := os.Rename(dest, dest+".old"); err != nil {
					t.Fatal(err)
				}
				body := []byte("old-body")
				if err := os.WriteFile(dest, body, 0o644); err != nil {
					t.Fatal(err)
				}
				return body
			},
			wantMaskPrefix: "inode",
		},
		{
			name: "tainted_ctime_mismatch",
			mutate: func(t *testing.T, dest string, _ metadata.SpoolBase) []byte {
				t.Helper()
				time.Sleep(5 * time.Millisecond)
				if err := os.Chmod(dest, 0o600); err != nil {
					t.Fatal(err)
				}
				return []byte("old-body")
			},
			foreignBefore: 1,
			wantMask:      "ctime_ns",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			if err := jmlog.Init(jmlog.Config{Stderr: &logs}); err != nil {
				t.Fatal(err)
			}
			spool, d := newTestDrainer(t, DrainerConfig{})
			name := filepath.Join("Film", "Project", tc.name+".dat")
			dest := writeBackendFile(t, d, name, []byte("old-body"))
			mirrorSeesBackend(t, spool, name, dest)
			e := writeSpoolEntry(t, spool, name, []byte("our-body"))
			base, accepted := d.rowDrainBase(rowOf(t, spool, e))
			if base.Kind != metadata.SpoolBasePresent {
				t.Fatalf("want exact present base, got %s", base.Kind)
			}
			wantBody := tc.mutate(t, dest, base)
			var current unix.Stat_t
			if err := unix.Lstat(dest, &current); err != nil {
				t.Fatal(err)
			}
			if tc.wantMask != "" && exactBaseMismatchFields(base, &current) != tc.wantMask {
				t.Fatalf("fixture mismatch %q, want %q", exactBaseMismatchFields(base, &current), tc.wantMask)
			}
			if ok, err := spool.Meta().MarkDraining(e.ID()); err != nil || !ok {
				t.Fatalf("claim row: %v, %v", ok, err)
			}
			row := rowOf(t, spool, e)
			lease := leaseDecision{foreignBefore: tc.foreignBefore}
			target, ok := d.openDrainTarget(row, dest, lease)
			if !ok || target == nil || target.stage == nil || target.conflict == "" {
				t.Fatalf("refusal bypassed: ok=%v target=%+v", ok, target)
			}
			_ = target.f.Close()
			target.stage.discard()
			if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, wantBody) {
				t.Fatalf("refusal changed destination: %q, %v", got, err)
			}
			diag := findExactBaseLog(t, logs.Bytes())
			if diag.RowID != row.ID || diag.Path != name || diag.BaseKind != "present" ||
				diag.SavedInode != base.Ino || diag.SavedMtimeNS != base.MtimeNs ||
				diag.SavedCtimeNS != base.CtimeNs || diag.SavedLength != base.Len ||
				diag.SavedMode != "unrecorded" || diag.CurrentInode != uint64(current.Ino) ||
				diag.CurrentMtimeNS != current.Mtim.Nano() || diag.CurrentCtimeNS != current.Ctim.Nano() ||
				diag.CurrentLength != current.Size || diag.CurrentMode == "" ||
				diag.AcceptedEpoch != accepted || diag.ForeignBeforeEpoch != tc.foreignBefore ||
				diag.LeaseTainted != (tc.foreignBefore > accepted) {
				t.Fatalf("wrong saved/current tuple or lease fields: %+v", diag)
			}
			if tc.wantMask != "" && diag.MismatchFields != tc.wantMask {
				t.Fatalf("mask=%q, want %q", diag.MismatchFields, tc.wantMask)
			}
			if tc.wantMaskPrefix != "" && !strings.HasPrefix(diag.MismatchFields, tc.wantMaskPrefix) {
				t.Fatalf("mask=%q, want prefix %q", diag.MismatchFields, tc.wantMaskPrefix)
			}
		})
	}
}

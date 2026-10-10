package nfscustody

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type fakeRoot struct{ detaches, closes int }
type countWriter struct{ writes atomic.Int32 }

func (w *countWriter) Write(b []byte) (int, error) { w.writes.Add(1); return len(b), nil }
func (*countWriter) Close() error                  { return nil }

func TestNFSSessionTimedOutCommandsReconcileSameIDs(t *testing.T) {
	w := &countWriter{}
	s := &Session{stdin: w, events: make(chan received, 2), waitDone: make(chan struct{}), gate: make(chan struct{}, 1), session: "0123456789abcdef0123456789abcdef", boot: "boot", operation: "abcdef0123456789abcdef0123456789", path: "/private/owned", source: "127.0.0.1:/jm-abcdef0123456789abcdef0123456789", ready: true, fsid: [2]int32{31, 41}}
	s.gate <- struct{}{}
	if err := s.Detach(10 * time.Millisecond); err == nil || s.Detached() {
		t.Fatal("unanswered detach completed")
	}
	s.events <- received{event: event{Version: protocolVersion, Session: s.session, ID: 1, State: "detached", Boot: s.boot, Operation: s.operation, Path: s.path, Source: s.source}}
	if err := s.Detach(time.Second); err != nil || !s.Detached() || w.writes.Load() != 1 {
		t.Fatalf("detach was replayed: err=%v writes=%d", err, w.writes.Load())
	}
	if err := s.Release(10 * time.Millisecond); err == nil {
		t.Fatal("unanswered release completed")
	}
	s.events <- received{event: event{Version: protocolVersion, Session: s.session, ID: 2, State: "released", Boot: s.boot, Operation: s.operation, Path: s.path, Source: s.source}}
	s.mu.Lock()
	s.readerErr = io.EOF
	s.mu.Unlock()
	close(s.waitDone)
	if err := s.Release(time.Second); err != nil || w.writes.Load() != 2 || !s.Settled() {
		t.Fatalf("release was replayed or did not join: err=%v writes=%d", err, w.writes.Load())
	}
}

func (*fakeRoot) FSID() [2]int32  { return [2]int32{31, 41} }
func (f *fakeRoot) Detach() error { f.detaches++; return nil }
func (f *fakeRoot) Close() error  { f.closes++; return nil }

func TestNFSHelperCorrelatedDescriptorDetachAndRelease(t *testing.T) {
	var in, out bytes.Buffer
	const session = "0123456789abcdef0123456789abcdef"
	const operation = "abcdef0123456789abcdef0123456789"
	root := &fakeRoot{}
	if err := writeFrame(&in, request{Version: protocolVersion, Session: session, ID: 1, Op: "detach"}); err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(&in, request{Version: protocolVersion, Session: session, ID: 2, Op: "release"}); err != nil {
		t.Fatal(err)
	}
	rc := runWithOpen(session, "boot", operation, "/private/owned", "127.0.0.1:/jm-"+operation, &in, &out, func(path, source string) (rootCustody, error) {
		if path != "/private/owned" || source != "127.0.0.1:/jm-"+operation {
			return nil, errors.New("wrong identity")
		}
		return root, nil
	})
	if rc != 0 || root.detaches != 1 || root.closes != 1 {
		t.Fatalf("rc=%d detach=%d close=%d", rc, root.detaches, root.closes)
	}
	reader := bufio.NewReader(&out)
	read := func(state string, id uint64) {
		t.Helper()
		var e event
		if err := readFrame(reader, &e); err != nil {
			t.Fatal(err)
		}
		if e.Version != protocolVersion || e.Session != session || e.Operation != operation || e.Boot != "boot" || e.Path != "/private/owned" || e.State != state || e.ID != id {
			t.Fatalf("unexpected event: %+v", e)
		}
	}
	read("root_admitted", 0)
	read("detached", 1)
	read("released", 2)
}

func TestNFSHelperRefusedAdmissionDoesNotDetach(t *testing.T) {
	const session = "0123456789abcdef0123456789abcdef"
	const operation = "abcdef0123456789abcdef0123456789"
	var out bytes.Buffer
	rc := runWithOpen(session, "boot", operation, "/private/owned", "127.0.0.1:/jm-"+operation, strings.NewReader(""), &out, func(string, string) (rootCustody, error) { return nil, errors.New("foreign mount") })
	var e event
	if err := readFrame(bufio.NewReader(&out), &e); err != nil {
		t.Fatal(err)
	}
	if e.State != "admission_refused" || e.ID != 0 || e.Session != session {
		t.Fatalf("unexpected refusal: %+v", e)
	}
	if rc != 78 {
		t.Fatalf("refusal exit=%d", rc)
	}
}

func TestNFSFrameAndMountIdentityAreBounded(t *testing.T) {
	if err := writeFrame(io.Discard, event{Version: 1, Path: strings.Repeat("x", maxFrame)}); err == nil {
		t.Fatal("oversized frame accepted")
	}
	spec := mountbroker.Spec{MountPath: "/private/owned", Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef"}
	base := mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{31, 41}}}}
	if row, err := matchingRow(base, spec, [2]int32{31, 41}); err != nil || row == nil {
		t.Fatalf("exact row: %v", err)
	}
	for name, mutate := range map[string]func(*mounttable.Snapshot){
		"replacement": func(s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{9, 9} },
		"duplicate-source": func(s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: "/private/other", Source: spec.Source, Type: "nfs", FSID: [2]int32{8, 8}})
		},
		"descendant": func(s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: spec.MountPath + "/child", Source: "disk", Type: "apfs", FSID: [2]int32{8, 8}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := mounttable.Snapshot{Version: base.Version, BootID: base.BootID, Entries: append([]mounttable.MountEntry(nil), base.Entries...)}
			mutate(&snapshot)
			if _, err := matchingRow(snapshot, spec, [2]int32{31, 41}); err == nil {
				t.Fatal("foreign or nested mount admitted")
			}
		})
	}
}

package nfscustody

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type cleanupRoot struct {
	mounted          *bool
	detaches, closes int
	failDetach       bool
}

func (*cleanupRoot) FSID() [2]int32 { return [2]int32{31, 41} }
func (r *cleanupRoot) Detach() error {
	r.detaches++
	if r.failDetach {
		return errors.New("original detach uncertain")
	}
	*r.mounted = false
	return nil
}
func (r *cleanupRoot) Close() error { r.closes++; return nil }

type cleanupHeld struct{}

func cleanupFixture(t *testing.T) (mountbroker.Creator, mountbroker.Record, *cleanupRoot) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	spec := mountbroker.Spec{MountPath: "/private/owned", Endpoint: "127.0.0.1:12049",
		ExportPath:   "/jm-abcdef0123456789abcdef0123456789",
		Source:       "127.0.0.1:/jm-abcdef0123456789abcdef0123456789",
		MetadataPath: "/private/meta.db"}
	mounted := false
	creator := mountbroker.Creator{Directory: dir}
	creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		rows := []mounttable.MountEntry{}
		if mounted {
			rows = append(rows, mounttable.MountEntry{Path: spec.MountPath, Source: spec.Source,
				Type: "nfs", FSID: [2]int32{31, 41}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: rows}, nil
	}
	result := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error {
		mounted = true
		return nil
	})
	if err := result.Failure(); err != nil {
		t.Fatal(err)
	}
	return creator, *result.Record, &cleanupRoot{mounted: &mounted}
}

func TestOriginalHelperArmedCleanEOFAndOutputFailure(t *testing.T) {
	for _, failAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "ack", true: "failed-ack"}[failAck], func(t *testing.T) {
			creator, record, root := cleanupFixture(t)
			const session = "0123456789abcdef0123456789abcdef"
			var in, out bytes.Buffer
			if err := writeRequestFrame(&in, request{Version: protocolVersion, Session: session,
				ID: 1, Op: "arm", Record: &record}); err != nil {
				t.Fatal(err)
			}
			writer := io.Writer(&out)
			if failAck {
				writer = &failSecondWrite{dest: &out}
			}
			rc := runWithOpenAndCreator(session, record.BootID, record.OperationID,
				record.Spec.MountPath, record.Spec.Source, &in, writer,
				func(string, string) (rootCustody, error) { return root, nil },
				func() (mountbroker.Creator, error) { return creator, nil })
			if rc != 0 || root.detaches != 1 || root.closes != 1 {
				t.Fatalf("clean original EOF did not settle: rc=%d detach=%d close=%d", rc, root.detaches, root.closes)
			}
			r := bufio.NewReader(&out)
			var admission event
			if err := readFrame(r, &admission); err != nil || admission.State != "root_admitted" {
				t.Fatal("admission missing")
			}
			if !failAck {
				var ack event
				if err := readFrame(r, &ack); err != nil || ack.State != "armed" || ack.ID != 1 {
					t.Fatal("arm ack missing")
				}
			}
		})
	}
}

func TestFailedArmAckDoesNotTreatLiveStdinAsDeath(t *testing.T) {
	creator, record, root := cleanupFixture(t)
	const session = "0123456789abcdef0123456789abcdef"
	reader, writer := io.Pipe()
	var out bytes.Buffer
	failed := make(chan struct{})
	done := make(chan int, 1)
	joined := false
	go func() {
		done <- runWithOpenAndCreator(session, record.BootID, record.OperationID,
			record.Spec.MountPath, record.Spec.Source, reader, &failSecondWrite{dest: &out, failed: failed},
			func(string, string) (rootCustody, error) { return root, nil },
			func() (mountbroker.Creator, error) { return creator, nil })
	}()
	defer func() {
		_ = writer.Close()
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original helper not joined during test cleanup")
			}
		}
		_ = reader.Close()
	}()
	if err := writeRequestFrame(writer, request{Version: protocolVersion, Session: session,
		ID: 1, Op: "arm", Record: &record}); err != nil {
		t.Fatal(err)
	}
	// Observe the failed acknowledgment rather than assuming scheduling time.
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("ack writer never failed")
	}
	select {
	case <-done:
		joined = true
		t.Fatal("output failure acted as controller EOF")
	default:
	}
	if root.detaches != 0 {
		t.Fatal("output failure detached while stdin remained open")
	}
	_ = writer.Close()
	select {
	case rc := <-done:
		joined = true
		if rc != 0 || root.detaches != 1 || root.closes != 1 {
			t.Fatal("clean EOF did not settle original")
		}
	case <-time.After(time.Second):
		t.Fatal("original helper did not join after clean EOF")
	}
}

type failSecondWrite struct {
	dest   io.Writer
	writes int
	failed chan struct{}
}

func (w *failSecondWrite) Write(b []byte) (int, error) {
	w.writes++
	if w.writes == 2 {
		if w.failed != nil {
			close(w.failed)
		}
		return 0, errors.New("controller output closed")
	}
	return w.dest.Write(b)
}

func TestOriginalHelperUnarmedOrPartialEOFNeverDetaches(t *testing.T) {
	for _, input := range []string{"", "{\"version\":1", "{\"version\":1,\"session\":\"wrong\"}\n"} {
		creator, record, root := cleanupFixture(t)
		var out bytes.Buffer
		func() {
			defer func() {
				if recover() != (cleanupHeld{}) {
					t.Fatal("lost control did not remain held")
				}
			}()
			runWithOpenAndCreatorHold("0123456789abcdef0123456789abcdef", record.BootID,
				record.OperationID, record.Spec.MountPath, record.Spec.Source,
				bytes.NewBufferString(input), &out,
				func(string, string) (rootCustody, error) { return root, nil },
				func() (mountbroker.Creator, error) { return creator, nil },
				func() { panic(cleanupHeld{}) })
		}()
		if root.detaches != 0 || root.closes != 0 {
			t.Fatal("unarmed or partial control detached")
		}
	}
}

func TestArmedPartialEOFAndNormalDetachNeverCleanup(t *testing.T) {
	for _, variant := range []string{"partial", "failed-normal-detach", "successful-normal-detach"} {
		creator, record, root := cleanupFixture(t)
		const session = "0123456789abcdef0123456789abcdef"
		var in, out bytes.Buffer
		if err := writeRequestFrame(&in, request{Version: protocolVersion,
			Session: session, ID: 1, Op: "arm", Record: &record}); err != nil {
			t.Fatal(err)
		}
		if variant == "partial" {
			in.WriteString("{\"version\":1")
		} else {
			root.failDetach = variant == "failed-normal-detach"
			if err := writeFrame(&in, request{Version: protocolVersion, Session: session,
				ID: 2, Op: "detach"}); err != nil {
				t.Fatal(err)
			}
		}
		func() {
			defer func() {
				if recover() != (cleanupHeld{}) {
					t.Fatal("ambiguous original control did not HOLD")
				}
			}()
			runWithOpenAndCreatorHold(session, record.BootID, record.OperationID,
				record.Spec.MountPath, record.Spec.Source, &in, &out,
				func(string, string) (rootCustody, error) { return root, nil },
				func() (mountbroker.Creator, error) { return creator, nil },
				func() { panic(cleanupHeld{}) })
		}()
		if root.closes != 0 || root.detaches != map[string]int{"partial": 0, "failed-normal-detach": 1, "successful-normal-detach": 1}[variant] {
			t.Fatal("ambiguous or normal detach was repeated by armed cleanup")
		}
	}
}

func TestOriginalHelperRejectsMalformedArmWithoutDetach(t *testing.T) {
	const session = "0123456789abcdef0123456789abcdef"
	mutations := map[string]func(*request){
		"session":   func(q *request) { q.Session = "wrong" },
		"sequence":  func(q *request) { q.ID = 2 },
		"boot":      func(q *request) { q.Record.BootID = "foreign" },
		"operation": func(q *request) { q.Record.OperationID = "foreign" },
		"path":      func(q *request) { q.Record.Spec.MountPath = "/private/foreign" },
		"source":    func(q *request) { q.Record.Spec.Source = "127.0.0.1:/foreign" },
		"fsid":      func(q *request) { q.Record.Mount.FSID = [2]int32{99, 99} },
		"endpoint":  func(q *request) { q.Record.Spec.Endpoint = "127.0.0.1:9999" },
		"metadata":  func(q *request) { q.Record.Spec.MetadataPath = "/private/foreign.db" },
		"export":    func(q *request) { q.Record.Spec.ExportPath = "/foreign" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			creator, record, root := cleanupFixture(t)
			copy := record
			mount := *record.Mount
			copy.Mount = &mount
			q := request{Version: protocolVersion, Session: session, ID: 1, Op: "arm", Record: &copy}
			mutate(&q)
			var input, out bytes.Buffer
			if err := writeRequestFrame(&input, q); err != nil {
				t.Fatal(err)
			}
			assertHelperHoldsWithoutDetach(t, creator, record, root, session, &input, &out)
		})
	}
	for name, frame := range map[string]func(request) []byte{
		"unknown_field": func(q request) []byte {
			b, _ := json.Marshal(q)
			return append(bytes.TrimSuffix(b, []byte("}")), []byte(",\"foreign\":1}\n")...)
		},
		"trailing_object": func(q request) []byte { b, _ := json.Marshal(q); return append(b, []byte(" {}\n")...) },
		"oversized_arm": func(q request) []byte {
			b, _ := json.Marshal(q)
			return append(append(b, bytes.Repeat([]byte(" "), maxArmFrame)...), '\n')
		},
		"oversized_nonarm": func(q request) []byte {
			q.Op = "detach"
			q.Record = nil
			b, _ := json.Marshal(q)
			return append(append(b, bytes.Repeat([]byte(" "), maxFrame)...), '\n')
		},
	} {
		t.Run(name, func(t *testing.T) {
			creator, record, root := cleanupFixture(t)
			q := request{Version: protocolVersion, Session: session, ID: 1, Op: "arm", Record: &record}
			var out bytes.Buffer
			assertHelperHoldsWithoutDetach(t, creator, record, root, session, bytes.NewReader(frame(q)), &out)
		})
	}
}

func assertHelperHoldsWithoutDetach(t *testing.T, creator mountbroker.Creator, record mountbroker.Record, root *cleanupRoot, session string, input io.Reader, out io.Writer) {
	t.Helper()
	defer func() {
		if recover() != (cleanupHeld{}) {
			t.Fatal("invalid original frame did not HOLD")
		}
		if root.detaches != 0 || root.closes != 0 {
			t.Fatal("invalid original frame detached or closed")
		}
	}()
	runWithOpenAndCreatorHold(session, record.BootID, record.OperationID, record.Spec.MountPath, record.Spec.Source,
		input, out, func(string, string) (rootCustody, error) { return root, nil },
		func() (mountbroker.Creator, error) { return creator, nil }, func() { panic(cleanupHeld{}) })
}

func TestArmPendingPayloadAndAcknowledgedIdentity(t *testing.T) {
	w := &countWriter{}
	s := &Session{stdin: w, events: make(chan received, 2), waitDone: make(chan struct{}),
		gate: make(chan struct{}, 1), session: "0123456789abcdef0123456789abcdef",
		boot: "boot", operation: "abcdef0123456789abcdef0123456789",
		path: "/private/owned", source: "127.0.0.1:/jm-abcdef0123456789abcdef0123456789",
		ready: true, fsid: [2]int32{31, 41}}
	s.gate <- struct{}{}
	r := mountbroker.Record{Version: 1, OperationID: s.operation, BootID: s.boot,
		Spec: mountbroker.Spec{MountPath: s.path, Source: s.source,
			Endpoint: "127.0.0.1:12049", ExportPath: "/jm-abcdef0123456789abcdef0123456789",
			MetadataPath: "/private/meta.db"}, State: mountbroker.Observed,
		Mount: &mounttable.MountEntry{Path: s.path, Source: s.source, Type: "nfs", FSID: s.fsid}}
	if err := s.Arm(r, 10*time.Millisecond); err == nil || !s.ArmPending() {
		t.Fatal("unanswered arm was lost")
	}
	changed := r
	changed.Spec.MetadataPath = "/private/other.db"
	if err := s.Arm(changed, time.Second); err == nil || w.writes.Load() != 1 {
		t.Fatal("changed pending payload replayed")
	}
	s.events <- received{event: event{Version: protocolVersion, Session: s.session, ID: 1,
		State: "armed", Boot: s.boot, Operation: s.operation, Path: s.path, Source: s.source, FSID: s.fsid}}
	if err := s.Arm(r, time.Second); err != nil || !s.Armed() || w.writes.Load() != 1 {
		t.Fatal("same original arm did not reconcile one ID")
	}
	if err := s.Arm(changed, time.Second); err == nil {
		t.Fatal("acknowledged authority rebound")
	}
}

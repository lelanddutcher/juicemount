package fusecustody

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

type fakeRoot struct {
	detachCalls, closeCalls int
	failDetach              bool
}

func (*fakeRoot) UUID() string   { return "12345678-1234-1234-1234-123456789012" }
func (*fakeRoot) ReadOnly() bool { return false }
func (*fakeRoot) FSID() [2]int32 { return [2]int32{7, 9} }
func (f *fakeRoot) Detach() error {
	f.detachCalls++
	if f.failDetach {
		return errors.New("busy")
	}
	return nil
}
func (f *fakeRoot) Close() error { f.closeCalls++; return nil }
func TestCustodianProtocolRetainsUntilDetachThenReleases(t *testing.T) {
	const session = "0123456789abcdef0123456789abcdef"
	f := &fakeRoot{}
	var out bytes.Buffer
	input := strings.NewReader(`{"version":1,"session":"0123456789abcdef0123456789abcdef","id":1,"op":"release"}
{"version":1,"session":"0123456789abcdef0123456789abcdef","id":2,"op":"detach"}
{"version":1,"session":"0123456789abcdef0123456789abcdef","id":3,"op":"detach"}
{"version":1,"session":"0123456789abcdef0123456789abcdef","id":4,"op":"release"}
`)
	status := runWithOpen("/owned", 11, 22, "source", session, input, &out, func(string, int, int, string) (rootCustody, error) { return f, nil })
	if status != 0 || f.detachCalls != 1 || f.closeCalls != 1 {
		t.Fatalf("lifecycle status=%d detach=%d close=%d", status, f.detachCalls, f.closeCalls)
	}
	want := []string{"root_admitted", "command_refused", "detached_child_join_required", "already_detached", "released"}
	dec := json.NewDecoder(&out)
	for i, state := range want {
		var event Event
		if err := dec.Decode(&event); err != nil || event.State != state || event.Version != protocolVersion || event.Session != session || event.ID != uint64(i) {
			t.Fatalf("event %d %+v %v", i, event, err)
		}
	}
	var extra Event
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("extra %+v %v", extra, err)
	}
}
func TestCustodyFrameRejectsOversizeAndUUIDShape(t *testing.T) {
	var req request
	err := readFrame(bufio.NewReaderSize(strings.NewReader(strings.Repeat("x", maxFrame+1)+"\n"), maxFrame+1), &req)
	if err == nil {
		t.Fatal("oversized command accepted")
	}
	if CanonicalUUID("12345678-1234-1234-1234-123456789012") != true || CanonicalUUID("12345678-1234-1234-1234-12345678901X") {
		t.Fatal("UUID shape")
	}
}

func TestCustodianReportsPermissionDeniedRootOpenAsTypedState(t *testing.T) {
	const session = "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"EPERM", &os.PathError{Op: "open", Path: "/owned", Err: syscall.EPERM}, "admission_permission_denied"},
		{"EACCES", syscall.EACCES, "admission_permission_denied"},
		{"identity", errors.New("root is not the selected owned macFUSE mount"), "admission_refused"},
		{"unsupported", ErrUnsupported, "unsupported_os"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			status := runWithOpen("/owned", 11, 22, "source", session, strings.NewReader(""), &out,
				func(string, int, int, string) (rootCustody, error) { return nil, tc.err })
			var event Event
			if err := json.NewDecoder(&out).Decode(&event); err != nil {
				t.Fatal(err)
			}
			if status != 78 || event.State != tc.want || event.ID != 0 || event.Session != session {
				t.Fatalf("status=%d event=%+v want %s", status, event, tc.want)
			}
		})
	}
}

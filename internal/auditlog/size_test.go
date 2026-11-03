package auditlog

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// A record that encodes to more than MaxRecordBytes is never written as
// is: one line longer than the readers' limit made Query, Verify and Follow
// fail for the whole log (review round 3, H2). The writer writes a stub in
// its place that keeps the chain, the seq and who wrote it, and says how
// large the record was; AppendDurable reports ErrTooLarge, so a caller that
// needs its record on disk does not go ahead.
func TestOversizedRecordIsWrittenAsStub(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{})
	defer w.Close()
	big := strings.Repeat("\x01", maxLine/3) // escaped as \u0001: twice the readers' limit
	if _, err := w.Append(&Record{Type: TypeIntent, Principal: "mac-x", Person: "p-x", Action: "create",
		Ops: []Op{{Op: "add", Parent: 5, Name: big}}}); err != nil {
		t.Fatal(err)
	}
	seq, err := w.AppendDurable(&Record{Type: TypeIntent, Principal: "mac-y", Action: "create", Detail: big}, 10*time.Second)
	if !errors.Is(err, ErrTooLarge) || seq == 0 {
		t.Errorf("AppendDurable of an oversized record: seq %d, %v; want ErrTooLarge", seq, err)
	}
	if _, err := w.Append(&Record{Type: TypeIntent, Principal: "mac-z", Action: "remove"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	rep, err := Verify(dir)
	if err != nil || !rep.OK {
		t.Fatalf("Verify: %+v, %v", rep, err)
	}
	page, err := Query(dir, Filter{Types: []string{TypeIntent}}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("records: %+v", page.Records)
	}
	for _, r := range page.Records[1:] { // newest first: the two stubs
		if r.Omitted == 0 || !strings.HasPrefix(r.Digest, "sha256:") || !strings.Contains(r.Detail, "record not written") ||
			len(r.Ops) != 0 || r.Principal == "" || r.Action != "create" {
			t.Errorf("stub: %+v", r)
		}
	}
	if page.Records[0].Principal != "mac-z" || page.Records[0].Omitted != 0 {
		t.Errorf("record after the stubs: %+v", page.Records[0])
	}
}

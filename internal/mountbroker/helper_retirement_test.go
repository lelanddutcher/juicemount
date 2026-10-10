package mountbroker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// The helper's RETIRED outcome settles only the exact operation it names,
// and never skips Retire's own fresh absence check.
func TestRetireWithHelperProofAcceptsOnlyExactRetiredOp(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal")
	if err := os.Mkdir(journal, 0o700); err != nil {
		t.Fatal(err)
	}
	const gen = "0123456789abcdef0123456789abcdef"
	spec := Spec{MountPath: filepath.Join(dir, "vol"), Source: "127.0.0.1:/jm-" + gen,
		Endpoint: "127.0.0.1:2049", ExportPath: "/jm-" + gen, MetadataPath: filepath.Join(dir, "m.db")}
	present := false
	row := mounttable.MountEntry{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{3, 4}}
	c := Creator{Directory: journal, Observe: func(context.Context) (mounttable.Snapshot, error) {
		rows := []mounttable.MountEntry{}
		if present {
			rows = append(rows, row)
		}
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: rows}, nil
	}}
	res := c.CreateBound(context.Background(), spec, func(Record) error { present = true; return nil })
	if res.State != Observed || res.Record == nil {
		t.Fatalf("fixture: %+v", res)
	}
	rec := *res.Record
	op := HelperOpID(rec)
	if op != "nfs-"+rec.OperationID {
		t.Fatalf("op id %q", op)
	}
	ok := func() error { return nil }
	present = false
	for name, p := range map[string]HelperRetirement{
		"other op":  {OpID: "nfs-ffffffffffffffffffffffffffffffff", State: HelperRetiredState},
		"not final": {OpID: op, State: "ORDINARY"},
		"refused":   {OpID: op, State: "REFUSED_IDENTITY"},
	} {
		if err := c.RetireWithHelperProof(context.Background(), rec, p, ok); err == nil {
			t.Fatalf("%s accepted as proof", name)
		}
	}
	present = true // RETIRED but the fsid is still mounted: absence check wins
	if err := c.RetireWithHelperProof(context.Background(), rec, HelperRetirement{OpID: op, State: HelperRetiredState}, ok); err == nil {
		t.Fatal("helper proof bypassed the fresh absence check")
	}
	present = false
	if err := c.RetireWithHelperProof(context.Background(), rec, HelperRetirement{OpID: op, State: HelperRetiredState}, ok); err != nil {
		t.Fatalf("exact proof refused: %v", err)
	}
	after, err := ReadRecordReadonly(journal, spec.MountPath)
	if err != nil || after.State != Retired {
		t.Fatalf("journal %+v %v", after, err)
	}
}

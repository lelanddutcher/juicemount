package replaceguard

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func testRecord() Record {
	return Record{Version: 1, OperationID: "0123456789abcdef0123456789abcdef", Owner: "mac-a", Proxy: "proxy-a",
		BackendUUID: "volume-a", BackendDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Project: 2, AcceptedEpoch: 3, Epoch: 4, Parents: []Parent{{Inode: 1}, {Name: "media", Inode: 2}},
		Stage:       File{Name: ".juicemount-streaming-drain-abc", Parent: 2, Inode: 7, Length: 9, MtimeNS: 12, CtimeNS: 13},
		Destination: File{Name: "clip.mov", Parent: 2, Inode: 8, Length: (64 << 20) + 12, MtimeNS: 10, CtimeNS: 11},
		GraphDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ImageSHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Phase: "prepared"}
}

func TestRecordCanonicalRoundTripAndKeys(t *testing.T) {
	r := testRecord()
	b, err := Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("decode=%+v err=%v", got, err)
	}
	if Key(7) != "jmlease:v1:replace:7" {
		t.Fatal(Key(7))
	}
	keys := RequiredKeys(r)
	for _, want := range []string{Key(7), "setting", "jmlease:v1:proxy", "jmlease:v1:p:2", "jmlease:v1:epoch:2", "i1", "d1", "i2", "d2", "i7", "d7", "i8", "d8", "c8_0", "c8_1"} {
		found := false
		for _, key := range keys {
			if key == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing key %q", want)
		}
	}
	if _, err := Decode(append([]byte(" "), b...)); err == nil {
		t.Fatal("accepted noncanonical JSON")
	}
	var expanded map[string]any
	if err := json.Unmarshal(b, &expanded); err != nil {
		t.Fatal(err)
	}
	expanded["extra"] = true
	unknown, _ := json.Marshal(expanded)
	if _, err := Decode(unknown); err == nil {
		t.Fatal("accepted unknown field")
	}
}

func TestRecordRejectsInvalidAuthorityAndNames(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Record)
	}{
		{"short-op", func(r *Record) { r.OperationID = "abc" }},
		{"bad-digest", func(r *Record) { r.GraphDigest = "ABC" }},
		{"epoch", func(r *Record) { r.AcceptedEpoch = r.Epoch + 1 }},
		{"missing-owner", func(r *Record) { r.Owner = "" }},
		{"wrong-project", func(r *Record) { r.Project = 99 }},
		{"duplicate-parent", func(r *Record) { r.Parents = append(r.Parents, Parent{Name: "again", Inode: 2}) }},
		{"stage-name", func(r *Record) { r.Stage.Name = "ordinary" }},
		{"parent", func(r *Record) { r.Destination.Parent = 1 }},
		{"same-inode", func(r *Record) { r.Destination.Inode = r.Stage.Inode }},
		{"huge-graph", func(r *Record) { r.Destination.Length = (4097 * 64) << 20 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := testRecord()
			tc.mutate(&r)
			if _, err := Encode(r); err == nil {
				t.Fatal("accepted invalid record")
			}
			if RequiredKeys(r) != nil {
				t.Fatal("invalid record has key list")
			}
		})
	}
	r := testRecord()
	b, _ := Encode(r)
	if _, err := Decode(bytes.Repeat(b, MaxBytes/len(b)+2)); err == nil {
		t.Fatal("accepted oversized record")
	}
}

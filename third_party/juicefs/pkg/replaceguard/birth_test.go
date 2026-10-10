package replaceguard

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

const birthOp = "0123456789abcdef0123456789abcdef"

func validBirth() Birth {
	return Birth{
		Version: 1, OperationID: birthOp, Owner: "mac-a", Proxy: "proxy-birth-1",
		Database: 3, BackendUUID: "12345678-1234-abcd-5678-123456789abc", Project: 2,
		Epoch: 4, Parents: []Parent{{Inode: 1}, {Name: "media", Inode: 2}},
		Stage: File{Name: BirthStageName(birthOp), Parent: 2, Inode: 7, Length: 0, MtimeNS: 12, CtimeNS: 13},
		Type:  1, Nlink: 1, Mode: 0644, AttrSHA256: strings.Repeat("a", 64),
	}
}

func TestBirthCanonicalRoundTripAndRequiredKeys(t *testing.T) {
	b := validBirth()
	raw, err := EncodeBirth(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBirth(raw)
	if err != nil || !reflect.DeepEqual(got, b) {
		t.Fatalf("decode=%+v err=%v", got, err)
	}
	want := []string{
		"jmlease:v1:birth:op:" + birthOp, "jmlease:v1:birth:inode:7",
		"jmlease:v1:replace:7", "setting", "jmlease:v1:proxy",
		"jmlease:v1:p:2", "jmlease:v1:epoch:2", "usedSpace", "totalInodes",
		"i1", "d1", "i2", "d2", "i7", "d7",
	}
	if keys := BirthRequiredKeys(b); !reflect.DeepEqual(keys, want) {
		t.Fatalf("WATCH keys=%q, want=%q", keys, want)
	}
	seen := map[string]bool{}
	for _, key := range want {
		if seen[key] {
			t.Fatalf("duplicate WATCH key %q", key)
		}
		seen[key] = true
	}
	if BirthOperationKey("BAD") != "" || BirthInodeKey(0) != "" || BirthInodeKey(math.MaxUint64) != "" || BirthStageName("BAD") != "" {
		t.Fatal("invalid key/name input was accepted")
	}
	for _, db := range []int64{0, math.MaxInt32} {
		b.Database = db // Both are valid actual Redis database boundaries.
		raw, err := EncodeBirth(b)
		if err != nil {
			t.Fatalf("database %d rejected: %v", db, err)
		}
		if decoded, err := DecodeBirth(raw); err != nil || decoded.Database != db {
			t.Fatalf("database %d roundtrip: %+v, %v", db, decoded, err)
		}
	}
}

func TestBirthRejectsCanonicalJSONOverLimit(t *testing.T) {
	b := validBirth()
	b.Parents = []Parent{{Inode: 1}}
	for inode := uint64(2); inode <= 256; inode++ {
		b.Parents = append(b.Parents, Parent{Name: strings.Repeat("a", 255), Inode: inode})
	}
	b.Stage.Parent, b.Stage.Inode = 256, 257
	b.Project = 2
	if !errors.Is(b.Validate(), ErrInvalid) || BirthRequiredKeys(b) != nil {
		t.Fatal("oversized canonical birth was valid or got WATCH keys")
	}
	if _, err := EncodeBirth(b); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized canonical birth encoded: %v", err)
	}
}

func TestBirthExactStageGrammar(t *testing.T) {
	name := BirthStageName(birthOp)
	if op, ok := ParseBirthStageName(name); !ok || op != birthOp {
		t.Fatalf("parse=%q,%v", op, ok)
	}
	for _, bad := range []string{
		".juicemount-streaming-drain-" + birthOp, name + "x", name[:len(name)-1],
		strings.Replace(name, "a", "A", 1), ".juicemount-streaming-drain-replacement-" + strings.Repeat("g", 32),
	} {
		if _, ok := ParseBirthStageName(bad); ok {
			t.Fatalf("accepted name %q", bad)
		}
	}
	b := validBirth()
	b.Stage.Name = BirthStageName(strings.Repeat("b", 32))
	if !errors.Is(b.Validate(), ErrInvalid) {
		t.Fatal("accepted name with different operation ID")
	}
}

func TestBirthRejectsInvalidAuthorityAndStage(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Birth)
	}{
		{"operation", func(b *Birth) { b.OperationID = strings.ToUpper(b.OperationID) }},
		{"owner", func(b *Birth) { b.Owner = "" }},
		{"proxy", func(b *Birth) { b.Proxy = "" }},
		{"negative-database", func(b *Birth) { b.Database = -1 }},
		{"large-database", func(b *Birth) { b.Database = math.MaxInt32 + 1 }},
		{"uuid-case", func(b *Birth) { b.BackendUUID = strings.ToUpper(b.BackendUUID) }},
		{"uuid-shape", func(b *Birth) { b.BackendUUID = "volume-a" }},
		{"zero-epoch", func(b *Birth) { b.Epoch = 0 }},
		{"large-epoch", func(b *Birth) { b.Epoch = math.MaxInt64 + 1 }},
		{"wrong-project", func(b *Birth) { b.Project = 99 }},
		{"duplicate-ancestor", func(b *Birth) { b.Parents = append(b.Parents, Parent{Name: "again", Inode: 2}) }},
		{"too-many-parents", func(b *Birth) { b.Parents = make([]Parent, 257) }},
		{"wrong-root", func(b *Birth) { b.Parents[0].Name = "root" }},
		{"wrong-parent", func(b *Birth) { b.Stage.Parent = 1 }},
		{"ancestor-inode", func(b *Birth) { b.Stage.Inode = 2 }},
		{"root-inode", func(b *Birth) { b.Stage.Inode = 1 }},
		{"length", func(b *Birth) { b.Stage.Length = 1 }},
		{"time", func(b *Birth) { b.Stage.MtimeNS = -1 }},
		{"type", func(b *Birth) { b.Type = 2 }},
		{"hardlink", func(b *Birth) { b.Nlink = 2 }},
		{"mode", func(b *Birth) { b.Mode = 010000 }},
		{"attr-digest", func(b *Birth) { b.AttrSHA256 = strings.Repeat("A", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := validBirth()
			tc.change(&b)
			if !errors.Is(b.Validate(), ErrInvalid) || BirthRequiredKeys(b) != nil {
				t.Fatal("invalid birth accepted or given WATCH keys")
			}
		})
	}
}

func TestDecodeBirthRefusesNoncanonicalInput(t *testing.T) {
	raw, err := EncodeBirth(validBirth())
	if err != nil {
		t.Fatal(err)
	}
	reordered := bytes.Replace(raw, []byte(`{"version":1,"operation_id":"`+birthOp+`"`),
		[]byte(`{"operation_id":"`+birthOp+`","version":1`), 1)
	cases := map[string][]byte{
		"whitespace":     append([]byte(" "), raw...),
		"duplicate":      bytes.Replace(raw, []byte(`"version":1,`), []byte(`"version":1,"version":1,`), 1),
		"unknown":        bytes.Replace(raw, []byte(`"version":1,`), []byte(`"extra":1,"version":1,`), 1),
		"nested-unknown": bytes.Replace(raw, []byte(`"stage":{"name":`), []byte(`"stage":{"extra":1,"name":`), 1),
		"null":           bytes.Replace(raw, []byte(`"owner":"mac-a"`), []byte(`"owner":null`), 1),
		"reordered":      reordered,
		"numeric":        bytes.Replace(raw, []byte(`"database":3`), []byte(`"database":3.0`), 1),
		"invalid-utf8":   bytes.Replace(raw, []byte("mac-a"), []byte{'m', 'a', 'c', '-', 0xff}, 1),
		"overlong":       bytes.Repeat([]byte("x"), MaxBytes+1),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBirth(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted malformed input: %v", err)
			}
		})
	}
}

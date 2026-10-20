package continuity

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const abcSHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func vectorManifest() Manifest {
	return Manifest{
		Version:   1,
		LibraryID: "lib_000102030405060708090a0b0c0d0e0f",
		AssetID:   "asset_101112131415161718191a1b1c1d1e1f",
		Content:   Content{SHA256: abcSHA256, Size: 3},
		Parents: []RevisionID{
			"rev1_2222222222222222222222222222222222222222222222222222222222222222",
			"rev1_1111111111111111111111111111111111111111111111111111111111111111",
		},
	}
}

func TestManifestFixedVector(t *testing.T) {
	// Independently generated with Python hashlib/struct from the v1 contract.
	const canonicalHex = "6a756963656d6f756e742e7265766973696f6e0001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0000000000000003ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad000211111111111111111111111111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222"
	const revision = "rev1_43539895b296b2524ddc51819964e8938009918c15a4f47ac0e576197ebb0106"
	m := vectorManifest()
	canonical, err := m.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(canonical); got != canonicalHex {
		t.Fatalf("canonical = %s, want %s", got, canonicalHex)
	}
	id, err := m.RevisionID()
	if err != nil || id != revision {
		t.Fatalf("revision = %s, %v; want %s", id, err, revision)
	}
}

func TestManifestParentsAreAnUnmodifiedSet(t *testing.T) {
	m := vectorManifest()
	before := append([]RevisionID(nil), m.Parents...)
	a, err := m.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Parents, before) {
		t.Fatal("Canonical mutated caller parents")
	}
	m.Parents = []RevisionID{before[1], before[0]}
	b, err := m.Canonical()
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("parent order changed canonical encoding: %v", err)
	}
	m.Parents = nil
	nilParents, err := m.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	m.Parents = []RevisionID{}
	emptyParents, err := m.Canonical()
	if err != nil || !bytes.Equal(nilParents, emptyParents) {
		t.Fatalf("nil and empty parent sets differ: %v", err)
	}
}

func TestManifestRevisionBindsEveryField(t *testing.T) {
	original, err := vectorManifest().RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Manifest){
		"library": func(m *Manifest) { m.LibraryID = "lib_101112131415161718191a1b1c1d1e1f" },
		"asset":   func(m *Manifest) { m.AssetID = "asset_000102030405060708090a0b0c0d0e0f" },
		"size":    func(m *Manifest) { m.Content.Size++ },
		"digest":  func(m *Manifest) { m.Content.SHA256 = strings.Repeat("0", 64) },
		"parents": func(m *Manifest) { m.Parents = m.Parents[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			m := vectorManifest()
			change(&m)
			id, err := m.RevisionID()
			if err != nil || id == original {
				t.Fatalf("changed field produced %s, %v", id, err)
			}
		})
	}
}

func TestManifestRejectsInvalidDeclarations(t *testing.T) {
	for name, change := range map[string]func(*Manifest){
		"zero version":     func(m *Manifest) { m.Version = 0 },
		"unknown version":  func(m *Manifest) { m.Version = 2 },
		"negative size":    func(m *Manifest) { m.Content.Size = -1 },
		"bad digest":       func(m *Manifest) { m.Content.SHA256 = "abc" },
		"uppercase digest": func(m *Manifest) { m.Content.SHA256 = strings.ToUpper(abcSHA256) },
		"bad library":      func(m *Manifest) { m.LibraryID = LibraryID("lib_" + strings.Repeat("0", 32)) },
		"bad asset":        func(m *Manifest) { m.AssetID = AssetID("asset_" + strings.Repeat("0", 32)) },
		"bad parent":       func(m *Manifest) { m.Parents[0] = RevisionID("rev2_" + strings.Repeat("1", 64)) },
		"uppercase parent": func(m *Manifest) { m.Parents[0] = RevisionID("rev1_" + strings.Repeat("A", 64)) },
		"duplicate parents": func(m *Manifest) {
			m.Parents = append(m.Parents, m.Parents[0])
		},
		"too many parents": func(m *Manifest) {
			m.Parents = make([]RevisionID, MaxParents+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := vectorManifest()
			change(&m)
			if encoded, err := m.Canonical(); err == nil || encoded != nil {
				t.Fatalf("Canonical = %x, %v; want nil and error", encoded, err)
			}
			if id, err := m.RevisionID(); err == nil || id != "" {
				t.Fatalf("RevisionID = %s, %v; want empty and error", id, err)
			}
		})
	}
}

func TestManifestAcceptsMaximumParents(t *testing.T) {
	m := vectorManifest()
	m.Parents = make([]RevisionID, MaxParents)
	for i := range m.Parents {
		m.Parents[i] = RevisionID(fmt.Sprintf("rev1_%064x", i))
	}
	encoded, err := m.Canonical()
	if err != nil || len(encoded) != 95+64*32 {
		t.Fatalf("maximum parent encoding: length %d, %v", len(encoded), err)
	}
}

func TestManifestMaximumContentSize(t *testing.T) {
	m := vectorManifest()
	m.Content.Size = 1<<63 - 1
	encoded, err := m.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(encoded[53:61]); got != "7fffffffffffffff" {
		t.Fatalf("maximum signed size encoding = %s", got)
	}
}

func TestIdentityValidation(t *testing.T) {
	for _, kind := range []struct {
		prefix   string
		bytes    int
		zeroOK   bool
		validate func(string) error
	}{
		{"lib_", 16, false, func(s string) error { return LibraryID(s).Validate() }},
		{"asset_", 16, false, func(s string) error { return AssetID(s).Validate() }},
		{"rev1_", 32, true, func(s string) error { return RevisionID(s).Validate() }},
	} {
		t.Run(kind.prefix, func(t *testing.T) {
			valid := kind.prefix + strings.Repeat("ab", kind.bytes)
			if err := kind.validate(valid); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []string{
				"", strings.ToUpper(valid), valid + "0", valid[:len(valid)-1],
				kind.prefix + strings.Repeat("gg", kind.bytes),
				"other_" + strings.Repeat("ab", kind.bytes),
				kind.prefix + strings.Repeat("Ab", kind.bytes),
				" " + valid, valid + "\n",
			} {
				if err := kind.validate(invalid); err == nil {
					t.Errorf("accepted invalid identity %q", invalid)
				}
			}
			zero := kind.prefix + strings.Repeat("0", kind.bytes*2)
			if err := kind.validate(zero); (err == nil) != kind.zeroOK {
				t.Fatalf("zero identity error = %v, zeroOK = %v", err, kind.zeroOK)
			}
		})
	}
	if err := RevisionID("rev2_" + strings.Repeat("1", 64)).Validate(); err == nil {
		t.Fatal("accepted unknown revision version")
	}
}

func TestNewIdentities(t *testing.T) {
	library, err := NewLibraryID()
	if err != nil {
		t.Fatal(err)
	}
	if err := library.Validate(); err != nil {
		t.Fatal(err)
	}
	asset, err := NewAssetID()
	if err != nil {
		t.Fatal(err)
	}
	if err := asset.Validate(); err != nil {
		t.Fatal(err)
	}
}

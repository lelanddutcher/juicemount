package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type collectionIndependentVector struct {
	Name         string  `json:"name"`
	Codec        string  `json:"codec"`
	Literal      string  `json:"canonical_literal"`
	Domain       *string `json:"domain"`
	Valid        bool    `json:"expected_valid"`
	Digest       string  `json:"expected_digest_sha256"`
	LiteralBytes int     `json:"literal_bytes"`
	Reason       string  `json:"reason"`
}

func checkCollectionIndependentVector[T any](t *testing.T, v collectionIndependentVector, decode func([]byte) (T, error), canonical func(T) ([]byte, string, error)) {
	t.Helper()
	input := []byte(v.Literal)
	if len(input) != v.LiteralBytes {
		t.Fatal("independent literal byte count mismatch")
	}
	got, err := decode(input)
	if !v.Valid {
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid independent vector must return ErrInvalid: %v", err)
		}
		var zero T
		if !reflect.DeepEqual(got, zero) {
			t.Fatal("invalid independent vector returned a nonzero value")
		}
		return
	}
	if err != nil {
		t.Fatalf("valid independent vector decode: %v", err)
	}
	encoded, digest, err := canonical(got)
	if err != nil {
		t.Fatalf("valid independent vector canonical encoding: %v", err)
	}
	if !bytes.Equal(encoded, input) {
		t.Fatal("independent canonical bytes changed")
	}
	if v.Domain == nil {
		if v.Digest != "" || digest != "" {
			t.Fatal("receipt-only codec has an unexpected digest contract")
		}
	} else {
		if len(v.Digest) != 64 || digest != v.Digest {
			t.Fatal("independent expected digest mismatch")
		}
		h := sha256.New()
		_, _ = h.Write([]byte(*v.Domain))
		_, _ = h.Write(input)
		if hex.EncodeToString(h.Sum(nil)) != v.Digest {
			t.Fatal("independent fixture domain and literal do not bind expected digest")
		}
	}
	roundTrip, err := decode(encoded)
	if err != nil || !reflect.DeepEqual(got, roundTrip) {
		t.Fatalf("independent typed round trip changed: %v", err)
	}
}

func TestCollectionIndependentCanonicalVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "continuity-c04b-codec-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The root froze these independently authored bytes before this adapter.
	fixtureHash := sha256.Sum256(data)
	if hex.EncodeToString(fixtureHash[:]) != "274ddc967a951bab6e68efeceab8a72f927d66df23a8b38a9ed15de0f77d5a9f" {
		t.Fatal("frozen independent fixture bytes changed")
	}
	var fixture struct {
		Version int                           `json:"version"`
		Vectors []collectionIndependentVector `json:"vectors"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatal("independent fixture has trailing content")
	}
	if fixture.Version != 1 || len(fixture.Vectors) != 25 {
		t.Fatal("independent fixture version or case count changed")
	}
	valid, invalid := 0, 0
	names := make(map[string]bool)
	coverage := make(map[string]int)
	for _, v := range fixture.Vectors {
		if v.Name == "" || names[v.Name] || v.Reason == "" {
			t.Fatal("independent fixture needs unique named cases with reasons")
		}
		names[v.Name] = true
		coverage[v.Codec]++
		if v.Valid {
			valid++
		} else {
			invalid++
		}
	}
	if valid != 12 || invalid != 13 {
		t.Fatalf("independent coverage changed: valid=%d invalid=%d", valid, invalid)
	}
	wantCoverage := map[string]int{
		"CollectionRequest":       10,
		"CollectionRecord":        3,
		"CollectionReceipt":       1,
		"CollectionPolicyRequest": 7,
		"CollectionPolicyRecord":  1,
		"CollectionPolicyReceipt": 1,
		"AdminRequestV2":          2,
	}
	if !reflect.DeepEqual(coverage, wantCoverage) {
		t.Fatalf("independent codec coverage changed: %v", coverage)
	}
	for _, v := range fixture.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			switch v.Codec {
			case "CollectionRequest":
				checkCollectionIndependentVector(t, v, DecodeCollectionRequest, CanonicalCollectionRequest)
			case "CollectionRecord":
				checkCollectionIndependentVector(t, v, DecodeCollectionRecord, CanonicalCollectionRecord)
			case "CollectionReceipt":
				checkCollectionIndependentVector(t, v, DecodeCollectionReceipt, func(value CollectionReceipt) ([]byte, string, error) {
					b, err := CanonicalCollectionReceipt(value)
					return b, "", err
				})
			case "CollectionPolicyRequest":
				checkCollectionIndependentVector(t, v, DecodeCollectionPolicyRequest, CanonicalCollectionPolicyRequest)
			case "CollectionPolicyRecord":
				checkCollectionIndependentVector(t, v, DecodeCollectionPolicyRecord, CanonicalCollectionPolicyRecord)
			case "CollectionPolicyReceipt":
				checkCollectionIndependentVector(t, v, DecodeCollectionPolicyReceipt, func(value CollectionPolicyReceipt) ([]byte, string, error) {
					b, err := CanonicalCollectionPolicyReceipt(value)
					return b, "", err
				})
			case "AdminRequestV2":
				checkCollectionIndependentVector(t, v, DecodeAdminRequestV2, CanonicalAdminRequestV2)
			default:
				t.Fatalf("unhandled independent codec: %s", v.Codec)
			}
		})
	}
}

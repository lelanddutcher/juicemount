package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

type savedQueryVector struct {
	Name         string  `json:"name"`
	Codec        string  `json:"codec"`
	Valid        bool    `json:"valid"`
	UTF8         *string `json:"utf8"`
	Hex          *string `json:"hex"`
	Bytes        int     `json:"bytes"`
	SHA256       string  `json:"sha256"`
	DomainDigest string  `json:"domain_digest"`
}

func savedQueryVectors(t *testing.T) []savedQueryVector {
	t.Helper()
	b, err := os.ReadFile("testdata/continuity-c04c-codec-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != "f26c4ab1a777fd3525dd2848179b82812477ca03531c699aaaeb6a57a9235239" {
		t.Fatal("independent saved query fixture changed")
	}
	var fixture struct {
		Version int                `json:"version"`
		Cases   []savedQueryVector `json:"cases"`
	}
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || len(fixture.Cases) != 152 {
		t.Fatal("fixture inventory")
	}
	names := make(map[string]bool)
	positives := 0
	for _, v := range fixture.Cases {
		if v.Name == "" || names[v.Name] {
			t.Fatal("fixture name inventory")
		}
		names[v.Name] = true
		if v.Valid {
			positives++
		}
		switch v.Codec {
		case "definition", "request", "record", "receipt":
		default:
			t.Fatalf("unhandled fixture codec: %s", v.Codec)
		}
	}
	if positives != 31 || len(fixture.Cases)-positives != 121 {
		t.Fatal("fixture verdict inventory")
	}
	return fixture.Cases
}

func savedQueryVectorBytes(t *testing.T, v savedQueryVector) []byte {
	t.Helper()
	if (v.UTF8 == nil) == (v.Hex == nil) {
		t.Fatal("fixture requires exactly one byte representation")
	}
	var b []byte
	if v.UTF8 != nil {
		b = []byte(*v.UTF8)
	} else {
		var err error
		b, err = hex.DecodeString(*v.Hex)
		if err != nil {
			t.Fatal(err)
		}
	}
	sum := sha256.Sum256(b)
	if len(b) != v.Bytes || hex.EncodeToString(sum[:]) != v.SHA256 {
		t.Fatal("fixture byte identity")
	}
	return b
}

func savedQueryLiteral(t *testing.T, name string, target any) []byte {
	t.Helper()
	for _, v := range savedQueryVectors(t) {
		if v.Name == name {
			if !v.Valid {
				t.Fatal("positive literal required")
			}
			b := savedQueryVectorBytes(t, v)
			if target != nil {
				// Independent typed expectation: do not derive it with the codec under test.
				if err := json.Unmarshal(b, target); err != nil {
					t.Fatal(err)
				}
			}
			return b
		}
	}
	t.Fatalf("missing saved query literal %s", name)
	return nil
}

func decodeSavedQueryForTest(t *testing.T, codec string, b []byte) (any, error) {
	t.Helper()
	switch codec {
	case "definition":
		return DecodeSavedQueryDefinition(b)
	case "request":
		return DecodeSavedQueryRequest(b)
	case "record":
		return DecodeSavedQueryRecord(b)
	case "receipt":
		return DecodeSavedQueryReceipt(b)
	default:
		t.Fatalf("unknown codec %s", codec)
		return nil, nil
	}
}

func canonicalSavedQueryForTest(t *testing.T, v any) ([]byte, string, error) {
	t.Helper()
	switch v := v.(type) {
	case SavedQueryDefinition:
		return CanonicalSavedQueryDefinition(v)
	case SavedQueryRequest:
		return CanonicalSavedQueryRequest(v)
	case SavedQueryRecord:
		return CanonicalSavedQueryRecord(v)
	case SavedQueryReceipt:
		b, err := CanonicalSavedQueryReceipt(v)
		return b, "", err
	default:
		t.Fatalf("unknown saved query type %T", v)
		return nil, "", nil
	}
}

func requireSavedQueryZeroError(t *testing.T, value any, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(value, reflect.Zero(reflect.TypeOf(value)).Interface()) {
		t.Fatalf("expected ErrInvalid and zero %T, got %#v, %v", value, value, err)
	}
}

func TestSavedQueryIndependentVectors(t *testing.T) {
	for _, v := range savedQueryVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			input := savedQueryVectorBytes(t, v)
			before := append([]byte(nil), input...)
			got, err := decodeSavedQueryForTest(t, v.Codec, input)
			if !v.Valid {
				requireSavedQueryZeroError(t, got, err)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				want := reflect.New(reflect.TypeOf(got))
				if err := json.Unmarshal(input, want.Interface()); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want.Elem().Interface()) {
					t.Fatal("typed decode differs from fixed literal")
				}
				encoded, digest, err := canonicalSavedQueryForTest(t, want.Elem().Interface())
				if err != nil || !bytes.Equal(encoded, before) || digest != v.DomainDigest {
					t.Fatalf("independent canonical bytes/digest: digest=%s error=%v", digest, err)
				}
				roundtrip, err := decodeSavedQueryForTest(t, v.Codec, encoded)
				if err != nil || !reflect.DeepEqual(roundtrip, want.Elem().Interface()) {
					t.Fatal("typed round trip")
				}
			}
			if !bytes.Equal(input, before) {
				t.Fatal("decoder mutated caller bytes")
			}
		})
	}
}

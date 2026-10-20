package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func readAuthorityVector(t *testing.T, name string, value any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "continuity-c03a-"+name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, value); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityIndependentCanonicalVectors(t *testing.T) {
	type record struct {
		Name    string `json:"name"`
		Request string `json:"request_json"`
		Receipt string `json:"receipt_json"`
		Digest  string `json:"payload_sha256"`
	}
	var policy struct {
		Events []record `json:"events"`
	}
	readAuthorityVector(t, "policy", &policy)
	if len(policy.Events) != 4 {
		t.Fatal("policy fixture incomplete")
	}
	for _, record := range policy.Events {
		t.Run(record.Name, func(t *testing.T) {
			op, err := DecodePolicyRequest([]byte(record.Request))
			if err != nil {
				t.Fatal(err)
			}
			b, digest, err := CanonicalPolicyRequest(op)
			if err != nil || string(b) != record.Request || digest != record.Digest {
				t.Fatalf("independent policy bytes/hash: %v", err)
			}
			var r Receipt
			if err := decodeCanonicalValue([]byte(record.Receipt), &r); err != nil {
				t.Fatal(err)
			}
			e := eventRow{r.Sequence, testLibrary, r.OperationID, b, digest, r.RecordedUnixNano, []byte(record.Receipt)}
			if _, _, err := e.decodePolicy(testLibrary); err != nil {
				t.Fatal(err)
			}
		})
	}
	var admin struct {
		Cases []record `json:"cases"`
	}
	readAuthorityVector(t, "admin", &admin)
	if len(admin.Cases) != 8 {
		t.Fatal("admin fixture incomplete")
	}
	for _, record := range admin.Cases {
		t.Run(record.Name, func(t *testing.T) {
			op, err := DecodeAdminRequest([]byte(record.Request))
			if err != nil {
				t.Fatal(err)
			}
			b, digest, err := CanonicalAdminRequest(op)
			if err != nil || string(b) != record.Request || digest != record.Digest {
				t.Fatalf("independent admin bytes/hash: %v", err)
			}
			var receipt AdminReceipt
			if err := decodeCanonicalValue([]byte(record.Receipt), &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.PayloadSHA256 != digest || receipt.OperationID != op.ID || receipt.LibraryID != op.LibraryID {
				t.Fatal("independent receipt binding")
			}
		})
	}
}

func TestAuthorityIndependentVerifierVector(t *testing.T) {
	var v struct {
		Incarnation string `json:"incarnation"`
		Selector    string `json:"selector"`
		Secret      string `json:"secret_hex"`
		Verifier    string `json:"verifier_sha256"`
	}
	readAuthorityVector(t, "credential", &v)
	secret, err := hex.DecodeString(v.Secret)
	if err != nil {
		t.Fatal(err)
	}
	digest := credentialVerifier(testLibrary, v.Incarnation, v.Selector, secret)
	if hex.EncodeToString(digest[:]) != v.Verifier {
		t.Fatal("independent credential verifier")
	}
	changed := credentialVerifier(otherLibrary, v.Incarnation, v.Selector, secret)
	if changed == digest {
		t.Fatal("verifier did not bind library")
	}
}

func TestAuthorityIndependentPortableVectors(t *testing.T) {
	for _, name := range []string{"empty", "policy-rich"} {
		t.Run(name, func(t *testing.T) {
			var v struct {
				portabilityVector
				PolicyEventCount int64 `json:"policy_event_count"`
				Projections      []struct {
					Kind    byte   `json:"kind"`
					Payload string `json:"payload_json"`
				} `json:"projections"`
			}
			readAuthorityVector(t, name, &v)
			stream, err := hex.DecodeString(v.StreamHex)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(stream)
			if len(stream) != v.StreamBytes || hex.EncodeToString(sum[:]) != v.FileSHA256 {
				t.Fatal("independent stream fixture damaged")
			}
			path := filepath.Join(t.TempDir(), "restored.sqlite")
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits()); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var actual bytes.Buffer
			if err := s.Export(context.Background(), testLibrary, &actual, portabilityLimits()); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual.Bytes(), stream) {
				t.Fatal("restored export changed independent bytes")
			}
			p, rows, err := projectionDigest(context.Background(), s.db, testLibrary, 4096)
			if err != nil || p != v.ProjectionSHA256 || rows != v.ProjectionRows {
				t.Fatalf("independent projection: %d %s %v", rows, p, err)
			}
			count, err := policyTotal(context.Background(), s.db)
			if err != nil || count != v.PolicyEventCount {
				t.Fatal("policy history count")
			}
			for _, frame := range v.Projections {
				switch frame.Kind {
				case 0x17:
					var want Subject
					if err := json.Unmarshal([]byte(frame.Payload), &want); err != nil {
						t.Fatal(err)
					}
					got, err := readSubject(context.Background(), s.db, testLibrary, want.ID)
					if err != nil || got != want {
						t.Fatalf("independent subject projection: %+v %v", got, err)
					}
				case 0x18:
					var want Grant
					if err := json.Unmarshal([]byte(frame.Payload), &want); err != nil {
						t.Fatal(err)
					}
					got, err := readGrant(context.Background(), s.db, testLibrary, want.ID)
					if err != nil {
						t.Fatal(err)
					}
					b, _ := json.Marshal(got)
					if string(b) != frame.Payload {
						t.Fatal("independent grant projection")
					}
				}
			}
			owner, err := s.LocalOwner(testLibrary)
			if err != nil {
				t.Fatal(err)
			}
			status, err := owner.Status(context.Background())
			if err != nil || status != (AuthorityStatus{}) {
				t.Fatal("golden import activated authority")
			}
		})
	}
}

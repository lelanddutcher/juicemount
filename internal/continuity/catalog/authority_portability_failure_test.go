package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const authorityGoldenMagic = "juicemount.catalog.export\x00\x02"

func authorityGoldenStream(t *testing.T, name string) []byte {
	t.Helper()
	var v portabilityVector
	readAuthorityVector(t, name, &v)
	wire, err := hex.DecodeString(v.StreamHex)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	if len(wire) != v.StreamBytes || hex.EncodeToString(digest[:]) != v.FileSHA256 {
		t.Fatal("authority golden stream damaged")
	}
	return wire
}

func authorityGoldenFrame(kind byte, payload []byte) []byte {
	out := make([]byte, 5, 5+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:], uint32(len(payload)))
	return append(out, payload...)
}

func authorityGoldenJoin(frames [][]byte) []byte {
	out := []byte(authorityGoldenMagic)
	for _, frame := range frames {
		out = append(out, frame...)
	}
	return out
}

func authorityRequireAbsentTarget(t *testing.T, path string) {
	t.Helper()
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("early refusal reserved target %q: %v", suffix, err)
		}
	}
}

func authorityIncompleteCounts(t *testing.T, path string) (metadata, policies, subjects, grants int) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("post-header failure lost reserved target: %v", err)
	}
	assertIncomplete(t, path)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM policy_events),(SELECT count(*) FROM policy_subjects),(SELECT count(*) FROM policy_grants)`).Scan(&metadata, &policies, &subjects, &grants); err != nil {
		t.Fatal(err)
	}
	return
}

func TestAuthorityFormat2HeaderRefusalBeforeReservation(t *testing.T) {
	valid := streamFrames(t, authorityGoldenStream(t, "empty"))[0][5:]
	for _, tc := range []struct{ name, old, replacement string }{
		{"missing_zero_policy_count", `,"policy_event_count":0`, ""},
		{"missing_zero_metadata_count", `,"event_count":0`, ""},
		{"null_policy_count", `"policy_event_count":0`, `"policy_event_count":null`},
		{"negative_policy_count", `"policy_event_count":0`, `"policy_event_count":-1`},
		{"duplicate_policy_count", `"policy_event_count":0`, `"policy_event_count":0,"policy_event_count":0`},
		{"unknown_field", `"version":2`, `"version":2,"unknown":0`},
		{"cross_pair_2_2", `"schema_version":3`, `"schema_version":2`},
		{"cross_pair_1_3", `"version":2`, `"version":1`},
		{"wrong_library", string(testLibrary), string(otherLibrary)},
		{"noncanonical_whitespace", `"version":2`, `"version": 2`},
		{"policy_count_overflow", `"policy_event_count":0`, `"policy_event_count":9223372036854775808`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if bytes.Count(valid, []byte(tc.old)) != 1 {
				t.Fatal("header mutation target absent")
			}
			header := bytes.Replace(valid, []byte(tc.old), []byte(tc.replacement), 1)
			path := filepath.Join(t.TempDir(), "rejected.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(authorityGoldenJoin([][]byte{authorityGoldenFrame(1, header)})), portabilityLimits())
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("invalid header: %v", err)
			}
			authorityRequireAbsentTarget(t, path)
		})
	}
	t.Run("format1_magic_with_format2_header", func(t *testing.T) {
		wire := append([]byte("juicemount.catalog.export\x00\x01"), authorityGoldenFrame(1, valid)...)
		path := filepath.Join(t.TempDir(), "cross-magic.sqlite")
		if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), portabilityLimits()); !errors.Is(err, ErrFormat) {
			t.Fatalf("cross magic/header: %v", err)
		}
		authorityRequireAbsentTarget(t, path)
	})
}

func TestAuthorityFormat2CombinedEventsAndIncrementalPolicyRows(t *testing.T) {
	wire := authorityGoldenStream(t, "policy-rich")
	t.Run("combined_events_preflight", func(t *testing.T) {
		limits := portabilityLimits()
		limits.MaxEvents = 5 // Each stream fits separately (2 and 4); their sum does not.
		path := filepath.Join(t.TempDir(), "sum.sqlite")
		if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), limits); !errors.Is(err, ErrLimit) {
			t.Fatalf("combined event budget: %v", err)
		}
		authorityRequireAbsentTarget(t, path)
	})
	for _, tc := range []struct {
		name               string
		frameCount         int
		rowLimit           int64
		policies, subjects int
	}{
		{"first_subject", 4, 5, 0, 0},
		{"second_policy_first_grant", 5, 8, 1, 1},
		{"second_policy_second_grant", 5, 9, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames := streamFrames(t, wire)
			prefix := bytes.NewReader(authorityGoldenJoin(frames[:tc.frameCount]))
			overreads := 0
			reader := portReadFunc(func(p []byte) (int, error) {
				if prefix.Len() == 0 {
					overreads++
					return 0, errors.New("read beyond over-budget policy")
				}
				return prefix.Read(p)
			})
			limits := portabilityLimits()
			limits.MaxProjectionRows = tc.rowLimit
			path := filepath.Join(t.TempDir(), "rows.sqlite")
			if err := Restore(context.Background(), path, testLibrary, reader, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("incremental policy row limit: %v", err)
			}
			if overreads != 0 {
				t.Fatal("row budget deferred until a later frame/footer")
			}
			m, p, s, g := authorityIncompleteCounts(t, path)
			if m != 2 || p != tc.policies || s != tc.subjects || g != 0 {
				t.Fatalf("over-budget policy batch partially committed: %d/%d/%d/%d", m, p, s, g)
			}
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), portabilityLimits()); err == nil {
				t.Fatal("restore adopted incomplete target")
			}
			m2, p2, s2, g2 := authorityIncompleteCounts(t, path)
			if m2 != m || p2 != p || s2 != s || g2 != g {
				t.Fatal("retry changed incomplete state")
			}
		})
	}
	t.Run("exact_limits_include_tombstones_and_corrections", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "exact.sqlite")
		limits := Limits{MaxBytes: int64(len(wire)), MaxEvents: 6, MaxProjectionRows: 10}
		if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), limits); err != nil {
			t.Fatal(err)
		}
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var out bytes.Buffer
		if err := s.Export(context.Background(), testLibrary, &out, limits); err != nil || !bytes.Equal(out.Bytes(), wire) {
			t.Fatalf("exact limit roundtrip: %v", err)
		}
		out.Reset()
		limits.MaxEvents = 5
		if err := s.Export(context.Background(), testLibrary, &out, limits); !errors.Is(err, ErrLimit) || out.Len() != 0 {
			t.Fatalf("combined export budget was not preflighted: %d %v", out.Len(), err)
		}
	})
}

// Rebind the stream digest independently of the production framing/hash helpers.
// The caller deliberately retains or forges P2; valid H2 must not substitute for
// replay-versus-source projection equality.
func authorityRehashFrames(t *testing.T, frames [][]byte, projectionOverride string) []byte {
	t.Helper()
	var footer struct {
		EventCount       int64  `json:"event_count"`
		PolicyEventCount int64  `json:"policy_event_count"`
		ProjectionRows   int64  `json:"projection_rows"`
		ProjectionSHA    string `json:"projection_sha256"`
		StreamSHA        string `json:"stream_sha256"`
	}
	last := len(frames) - 1
	if err := json.Unmarshal(frames[last][5:], &footer); err != nil {
		t.Fatal(err)
	}
	if projectionOverride != "" {
		footer.ProjectionSHA = projectionOverride
	}
	projection, err := hex.DecodeString(footer.ProjectionSHA)
	if err != nil || len(projection) != 32 {
		t.Fatal("invalid fixture projection hash")
	}
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x02"))
	h.Write(authorityGoldenJoin(frames[:last]))
	var counters [24]byte
	binary.BigEndian.PutUint64(counters[0:8], uint64(footer.EventCount))
	binary.BigEndian.PutUint64(counters[8:16], uint64(footer.PolicyEventCount))
	binary.BigEndian.PutUint64(counters[16:24], uint64(footer.ProjectionRows))
	h.Write(counters[:])
	h.Write(projection)
	footer.StreamSHA = hex.EncodeToString(h.Sum(nil))
	data, err := json.Marshal(footer)
	if err != nil {
		t.Fatal(err)
	}
	frames[last] = authorityGoldenFrame(3, data)
	return authorityGoldenJoin(frames)
}

func TestAuthorityFormat2RehashedForgeryStillRequiresProjectionEquality(t *testing.T) {
	wire := authorityGoldenStream(t, "policy-rich")
	for _, name := range []string{"changed_grant_history", "forged_projection_digest"} {
		t.Run(name, func(t *testing.T) {
			frames := streamFrames(t, wire)
			override := ""
			if name == "changed_grant_history" {
				payload := frames[6][5:] // metadata2 + policy4; final policy reactivates grant.
				n := int(binary.BigEndian.Uint32(payload[:4]))
				request := append([]byte(nil), payload[4:4+n]...)
				old := []byte(`"actions":["assets.discover","locations.read"]`)
				if bytes.Count(request, old) != 1 {
					t.Fatal("final corrected grant fixture absent")
				}
				request = bytes.Replace(request, old, []byte(`"actions":["assets.discover","locations.read","revisions.read"]`), 1)
				var receipt Receipt
				if err := json.Unmarshal(payload[4+n+4:], &receipt); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(append([]byte("juicemount.catalog.policy.operation\x00\x01"), request...))
				receipt.PayloadSHA256 = hex.EncodeToString(digest[:])
				receiptBytes, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				encoded := make([]byte, 4)
				binary.BigEndian.PutUint32(encoded, uint32(len(request)))
				encoded = append(encoded, request...)
				var length [4]byte
				binary.BigEndian.PutUint32(length[:], uint32(len(receiptBytes)))
				encoded = append(encoded, length[:]...)
				encoded = append(encoded, receiptBytes...)
				frames[6] = authorityGoldenFrame(4, encoded)
			} else {
				override = strings.Repeat("0", 64)
			}
			forged := authorityRehashFrames(t, frames, override)
			path := filepath.Join(t.TempDir(), "forged.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(forged), portabilityLimits())
			if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "restored projection does not match source snapshot") {
				t.Fatalf("format2 source-history/projection divergence was not rejected at projection equality: %v", err)
			}
			m, p, s, g := authorityIncompleteCounts(t, path)
			if m != 2 || p != 4 || s != 3 || g != 2 {
				t.Fatalf("forgery failed before complete valid replay: %d/%d/%d/%d", m, p, s, g)
			}
		})
	}
}

func TestAuthorityFormat2TruncationCorruptionAndReaderErrors(t *testing.T) {
	wire := authorityGoldenStream(t, "policy-rich")
	frames := streamFrames(t, wire)
	readerErr := errors.New("format2 reader acknowledged bytes with failure")
	for _, name := range []string{"truncated_policy", "missing_footer", "truncated_footer", "corrupt_policy_request", "full_footer_plus_error", "late_error_after_footer", "trailing_byte"} {
		t.Run(name, func(t *testing.T) {
			var reader io.Reader
			switch name {
			case "truncated_policy":
				prefix := authorityGoldenJoin(frames[:4])
				reader = bytes.NewReader(prefix[:len(prefix)-1])
			case "missing_footer":
				reader = bytes.NewReader(authorityGoldenJoin(frames[:len(frames)-1]))
			case "truncated_footer":
				reader = bytes.NewReader(wire[:len(wire)-1])
			case "corrupt_policy_request":
				copyFrames := streamFrames(t, wire)
				payload := bytes.Replace(copyFrames[3][5:], []byte("SYNTHETIC owner"), []byte("SYNTHETIC other"), 1)
				if bytes.Equal(payload, copyFrames[3][5:]) {
					t.Fatal("policy corruption target absent")
				}
				copyFrames[3] = authorityGoldenFrame(4, payload)
				reader = bytes.NewReader(authorityGoldenJoin(copyFrames))
			case "full_footer_plus_error":
				source := bytes.NewReader(wire)
				reader = portReadFunc(func(p []byte) (int, error) {
					n, err := source.Read(p)
					if source.Len() == 0 {
						return n, readerErr
					}
					return n, err
				})
			case "late_error_after_footer":
				reader = io.MultiReader(bytes.NewReader(wire), portReadFunc(func([]byte) (int, error) { return 0, readerErr }))
			case "trailing_byte":
				reader = bytes.NewReader(append(append([]byte(nil), wire...), '\n'))
			}
			path := filepath.Join(t.TempDir(), "failed.sqlite")
			err := Restore(context.Background(), path, testLibrary, reader, portabilityLimits())
			if err == nil {
				t.Fatal("damaged format2 input accepted")
			}
			if strings.Contains(name, "error") && !errors.Is(err, readerErr) {
				t.Fatalf("reader error lost: %v", err)
			}
			authorityIncompleteCounts(t, path)
		})
	}
}

func TestAuthorityFormat2ImportedHistoryHasNoOperationalAuthority(t *testing.T) {
	wire := authorityGoldenStream(t, "policy-rich")
	path := filepath.Join(t.TempDir(), "imported.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	authorityRequireOperationalEmpty(t, s)
	var vectors struct {
		Events []struct {
			Request string `json:"request_json"`
			Receipt string `json:"receipt_json"`
		} `json:"events"`
	}
	readAuthorityVector(t, "policy", &vectors)
	if len(vectors.Events) != 4 {
		t.Fatal("policy oracle incomplete")
	}
	for i, event := range vectors.Events {
		var request, receipt []byte
		if err := s.db.QueryRow("SELECT request,receipt FROM policy_events WHERE sequence=?", i+1).Scan(&request, &receipt); err != nil ||
			string(request) != event.Request || string(receipt) != event.Receipt {
			t.Fatalf("imported policy bytes changed: %d %v", i+1, err)
		}
	}
	var credential struct {
		Bearer string `json:"bearer"`
	}
	readAuthorityVector(t, "credential", &credential)
	if _, err := s.Authenticate(context.Background(), testLibrary, []byte(credential.Bearer)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("imported synthetic credential authorized: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("restored authority database is not private: %v", err)
	}
}

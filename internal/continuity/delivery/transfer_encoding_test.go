package delivery

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

const transferRequestGolden = `{"version":1,"library_id":"lib_11111111111111111111111111111111","selection_id":"dsel_22222222222222222222222222222222","operation_id":"copy-op","offset":0,"prefix_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`

// Independently fixed SHA256(domain || literal request), not an exporter/decoder
// self-comparison. Content's published Go field names are intentionally retained.
const transferRequestDigestGolden = "42c6e5b51f120d52fb6a8d4f5518963f0ae50d4d8ca276c0f875d225c5db0c60"
const transferHeaderGolden = `{"version":1,"request_sha256":"42c6e5b51f120d52fb6a8d4f5518963f0ae50d4d8ca276c0f875d225c5db0c60","authority_incarnation":"auth_33333333333333333333333333333333","selection":{"version":1,"library_id":"lib_11111111111111111111111111111111","selection_id":"dsel_22222222222222222222222222222222","operation_id":"select-op","asset_id":"asset_44444444444444444444444444444444","revision_id":"rev1_5555555555555555555555555555555555555555555555555555555555555555","source_id":"dsrc_66666666666666666666666666666666","source_version":1,"content":{"SHA256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","Size":3},"expires_unix":1800000000,"canceled":false},"attempt":{"attempt_id":"dmat_77777777777777777777777777777777","selection_id":"dsel_22222222222222222222222222222222","operation_id":"copy-op","state":"COMPLETED","content":{"SHA256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","Size":3}},"offset":0,"length":3,"prefix_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`
const transferEndGolden = `{"version":1,"request_sha256":"42c6e5b51f120d52fb6a8d4f5518963f0ae50d4d8ca276c0f875d225c5db0c60","attempt_id":"dmat_77777777777777777777777777777777","offset":0,"length":3,"result":"server_stream_complete"}`

func transferWireFixture() (TransferRequest, catalog.DeliverySelection, TransferHeader, TransferEnd) {
	r := TransferRequest{1, continuity.LibraryID("lib_" + strings.Repeat("1", 32)), "dsel_" + strings.Repeat("2", 32), "copy-op", 0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	s := catalog.DeliverySelection{Version: 1, LibraryID: r.LibraryID, SelectionID: r.SelectionID, OperationID: "select-op", AssetID: continuity.AssetID("asset_" + strings.Repeat("4", 32)), RevisionID: continuity.RevisionID("rev1_" + strings.Repeat("5", 64)), SourceID: "dsrc_" + strings.Repeat("6", 32), SourceVersion: 1, Content: continuity.Content{SHA256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", Size: 3}, ExpiresUnix: 1800000000}
	a := catalog.MaterializationAttempt{ID: "dmat_" + strings.Repeat("7", 32), SelectionID: r.SelectionID, OperationID: r.OperationID, State: "COMPLETED", Content: s.Content}
	h := TransferHeader{1, transferRequestDigestGolden, "auth_" + strings.Repeat("3", 32), s, a, 0, 3, r.PrefixSHA256}
	e := TransferEnd{1, transferRequestDigestGolden, a.ID, 0, 3, "server_stream_complete"}
	return r, s, h, e
}

func TestTransferCanonicalWireLiteral(t *testing.T) {
	r, s, h, e := transferWireFixture()
	b, digest, err := CanonicalTransferRequest(r)
	if err != nil || string(b) != transferRequestGolden || digest != transferRequestDigestGolden {
		t.Fatalf("request/domain changed: %s %s %v", b, digest, err)
	}
	hb, err := CanonicalTransferHeader(h)
	if err != nil || string(hb) != transferHeaderGolden {
		t.Fatalf("header bytes changed: %s %v", hb, err)
	}
	eb, err := CanonicalTransferEnd(e)
	if err != nil || string(eb) != transferEndGolden {
		t.Fatalf("end bytes changed: %s %v", eb, err)
	}
	if got, err := DecodeTransferRequest([]byte(transferRequestGolden)); err != nil || got != r {
		t.Fatalf("literal request: %+v %v", got, err)
	}
	if got, err := DecodeTransferHeader([]byte(transferHeaderGolden)); err != nil || got != h {
		t.Fatalf("literal header: %+v %v", got, err)
	}
	if got, err := DecodeTransferEnd([]byte(transferEndGolden)); err != nil || got != e {
		t.Fatalf("literal end: %+v %v", got, err)
	}
	if err := ValidateTransferResponse(r, s, h, &e); err != nil {
		t.Fatal(err)
	}
}

func TestTransferCanonicalMaximumVectors(t *testing.T) {
	// Actual Go encoding/json vectors for existing DTOs and new typed wire.
	// No media allocation is needed to validate the 1GiB content claim's width.
	r, s, h, e := transferWireFixture()
	r.LibraryID = continuity.LibraryID("lib_" + strings.Repeat("f", 32))
	r.SelectionID = "dsel_" + strings.Repeat("f", 32)
	r.OperationID = strings.Repeat("a", 128)
	r.Offset = 1 << 30
	r.PrefixSHA256 = strings.Repeat("f", 64)
	s.LibraryID = r.LibraryID
	s.SelectionID = r.SelectionID
	s.OperationID = strings.Repeat("a", 128)
	s.AssetID = continuity.AssetID("asset_" + strings.Repeat("f", 32))
	s.RevisionID = continuity.RevisionID("rev1_" + strings.Repeat("f", 64))
	s.SourceID = "dsrc_" + strings.Repeat("f", 32)
	s.SourceVersion = math.MaxInt64
	s.Content = continuity.Content{SHA256: strings.Repeat("f", 64), Size: 1 << 30}
	s.ExpiresUnix = math.MaxInt64
	h = TransferHeader{1, strings.Repeat("f", 64), "auth_" + strings.Repeat("f", 32), s, catalog.MaterializationAttempt{ID: "dmat_" + strings.Repeat("f", 32), SelectionID: s.SelectionID, OperationID: r.OperationID, State: "COMPLETED", Content: s.Content}, 1 << 29, 1 << 29, r.PrefixSHA256}
	e = TransferEnd{1, h.RequestSHA256, h.Attempt.ID, h.Offset, h.Length, "server_stream_complete"}
	selectRequest := catalog.DeliverySelectRequest{Version: 1, LibraryID: s.LibraryID, ID: s.OperationID, AssetID: s.AssetID, Mode: "exact", RevisionID: s.RevisionID, ExpiresUnix: math.MaxInt64, MaxBytes: 1 << 30}
	identity := privatefile.Identity{Device: math.MaxUint64, Inode: math.MaxUint64, Size: 1 << 30, UID: math.MaxUint32, Mode: 33152}
	for _, tc := range []struct {
		name  string
		value any
		size  int
	}{
		{"content", s.Content, 95}, {"selection", s, 651}, {"attempt", h.Attempt, 381}, {"select_request", selectRequest, 422}, {"identity", identity, 108},
		{"request", r, 369}, {"header", h, 1338}, {"end", e, 222},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.value)
			if err != nil || len(b) != tc.size {
				t.Fatalf("maximum vector = %d want %d: %s %v", len(b), tc.size, b, err)
			}
		})
	}
	if b, _, err := CanonicalTransferRequest(r); err != nil || len(b) != 369 {
		t.Fatalf("maximum request: %d %v", len(b), err)
	}
	if b, err := CanonicalTransferHeader(h); err != nil || len(b) != 1338 {
		t.Fatalf("maximum header: %d %v", len(b), err)
	}
	if b, err := CanonicalTransferEnd(e); err != nil || len(b) != 222 {
		t.Fatalf("maximum end: %d %v", len(b), err)
	}
	// Offset/length subtraction must refuse overflow rather than wrap.
	h.Length = math.MaxInt64
	if _, err := CanonicalTransferHeader(h); !errors.Is(err, ErrInvalidTransfer) {
		t.Fatal("overflow header accepted")
	}
	e.Offset = math.MaxInt64
	e.Length = math.MaxInt64
	if _, err := CanonicalTransferEnd(e); !errors.Is(err, ErrInvalidTransfer) {
		t.Fatal("overflow end accepted")
	}
}

func TestTransferCanonicalStrictDecoders(t *testing.T) {
	for _, tc := range []struct {
		name, valid string
		limit       int
		decode      func([]byte) error
	}{
		{"request", transferRequestGolden, MaxTransferRequestBytes, func(b []byte) error { _, err := DecodeTransferRequest(b); return err }},
		{"header", transferHeaderGolden, MaxTransferHeaderBytes, func(b []byte) error { _, err := DecodeTransferHeader(b); return err }},
		{"end", transferEndGolden, MaxTransferEndBytes, func(b []byte) error { _, err := DecodeTransferEnd(b); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cases := map[string][]byte{
				"empty": nil, "null": []byte("null"), "array": []byte("[]"), "missing": []byte("{}"), "trailing": []byte(tc.valid + "{}"), "whitespace": []byte(tc.valid + "\n"),
				"duplicate":     []byte(strings.Replace(tc.valid, `"version":1`, `"version":1,"version":1`, 1)),
				"unknown":       []byte(strings.Replace(tc.valid, `"version":1`, `"version":1,"unknown":1`, 1)),
				"null_version":  []byte(strings.Replace(tc.valid, `"version":1`, `"version":null`, 1)),
				"wrong_version": []byte(strings.Replace(tc.valid, `"version":1`, `"version":2`, 1)),
				"invalid_utf8":  append([]byte(tc.valid[:len(tc.valid)-1]), 0xff, '}'), "oversize": bytes.Repeat([]byte("x"), tc.limit+1),
			}
			if tc.name == "header" {
				selectionAt := strings.Index(tc.valid, `"selection":`)
				attemptAt := strings.Index(tc.valid, `,"attempt":`)
				cases["null_selection"] = []byte(tc.valid[:selectionAt] + `"selection":null` + tc.valid[attemptAt:])
				cases["canceled"] = []byte(strings.Replace(tc.valid, `"canceled":false`, `"canceled":true`, 1))
				cases["prepared"] = []byte(strings.Replace(tc.valid, `"state":"COMPLETED"`, `"state":"PREPARED"`, 1))
			}
			if tc.name == "request" {
				cases["zero_selection_id"] = []byte(strings.Replace(tc.valid, "dsel_"+strings.Repeat("2", 32), "dsel_"+strings.Repeat("0", 32), 1))
				cases["negative_offset"] = []byte(strings.Replace(tc.valid, `"offset":0`, `"offset":-1`, 1))
				cases["past_limit"] = []byte(strings.Replace(tc.valid, `"offset":0`, `"offset":1073741825`, 1))
				cases["wrong_empty_prefix"] = []byte(strings.Replace(tc.valid, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", strings.Repeat("a", 64), 1))
				cases["unsafe_operation"] = []byte(strings.Replace(tc.valid, "copy-op", "copy/op", 1))
			}
			for name, b := range cases {
				t.Run(name, func(t *testing.T) {
					if err := tc.decode(b); !errors.Is(err, ErrInvalidTransfer) {
						t.Fatalf("strict refusal: %s %v", b, err)
					}
				})
			}
		})
	}
}

func TestTransferResponseBindsCompleteRetainedPin(t *testing.T) {
	r, s, h, e := transferWireFixture()
	// Mutating the retained expected pin leaves the response fully canonical.
	// Every selection field, including canceled/version, participates in equality.
	changes := map[string]func(*catalog.DeliverySelection){
		"version": func(s *catalog.DeliverySelection) { s.Version++ }, "library": func(s *catalog.DeliverySelection) {
			s.LibraryID = continuity.LibraryID("lib_" + strings.Repeat("a", 32))
		}, "selection": func(s *catalog.DeliverySelection) { s.SelectionID = "dsel_" + strings.Repeat("a", 32) }, "operation": func(s *catalog.DeliverySelection) { s.OperationID = "other-select" }, "asset": func(s *catalog.DeliverySelection) { s.AssetID = continuity.AssetID("asset_" + strings.Repeat("a", 32)) }, "revision": func(s *catalog.DeliverySelection) {
			s.RevisionID = continuity.RevisionID("rev1_" + strings.Repeat("a", 64))
		}, "source": func(s *catalog.DeliverySelection) { s.SourceID = "dsrc_" + strings.Repeat("a", 32) }, "source_version": func(s *catalog.DeliverySelection) { s.SourceVersion++ }, "digest": func(s *catalog.DeliverySelection) { s.Content.SHA256 = strings.Repeat("a", 64) }, "size": func(s *catalog.DeliverySelection) { s.Content.Size++ }, "expiry": func(s *catalog.DeliverySelection) { s.ExpiresUnix++ }, "canceled": func(s *catalog.DeliverySelection) { s.Canceled = true },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			expected := s
			change(&expected)
			if err := ValidateTransferResponse(r, expected, h, &e); !errors.Is(err, ErrInvalidTransfer) {
				t.Fatalf("changed retained %s accepted: %v", name, err)
			}
		})
	}
	for _, name := range []string{"revision", "source", "source_version", "expiry", "digest", "size"} {
		t.Run("canonical_header_"+name, func(t *testing.T) {
			changed := h
			changes[name](&changed.Selection)
			changed.Attempt.Content = changed.Selection.Content
			changed.Length = changed.Selection.Content.Size - changed.Offset
			if _, err := CanonicalTransferHeader(changed); err != nil {
				t.Fatalf("test must retain internally valid wire: %v", err)
			}
			if err := ValidateTransferResponse(r, s, changed, nil); !errors.Is(err, ErrInvalidTransfer) {
				t.Fatalf("same-ID changed %s pin accepted: %v", name, err)
			}
		})
	}
	for name, change := range map[string]func(*TransferEnd){"request": func(e *TransferEnd) { e.RequestSHA256 = strings.Repeat("a", 64) }, "attempt": func(e *TransferEnd) { e.AttemptID = "dmat_" + strings.Repeat("a", 32) }, "offset": func(e *TransferEnd) { e.Offset++ }, "length": func(e *TransferEnd) { e.Length-- }} {
		t.Run("end_"+name, func(t *testing.T) {
			changed := e
			change(&changed)
			if err := ValidateTransferResponse(r, s, h, &changed); !errors.Is(err, ErrInvalidTransfer) {
				t.Fatalf("changed end link accepted: %v", err)
			}
		})
	}
}

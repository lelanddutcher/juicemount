package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This bounded wire-only harness deliberately does not stand in for Restore.
// In particular ABA, historical CAS, quota and final projection negatives must
// be exercised by real transactional replay once that implementation exists.
func checkCollectionWireFixture(f collectionWireFixture) (string, error) {
	raw, err := hexDecodeCollectionFixture(f.StreamHex)
	if err != nil {
		return "fixture", err
	}
	in := streamInput{ctx: context.Background(), r: bytes.NewReader(raw), max: MaxStreamBytes}
	magic := make([]byte, len(streamMagic3))
	if err := in.exact(magic); err != nil {
		return "magic", err
	}
	if string(magic) != streamMagic3 {
		return "magic", fmt.Errorf("magic")
	}
	h := collectionStreamHash()
	h.Write(magic)
	_, data, err := in.collectionFrame(false, 1, h)
	if err != nil {
		return "header", err
	}
	header, err := decodeCollectionHeader(data, f.LibraryID, collectionWireLimits())
	if err != nil {
		return "header", err
	}
	for n := int64(1); n <= header.EventCount; n++ {
		_, data, err := in.collectionFrame(false, 2, h)
		if err != nil {
			return "metadata", err
		}
		if _, _, err := decodeStreamEvent(data, f.LibraryID, n); err != nil {
			return "metadata", err
		}
	}
	var last CollectionID
	for n := int64(0); n < header.CollectionCount; n++ {
		_, data, err := in.collectionFrame(false, 5, h)
		if err != nil {
			return "declaration", err
		}
		var d collectionDeclaration
		if err := canonicalJSON(data, &d); err != nil {
			return "declaration", err
		}
		if err := d.validate(f.LibraryID); err != nil {
			return "declaration", err
		}
		if d.CollectionID <= last {
			return "declaration", fmt.Errorf("order")
		}
		last = d.CollectionID
	}
	var legacy, content, policy int64
	for n := int64(0); n < header.TransactionCount; n++ {
		kind, data, err := in.collectionFrame(true, 0, h)
		if err != nil {
			return "transaction", err
		}
		switch kind {
		case 4:
			legacy++
			if _, _, err := decodeStreamPolicyEvent(data, f.LibraryID, legacy); err != nil {
				return "legacy", err
			}
		case 6:
			policy++
			p, _, err := decodeCollectionPolicyPair(data, f.LibraryID)
			if err != nil {
				return "owner", err
			}
			if p.Administration != "local_owner" {
				return "owner", fmt.Errorf("generated owner")
			}
		case 7:
			content++
			_, _, p, _, err := decodeCollectionContentPair(data, f.LibraryID)
			if err != nil {
				return "content", err
			}
			if p != nil {
				policy++
			}
		}
	}
	if legacy != header.PolicyEventCount || content != header.CollectionEventCount || policy != header.CollectionPolicyEventCount {
		return "counts", fmt.Errorf("family counts")
	}
	_, data, err = in.collectionFrame(false, 3, nil)
	if err != nil {
		return "footer", err
	}
	footer, err := decodeCollectionFooter(data, header.collectionStreamCounts, collectionWireLimits())
	if err != nil {
		return "footer", err
	}
	digest, err := finishCollectionStreamHash(h, header.collectionStreamCounts, footer.ProjectionRows, footer.ProjectionSHA256)
	if err != nil {
		return "digest", err
	}
	if digest != footer.StreamSHA256 {
		return "digest", fmt.Errorf("digest")
	}
	return "end", in.end()
}
func hexDecodeCollectionFixture(s string) ([]byte, error) { return hex.DecodeString(s) }
func TestCollectionPortabilityFrozenWireNegatives(t *testing.T) {
	b, err := os.ReadFile("testdata/continuity-c04b-operations.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		NegativeStreams []struct {
			ID     string                `json:"id"`
			Vector collectionWireFixture `json:"vector"`
		} `json:"negative_streams"`
	}
	if err := json.Unmarshal(b, &vectors); err != nil {
		t.Fatal(err)
	}
	wanted := map[string]string{"generated_child_actor_mismatch": "content", "generated_child_wrong_parent_digest": "content", "generated_child_time_mismatch": "content", "content_receipt_token_mismatch": "content", "missing_zero_header_field": "header", "wrong_schema_for_format3": "header", "header_count_overflow": "header", "impossible_owner_child_count": "header", "final_trailing_byte": "end", "missing_footer_last_byte": "footer"}
	for _, name := range []string{"empty", "shared"} {
		f := readCollectionWireFixture(t, name)
		if stage, err := checkCollectionWireFixture(f); err != nil {
			t.Fatalf("unchanged %s positive: %s: %v", name, stage, err)
		}
	}
	seen := 0
	for _, v := range vectors.NegativeStreams {
		expected, ok := wanted[v.ID]
		if !ok {
			continue
		}
		seen++
		t.Run(v.ID, func(t *testing.T) {
			stage, err := checkCollectionWireFixture(v.Vector)
			if err == nil || stage != expected {
				t.Fatalf("expected %s rejection, got %s: %v", expected, stage, err)
			}
		})
	}
	if seen != len(wanted) {
		t.Fatalf("executed %d frozen controls, want %d", seen, len(wanted))
	}
}
func TestCollectionPortabilityStrictHeader(t *testing.T) {
	f := readCollectionWireFixture(t, "empty")
	data := f.HeaderJSON
	cases := map[string]string{
		"missing_zero":  strings.Replace(data, `"collection_count":0,`, "", 1),
		"unknown":       strings.TrimSuffix(data, "}") + `,"extra":0}`,
		"duplicate":     strings.TrimSuffix(data, "}") + `,"event_count":0}`,
		"null":          strings.Replace(data, `"collection_count":0`, `"collection_count":null`, 1),
		"whitespace":    data + " ",
		"wrong_library": strings.Replace(data, string(f.LibraryID), "lib_00000000000000000000000000000002", 1),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeCollectionHeader([]byte(bad), f.LibraryID, collectionWireLimits()); err == nil {
				t.Fatal("accepted malformed header")
			}
		})
	}
}
func TestCollectionPortabilityFrameAdmissionBeforePayload(t *testing.T) {
	for _, kind := range []byte{1, 2, 3, 4, 5, 6, 7} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			maximum := uint32(1024)
			transaction := kind == 4 || kind == 6 || kind == 7
			switch kind {
			case 2, 4:
				maximum = maxEventPayload
			case 6:
				maximum = maxCollectionOwnerPayload
			case 7:
				maximum = maxCollectionContentPayload
			}
			for _, length := range []uint32{0, maximum + 1} {
				header := frameHeader(kind, int(length))
				r := bytes.NewReader(append(header[:], 0xaa))
				in := streamInput{ctx: context.Background(), r: r, max: MaxStreamBytes}
				if _, _, err := in.collectionFrame(transaction, kind, nil); err == nil {
					t.Fatal("accepted invalid length")
				}
				if r.Len() != 1 {
					t.Fatal("read body before length admission")
				}
			}
			header := frameHeader(kind, int(maximum))
			r := bytes.NewReader(append(header[:], 0xaa))
			in := streamInput{ctx: context.Background(), r: r, max: 5 + int64(maximum) - 1}
			if _, _, err := in.collectionFrame(transaction, kind, nil); !errors.Is(err, ErrLimit) {
				t.Fatalf("remaining byte budget: %v", err)
			}
			if r.Len() != 1 {
				t.Fatal("read body before budget admission")
			}
		})
	}
}
func TestCollectionPortabilityPairBoundaries(t *testing.T) {
	valid := appendCollectionPair(nil, []byte("a"), []byte("b"))
	for end := 0; end < len(valid); end++ {
		if _, _, _, err := takeCollectionPair(valid[:end], 1, false); err == nil {
			t.Fatalf("accepted truncated pair at %d", end)
		}
	}
	empty := make([]byte, 8)
	if _, _, rest, err := takeCollectionPair(empty, 1, true); err != nil || len(rest) != 0 {
		t.Fatalf("absent child: %v", err)
	}
	if _, _, _, err := takeCollectionPair(empty, 1, false); err == nil {
		t.Fatal("accepted absent required pair")
	}
	for _, pair := range [][]byte{appendCollectionPair(nil, nil, []byte("b")), appendCollectionPair(nil, []byte("a"), nil)} {
		if _, _, _, err := takeCollectionPair(pair, 1, true); err == nil {
			t.Fatal("accepted half absent pair")
		}
	}
	// Announced record size is checked before an integer conversion or slice.
	huge := make([]byte, 8)
	binary.BigEndian.PutUint32(huge, ^uint32(0))
	if _, _, _, err := takeCollectionPair(huge, MaxCollectionRecordBytes, false); err == nil {
		t.Fatal("accepted oversized record")
	}
}

func TestCollectionPortabilityPublicFrozenNegatives(t *testing.T) {
	data, err := os.ReadFile("testdata/continuity-c04b-operations.json")
	if err != nil {
		t.Fatal(err)
	}
	var inputs struct {
		Negatives []struct {
			ID       string                `json:"id"`
			Expected string                `json:"expected"`
			Vector   collectionWireFixture `json:"vector"`
		} `json:"negative_streams"`
	}
	if err := json.Unmarshal(data, &inputs); err != nil {
		t.Fatal(err)
	}
	if len(inputs.Negatives) != 16 {
		t.Fatalf("frozen negative count%d", len(inputs.Negatives))
	}
	for _, negative := range inputs.Negatives {
		t.Run(negative.ID, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.sqlite")
			wire := collectionFixtureHex(t, negative.Vector.StreamHex)
			err := Restore(context.Background(), path, negative.Vector.LibraryID, bytes.NewReader(wire), collectionWireLimits())
			if err == nil {
				t.Fatal("accepted frozen format3 negative")
			}
			if negative.Expected == "reject_before_reservation" {
				if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
					t.Fatalf("header failure reserved target: %v", statErr)
				}
			} else {
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("later failure lost incomplete target: %v", statErr)
				}
				assertIncomplete(t, path)
			}
			if negative.ID == "forged_in_cap_policy_generation" || negative.ID == "forged_in_cap_reserve_counters" {
				if !strings.Contains(err.Error(), "restored projection") {
					t.Fatalf("forged footer did not reach final replay equality: %v", err)
				}
			}
		})
	}
}
func TestCollectionPortabilityPublicBudgetsAndAtomicStop(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	wire := collectionFixtureHex(t, f.StreamHex)
	r := bytes.NewReader(wire)
	path := filepath.Join(t.TempDir(), "rows.sqlite")
	if err := Restore(context.Background(), path, f.LibraryID, r, Limits{int64(len(wire)), 24, 23}); !errors.Is(err, ErrLimit) {
		t.Fatalf("C1 incremental row admission: %v", err)
	}
	assertIncomplete(t, path)
	stop := len(streamMagic3)
	for _, frame := range f.Frames {
		stop += len(collectionFixtureHex(t, frame.FrameHex))
		if frame.Kind == 7 {
			break
		}
	}
	if consumed := len(wire) - r.Len(); consumed != stop {
		t.Fatalf("failed transaction consumed later frame: %d want%d", consumed, stop)
	}
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var identities, definitions, events, policy, results, order int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM collection_identities),(SELECT count(*) FROM collection_definitions),(SELECT count(*) FROM collection_events),(SELECT count(*) FROM collection_policy_events),(SELECT count(*) FROM collection_policy_results),(SELECT count(*) FROM collection_order)`).Scan(&identities, &definitions, &events, &policy, &results, &order); err != nil {
		t.Fatal(err)
	}
	if identities != 2 || definitions != 0 || events != 0 || policy != 1 || results != 2 || order != 3 {
		t.Fatalf("partial parent/child transaction: %d %d %d %d %d %d", identities, definitions, events, policy, results, order)
	}
	for _, name := range []string{"empty", "shared"} {
		f := readCollectionWireFixture(t, name)
		wire := collectionFixtureHex(t, f.StreamHex)
		t.Run(name+"_bytes_one_short", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "short.sqlite")
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), Limits{int64(len(wire) - 1), 24, f.ProjectionRows}); !errors.Is(err, ErrLimit) {
				t.Fatalf("byte budget: %v", err)
			}
			assertIncomplete(t, path)
		})
	}
	eventPath := filepath.Join(t.TempDir(), "events.sqlite")
	if err := Restore(context.Background(), eventPath, f.LibraryID, bytes.NewReader(wire), Limits{int64(len(wire)), 23, 51}); !errors.Is(err, ErrLimit) {
		t.Fatalf("generated child not event-budgeted: %v", err)
	}
	if _, err := os.Lstat(eventPath); !os.IsNotExist(err) {
		t.Fatal("event budget reserved target")
	}
}
func TestCollectionPortabilityPublicTruncationBoundaries(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	wire := collectionFixtureHex(t, f.StreamHex)
	offset := len(streamMagic3)
	cuts := []int{0, len(streamMagic3) - 1}
	for _, frame := range f.Frames {
		length := len(collectionFixtureHex(t, frame.FrameHex))
		if frame.Kind == 1 || frame.Kind == 5 || frame.Kind == 6 || frame.Kind == 7 || frame.Kind == 3 {
			cuts = append(cuts, offset+1, offset+4, offset+5, offset+length-1)
		}
		offset += length
	}
	for _, cut := range cuts {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "truncated.sqlite")
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire[:cut]), collectionWireLimits()); err == nil {
				t.Fatal("accepted truncated new-format boundary")
			}
			assertIncomplete(t, path)
		})
	}
}

type collectionCompleteErrorReader struct {
	reader    *bytes.Reader
	err       error
	withBytes bool
}

func (r *collectionCompleteErrorReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if r.reader.Len() == 0 && (r.withBytes || n == 0) {
		return n, r.err
	}
	return n, err
}

type collectionNoProgressReader struct{}

func (collectionNoProgressReader) Read([]byte) (int, error) { return 0, nil }

type collectionFooterErrorWriter struct {
	bytes.Buffer
	err error
}

func (w *collectionFooterErrorWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if err == nil && bytes.Contains(p, []byte(`"stream_sha256"`)) {
		return n, w.err
	}
	return n, err
}
func TestCollectionPortabilityPublicIOAndFooterUncertainty(t *testing.T) {
	f := readCollectionWireFixture(t, "empty")
	wire := collectionFixtureHex(t, f.StreamHex)
	sentinel := errors.New("owned portability IO fault")
	for _, withBytes := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal_%t", withBytes), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "read-error.sqlite")
			r := &collectionCompleteErrorReader{bytes.NewReader(wire), sentinel, withBytes}
			if err := Restore(context.Background(), path, f.LibraryID, r, collectionWireLimits()); !errors.Is(err, sentinel) {
				t.Fatalf("reader error lost: %v", err)
			}
			assertIncomplete(t, path)
		})
	}
	path := filepath.Join(t.TempDir(), "no-progress.sqlite")
	if err := Restore(context.Background(), path, f.LibraryID, collectionNoProgressReader{}, collectionWireLimits()); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("no-progress reader: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("no-progress header reserved target")
	}
	s := collectionProjectionFixtureStore(t, readCollectionProjectionFixture(t, "empty"))
	if err := changedOne(s.db.Exec("UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0")); err != nil {
		t.Fatal(err)
	}
	writer := &collectionFooterErrorWriter{err: sentinel}
	if err := s.Export(context.Background(), f.LibraryID, writer, collectionWireLimits()); !errors.Is(err, sentinel) {
		t.Fatalf("full-footer writer error lost: %v", err)
	}
	if !bytes.Equal(writer.Bytes(), wire) {
		t.Fatal("full-footer error did not preserve complete artifact")
	}
	target := filepath.Join(t.TempDir(), "valid-artifact.sqlite")
	if err := Restore(context.Background(), target, f.LibraryID, bytes.NewReader(writer.Bytes()), collectionWireLimits()); err != nil {
		t.Fatalf("complete uncertain artifact not independently restorable: %v", err)
	}
}
func TestCollectionPortabilityPublicCancellationAndFinalBarrier(t *testing.T) {
	f := readCollectionWireFixture(t, "empty")
	wire := collectionFixtureHex(t, f.StreamHex)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "cancel.sqlite")
	if err := Restore(ctx, path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("canceled header reserved target")
	}
	for _, mode := range []string{"directory_error", "acknowledgment_cancel"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "barrier.sqlite")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("directory barrier fault")
			barrier := func(string) error {
				if mode == "acknowledgment_cancel" {
					cancel()
					return nil
				}
				return sentinel
			}
			err := restoreCatalog(ctx, path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits(), barrier)
			if err == nil || !strings.Contains(err.Error(), "uncertain") {
				t.Fatalf("finalization falsely certain: %v", err)
			}
			if mode == "directory_error" && !errors.Is(err, sentinel) || mode == "acknowledgment_cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("finalization error lost: %v", err)
			}
			ready, err := Open(path)
			if err != nil {
				t.Fatalf("uncertain finalization falsely promised ready0: %v", err)
			}
			defer ready.Close()
			var out bytes.Buffer
			if err := ready.Export(context.Background(), f.LibraryID, &out, collectionWireLimits()); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("uncertain ready catalog changed: %v", err)
			}
		})
	}
}

package nativeclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

type nativeMaxVectors struct {
	content               continuity.Content
	selection             catalog.DeliverySelection
	attempt               catalog.MaterializationAttempt
	selectRequest         catalog.DeliverySelectRequest
	transferRequest       delivery.TransferRequest
	header                delivery.TransferHeader
	end                   delivery.TransferEnd
	file, lock, directory privatefile.Identity
	intent                Intent
	selectionRecord       SelectionRecord
	checkpoint            Checkpoint
	prepared              Prepared
	verified              Verified
	released              Released
}

func maxVectors() nativeMaxVectors {
	f32, f64, op := strings.Repeat("f", 32), strings.Repeat("f", 64), strings.Repeat("z", 128)
	root := "https://" + strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61)}, ".") + ":65535/manager"
	id := func(prefix string) string { return prefix + f32 }
	content := continuity.Content{SHA256: f64, Size: 1 << 30}
	selection := catalog.DeliverySelection{Version: 1, LibraryID: continuity.LibraryID(id("lib_")), SelectionID: id("dsel_"), OperationID: op, AssetID: continuity.AssetID(id("asset_")), RevisionID: continuity.RevisionID(id("rev1_") + strings.Repeat("f", 32)), SourceID: id("dsrc_"), SourceVersion: math.MaxInt64, Content: content, ExpiresUnix: math.MaxInt64}
	attempt := catalog.MaterializationAttempt{ID: id("dmat_"), SelectionID: selection.SelectionID, OperationID: op, State: "COMPLETED", Content: content}
	selectRequest := catalog.DeliverySelectRequest{Version: 1, LibraryID: selection.LibraryID, ID: op, AssetID: selection.AssetID, Mode: "exact", RevisionID: selection.RevisionID, ExpiresUnix: math.MaxInt64, MaxBytes: 1 << 30}
	transferRequest := delivery.TransferRequest{Version: 1, LibraryID: selection.LibraryID, SelectionID: selection.SelectionID, OperationID: op, Offset: 1 << 30, PrefixSHA256: f64}
	header := delivery.TransferHeader{Version: 1, RequestSHA256: f64, AuthorityIncarnation: id("auth_"), Selection: selection, Attempt: attempt, Offset: 1 << 29, Length: 1 << 29, PrefixSHA256: f64}
	end := delivery.TransferEnd{Version: 1, RequestSHA256: f64, AttemptID: attempt.ID, Offset: 1 << 29, Length: 1 << 29, Result: "server_stream_complete"}
	directory := privatefile.Identity{Device: math.MaxUint64, Inode: math.MaxUint64, Size: 0, UID: math.MaxUint32, Mode: 0o40700}
	lock := privatefile.Identity{Device: math.MaxUint64, Inode: math.MaxUint64, Size: 0, UID: math.MaxUint32, Mode: 0o100600}
	file := privatefile.Identity{Device: math.MaxUint64, Inode: math.MaxUint64, Size: 1 << 30, UID: math.MaxUint32, Mode: 0o100600}
	intent := Intent{Version: 1, Endpoint: root, LibraryID: selection.LibraryID, OperationID: op, CopyID: id("copy_"), SelectRequest: selectRequest, Basename: strings.Repeat("&", 255), MaxBytes: 1 << 30, MetadataReservationBytes: MetadataReservationBytes}
	selectionRecord := SelectionRecord{Version: 1, IntentSHA256: f64, Selection: selection, LogicalReservationBytes: 1 << 30}
	checkpoint := Checkpoint{Version: 1, IntentSHA256: f64, Root: directory, Directory: directory, Lock: lock, File: file, PrefixLength: 1 << 30, PrefixSHA256: f64}
	prepared := Prepared{Version: 1, IntentSHA256: f64, Header: header, End: end, Root: directory, Directory: directory, Lock: lock, File: file, TargetDirectory: directory}
	verified := Verified{Version: 1, IntentSHA256: f64, PreparedSHA256: f64, CopyID: intent.CopyID, FinalFile: file}
	released := Released{Version: 1, IntentSHA256: f64, BasisKind: "checkpoint", BasisSHA256: f64, State: "released"}
	return nativeMaxVectors{content, selection, attempt, selectRequest, transferRequest, header, end, file, lock, directory, intent, selectionRecord, checkpoint, prepared, verified, released}
}

func assertSHA(t *testing.T, name string, b []byte, size int, want string) {
	t.Helper()
	sum := sha256.Sum256(b)
	if len(b) != size || hex.EncodeToString(sum[:]) != want {
		t.Fatalf("%s Go vector: bytes=%d sha256=%x want bytes=%d sha256=%s", name, len(b), sum, size, want)
	}
}

func TestNativeRecordMaxGoVectors(t *testing.T) {
	v := maxVectors()
	if len(v.intent.Endpoint) != 275 || len(v.intent.Basename) != 255 {
		t.Fatal("maximal variable-field recipe changed")
	}
	for _, vector := range []struct {
		name  string
		value any
		size  int
		sha   string
	}{
		{"Content", v.content, 95, "323971e07bb8b35c43445bc6e07855d3c87211b2ed7e85f967836ecec518b796"},
		{"Selection", v.selection, 651, "a828dfccfe48c58bc8dd399747aeaa8aa0276df092f99b18aa0c132529a3143c"},
		{"Attempt", v.attempt, 381, "1444e0c0f5f386d49fe1ea343eeab74f021b5d763e07c511ac42ad473619bb71"},
		{"SelectRequest", v.selectRequest, 422, "4bc3ea7f6953659c019b4f2e3dcffc7547a9cb75768b422cf8ab16ea4e3bbf56"},
		{"TransferRequest", v.transferRequest, 369, "61198c4374739d55e6c46e359d0ca1a07c66e778f5bf5a6809a59fc8cbecad73"},
		{"Header", v.header, 1338, "a1ab1ec0fba8138651cc6c02ace8d5213f248df6eacd87f40f7ed9cb395dd50e"},
		{"End", v.end, 222, "5c4ec2381485b1123e63a1184b0c4a26a466eabd127370573c0e6ee9cca6f153"},
		{"Identity", v.file, 108, "95bd8ac3dd9c20e6599c493ccc30685584e09f9689dff44a7e9e75825026308a"},
	} {
		b, err := json.Marshal(vector.value)
		if err != nil {
			t.Fatalf("%s: %v", vector.name, err)
		}
		assertSHA(t, vector.name, b, vector.size, vector.sha)
	}
	for _, vector := range []struct {
		name   string
		encode func() ([]byte, string, error)
		decode func([]byte) (string, error)
		size   int
		sha    string
	}{
		// The 275-byte endpoint here spells out a63.b63.c63.d61. The
		// historical review specified label lengths, not these literal bytes.
		{"Intent", func() ([]byte, string, error) { return EncodeIntent(v.intent, false) }, func(b []byte) (string, error) { _, d, e := DecodeIntent(b, false); return d, e }, 2593, "f58b0dc024197fb9f6ab5614fb2d4685eca22b94f3f8a8625f32b22d58397bfd"},
		{"Selection", func() ([]byte, string, error) { return EncodeSelection(v.selectionRecord) }, func(b []byte) (string, error) { _, d, e := DecodeSelection(b); return d, e }, 800, "da273a507fd6577283de63767674ea8d6afbed8c09c0c7bfd6de7992317f785c"},
		{"Checkpoint", func() ([]byte, string, error) { return EncodeCheckpoint(v.checkpoint) }, func(b []byte) (string, error) { _, d, e := DecodeCheckpoint(b); return d, e }, 649, "475ed88924a066daae8884e0e44d442acb7a59da9835ad28383b412fa6061c4a"},
		{"Prepared", func() ([]byte, string, error) { return EncodePrepared(v.prepared) }, func(b []byte) (string, error) { _, d, e := DecodePrepared(b); return d, e }, 2235, "60383b9e4877e6ce6dd1a4b0e380418dfb29af114c4de5bbd471ba93daf55d04"},
		{"Verified", func() ([]byte, string, error) { return EncodeVerified(v.verified) }, func(b []byte) (string, error) { _, d, e := DecodeVerified(b); return d, e }, 354, "8eee553cb1f1d806e2e155b6a46396b9b0fc2488eb3319e559d52444bc534847"},
		{"Released", func() ([]byte, string, error) { return EncodeReleased(v.released) }, func(b []byte) (string, error) { _, d, e := DecodeReleased(b); return d, e }, 224, "78c6db52b0fa4703cf891545b08d8219d932a0d54f1930c9e0f29b8c950fe01f"},
	} {
		line, digest, err := vector.encode()
		if err != nil {
			t.Fatalf("encode %s: %v", vector.name, err)
		}
		assertSHA(t, vector.name+" line", line, vector.size, vector.sha)
		if len(line) > RecordSlotBytes || line[len(line)-1] != '\n' || bytes.Count(line, []byte{'\n'}) != 1 {
			t.Fatalf("%s exceeded one 4096-byte slot or had invalid LF framing", vector.name)
		}
		decodedDigest, err := vector.decode(line)
		if err != nil || decodedDigest != digest {
			t.Fatalf("%s decode/domain digest: %s %s %v", vector.name, digest, decodedDigest, err)
		}
		check := sha256.Sum256(append([]byte("juicemount.continuity.native."+strings.ToLower(vector.name)+"\x00\x01"), line[:len(line)-1]...))
		if digest != hex.EncodeToString(check[:]) {
			t.Fatalf("%s digest included LF or used wrong domain", vector.name)
		}
	}
}

func TestNativeRecordStrictCanonicalAndOverflow(t *testing.T) {
	v := maxVectors()
	line, _, err := EncodeIntent(v.intent, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		bytes.TrimSuffix(line, []byte{'\n'}),
		append(append([]byte(nil), line...), '\n'),
		append(append([]byte(nil), line...), 'x'),
		[]byte("null\n"),
		[]byte("{}\n"),
		[]byte(strings.Replace(string(line), `"version":1,`, `"version":1,"version":1,`, 1)),
		[]byte(strings.Replace(string(line), `"version":1,`, `"version":1,"unknown":1,`, 1)),
		[]byte(strings.Replace(string(line), `"version":1,`, `"version":1, `, 1)),
		[]byte(strings.Replace(string(line), `\u0026`, `&`, 1)),
		bytes.Repeat([]byte{'x'}, RecordSlotBytes+1),
	} {
		if got, digest, err := DecodeIntent(bad, false); !errors.Is(err, ErrInvalidRecord) || got != (Intent{}) || digest != "" {
			t.Fatalf("malformed Intent admitted or leaked partial decode: length=%d err=%v", len(bad), err)
		}
	}
	wrong := v.intent
	wrong.CopyID = "copy_" + strings.Repeat("0", 32)
	if _, _, err := EncodeIntent(wrong, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("zero copy ID admitted: %v", err)
	}
	wrong = v.intent
	wrong.CopyID = "copy_" + strings.Repeat("F", 32)
	if _, _, err := EncodeIntent(wrong, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("uppercase copy ID admitted: %v", err)
	}
	wrong = v.intent
	wrong.SelectRequest.MaxBytes = math.MaxInt64
	wrong.MaxBytes = math.MaxInt64
	if _, _, err := EncodeIntent(wrong, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("select overflow admitted: %v", err)
	}
	wrong = v.intent
	wrong.MetadataReservationBytes++
	if _, _, err := EncodeIntent(wrong, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("reservation change admitted: %v", err)
	}
	wrong = v.intent
	wrong.Endpoint = "http://127.0.0.1:8080"
	if _, _, err := EncodeIntent(wrong, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("HTTP without opt-in admitted: %v", err)
	}
	httpLine, _, err := EncodeIntent(wrong, true)
	if err != nil {
		t.Fatalf("numeric loopback HTTP with opt-in refused: %v", err)
	}
	if _, _, err := DecodeIntent(httpLine, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("HTTP decoded without opt-in: %v", err)
	}
	if _, _, err := DecodeIntent(httpLine, true); err != nil {
		t.Fatalf("opted-in HTTP decode refused: %v", err)
	}
	selection := v.selectionRecord
	selection.Selection.Content.Size = (1 << 30) + 1
	selection.LogicalReservationBytes++
	if _, _, err := EncodeSelection(selection); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("logical media overflow admitted: %v", err)
	}
	checkpoint := v.checkpoint
	checkpoint.File.Mode = 0o100644
	if _, _, err := EncodeCheckpoint(checkpoint); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unsafe file mode admitted: %v", err)
	}
	prepared := v.prepared
	prepared.TargetDirectory = privatefile.Identity{}
	if _, _, err := EncodePrepared(prepared); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("missing fifth target identity admitted: %v", err)
	}
	released := v.released
	released.BasisKind = "unknown"
	if _, _, err := EncodeReleased(released); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unknown release basis admitted: %v", err)
	}
	selection = v.selectionRecord
	selection.IntentSHA256 = strings.Repeat("F", 64)
	if _, _, err := EncodeSelection(selection); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("uppercase link admitted: %v", err)
	}
	checkpoint = v.checkpoint
	checkpoint.PrefixLength = 0
	checkpoint.File.Size = 0
	if _, _, err := EncodeCheckpoint(checkpoint); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("nonempty digest for zero prefix admitted: %v", err)
	}
	prepared = v.prepared
	prepared.End.Length++
	if _, _, err := EncodePrepared(prepared); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("mismatched End admitted: %v", err)
	}
	verified := v.verified
	verified.FinalFile.Mode = 0o100644
	if _, _, err := EncodeVerified(verified); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unsafe final file admitted: %v", err)
	}
	released = v.released
	released.State = "uncertain"
	if _, _, err := EncodeReleased(released); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unknown release state admitted: %v", err)
	}
	for _, record := range []struct {
		name   string
		encode func() ([]byte, string, error)
		decode func([]byte) error
	}{
		{"Selection", func() ([]byte, string, error) { return EncodeSelection(v.selectionRecord) }, func(b []byte) error { _, _, e := DecodeSelection(b); return e }},
		{"Checkpoint", func() ([]byte, string, error) { return EncodeCheckpoint(v.checkpoint) }, func(b []byte) error { _, _, e := DecodeCheckpoint(b); return e }},
		{"Prepared", func() ([]byte, string, error) { return EncodePrepared(v.prepared) }, func(b []byte) error { _, _, e := DecodePrepared(b); return e }},
		{"Verified", func() ([]byte, string, error) { return EncodeVerified(v.verified) }, func(b []byte) error { _, _, e := DecodeVerified(b); return e }},
		{"Released", func() ([]byte, string, error) { return EncodeReleased(v.released) }, func(b []byte) error { _, _, e := DecodeReleased(b); return e }},
	} {
		line, _, err := record.encode()
		if err != nil {
			t.Fatalf("%s fixture: %v", record.name, err)
		}
		for _, bad := range [][]byte{
			bytes.TrimSuffix(line, []byte{'\n'}),
			append(append([]byte(nil), line...), '\n'),
			[]byte(strings.Replace(string(line), `"version":1,`, `"version":1,"version":1,`, 1)),
			[]byte(strings.Replace(string(line), `"version":1,`, `"version":1,"unknown":1,`, 1)),
		} {
			if err := record.decode(bad); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("%s malformed record admitted: %v", record.name, err)
			}
		}
	}
}

func TestNativeRecordChainLinksNotJournalAuthority(t *testing.T) {
	v := maxVectors()
	_, intentDigest, err := EncodeIntent(v.intent, false)
	if err != nil {
		t.Fatal(err)
	}
	v.selectionRecord.IntentSHA256 = intentDigest
	v.checkpoint.IntentSHA256 = intentDigest
	v.checkpoint.File.Size = 1 << 29
	v.checkpoint.PrefixLength = 1 << 29
	v.prepared.IntentSHA256 = intentDigest
	v.verified.IntentSHA256 = intentDigest
	v.released.IntentSHA256 = intentDigest
	_, requestDigest, err := delivery.CanonicalTransferRequest(delivery.TransferRequest{Version: 1, LibraryID: v.intent.LibraryID, SelectionID: v.selection.SelectionID, OperationID: v.intent.OperationID, Offset: v.header.Offset, PrefixSHA256: v.header.PrefixSHA256})
	if err != nil {
		t.Fatal(err)
	}
	v.prepared.Header.RequestSHA256 = requestDigest
	v.prepared.End.RequestSHA256 = requestDigest
	_, preparedDigest, err := EncodePrepared(v.prepared)
	if err != nil {
		t.Fatal(err)
	}
	v.verified.PreparedSHA256 = preparedDigest
	_, verifiedDigest, err := EncodeVerified(v.verified)
	if err != nil {
		t.Fatal(err)
	}
	v.released.BasisKind = "verified"
	v.released.BasisSHA256 = verifiedDigest
	chain := Chain{Intent: v.intent, Selection: &v.selectionRecord, Checkpoint: &v.checkpoint, Prepared: &v.prepared, Verified: &v.verified, Released: &v.released}
	if err := ValidateChain(chain, false); err != nil {
		t.Fatalf("linked chain refused: %v", err)
	}
	broken := chain
	changed := *chain.Selection
	changed.Selection.OperationID = "other"
	broken.Selection = &changed
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("foreign selection admitted: %v", err)
	}
	broken = chain
	prepared := *chain.Prepared
	prepared.Header.RequestSHA256 = strings.Repeat("f", 64)
	prepared.End.RequestSHA256 = prepared.Header.RequestSHA256
	broken.Prepared = &prepared
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unbound transfer request admitted: %v", err)
	}
	broken = chain
	verified := *chain.Verified
	verified.CopyID = "copy_" + strings.Repeat("e", 32)
	broken.Verified = &verified
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("foreign copy admitted: %v", err)
	}
	broken = chain
	released := *chain.Released
	released.BasisSHA256 = strings.Repeat("f", 64)
	broken.Released = &released
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("foreign release basis admitted: %v", err)
	}
	broken = chain
	released = *chain.Released
	released.BasisKind = "checkpoint"
	_, released.BasisSHA256, err = EncodeCheckpoint(*chain.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	broken.Released = &released
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("stale release basis admitted: %v", err)
	}
	broken = chain
	broken.Checkpoint = nil
	if err := ValidateChain(broken, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("release without basis admitted: %v", err)
	}
}

func TestNativeRecordCheckpointBoundToSelectedContent(t *testing.T) {
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	v := maxVectors()
	v.intent.MaxBytes = 100 // The requested budget exceeds the ten-byte pin.
	v.intent.SelectRequest.MaxBytes = 100
	_, intentDigest, err := EncodeIntent(v.intent, false)
	if err != nil {
		t.Fatal(err)
	}
	v.selectionRecord.IntentSHA256 = intentDigest
	v.selectionRecord.Selection.Content = continuity.Content{SHA256: digest("0123456789"), Size: 10}
	v.selectionRecord.LogicalReservationBytes = 10
	v.checkpoint.IntentSHA256 = intentDigest
	v.checkpoint.File.Size = 5
	v.checkpoint.PrefixLength = 5
	v.checkpoint.PrefixSHA256 = digest("01234")
	chain := Chain{Intent: v.intent, Selection: &v.selectionRecord, Checkpoint: &v.checkpoint}
	if err := ValidateChain(chain, false); err != nil {
		t.Fatalf("short partial checkpoint refused: %v", err)
	}

	oversize := v.checkpoint
	oversize.File.Size = 11
	oversize.PrefixLength = 11
	oversize.PrefixSHA256 = digest("01234567890")
	chain.Checkpoint = &oversize
	if _, _, err := EncodeCheckpoint(oversize); err != nil {
		t.Fatalf("oversize control must be standalone-valid: %v", err)
	}
	if err := ValidateChain(chain, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("eleven-byte prefix admitted against ten-byte selection: %v", err)
	}
	_, basisDigest, err := EncodeCheckpoint(oversize)
	if err != nil {
		t.Fatal(err)
	}
	chain.Released = &Released{Version: 1, IntentSHA256: intentDigest, BasisKind: "checkpoint", BasisSHA256: basisDigest, State: "released"}
	if err := ValidateChain(chain, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("release admitted invalid oversize checkpoint: %v", err)
	}
	chain.Released = nil

	full := v.checkpoint
	full.File.Size = 10
	full.PrefixLength = 10
	full.PrefixSHA256 = digest("abcdefghij")
	chain.Checkpoint = &full
	if err := ValidateChain(chain, false); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("full-length wrong hash admitted: %v", err)
	}
	full.PrefixSHA256 = v.selectionRecord.Selection.Content.SHA256
	if err := ValidateChain(chain, false); err != nil {
		t.Fatalf("full correct checkpoint refused: %v", err)
	}

	emptySelection := v.selectionRecord
	emptySelection.Selection.Content = continuity.Content{SHA256: digest(""), Size: 0}
	emptySelection.LogicalReservationBytes = 0
	emptyCheckpoint := v.checkpoint
	emptyCheckpoint.File.Size = 0
	emptyCheckpoint.PrefixLength = 0
	emptyCheckpoint.PrefixSHA256 = digest("")
	chain.Selection, chain.Checkpoint = &emptySelection, &emptyCheckpoint
	if err := ValidateChain(chain, false); err != nil {
		t.Fatalf("empty full checkpoint refused: %v", err)
	}
}

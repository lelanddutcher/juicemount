package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func deliveryExportTest(t *testing.T, s *Store) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := s.Export(context.Background(), s.library, &out, collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(out.Bytes(), []byte(streamMagic5)) {
		t.Fatal("schema7 silently down-exported")
	}
	return out.Bytes()
}
func deliveryOpenTest(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// Production metadata/policy/collection/query APIs supply the portable history.
// The existing clock fixture's synthetic source is used only to prove omission
// of operational records; this is not source verification or media backup proof.
func deliveryPortableHistory(t *testing.T) (deliveryClockFixture, BytePolicyRequest, BytePolicyReceipt) {
	t.Helper()
	ctx := context.Background()
	f := newDeliveryClockFixture(t, 3600)
	s := f.store
	o, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	a, err := o.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	applyTest(t, s, testOperation("portable-other", Change{CreateAsset: &CreateAsset{otherAsset, "other"}}))
	alternate := SubjectID("sub_77777777777777777777777777777777")
	policyTest(t, o, a, "portable-owner-metadata",
		NewPolicyChange("create_subject", CreateSubject{alternate, SubjectValue{"person", "next owner", true}}),
		NewPolicyChange("put_grant", PutGrant{"portable-owner-metadata", nil, grantForTest(ownerSubject, Scope{"library", ""})}))
	value := CollectionGrantValue{ownerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(711, "", "library_create", CollectionGrantChange{"put", "cgr_77777777777777777777777777777777", nil, &value}))); err != nil {
		t.Fatal(err)
	}
	issued, _ := collectionTxIssue(t, o, a, "portable-owner-issue", ownerSubject, collectionTxActions)
	d, err := NewDiscoveryService(ctx, s, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	collectionTxCall(t, d, issued.Bearer, collectionTxCreate(t, 712))
	ownerBytes := ByteGrantValue{ownerSubject, Scope{"library", ""}, "bytes.read"}
	if _, err := o.ApplyBytePolicy(ctx, BytePolicyRequest{1, testLibrary, "portable-byte-owner", a.Incarnation, []ByteGrantChange{{"put", "bgr_44444444444444444444444444444444", nil, &ownerBytes}}}); err != nil {
		t.Fatal(err)
	}
	// This old request records the authorization generation AFTER byte policy.
	owner, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	renamed := owner.Value
	renamed.Label = "owner after bytes"
	policyTest(t, o, a, "portable-old-after-byte", NewPolicyChange("update_subject", UpdateSubject{ownerSubject, owner.Expected(), renamed}))
	savedQueryTestCall(t, d, issued.Bearer, savedQueryTestCreate(t, 713, testAsset))
	prior, err := readByteGrant(ctx, s.db, string(testLibrary), "bgr_11111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	remove := BytePolicyRequest{1, testLibrary, "portable-byte-remove", a.Incarnation, []ByteGrantChange{{"remove", "bgr_11111111111111111111111111111111", &prior.expected, nil}}}
	receipt, err := o.ApplyBytePolicy(ctx, remove)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := o.Recover(ctx, adminTestRequest("portable-recover", a.Incarnation, "recover", RecoverRequest{ownerSubject}))
	if err != nil {
		t.Fatal(err)
	}
	a = AuthorityStatus{recovery.Result.Incarnation, ownerSubject}
	// Recover rotates the existing local owner's incarnation; it never changes
	// owner identity. Rebind an inactive public restore to the other retained
	// person before disabling the historical owner through production policy.
	recoveryRetry, err := o.ApplyBytePolicy(ctx, remove)
	recoveryWant, _ := CanonicalBytePolicyReceipt(receipt)
	recoveryHave, _ := CanonicalBytePolicyReceipt(recoveryRetry)
	if err != nil || !bytes.Equal(recoveryWant, recoveryHave) {
		t.Fatalf("exact retry after recovery: %v", err)
	}
	portable := deliveryExportTest(t, s)
	path := filepath.Join(t.TempDir(), "rebound.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(portable), collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	s = deliveryOpenTest(t, path)
	s.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
	f.store, f.path = s, path
	o, err = s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := o.Bootstrap(ctx, adminTestRequest("portable-rebind-owner", "", "bootstrap", BootstrapRequest{alternate, nil}))
	if err != nil {
		t.Fatal(err)
	}
	a = AuthorityStatus{boot.Result.Incarnation, alternate}
	owner, err = readSubject(ctx, s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	disabled := owner.Value
	disabled.Enabled = false
	policyTest(t, o, a, "portable-owner-later-disabled", NewPolicyChange("update_subject", UpdateSubject{ownerSubject, owner.Expected(), disabled}))
	// Existing exact operation receipts survive recovery without fresh effects.
	retry, err := o.ApplyBytePolicy(ctx, remove)
	want, _ := CanonicalBytePolicyReceipt(receipt)
	have, _ := CanonicalBytePolicyReceipt(retry)
	if err != nil || !bytes.Equal(want, have) {
		t.Fatalf("byte retry after owner recovery: %v", err)
	}
	newValue := ByteGrantValue{readerSubject, Scope{"library", ""}, "bytes.read"}
	if _, err := o.ApplyBytePolicy(ctx, BytePolicyRequest{1, testLibrary, "portable-byte-new-owner", a.Incarnation, []ByteGrantChange{{"put", "bgr_55555555555555555555555555555555", nil, &newValue}}}); err != nil {
		t.Fatal(err)
	}
	return f, remove, receipt
}

func TestDeliveryPortabilityPublicMixedHistoryInactiveRoundtrip(t *testing.T) {
	ctx := context.Background()
	f, originalRequest, originalReceipt := deliveryPortableHistory(t)
	wire := deliveryExportTest(t, f.store)
	seen := map[byte]int{}
	for _, frame := range savedQueryPortableFrames(t, wire, len(streamMagic5)) {
		seen[frame.kind]++
	}
	for _, kind := range []byte{2, 4, 5, 6, 7, 8} {
		if seen[kind] == 0 {
			t.Fatalf("mixed family %d untested", kind)
		}
	}
	if seen[8] != 4 || seen[7] != 2 {
		t.Fatalf("byte/manual/query coverage: %v", seen)
	}
	path := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(wire), collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	restored := deliveryOpenTest(t, path)
	if restored.version != 7 {
		t.Fatal("format5 restored wrong schema")
	}
	if !bytes.Equal(wire, deliveryExportTest(t, restored)) {
		t.Fatal("exact format5 history/receipts were not retained")
	}
	for _, id := range []SubjectID{ownerSubject, readerSubject, "sub_77777777777777777777777777777777"} {
		before, err := readSubject(ctx, f.store.db, testLibrary, id)
		if err != nil {
			t.Fatal(err)
		}
		after, err := readSubject(ctx, restored.db, testLibrary, id)
		if err != nil || before != after {
			t.Fatalf("mixed subject generation/value %s: %+v %+v %v", id, before, after, err)
		}
	}
	owner, err := readSubject(ctx, restored.db, testLibrary, ownerSubject)
	if err != nil || owner.Value.Enabled {
		t.Fatalf("later-disabled historical owner: %+v %v", owner, err)
	}
	grant, err := readByteGrant(ctx, restored.db, string(testLibrary), originalRequest.Changes[0].ID)
	if err != nil || !grant.expected.Removed || grant.expected.Version != originalReceipt.Results[0].Version {
		t.Fatalf("byte tombstone was lost: %+v %v", grant, err)
	}
	beforeQuota, err := readBytePolicyQuotaProjection(ctx, f.store.db, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	afterQuota, err := readBytePolicyQuotaProjection(ctx, restored.db, testLibrary)
	if err != nil || beforeQuota != afterQuota || afterQuota.ReservedRemovals != 2 || afterQuota.LifetimeGrants != 3 || afterQuota.Events != 4 {
		t.Fatalf("exact reserved quota replay: %+v %+v %v", beforeQuota, afterQuota, err)
	}
	if err := validateInactiveCollectionImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	if err := validateInactiveDeliveryImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Authenticate(ctx, testLibrary, f.bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("source credential survived restore: %v", err)
	}
	o, err := restored.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := o.Status(ctx)
	if err != nil || status.Incarnation != "" {
		t.Fatalf("restore activated authority: %+v %v", status, err)
	}
	// Recover against portable enabled person, then exercise preserved grant CAS.
	recovered, err := o.Bootstrap(ctx, adminTestRequest("portable-target-bootstrap", "", "bootstrap", BootstrapRequest{"sub_77777777777777777777777777777777", nil}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.SelectDelivery(ctx, f.bearer, DeliverySelectRequest{1, testLibrary, "portable-denied-selection", testAsset, "latest", "", deliveryClockBase + 600, 3}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("recovery revived source bearer: %v", err)
	}
	fresh, err := o.IssueCredentialV3(ctx, AdminRequestV3{3, testLibrary, "portable-fresh-issue", recovered.Result.Incarnation, "issue_credential", collectionTxRaw(t, IssueCredentialRequestV3{readerSubject, Scope{"asset", testAsset}, []string{"assets.discover", "bytes.read", "revisions.read"}, restored.now().Unix() + 3600})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.SelectDelivery(ctx, fresh.Bearer, DeliverySelectRequest{1, testLibrary, "portable-no-source", testAsset, "latest", "", restored.now().Unix() + 600, 3}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fresh credential restored operational source rights: %v", err)
	}
	oldReceipt, err := o.ApplyBytePolicy(ctx, originalRequest)
	want, _ := CanonicalBytePolicyReceipt(originalReceipt)
	have, _ := CanonicalBytePolicyReceipt(oldReceipt)
	if err != nil || !bytes.Equal(want, have) {
		t.Fatalf("restored exact historical retry: %v", err)
	}
	stale := originalRequest
	stale.ID, stale.ExpectedIncarnation = "portable-stale-remove", recovered.Result.Incarnation
	if _, err := o.ApplyBytePolicy(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale pre-tombstone CAS accepted: %v", err)
	}
	revive := BytePolicyRequest{1, testLibrary, "portable-regrant", recovered.Result.Incarnation, []ByteGrantChange{{"put", originalRequest.Changes[0].ID, &grant.expected, &grant.expected.Value}}}
	if _, err := o.ApplyBytePolicy(ctx, revive); err != nil {
		t.Fatalf("exact restored tombstone CAS: %v", err)
	}
	if _, err := o.ApplyBytePolicy(ctx, revive); err != nil {
		t.Fatalf("restored regrant exact retry: %v", err)
	}
}

func TestDeliveryPortabilityOperationalChangesOmitted(t *testing.T) {
	ctx := context.Background()
	f := newDeliveryClockFixture(t, 3600)
	before := deliveryExportTest(t, f.store)
	_, selection := f.selectLatest(t, "portable-omit-selection", deliveryClockBase+600)
	clockPrepared(t, f, selection.SelectionID)
	if clockFloor(t, f.store) == 0 {
		t.Fatal("clock omission test did not advance operational clock")
	}
	if after := deliveryExportTest(t, f.store); !bytes.Equal(before, after) {
		t.Fatal("selection/materialization/clock changed portable bytes")
	}
	path := filepath.Join(t.TempDir(), "inactive.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(before), collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	restored := deliveryOpenTest(t, path)
	if err := validateInactiveDeliveryImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.ReserveMaterialization(ctx, f.bearer, selection.SelectionID, "portable-no-output"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("restored output authority: %v", err)
	}
}

func TestDeliveryPortabilityEmptyIndependentGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/continuity-c05-empty.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		LibraryID        string `json:"library_id"`
		StreamHex        string `json:"stream_hex"`
		FileSHA256       string `json:"file_sha256"`
		ProjectionRows   int64  `json:"projection_rows"`
		ProjectionSHA256 string `json:"projection_sha256"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	wire := collectionFixtureHex(t, f.StreamHex)
	if fmt.Sprintf("%x", sha256.Sum256(wire)) != f.FileSHA256 {
		t.Fatal("independent empty fixture changed")
	}
	s, err := CreateDelivery(context.Background(), filepath.Join(t.TempDir(), "empty.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(deliveryExportTest(t, s), wire) {
		t.Fatal("format5 empty literal bytes changed")
	}
	projection, rows, err := projectionDigest(context.Background(), s.db, testLibrary, 3)
	if err != nil || projection != f.ProjectionSHA256 || rows != 3 {
		t.Fatalf("independent empty full projection: %s/%d %v", projection, rows, err)
	}
	path := filepath.Join(t.TempDir(), "literal.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), Limits{int64(len(wire)), 1, 3}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(deliveryExportTest(t, deliveryOpenTest(t, path)), wire) {
		t.Fatal("empty literal public roundtrip changed")
	}
}

// Rebuild only framing and the outer digest using an independent SHA256 recipe.
// Forged semantic claims therefore reach replay/final projection checks, rather
// than merely failing because a corruption left the stream digest stale.
func deliveryRewriteTest(t *testing.T, wire []byte, mutate func([]savedQueryPortableFrame) []savedQueryPortableFrame) []byte {
	t.Helper()
	frames := savedQueryPortableFrames(t, wire, len(streamMagic5))
	for i := range frames {
		frames[i].payload = append([]byte(nil), frames[i].payload...)
	}
	frames = mutate(frames)
	if len(frames) < 2 || frames[0].kind != 1 || frames[len(frames)-1].kind != 3 {
		t.Fatal("rewrite phases")
	}
	var header deliveryStreamHeader
	var footer deliveryStreamFooter
	if err := json.Unmarshal(frames[0].payload, &header); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(frames[len(frames)-1].payload, &footer); err != nil {
		t.Fatal(err)
	}
	out := append([]byte{}, []byte(streamMagic5)...)
	for _, frame := range frames[:len(frames)-1] {
		h := frameHeader(frame.kind, len(frame.payload))
		out = append(out, h[:]...)
		out = append(out, frame.payload...)
	}
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x05"))
	h.Write(out)
	for _, n := range []int64{header.EventCount, header.PolicyEventCount, header.CollectionCount, header.CollectionEventCount, header.CollectionPolicyEventCount, header.TransactionCount, header.BytePolicyEventCount, footer.ProjectionRows} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
	}
	digest, err := hex.DecodeString(footer.ProjectionSHA256)
	if err != nil {
		t.Fatal(err)
	}
	h.Write(digest)
	footer.StreamSHA256 = hex.EncodeToString(h.Sum(nil))
	payload, err := json.Marshal(footer)
	if err != nil {
		t.Fatal(err)
	}
	fh := frameHeader(3, len(payload))
	out = append(out, fh[:]...)
	return append(out, payload...)
}
func deliveryByteFrameTest(t *testing.T, frames []savedQueryPortableFrame) int {
	t.Helper()
	for i, frame := range frames {
		if frame.kind == 8 {
			return i
		}
	}
	t.Fatal("no byte frame")
	return 0
}
func deliveryRecordFrameTest(t *testing.T, frame *savedQueryPortableFrame, change func(*BytePolicyRecord)) {
	t.Helper()
	r, err := DecodeBytePolicyRecord(frame.payload)
	if err != nil {
		t.Fatal(err)
	}
	change(&r)
	frame.payload, _, err = CanonicalBytePolicyRecord(r)
	if err != nil {
		t.Fatal(err)
	}
}
func TestDeliveryPortabilityPublicCorruptionRefusal(t *testing.T) {
	f, _, _ := deliveryPortableHistory(t)
	wire := deliveryExportTest(t, f.store)
	cases := []struct {
		name   string
		mutate func([]savedQueryPortableFrame) []savedQueryPortableFrame
	}{
		{"missing_byte_frame", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			i := deliveryByteFrameTest(t, fs)
			return append(fs[:i], fs[i+1:]...)
		}},
		{"duplicate_byte_frame", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			i := deliveryByteFrameTest(t, fs)
			return append(fs[:i], append([]savedQueryPortableFrame{fs[i]}, fs[i:]...)...)
		}},
		{"reordered_byte_frames", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			i := deliveryByteFrameTest(t, fs)
			for j := i + 1; j < len(fs); j++ {
				if fs[j].kind == 8 {
					fs[i], fs[j] = fs[j], fs[i]
					return fs
				}
			}
			t.Fatal("second byte frame absent")
			return fs
		}},
		{"owner_absent_at_prefix", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			i := deliveryByteFrameTest(t, fs)
			deliveryRecordFrameTest(t, &fs[i], func(r *BytePolicyRecord) { r.OperatorSubjectID = "sub_77777777777777777777777777777777" })
			return fs
		}},
		{"owner_disabled_at_prefix", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			for i := len(fs) - 1; i >= 0; i-- {
				if fs[i].kind == 8 {
					deliveryRecordFrameTest(t, &fs[i], func(r *BytePolicyRecord) { r.OperatorSubjectID = ownerSubject })
					return fs
				}
			}
			t.Fatal("no last byte frame")
			return fs
		}},
		{"bad_byte_sequence", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			i := deliveryByteFrameTest(t, fs)
			deliveryRecordFrameTest(t, &fs[i], func(r *BytePolicyRecord) { r.Receipt.Sequence++ })
			return fs
		}},
		{"projection_digest", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			var v deliveryStreamFooter
			json.Unmarshal(fs[len(fs)-1].payload, &v)
			v.ProjectionSHA256 = strings.Repeat("0", 64)
			fs[len(fs)-1].payload, _ = json.Marshal(v)
			return fs
		}},
		{"projection_rows", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			var v deliveryStreamFooter
			json.Unmarshal(fs[len(fs)-1].payload, &v)
			v.ProjectionRows++
			fs[len(fs)-1].payload, _ = json.Marshal(v)
			return fs
		}},
		{"footer_byte_count", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			var v deliveryStreamFooter
			json.Unmarshal(fs[len(fs)-1].payload, &v)
			v.BytePolicyEventCount++
			fs[len(fs)-1].payload, _ = json.Marshal(v)
			return fs
		}},
		{"header_byte_count", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			var v deliveryStreamHeader
			json.Unmarshal(fs[0].payload, &v)
			v.BytePolicyEventCount++
			fs[0].payload, _ = json.Marshal(v)
			return fs
		}},
		{"wrong_transaction_kind", func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
			fs[deliveryByteFrameTest(t, fs)].kind = 9
			return fs
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := deliveryRewriteTest(t, wire, tc.mutate)
			path := filepath.Join(t.TempDir(), "refused.sqlite")
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(bad), collectionWireLimits()); err == nil {
				t.Fatal("public restore accepted semantic corruption")
			}
			if s, err := Open(path); err == nil {
				s.Close()
				t.Fatal("failed restore adopted incomplete target")
			}
		})
	}
	for _, tc := range []struct {
		name string
		wire []byte
	}{{"trailing", append(append([]byte(nil), wire...), 0)}, {"truncated", wire[:len(wire)-1]}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := Restore(context.Background(), filepath.Join(t.TempDir(), "bad.sqlite"), testLibrary, bytes.NewReader(tc.wire), collectionWireLimits()); err == nil {
				t.Fatal("accepted incomplete/trailing stream")
			}
		})
	}
}

func TestDeliveryPortabilityByteCanonicalRefusal(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	wire := deliveryExportTest(t, f.store)
	prefix, _, _ := deliveryBytePrefixTest(t, wire)
	before := deliveryPortableSnapshot(t, prefix)
	_, ordinal := deliveryByteOrdinalFrame(t, savedQueryPortableFrames(t, wire, len(streamMagic5)), 1)
	for _, tc := range []struct {
		name    string
		rewrite func([]byte) []byte
	}{
		{"duplicate", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`{"version":1,`), []byte(`{"version":1,"version":1,`), 1)
		}},
		{"unknown", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`{"version":1,`), []byte(`{"extra":0,"version":1,`), 1)
		}},
		{"null_request", func(b []byte) []byte {
			var r map[string]json.RawMessage
			json.Unmarshal(b, &r)
			r["request"] = []byte("null")
			out, _ := json.Marshal(r)
			return out
		}},
		{"trailing_json", func(b []byte) []byte { return append(b, []byte(`{}`)...) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := deliveryRewriteTest(t, wire, func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
				i := deliveryByteFrameTest(t, fs)
				fs[i].payload = tc.rewrite(fs[i].payload)
				return fs
			})
			path := filepath.Join(t.TempDir(), "canonical.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(bad), collectionWireLimits())
			if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "byte policy record JSON") {
				t.Fatalf("wrong canonical byte decoder refusal: %v", err)
			}
			deliveryAssertIncompletePrefix(t, path, before, ordinal-1)
		})
	}
}

// Include every schema7 table, SQLite cell types and sorted complete rows.
func deliveryPortableSnapshot(t *testing.T, s *Store) []byte {
	t.Helper()
	var tables []string
	for _, o := range expectedSchemaFor(7) {
		if o.Type == "table" {
			tables = append(tables, o.Name)
		}
	}
	sort.Strings(tables)
	var all []any
	for _, table := range tables {
		rows, err := s.db.Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		records := []string{}
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			cells := make([]string, len(values))
			for i, value := range values {
				switch v := value.(type) {
				case nil:
					cells[i] = "null"
				case []byte:
					cells[i] = "blob:" + hex.EncodeToString(v)
				case string:
					cells[i] = "text:" + hex.EncodeToString([]byte(v))
				case int64:
					cells[i] = fmt.Sprintf("integer:%d", v)
				default:
					rows.Close()
					t.Fatalf("unexpected SQLite value %T", v)
				}
			}
			encoded, err := json.Marshal(cells)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			records = append(records, string(encoded))
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(records)
		all = append(all, table, columns, records)
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Stage the actual source stream up to its first byte frame in an incomplete
// target, for exact transaction rollback assertions and causal fault injection.
func deliveryBytePrefixTest(t *testing.T, wire []byte) (*Store, int64, BytePolicyRecord) {
	t.Helper()
	ctx := context.Background()
	s, err := openStoreVersionContext(ctx, filepath.Join(t.TempDir(), "prefix.sqlite"), testLibrary, true, false, 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	rows, metadata, legacy := int64(3), int64(0), int64(0)
	for _, f := range savedQueryPortableFrames(t, wire, len(streamMagic5)) {
		switch f.kind {
		case 1:
			continue
		case 2:
			metadata++
			e, op, eerr := decodeStreamEvent(f.payload, testLibrary, metadata)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayEvent(ctx, e, op, rows, MaxStreamProjectionRows)
		case 4:
			legacy++
			e, op, eerr := decodeStreamPolicyEvent(f.payload, testLibrary, legacy)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayPolicyEvent(ctx, e, op, rows, MaxStreamProjectionRows)
		case 5:
			var d collectionDeclaration
			if eerr := canonicalJSON(f.payload, &d); eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayCollectionDeclaration(ctx, d, rows, MaxStreamProjectionRows)
		case 6:
			r, receipt, eerr := decodeCollectionPolicyPair(f.payload, testLibrary)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayCollectionOwnerPolicy(ctx, r, receipt, rows, MaxStreamProjectionRows)
		case 7:
			replay, _, eerr := decodePortableCollectionContent(f.payload, testLibrary, 5)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = replay(ctx, s, rows, MaxStreamProjectionRows)
		case 8:
			r, err := DecodeBytePolicyRecord(f.payload)
			if err != nil {
				t.Fatal(err)
			}
			return s, rows, r
		default:
			t.Fatal("unexpected prefix frame")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("no first byte record")
	return nil, 0, BytePolicyRecord{}
}
func TestDeliveryPortabilityByteReplayRollback(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	wire := deliveryExportTest(t, f.store)
	for _, name := range []string{"row_budget", "late_result_fault", "deferred_commit_fault"} {
		t.Run(name, func(t *testing.T) {
			s, rows, record := deliveryBytePrefixTest(t, wire)
			before := deliveryPortableSnapshot(t, s)
			max := MaxStreamProjectionRows
			switch name {
			case "row_budget":
				max = rows + 2 // grant+result+order requires three
			case "late_result_fault":
				if _, err := s.db.Exec(`CREATE TEMP TRIGGER delivery_restore_fault AFTER INSERT ON main.byte_policy_results BEGIN SELECT RAISE(ABORT,'byte replay fault'); END`); err != nil {
					t.Fatal(err)
				}
			case "deferred_commit_fault":
				for _, sql := range []string{`CREATE TEMP TABLE delivery_fk_parent(id INTEGER PRIMARY KEY)`, `CREATE TEMP TABLE delivery_fk_child(id INTEGER REFERENCES delivery_fk_parent(id) DEFERRABLE INITIALLY DEFERRED)`, `CREATE TEMP TRIGGER delivery_restore_fault AFTER INSERT ON main.byte_policy_results BEGIN INSERT INTO delivery_fk_child VALUES(1); END`} {
					if _, err := s.db.Exec(sql); err != nil {
						t.Fatal(err)
					}
				}
			}
			next, err := s.replayBytePolicy(context.Background(), record, rows, max)
			if err == nil || next != 0 {
				t.Fatalf("rollback fault not reached: %d %v", next, err)
			}
			if name == "row_budget" && !errors.Is(err, ErrLimit) {
				t.Fatalf("wrong budget refusal: %v", err)
			}
			if name == "late_result_fault" && !strings.Contains(err.Error(), "byte replay fault") {
				t.Fatalf("wrong injected refusal: %v", err)
			}
			if name == "deferred_commit_fault" && !strings.Contains(err.Error(), "commit uncertain") {
				t.Fatalf("did not reach deferred commit: %v", err)
			}
			if !bytes.Equal(before, deliveryPortableSnapshot(t, s)) || s.db.Stats().InUse != 0 {
				t.Fatal("failed byte replay changed complete state or retained SQL ownership")
			}
			if name != "row_budget" {
				if _, err := s.db.Exec(`DROP TRIGGER delivery_restore_fault`); err != nil {
					t.Fatal(err)
				}
			}
			next, err = s.replayBytePolicy(context.Background(), record, rows, MaxStreamProjectionRows)
			if err != nil || next != rows+3 {
				t.Fatalf("exact record positive after fault removal: %d %v", next, err)
			}
		})
	}
}

func TestDeliveryPortabilityOldFormatExactRegression(t *testing.T) {
	for _, name := range []string{"continuity-c02c-empty", "continuity-c02c-relational", "continuity-c03a-policy-rich", "continuity-c04b-shared"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var f collectionWireFixture
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			wire := collectionFixtureHex(t, f.StreamHex)
			path := filepath.Join(t.TempDir(), "old.sqlite")
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits()); err != nil {
				t.Fatal(err)
			}
			s := deliveryOpenTest(t, path)
			var out bytes.Buffer
			if err := s.Export(context.Background(), f.LibraryID, &out, collectionWireLimits()); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("old stream changed: %v", err)
			}
		})
	}
	for _, f := range readSavedQueryPortableFixture(t).Positive {
		t.Run("format4_"+f.Name, func(t *testing.T) {
			wire := collectionFixtureHex(t, f.StreamHex)
			path := filepath.Join(t.TempDir(), "old4.sqlite")
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits()); err != nil {
				t.Fatal(err)
			}
			s := deliveryOpenTest(t, path)
			var out bytes.Buffer
			if err := s.Export(context.Background(), f.LibraryID, &out, collectionWireLimits()); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("format4 stream changed: %v", err)
			}
		})
	}
}

func TestDeliveryPortabilityQuotaAndProjectionBindsPolicy(t *testing.T) {
	ctx := context.Background()
	f := newDeliveryClockFixture(t, 3600)
	before, rows, err := projectionDigest(ctx, f.store.db, testLibrary, MaxStreamProjectionRows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`UPDATE byte_policy_quota SET reserved_removals=reserved_removals+1,lifetime_grants=lifetime_grants+1 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	after, observed, err := projectionDigest(ctx, f.store.db, testLibrary, MaxStreamProjectionRows)
	if err != nil || observed != rows || after == before {
		t.Fatalf("byte reserve projection not bound: %s/%d %v", after, observed, err)
	}
	var out bytes.Buffer
	if err := f.store.Export(ctx, testLibrary, &out, collectionWireLimits()); err == nil || !strings.Contains(err.Error(), "byte policy quota does not match") {
		t.Fatalf("forged byte quota exported: %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("failed byte quota preflight wrote stream")
	}
}

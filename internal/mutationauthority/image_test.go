package mutationauthority

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func imageTestConfig() ImageConfig {
	return ImageConfig{1, "ims_11111111111111111111111111111111", true, 2, 0, 1 << 20}
}
func emptyImage() ImageSeal {
	return ImageSeal{1, "ims_11111111111111111111111111111111", "volume", "lib_11111111111111111111111111111111", "img_11111111111111111111111111111111", "iop_11111111111111111111111111111111", "incarnation-1", strings.Repeat("a", 64), strings.Repeat("b", 64), ImageContent{0, DigestBytes(nil)}, []ImageCustodyReceipt{}}
}
func TestImageCanonicalGoldens(t *testing.T) {
	empty := emptyImage()
	const golden = `{"version":1,"store_id":"ims_11111111111111111111111111111111","volume":"volume","library_id":"lib_11111111111111111111111111111111","image_id":"img_11111111111111111111111111111111","operation_id":"iop_11111111111111111111111111111111","incarnation":"incarnation-1","producer":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","initial_reader":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","content":{"size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},"receipts":[]}`
	b, digest, err := empty.Canonical()
	if err != nil || string(b) != golden || !ImageHashValid(digest) {
		t.Fatalf("empty golden: %s %v", b, err)
	}
	change := ImageReaderChange{1, empty.StoreID, empty.ImageID, "irg_11111111111111111111111111111111", 1, nil}
	const revoke = `{"version":1,"store_id":"ims_11111111111111111111111111111111","image_id":"img_11111111111111111111111111111111","operation_id":"irg_11111111111111111111111111111111","expected_generation":1,"reader":null}`
	rb, rd, err := change.Canonical()
	if err != nil || string(rb) != revoke || !ImageHashValid(rd) || rd == digest {
		t.Fatalf("revoke golden: %s %v", rb, err)
	}
	// Literal nested codec golden; custody authority is independently checked by
	// reservation against committed operation rows, not inferred by this codec.
	const part = `{"kind":"custody-v1","volume":"volume","chunk":{"session":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","epoch":1,"id":"chunk-1","file_id":"incarnation-1","offset":0,"size":1,"sha256":"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"},"artifact_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","artifact_size":512,"request_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","receipt_id":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","committed_at_unix_ns":1}`
	nonempty := strings.Replace(golden, `"size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`, `"size":1,"sha256":"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"`, 1)
	nonempty = strings.Replace(nonempty, `"receipts":[]`, `"receipts":[`+part+`]`, 1)
	var parsed ImageSeal
	if DecodeImageCanonical([]byte(nonempty), &parsed) != nil {
		t.Fatal("nonempty decode")
	}
	nb, nd, err := parsed.Canonical()
	if err != nil || string(nb) != nonempty || nd == digest {
		t.Fatalf("nonempty golden: %v", err)
	}
	variants := []string{
		strings.Replace(golden, `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(golden, `"size":0`, `"size":0,"size":0`, 1),
		strings.Replace(golden, `"size":0`, `"size":0e0`, 1),
		strings.Replace(golden, `"version":1,`, "", 1),
		strings.Replace(golden, `"content":{`, `"content":{"foreign":1,`, 1),
		strings.Replace(golden, `"receipts":[]`, `"receipts":null`, 1),
		golden + "\n", golden + "{}", strings.Replace(golden, `"volume":"volume"`, `"volume":"vol\u0075me"`, 1),
		strings.Replace(nonempty, `"offset":0`, `"offset":0,"offset":0`, 1),
		string(append([]byte(golden[:len(golden)-1]), 0xff, '}')),
	}
	for i, bad := range variants {
		var s ImageSeal
		err := DecodeImageCanonical([]byte(bad), &s)
		if err == nil {
			_, _, err = s.Canonical()
		}
		if err == nil {
			t.Fatalf("noncanonical variant %d accepted", i)
		}
	}
}
func TestImageOptionalPromotionReservationAndGrantHistory(t *testing.T) {
	ctx := context.Background()
	j, path := testJournal(t)
	var version int
	if err := j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 4 {
		t.Fatal("default journal promoted", version, err)
	}
	cfg := imageTestConfig()
	invalid := cfg
	invalid.MaxImages = MaxImages + 1
	if err := j.ConfigureImages(ctx, "volume", &invalid); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 4 {
		t.Fatal("invalid promotion mutated", version, err)
	}
	if err := j.ConfigureImages(ctx, "volume", &cfg); err != nil {
		t.Fatal(err)
	}
	if err := j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 5 {
		t.Fatal(version, err)
	}
	if err := j.ConfigureImages(ctx, "volume", nil); !errors.Is(err, ErrImageInactive) {
		t.Fatal("disabled initialized store", err)
	}
	seal := emptyImage()
	record, err := j.ReserveImage(ctx, seal)
	if err != nil || record.State != "RESERVED" || record.Receipt != nil {
		t.Fatal(record, err)
	}
	if _, err = j.ReserveImage(ctx, seal); err != nil {
		t.Fatal("retry charged", err)
	}
	changed := seal
	changed.InitialReader = strings.Repeat("c", 64)
	if _, err = j.ReserveImage(ctx, changed); !errors.Is(err, ErrChangedRequest) {
		t.Fatal("changed reservation", err)
	}
	ref := ImageRef{1, seal.StoreID, seal.LibraryID, seal.ImageID}
	receipt, err := j.SealImage(ctx, ref, record.Digest)
	if err != nil {
		t.Fatal(err)
	}
	change := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_11111111111111111111111111111111", 1, nil}
	granted, err := j.RebindImageReader(ctx, seal.Producer, change)
	if err != nil || granted.Generation != 2 || granted.Reader != nil {
		t.Fatal(granted, err)
	}
	if _, err = j.SealImage(ctx, ref, record.Digest); err != nil {
		t.Fatal(err)
	}
	after, err := j.InspectImage(ctx, ref)
	if err != nil || after.Generation != 2 || after.Reader != nil || *after.Receipt != receipt {
		t.Fatal("seal retry resurrected grant", after, err)
	}
	beforeStore, err := readImageStore(j.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.RebindImageReader(ctx, seal.Producer, change); err != nil {
		t.Fatal(err)
	}
	afterStore, err := readImageStore(j.db)
	if err != nil || beforeStore.metadata != afterStore.metadata {
		t.Fatal("reader retry charged", err)
	}
	stale := change
	stale.OperationID = "irg_22222222222222222222222222222222"
	if _, err = j.RebindImageReader(ctx, seal.Producer, stale); !errors.Is(err, ErrImageConflict) {
		t.Fatal("stale CAS", err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	cfg.Initialize = false
	if err = next.ConfigureImages(ctx, "volume", &cfg); err != nil {
		t.Fatal(err)
	}
	restored, err := next.InspectImage(ctx, ref)
	if err != nil || restored.Generation != 2 || restored.Reader != nil || *restored.Receipt != receipt {
		t.Fatal(restored, err)
	}
}
func TestImageReservationSurvivesRestartAndCountsEmptyQuota(t *testing.T) {
	ctx := context.Background()
	j, path := testJournal(t)
	cfg := imageTestConfig()
	cfg.MaxImages = 1
	if err := j.ConfigureImages(ctx, "volume", &cfg); err != nil {
		t.Fatal(err)
	}
	seal := emptyImage()
	if _, err := j.ReserveImage(ctx, seal); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	cfg.Initialize = false
	if err = next.ConfigureImages(ctx, "volume", &cfg); err != nil {
		t.Fatal(err)
	}
	if r, err := next.ReserveImage(ctx, seal); err != nil || r.State != "RESERVED" {
		t.Fatal(r, err)
	}
	other := seal
	other.ImageID = "img_22222222222222222222222222222222"
	other.OperationID = "iop_22222222222222222222222222222222"
	other.Incarnation = "empty-2"
	if _, err := next.ReserveImage(ctx, other); !errors.Is(err, ErrImageQuota) {
		t.Fatal("unfinished empty image did not count", err)
	}
}
func TestImageOpenRejectsCorruptHistoryAndInactiveAdmission(t *testing.T) {
	for _, mode := range []string{"counter", "part", "reader", "history", "digest", "downgrade", "partial", "inactive"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			j, path := testJournal(t)
			cfg := imageTestConfig()
			if err := j.ConfigureImages(ctx, "volume", &cfg); err != nil {
				t.Fatal(err)
			}
			seal := emptyImage()
			r, err := j.ReserveImage(ctx, seal)
			if err != nil {
				t.Fatal(err)
			}
			ref := ImageRef{1, seal.StoreID, seal.LibraryID, seal.ImageID}
			if _, err = j.SealImage(ctx, ref, r.Digest); err != nil {
				t.Fatal(err)
			}
			change := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_11111111111111111111111111111111", 1, nil}
			if _, err = j.RebindImageReader(ctx, seal.Producer, change); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			statements := map[string]string{"counter": `UPDATE image_store SET metadata=metadata+1`, "part": `INSERT INTO image_parts VALUES('img_11111111111111111111111111111111',0,'volume','custody-v1:absent',X'7b7d')`, "reader": `UPDATE image_readers SET generation=3`, "history": `DELETE FROM image_reader_events`, "digest": `UPDATE image_seals SET digest='bad'`, "downgrade": `PRAGMA user_version=4`, "partial": `DROP TABLE image_reader_events`, "inactive": `UPDATE image_store SET active=0`}
			if _, err = db.Exec(statements[mode]); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path)
			if mode == "inactive" {
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if err = reopened.ConfigureImages(ctx, "volume", &cfg); !errors.Is(err, ErrImageInactive) {
					t.Fatal("inactive authority enabled", err)
				}
				if _, err = reopened.InspectImage(ctx, ref); !errors.Is(err, ErrImageInactive) {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				reopened.Close()
				t.Fatal("corrupt image authority opened")
			}
		})
	}
}
func TestImageConfigAndEnvelopeBounds(t *testing.T) {
	s := emptyImage()
	b, _, _ := s.Canonical()
	var decoded ImageSeal
	if DecodeImageCanonical(b, &decoded) != nil || !bytes.Equal(b, mustImageJSON(decoded)) {
		t.Fatal("canonical round trip")
	}
	if DecodeImageCanonical(bytes.Repeat([]byte(" "), MaxImageEnvelope+1), &decoded) == nil {
		t.Fatal("oversize envelope admitted")
	}
	cfg := imageTestConfig()
	for _, field := range []string{"images", "logical", "metadata", "id", "version"} {
		bad := cfg
		switch field {
		case "images":
			bad.MaxImages = MaxImages + 1
		case "logical":
			bad.MaxLogicalBytes = MaxImageLogicalBytes + 1
		case "metadata":
			bad.MaxMetadataBytes = MaxImageMetadataBytes + 1
		case "id":
			bad.StoreID = "ims_" + strings.Repeat("0", 32)
		case "version":
			bad.Version = 2
		}
		if bad.Validate() == nil {
			t.Fatal(field)
		}
	}
}
func mustImageJSON(v any) []byte { b, _ := json.Marshal(v); return b }

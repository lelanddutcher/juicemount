package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const preferenceAge int64 = 30 * 24 * 60 * 60
const preferenceSubjects = 1024
const preferenceEntries = 65536
const preferenceReceipts = 32768

func preferencesSchema() []schemaObject {
	ddls := []string{
		`CREATE TABLE preference_control (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 clock_floor_unix INTEGER NOT NULL CHECK(clock_floor_unix>=0),
 subject_count INTEGER NOT NULL CHECK(subject_count BETWEEN 0 AND 1024),
 entry_count INTEGER NOT NULL CHECK(entry_count BETWEEN 0 AND 65536),
 receipt_count INTEGER NOT NULL CHECK(receipt_count BETWEEN 0 AND 32768)
)`,
		`CREATE TABLE preference_subjects (
 subject_id TEXT PRIMARY KEY NOT NULL REFERENCES policy_subjects(subject_id),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 generation INTEGER NOT NULL CHECK(generation>0),
 entry_count INTEGER NOT NULL CHECK(entry_count BETWEEN 0 AND 256),
 receipt_count INTEGER NOT NULL CHECK(receipt_count BETWEEN 0 AND 128)
)`,
		`CREATE TABLE preferences (
 subject_id TEXT NOT NULL REFERENCES preference_subjects(subject_id),
 asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 selection_token TEXT NOT NULL UNIQUE CHECK(length(selection_token)=69 AND substr(selection_token,1,5)='sel1_' AND substr(selection_token,6) NOT GLOB '*[^0-9a-f]*'),
 person_order INTEGER NOT NULL CHECK(person_order>0),
 selected_unix INTEGER NOT NULL CHECK(selected_unix>=0 AND selected_unix<=9223372036852183807),
 expires_unix INTEGER NOT NULL CHECK(expires_unix=selected_unix+2592000),
 PRIMARY KEY(subject_id,asset_id),
 UNIQUE(subject_id,person_order)
)`,
		`CREATE TABLE preference_receipts (
 subject_id TEXT NOT NULL REFERENCES preference_subjects(subject_id),
 operation_id TEXT NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 1024),
 payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
 receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 512),
 expires_unix INTEGER NOT NULL CHECK(expires_unix>0),
 PRIMARY KEY(subject_id,operation_id)
)`,
	}
	var objects []schemaObject
	for _, ddl := range ddls {
		name := strings.Fields(ddl)[2]
		objects = append(objects, schemaObject{"table", name, name, ddl})
	}
	for _, v := range []struct{ name, table, columns string }{
		{"preferences_recent", "preferences", "subject_id,person_order DESC,asset_id"},
		{"preferences_expiry", "preferences", "expires_unix,subject_id,asset_id"},
		{"preference_receipts_subject_expiry", "preference_receipts", "subject_id,expires_unix,operation_id"},
		{"preference_receipts_expiry", "preference_receipts", "expires_unix,subject_id,operation_id"},
	} {
		objects = append(objects, schemaObject{"index", v.name, v.table, "CREATE INDEX " + v.name + " ON " + v.table + "(" + v.columns + ")"})
	}
	triggers := []struct{ name, table, body string }{
		{"preference_control_identity", "preference_control", `BEFORE UPDATE ON preference_control WHEN NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id OR NEW.clock_floor_unix<OLD.clock_floor_unix BEGIN SELECT RAISE(ABORT,'preference control identity'); END`},
		{"preference_control_no_delete", "preference_control", `BEFORE DELETE ON preference_control BEGIN SELECT RAISE(ABORT,'preference control lifetime'); END`},
		{"preference_subjects_identity", "preference_subjects", `BEFORE UPDATE ON preference_subjects WHEN NEW.subject_id<>OLD.subject_id OR NEW.library_id<>OLD.library_id OR NEW.generation<OLD.generation OR (NEW.generation<>OLD.generation AND NEW.generation<>OLD.generation+1) BEGIN SELECT RAISE(ABORT,'preference generation identity'); END`},
		{"preference_subjects_no_delete", "preference_subjects", `BEFORE DELETE ON preference_subjects BEGIN SELECT RAISE(ABORT,'preference generation lifetime'); END`},
		{"preference_subjects_insert", "preference_subjects", `AFTER INSERT ON preference_subjects BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM policy_subjects WHERE subject_id=NEW.subject_id AND library_id=NEW.library_id AND kind='person') THEN RAISE(ABORT,'preference person membership') END; UPDATE preference_control SET subject_count=subject_count+1 WHERE singleton=1; END`},
		{"preferences_identity", "preferences", `BEFORE UPDATE ON preferences WHEN NEW.subject_id<>OLD.subject_id OR NEW.asset_id<>OLD.asset_id OR NEW.selection_token=OLD.selection_token OR NEW.person_order<=OLD.person_order BEGIN SELECT RAISE(ABORT,'preference replacement identity'); END`},
		{"preferences_insert", "preferences", `AFTER INSERT ON preferences BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM preference_subjects p JOIN assets a ON a.library_id=p.library_id WHERE p.subject_id=NEW.subject_id AND a.asset_id=NEW.asset_id AND p.generation=NEW.person_order) THEN RAISE(ABORT,'preference membership') END; UPDATE preference_subjects SET entry_count=entry_count+1 WHERE subject_id=NEW.subject_id; UPDATE preference_control SET entry_count=entry_count+1 WHERE singleton=1; END`},
		{"preferences_order", "preferences", `AFTER UPDATE ON preferences WHEN NEW.person_order<>(SELECT generation FROM preference_subjects WHERE subject_id=NEW.subject_id) BEGIN SELECT RAISE(ABORT,'preference order'); END`},
		{"preferences_delete", "preferences", `AFTER DELETE ON preferences BEGIN UPDATE preference_subjects SET entry_count=entry_count-1 WHERE subject_id=OLD.subject_id; UPDATE preference_control SET entry_count=entry_count-1 WHERE singleton=1; END`},
		{"preference_receipts_no_update", "preference_receipts", `BEFORE UPDATE ON preference_receipts BEGIN SELECT RAISE(ABORT,'immutable preference receipt'); END`},
		{"preference_receipts_delete_guard", "preference_receipts", `BEFORE DELETE ON preference_receipts WHEN OLD.expires_unix>(SELECT clock_floor_unix FROM preference_control WHERE singleton=1) BEGIN SELECT RAISE(ABORT,'unexpired preference receipt'); END`},
		{"preference_receipts_insert", "preference_receipts", `AFTER INSERT ON preference_receipts BEGIN UPDATE preference_subjects SET receipt_count=receipt_count+1 WHERE subject_id=NEW.subject_id; UPDATE preference_control SET receipt_count=receipt_count+1 WHERE singleton=1; END`},
		{"preference_receipts_delete", "preference_receipts", `AFTER DELETE ON preference_receipts BEGIN UPDATE preference_subjects SET receipt_count=receipt_count-1 WHERE subject_id=OLD.subject_id; UPDATE preference_control SET receipt_count=receipt_count-1 WHERE singleton=1; END`},
	}
	for _, t := range triggers {
		objects = append(objects, schemaObject{"trigger", t.name, t.table, "CREATE TRIGGER " + t.name + " " + t.body})
	}
	return objects
}

// Recognition/export validate the operational projection but never emit it.
func checkPreferences(ctx context.Context, q queryer, library continuity.LibraryID) error {
	c, err := readPreferenceControl(ctx, q, library)
	if err != nil {
		return err
	}
	var subjects, entries, receipts int64
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM preference_subjects),(SELECT count(*) FROM preferences),(SELECT count(*) FROM preference_receipts)`).Scan(&subjects, &entries, &receipts); err != nil {
		return err
	}
	if subjects != c.subjects || entries != c.entries || receipts != c.receipts {
		return corrupt("preference global counters")
	}
	var invalid int
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM preference_subjects p JOIN policy_subjects s ON s.subject_id=p.subject_id WHERE p.library_id<>? OR s.library_id<>p.library_id OR s.kind<>'person' OR typeof(p.generation)<>'integer' OR p.generation<=0 OR p.entry_count<>(SELECT count(*) FROM preferences e WHERE e.subject_id=p.subject_id) OR p.receipt_count<>(SELECT count(*) FROM preference_receipts r WHERE r.subject_id=p.subject_id))`, library).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return corrupt("preference subject counters")
	}
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM preferences e JOIN preference_subjects p ON p.subject_id=e.subject_id JOIN assets a ON a.asset_id=e.asset_id WHERE a.library_id<>p.library_id OR e.person_order>p.generation OR typeof(e.person_order)<>'integer' OR typeof(e.selected_unix)<>'integer' OR typeof(e.expires_unix)<>'integer')`).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return corrupt("preference membership")
	}
	return walkRows(ctx, q, `SELECT subject_id,operation_id,request,payload_sha256,receipt,expires_unix FROM preference_receipts`, func(rows *sql.Rows) error {
		var subject SubjectID
		var id, digest string
		var request, receipt []byte
		var expiry int64
		if err := rows.Scan(&subject, &id, &request, &digest, &receipt, &expiry); err != nil {
			return err
		}
		r, err := decodePreferenceRequest(request)
		if err != nil {
			return err
		}
		if r.LibraryID != library || r.OperationID != id || r.ExpiresUnix != expiry || payloadDigest(request) != digest || subject.Validate() != nil {
			return corrupt("preference receipt identity")
		}
		if _, err := decodePreferenceReceipt(receipt, r); err != nil {
			return fmt.Errorf("%w: preference receipt", err)
		}
		return nil
	})
}

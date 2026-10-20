package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Hash every operational value while retaining only one bounded row. The clock
// floor can be omitted when a successful maintenance phase is expected to survive
// a later failed mutation; that floor is then checked separately.
func preferenceOperationalSnapshot(t *testing.T, q queryer, includeFloor bool) string {
	t.Helper()
	h := sha256.New()
	control := `SELECT singleton,library_id,subject_count,entry_count,receipt_count FROM preference_control ORDER BY singleton`
	if includeFloor {
		control = `SELECT * FROM preference_control ORDER BY singleton`
	}
	for _, query := range []string{control, `SELECT * FROM preference_subjects ORDER BY subject_id`, `SELECT * FROM preferences ORDER BY subject_id,asset_id`, `SELECT * FROM preference_receipts ORDER BY subject_id,operation_id`} {
		h.Write([]byte(query))
		rows, err := q.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			h.Write(encoded)
			h.Write([]byte{'\n'})
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestPreferenceDeferredCommitFailuresAndReopenedRetry(t *testing.T) {
	for _, phase := range []string{"final_mutation", "maintenance"} {
		t.Run(phase, func(t *testing.T) {
			s, _, _, d, bearer, now := preferencesTestStore(t)
			initial := preferenceRequestTest("commit-existing", "select", testAsset, nil, now.Load())
			if phase == "maintenance" {
				initial.ExpiresUnix = now.Load() + 60
			}
			first := recordPreferenceTest(t, d, bearer, initial)
			beforeSubject, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
			if err != nil {
				t.Fatal(err)
			}
			beforeControl, err := readPreferenceControl(context.Background(), s.db, testLibrary)
			if err != nil {
				t.Fatal(err)
			}
			before := preferenceOperationalSnapshot(t, s.db, false)
			if phase == "maintenance" {
				now.Add(60)
			} else {
				now.Add(10)
			}
			maintenanceTime := now.Load()
			request := preferenceRequestTest("commit-target", "select", otherAsset, nil, maintenanceTime)
			// These TEMP objects exist only on this Store's one write connection. A
			// missing TEMP parent is deferred until the actual SQLite COMMIT. The
			// final trigger cannot fire in the maintenance phase (no receipt INSERT);
			// the cleanup trigger fires only when deleting the known expired receipt.
			statements := []string{`CREATE TEMP TABLE preference_fault_parent(id INTEGER PRIMARY KEY)`, `CREATE TEMP TABLE preference_fault_child(id INTEGER REFERENCES preference_fault_parent(id) DEFERRABLE INITIALLY DEFERRED)`}
			if phase == "final_mutation" {
				statements = append(statements, `CREATE TEMP TRIGGER preference_commit_fault AFTER INSERT ON main.preference_receipts WHEN NEW.operation_id='commit-target' BEGIN INSERT INTO preference_fault_child(id) VALUES(1); END`)
			} else {
				statements = append(statements, `CREATE TEMP TRIGGER preference_commit_fault AFTER DELETE ON main.preference_receipts WHEN OLD.operation_id='commit-existing' BEGIN INSERT INTO preference_fault_child(id) VALUES(1); END`)
			}
			for _, statement := range statements {
				if _, err := s.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			var calls atomic.Int64
			s.clock = func() time.Time {
				n := calls.Add(1)
				value := maintenanceTime
				if phase == "final_mutation" && n >= 6 {
					value += 10
				}
				return time.Unix(value, 0)
			}
			var out bytes.Buffer
			result, err := d.RecordPreference(context.Background(), bearer, request, &out)
			if err == nil || !strings.Contains(err.Error(), "preference commit outcome uncertain") || result.Started || out.Len() != 0 {
				t.Fatalf("%s did not reach real uncertain COMMIT without disclosure: %+v %v", phase, result, err)
			}
			wantCalls := int64(3)
			wantFloor := beforeControl.floor
			if phase == "final_mutation" {
				wantCalls = 6
				wantFloor = maintenanceTime
			}
			if calls.Load() != wantCalls {
				t.Fatalf("fault hit wrong transaction phase: calls=%d want=%d", calls.Load(), wantCalls)
			}
			path := s.path
			if err := d.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenContext(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			reopened.clock = func() time.Time { return time.Unix(now.Load(), 0) }
			next := discoveryServiceTest(t, reopened)
			defer next.Close(context.Background())
			// Reopening drops the connection-local fault. Main-table identity,
			// generation, entries and receipt bytes must all be atomic, not partial.
			afterSubject, err := readPreferenceSubject(context.Background(), reopened.db, testLibrary, ownerSubject)
			if err != nil || afterSubject != beforeSubject {
				t.Fatalf("failed COMMIT leaked generation/counters: %+v %+v %v", beforeSubject, afterSubject, err)
			}
			afterControl, err := readPreferenceControl(context.Background(), reopened.db, testLibrary)
			if err != nil || afterControl.floor != wantFloor {
				t.Fatalf("%s floor atomicity: %+v want=%d %v", phase, afterControl, wantFloor, err)
			}
			if after := preferenceOperationalSnapshot(t, reopened.db, false); after != before {
				t.Fatal("failed COMMIT left partial entry/receipt/subject state")
			}
			if current := preferenceStatusTest(t, next, bearer, testAsset); current == nil || *current != *first.Token {
				t.Fatal("failed COMMIT changed preexisting selection")
			}
			if preferenceStatusTest(t, next, bearer, otherAsset) != nil {
				t.Fatal("failed COMMIT exposed target selection")
			}
			recovered := recordPreferenceTest(t, next, bearer, request)
			retry := recordPreferenceTest(t, next, bearer, request)
			if recovered.Token == nil || retry.Token == nil || *recovered.Token != *retry.Token {
				t.Fatal("same-operation recovery minted another token")
			}
			finalSubject, err := readPreferenceSubject(context.Background(), reopened.db, testLibrary, ownerSubject)
			wantReceipts := int64(2)
			if phase == "maintenance" {
				wantReceipts = 1
			}
			if err != nil || finalSubject.generation != beforeSubject.generation+1 || finalSubject.entries != 2 || finalSubject.receipts != wantReceipts {
				t.Fatalf("recovery duplicated or lost state: %+v %v", finalSubject, err)
			}
			finalControl, err := readPreferenceControl(context.Background(), reopened.db, testLibrary)
			if err != nil || finalControl.floor != maintenanceTime || finalControl.subjects != 1 || finalControl.entries != 2 || finalControl.receipts != wantReceipts {
				t.Fatalf("recovery control counters/floor: %+v %v", finalControl, err)
			}
		})
	}
}

func preferenceCapacityPeople(t *testing.T, o *LocalOwnerService, a AuthorityStatus) []SubjectID {
	t.Helper()
	people := make([]SubjectID, 256)
	changes := make([]PolicyChange, 0, len(people))
	for i := range people {
		people[i] = SubjectID(fmt.Sprintf("sub_%032x", i+4096))
		changes = append(changes, NewPolicyChange("create_subject", CreateSubject{people[i], SubjectValue{"person", "synthetic global capacity", true}}))
	}
	policyTest(t, o, a, "capacity-people", changes...)
	return people
}

func seedGlobalPreferenceEntries(t *testing.T, s *Store, people []SubjectID, now int64) {
	t.Helper()
	var changes []Change
	// Together with the two existing assets these provide256 distinct entries
	// per filler person. The caller retains only its initial one-entry state.
	for i := 1; i <= 254; i++ {
		changes = append(changes, Change{CreateAsset: &CreateAsset{AssetID: preferenceCapacityAsset(i), Label: "synthetic capacity"}})
	}
	applyTest(t, s, testOperation("capacity-assets", changes...))
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	person, err := tx.Prepare(`INSERT INTO preference_subjects(subject_id,library_id,generation,entry_count,receipt_count) VALUES(?,?,1,0,0)`)
	if err != nil {
		t.Fatal(err)
	}
	defer person.Close()
	advance, err := tx.Prepare(`UPDATE preference_subjects SET generation=generation+1 WHERE subject_id=?`)
	if err != nil {
		t.Fatal(err)
	}
	defer advance.Close()
	entry, err := tx.Prepare(`INSERT INTO preferences(subject_id,asset_id,selection_token,person_order,selected_unix,expires_unix) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.Close()
	for i, subject := range people {
		if _, err := person.Exec(subject, testLibrary); err != nil {
			t.Fatal(err)
		}
		count := 256
		if i == len(people)-1 {
			count = 255
		}
		for j := 0; j < count; j++ {
			if j > 0 {
				if _, err := advance.Exec(subject); err != nil {
					t.Fatal(err)
				}
			}
			asset := testAsset
			if j == 1 {
				asset = otherAsset
			} else if j > 1 {
				asset = preferenceCapacityAsset(j - 1)
			}
			token := fmt.Sprintf("sel1_%064x", (i+1)*256+j)
			if _, err := entry.Exec(subject, asset, token, j+1, now, now+preferenceAge); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := errors.Join(person.Close(), advance.Close(), entry.Close()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func preferenceCapacityAsset(i int) continuity.AssetID {
	return continuity.AssetID(fmt.Sprintf("asset_%032x", i+4096))
}

func seedGlobalPreferenceReceipts(t *testing.T, s *Store, people []SubjectID, now int64) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	person, err := tx.Prepare(`INSERT INTO preference_subjects(subject_id,library_id,generation,entry_count,receipt_count) VALUES(?,?,1,0,0)`)
	if err != nil {
		t.Fatal(err)
	}
	defer person.Close()
	advance, err := tx.Prepare(`UPDATE preference_subjects SET generation=generation+1 WHERE subject_id=?`)
	if err != nil {
		t.Fatal(err)
	}
	defer advance.Close()
	insert, err := tx.Prepare(`INSERT INTO preference_receipts(subject_id,operation_id,request,payload_sha256,receipt,expires_unix) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	for i, subject := range people {
		count := 128
		if i == len(people)-1 {
			count = 127
		}
		if _, err := person.Exec(subject, testLibrary); err != nil {
			t.Fatal(err)
		}
		// Point-in-time valid bounded state: prior selections have been cleared;
		// those earlier unexpired receipts need not describe current preferences.
		for j := 0; j < count; j++ {
			if j > 0 {
				if _, err := advance.Exec(subject); err != nil {
					t.Fatal(err)
				}
			}
			id := fmt.Sprintf("seed-%03d", j)
			request := preferenceRequestTest(id, "select", testAsset, nil, now)
			_, canonical, err := canonicalPreferenceRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			token := fmt.Sprintf("sel1_%064x", (i+1)*256+j)
			receipt, err := json.Marshal(preferenceReceipt{2, id, "select", &token, "operation_result"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := insert.Exec(subject, id, canonical, payloadDigest(canonical), receipt, request.ExpiresUnix); err != nil {
				t.Fatal(err)
			}
		}
		// One later clear accounts for the absent current entry; every fixture
		// generation is advanced by exactly one, through the persistent guard.
		if _, err := advance.Exec(subject); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(person.Close(), advance.Close(), insert.Close()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPreferenceGlobalEntryLimitIndependentOfPersonCaps(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	original := preferenceRequestTest("caller-original", "select", testAsset, nil, now.Load())
	receipt := recordPreferenceTest(t, d, bearer, original)
	people := preferenceCapacityPeople(t, o, a)
	seedGlobalPreferenceEntries(t, s, people, now.Load())
	assertPreferenceGlobalLimit(t, s, d, bearer, original, receipt, now.Load(), 65536, 1)
}

func TestPreferenceGlobalReceiptLimitIndependentOfPersonCaps(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	original := preferenceRequestTest("caller-original", "select", testAsset, nil, now.Load())
	receipt := recordPreferenceTest(t, d, bearer, original)
	people := preferenceCapacityPeople(t, o, a)
	seedGlobalPreferenceReceipts(t, s, people, now.Load())
	assertPreferenceGlobalLimit(t, s, d, bearer, original, receipt, now.Load(), 1, 32768)
}

func assertPreferenceGlobalLimit(t *testing.T, s *Store, d *DiscoveryService, bearer []byte, original PreferenceRequest, receipt preferenceReceipt, now int64, wantEntries, wantReceipts int64) {
	t.Helper()
	if err := checkPreferences(context.Background(), s.db, testLibrary); err != nil {
		t.Fatalf("global fixture violates operational invariants: %v", err)
	}
	caller, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if err != nil || caller.entries != 1 || caller.receipts != 1 || caller.generation != 1 {
		t.Fatalf("caller is not below both own caps: %+v %v", caller, err)
	}
	control, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || control.entries != wantEntries || control.receipts != wantReceipts || control.subjects != 257 {
		t.Fatalf("global fixture capacity: %+v %v", control, err)
	}
	before := preferenceOperationalSnapshot(t, s.db, true)
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, preferenceRequestTest("global-full", "select", otherAsset, nil, now), &out)
	if DiscoveryErrorCode(err) != "busy" || result.Started || out.Len() != 0 {
		t.Fatalf("global-only capacity admission was not refused: %+v %v", result, err)
	}
	if after := preferenceOperationalSnapshot(t, s.db, true); after != before {
		t.Fatal("global refusal evicted live data or partially changed generation/entry/receipt/floor")
	}
	if _, err := readPreference(context.Background(), s.db, ownerSubject, otherAsset); !errors.Is(err, ErrNotFound) {
		t.Fatalf("global refusal left caller entry: %v", err)
	}
	if retry := recordPreferenceTest(t, d, bearer, original); retry.Token == nil || receipt.Token == nil || *retry.Token != *receipt.Token {
		t.Fatal("exact retry refused or changed token at global capacity")
	}
	if after := preferenceOperationalSnapshot(t, s.db, true); after != before {
		t.Fatal("exact retry changed globally full state or evicted live rows")
	}
}

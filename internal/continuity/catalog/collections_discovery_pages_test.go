package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// These capacity fixtures arrange structurally bounded FK-valid projections in
// one transaction. They are not historical replay, live-write or physical quota
// proofs; ordinary API admission/replay has separate coverage. Reads must still
// exercise production snapshots, rights, page construction and fresh fences.
func collectionReadCapacityMembers(t *testing.T, s *Store, d *DiscoveryService, owner []byte, count int, removed bool, oldGrants bool) CollectionID {
	t.Helper()
	ctx := context.Background()
	create := collectionTxCreate(t, 1801)
	create.Value = collectionTxRaw(t, CreateCollection{strings.Repeat("\x01", 256), "cgr_22222222222222222222222222222222", []NewCollectionEntry{}})
	collectionTxCall(t, d, owner, create)
	var sequence, metadataSequence, policySequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT sequence FROM collection_events WHERE collection_id=?`, create.CollectionID).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT min(sequence) FROM events`).Scan(&metadataSequence); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT min(sequence) FROM policy_events`).Scan(&policySequence); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	assetStmt, err := tx.PrepareContext(ctx, `INSERT INTO assets(library_id,asset_id,label,head,created_sequence,updated_sequence) VALUES(?,?,?,NULL,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer assetStmt.Close()
	entryStmt, err := tx.PrepareContext(ctx, `INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer entryStmt.Close()
	label := strings.Repeat("\x01", 4096)
	for i := 1; i <= count; i++ {
		asset := continuity.AssetID(fmt.Sprintf("asset_%032x", 100000+i))
		entry := CollectionEntryID(fmt.Sprintf("cent_%032x", 100000+i))
		if _, err := assetStmt.ExecContext(ctx, testLibrary, asset, label, metadataSequence, metadataSequence); err != nil {
			t.Fatal(err)
		}
		if _, err := entryStmt.ExecContext(ctx, entry, testLibrary, create.CollectionID, asset, removed, sequence, sequence); err != nil {
			t.Fatal(err)
		}
		if oldGrants && i <= 4095 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO policy_grants(library_id,grant_id,subject_id,scope_kind,asset_id,actions,created_sequence,updated_sequence,removed) VALUES(?,?,?,'asset',?,?,?,?,0)`, testLibrary, fmt.Sprintf("capacity-old-%d", i), ownerSubject, asset, []byte(`["assets.discover"]`), policySequence, policySequence); err != nil {
				t.Fatal(err)
			}
		}
	}
	active := count
	if removed {
		active = 0
	}
	for _, statement := range []string{`UPDATE collection_quota SET active_entries=?,lifetime_entries=? WHERE collection_id=?`, `UPDATE collection_library_quota SET active_entries=?,lifetime_entries=? WHERE library_id=?`} {
		arg := any(create.CollectionID)
		if strings.Contains(statement, "collection_library_quota") {
			arg = testLibrary
		}
		if _, err := tx.ExecContext(ctx, statement, active, count, arg); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return create.CollectionID
}
func collectionReadAuthBinding(t *testing.T, d *DiscoveryService, bearer []byte) discoveryBinding {
	t.Helper()
	tx, done, err := d.store.readSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h, c, s, err := d.store.authenticateSnapshot(context.Background(), tx, d.library, bearer)
	if e := done(); e != nil {
		t.Fatal(e)
	}
	if err != nil {
		t.Fatal(err)
	}
	return discoveryBinding{h, s.AuthorizationGeneration, c.issued, c.expires, nil}
}

func TestCollectionReadFullCapacityLabelsAndForwardBodyEviction(t *testing.T) {
	for _, removed := range []bool{false, true} {
		name := "active4096"
		count := 4096
		if removed {
			name = "removed8192"
			count = 8192
		}
		t.Run(name, func(t *testing.T) {
			s, _, _, d, owner, _ := collectionTxStore(t)
			col := collectionReadCapacityMembers(t, s, d, owner, count, removed, true)
			r := collectionReadRequest("members", col)
			if removed {
				r.State = "removed"
			}
			first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
			if first.Next == nil {
				t.Fatal("large collection had no successor")
			}
			var window *discoveryWindow
			var firstToken string
			d.mu.Lock()
			for _, w := range d.windows {
				if w.family == collectionReadFamily {
					window = w
					firstToken = w.tokens[0]
				}
			}
			d.mu.Unlock()
			if window == nil {
				t.Fatal("no retained member window")
			}
			// Pin an actual predecessor BODY while successor pages are constructed. This
			// proves eviction distinguishes body leases from window construction leases.
			w, p, cost, err := d.beginCollectionPage(context.Background(), firstToken, collectionReadAuthBinding(t, d, owner))
			if err != nil {
				t.Fatal(err)
			}
			released := false
			defer func() {
				if !released {
					d.releaseCollectionBody(w, p, cost)
				}
			}()
			seen := 0
			pages := 0
			bytesSeen := 0
			lastEntry := CollectionEntryID("")
			current := first
			for {
				pages++
				if pages > 8192 {
					t.Fatal("nonprogressing continuation")
				}
				if current.MatchedCount != count || !current.Complete || current.State != r.State && r.Kind != "page" {
					t.Fatal("frozen membership/count changed")
				}
				for _, raw := range current.Items {
					var member CollectionMember
					if err := json.Unmarshal(raw, &member); err != nil {
						t.Fatal(err)
					}
					if len(member.Label) != 4096 || member.Label != strings.Repeat("\x01", 4096) || member.Removed != removed || member.EntryID <= lastEntry {
						t.Fatal("full colliding label/order/removed state lost")
					}
					lastEntry = member.EntryID
					seen++
					bytesSeen += len(raw)
				}
				d.mu.Lock()
				if window.collection.bodyBytes > 2<<20 || d.bytes > discoveryQuota || window.charge > discoveryBuildBytes {
					t.Error("retained page accounting exceeded cap")
				}
				d.mu.Unlock()
				if current.Next == nil {
					break
				}
				q := collectionReadRequest("page", "")
				q.Token = *current.Next
				current = collectionReadEnvelope(t, collectionReadCall(t, d, owner, q))
			}
			if seen != count || bytesSeen <= 2<<20 || pages < 3 {
				t.Fatalf("capacity not actually traversed: %d/%d pages%d bytes%d", seen, count, pages, bytesSeen)
			}
			d.releaseCollectionBody(w, p, cost)
			released = true
			// Some old pages are irrevocably evicted, while the forward window survives.
			var evicted string
			d.mu.Lock()
			for i, p := range window.collection.pages {
				if p.evicted {
					evicted = window.tokens[i]
					break
				}
			}
			d.mu.Unlock()
			if evicted == "" {
				t.Fatal("large traversal never evicted a body")
			}
			q := collectionReadRequest("page", "")
			q.Token = evicted
			collectionReadDenied(t, d, owner, q, "restart_query")
			d.mu.Lock()
			if window.retired || len(d.building) != 0 {
				t.Error("old-body refusal retired forward window or leaked builder")
			}
			d.mu.Unlock()
		})
	}
}

func TestCollectionReadTokenFamilyOwnerBeforeCacheEffects(t *testing.T) {
	_, _, _, d, owner, reader := collectionTxStore(t)
	create := collectionTxCreate(t, 1901)
	collectionTxCall(t, d, owner, create)
	r := collectionReadRequest("members", create.CollectionID)
	r.PageSize = 1
	first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if first.Next == nil {
		t.Fatal("no continuation")
	}
	q := collectionReadRequest("page", "")
	q.Token = *first.Next
	d.mu.Lock()
	beforeBytes, beforeWindows, beforeTokens := d.bytes, len(d.windows), len(d.tokens)
	d.mu.Unlock()
	collectionReadDenied(t, d, reader, q, "restart_query")
	var out bytes.Buffer
	legacy := DiscoveryRequest{Version: 1, Kind: "page", LibraryID: testLibrary, Token: q.Token}
	result, err := d.Query(context.Background(), owner, legacy, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 {
		t.Fatal("collection token entered legacy family")
	}
	d.mu.Lock()
	if d.bytes != beforeBytes || len(d.windows) != beforeWindows || len(d.tokens) != beforeTokens {
		t.Error("wrong-family/owner changed cache")
	}
	d.mu.Unlock()
	_ = collectionReadCall(t, d, owner, q)
	assetRequest := discoveryRequestTest("search", "")
	assetRequest.PageSize = 1
	assetPage := discoveryItemsTest(t, discoveryQueryTest(t, d, owner, assetRequest))
	if assetPage.Next == nil {
		t.Fatal("no legacy asset continuation")
	}
	q.Token = *assetPage.Next
	d.mu.Lock()
	beforeBytes, beforeWindows, beforeTokens = d.bytes, len(d.windows), len(d.tokens)
	d.mu.Unlock()
	collectionReadDenied(t, d, owner, q, "restart_query")
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.bytes != beforeBytes || len(d.windows) != beforeWindows || len(d.tokens) != beforeTokens {
		t.Fatal("legacy token altered collection cache")
	}
}

func TestCollectionReadConcurrentContinuationReservationAndBusyBodies(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	col := collectionReadCapacityMembers(t, s, d, owner, 128, false, false)
	r := collectionReadRequest("members", col)
	first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if first.Next == nil {
		t.Fatal("no successor")
	}
	auth := collectionReadAuthBinding(t, d, owner)
	q := collectionReadRequest("page", "")
	q.Token = *first.Next
	// Hold the exact production admission primitive at its building state. The
	// competing PUBLIC query must refuse busy, not publish or allocate a cursor.
	w, p, cost, err := d.beginCollectionPage(context.Background(), q.Token, auth)
	if err != nil {
		t.Fatal(err)
	}
	abandoned := false
	defer func() {
		if !abandoned {
			d.abandonCollectionPage(w, p, cost)
		}
	}()
	collectionReadDenied(t, d, owner, q, "busy")
	d.abandonCollectionPage(w, p, cost)
	abandoned = true
	second := collectionReadEnvelope(t, collectionReadCall(t, d, owner, q))
	if second.Next == nil {
		t.Fatal("no third body")
	}
	firstWindow, firstBody, firstCost, err := d.beginCollectionPage(context.Background(), w.tokens[0], auth)
	if err != nil {
		t.Fatal(err)
	}
	secondWindow, secondBody, secondCost, err := d.beginCollectionPage(context.Background(), q.Token, auth)
	if err != nil {
		d.releaseCollectionBody(firstWindow, firstBody, firstCost)
		t.Fatal(err)
	}
	bodiesReleased := false
	defer func() {
		if !bodiesReleased {
			d.releaseCollectionBody(firstWindow, firstBody, firstCost)
			d.releaseCollectionBody(secondWindow, secondBody, secondCost)
		}
	}()
	q.Token = *second.Next
	collectionReadDenied(t, d, owner, q, "busy") // both ~1MiB predecessors are leased.
	d.releaseCollectionBody(firstWindow, firstBody, firstCost)
	d.releaseCollectionBody(secondWindow, secondBody, secondCost)
	bodiesReleased = true
	_ = collectionReadCall(t, d, owner, q)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.building[ownerSubject] != 0 {
		t.Fatal("continuation counted as a new window")
	}
	for _, p := range w.collection.pages {
		if p.building || p.leases != 0 {
			t.Fatal("continuation leaked a page owner")
		}
	}
}

func TestCollectionReadSharedSixteenWindowPressure(t *testing.T) {
	_, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 2001)
	collectionTxCall(t, d, owner, create)
	r := collectionReadRequest("members", create.CollectionID)
	r.PageSize = 1
	first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if first.Next == nil {
		t.Fatal("no oldest cursor")
	}
	for i := 0; i < 16; i++ {
		_ = collectionReadCall(t, d, owner, r)
	}
	q := collectionReadRequest("page", "")
	q.Token = *first.Next
	collectionReadDenied(t, d, owner, q, "restart_query")
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.windows) > 16 || d.bytes > discoveryQuota || len(d.building) != 0 {
		t.Fatal("shared window pressure not bounded")
	}
}

func TestCollectionReadMaximumDirectoryGrantUnionsAndEnvelope(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 2101)
	name := strings.Repeat("\x01", 256)
	create.Value = collectionTxRaw(t, CreateCollection{name, "cgr_22222222222222222222222222222222", []NewCollectionEntry{}})
	collectionTxCall(t, d, owner, create)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var policySequence int64
	if err := tx.QueryRowContext(ctx, `SELECT max(sequence) FROM collection_policy_events`).Scan(&policySequence); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1024; i++ {
		id := create.CollectionID
		grants := 16
		if i == 0 {
			grants = 14
		} else {
			id = CollectionID(fmt.Sprintf("col_%032x", 500000+i))
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_identities VALUES(?,?,?,?,?,1)`, id, testLibrary, ownerSubject, fmt.Sprintf("cop_%032x", 500000+i), strings.Repeat("a", 64)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_definitions VALUES(?,'manual',?,0,?,1,1)`, id, name, fmt.Sprintf("cv_%032x", 500000+i)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_quota(collection_id,`+collectionQuotaColumns+`) VALUES(?,0,0,0,16,0,0,0,0,0,16,16,49152)`, id); err != nil {
				t.Fatal(err)
			}
		}
		for j := 0; j < grants; j++ {
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_grants(grant_id,library_id,subject_id,scope_kind,collection_id,actions,grant_version,removed,created_sequence,updated_sequence) VALUES(?,?,?,'collection',?,?,?,0,?,?)`, fmt.Sprintf("cgr_%032x", 700000+i*16+j), testLibrary, ownerSubject, id, []byte(`["collections.read"]`), fmt.Sprintf("cgv_%032x", 700000+i*16+j), policySequence, policySequence); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collection_quota SET lifetime_grants=15,reserved_policy_events=15,reserved_policy_receipts=15,reserved_bytes=46080 WHERE collection_id=?`, create.CollectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collection_library_quota SET collections=1024,lifetime_grants=16384,reserved_policy_events=16384,reserved_policy_receipts=16384,reserved_bytes=50331648 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Page-size1 exercises repeated full envelopes in the frozen build. The first
	// page must remain useful at maximum rights-union and definition capacity.
	r := collectionReadRequest("directory", "")
	r.PageSize = 1
	first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if first.MatchedCount != 1024 || len(first.Items) != 1 || !first.CanCreate || first.Next == nil {
		t.Fatal("maximum directory construction/fence refused or truncated")
	}
	var summary CollectionSummary
	if json.Unmarshal(first.Items[0], &summary) != nil || summary.Name != name {
		t.Fatal("escaped full directory label lost")
	}
	r = collectionReadRequest("page", "")
	r.Token = *first.Next
	second := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if second.MatchedCount != 1024 || len(second.Items) != 1 {
		t.Fatal("maximum cached directory fence")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.bytes > discoveryQuota {
		t.Fatal("directory exceeded shared quota")
	}
}

// Build canonical structural rows in one transaction to isolate history read
// window limits. It bypasses historical live authority and does not qualify
// format3 replay. Large events alternate the same64 valid entry identities.
func collectionReadHistoryFixture(t *testing.T, s *Store, col CollectionID, count int, large bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var sequence, ordinal int64
	var version CollectionVersion
	if err := tx.QueryRowContext(ctx, `SELECT max(sequence) FROM collection_events`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT max(ordinal) FROM collection_order`).Scan(&ordinal); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT definition_version FROM collection_definitions WHERE collection_id=?`, col).Scan(&version); err != nil {
		t.Fatal(err)
	}
	var bytesAdded int64
	for i := 0; i < count; i++ {
		op := CollectionRequest{1, testLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", 900000+i)), col, version, "rename", collectionTxRaw(t, RenameCollection{strings.Repeat("<", 256)})}
		if large {
			changes := make([]CollectionEntryChange, 64)
			kind := "remove"
			if i%2 == 1 {
				kind = "reactivate"
			}
			for j := range changes {
				changes[j] = CollectionEntryChange{kind, CollectionEntryID(fmt.Sprintf("cent_%032x", 100001+j)), continuity.AssetID(fmt.Sprintf("asset_%032x", 100001+j))}
			}
			op.Kind = "entries"
			op.Value = collectionTxRaw(t, EditCollectionEntries{changes})
		}
		data, digest, err := CanonicalCollectionRequest(op)
		if err != nil {
			t.Fatal(err)
		}
		version = CollectionVersion(fmt.Sprintf("cv_%032x", 900000+i))
		receipt, err := CanonicalCollectionReceipt(CollectionReceipt{1, op.ID, col, version})
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		ordinal++
		bytesAdded += int64(len(data) + len(receipt))
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_events(sequence,library_id,actor_subject_id,operation_id,collection_id,request,payload_sha256,receipt,result_version,generated_policy_sequence,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,NULL,?)`, sequence, testLibrary, ownerSubject, op.ID, col, data, digest, receipt, version, int64(1800000000000000000)+int64(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_order(ordinal,content_sequence) VALUES(?,?)`, ordinal, sequence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collection_definitions SET definition_version=?,updated_sequence=? WHERE collection_id=?`, version, sequence, col); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"collection_quota", "collection_library_quota"} {
		where := "collection_id=?"
		arg := any(col)
		if table == "collection_library_quota" {
			where = "singleton=?"
			arg = 1
		}
		if _, err := tx.ExecContext(ctx, `UPDATE `+table+` SET content_events=content_events+?,content_receipts=content_receipts+?,canonical_bytes=canonical_bytes+? WHERE `+where, count, count, bytesAdded, arg); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionReadHistoryVisibleAndEncodedWindowCutoffs(t *testing.T) {
	for _, large := range []bool{false, true} {
		name := "visible1000"
		if large {
			name = "encoded8MiB"
		}
		t.Run(name, func(t *testing.T) {
			s, _, _, d, owner, _ := collectionTxStore(t)
			var col CollectionID
			if large {
				col = collectionReadCapacityMembers(t, s, d, owner, 64, false, false)
				collectionReadHistoryFixture(t, s, col, 600, true)
			} else {
				create := collectionTxCreate(t, 2201)
				collectionTxCall(t, d, owner, create)
				col = create.CollectionID
				collectionReadHistoryFixture(t, s, col, 1001, false)
			}
			first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, collectionReadRequest("history", col)))
			if first.Complete || first.MatchedCount < 1 || len(first.Items) < 1 {
				t.Fatal("large visible history became work failure/empty/complete")
			}
			if !large && first.MatchedCount != 1000 {
				t.Fatalf("visible cutoff %d", first.MatchedCount)
			}
			if large && first.MatchedCount >= 600 {
				t.Fatal("encoded history never reached bounded output window")
			}
			seen := len(first.Items)
			current := first
			for current.Next != nil {
				q := collectionReadRequest("page", "")
				q.Token = *current.Next
				current = collectionReadEnvelope(t, collectionReadCall(t, d, owner, q))
				seen += len(current.Items)
			}
			if seen != first.MatchedCount || current.Complete {
				t.Fatal("bounded history traversal falsely claimed completeness")
			}
			if large && (first.MatchedCount <= 100 || len(current.Items) >= 100) {
				t.Fatal("encoded history fixture did not exercise a short final constructor group")
			}
		})
	}
}

func TestCollectionReadSparseHiddenHistoryAtSourceRowCap(t *testing.T) {
	s, o, a, d, owner, reader := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 2601)
	created := collectionTxCall(t, d, owner, create)
	collectionReadGrant(t, o, a, 2602, create.CollectionID, readerSubject, []string{"collections.read"})
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var sequence, ordinal int64
	if err := tx.QueryRowContext(ctx, `SELECT max(sequence) FROM collection_events`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT max(ordinal) FROM collection_order`).Scan(&ordinal); err != nil {
		t.Fatal(err)
	}
	version := created.ResultVersion
	var addedBytes int64
	// Canonical rows use a real existing hidden entry and alternate valid removal/
	// reactivation. This bounded read-work fixture bypasses live historical auth
	// admission; it does not claim format3 replay or an over-cap corruption case.
	for i := 0; i < 8191; i++ {
		kind := "remove"
		if i%2 == 1 {
			kind = "reactivate"
		}
		change := CollectionEntryChange{kind, "cent_22222222222222222222222222222222", otherAsset}
		op := CollectionRequest{1, testLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", 2000000+i)), create.CollectionID, version, "entries", collectionTxRaw(t, EditCollectionEntries{[]CollectionEntryChange{change}})}
		request, digest, err := CanonicalCollectionRequest(op)
		if err != nil {
			t.Fatal(err)
		}
		version = CollectionVersion(fmt.Sprintf("cv_%032x", 2000000+i))
		receipt, err := CanonicalCollectionReceipt(CollectionReceipt{1, op.ID, op.CollectionID, version})
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		ordinal++
		addedBytes += int64(len(request) + len(receipt))
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_events(sequence,library_id,actor_subject_id,operation_id,collection_id,request,payload_sha256,receipt,result_version,generated_policy_sequence,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,NULL,?)`, sequence, testLibrary, ownerSubject, op.ID, op.CollectionID, request, digest, receipt, version, int64(1800000000000000001)+int64(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_order(ordinal,content_sequence) VALUES(?,?)`, ordinal, sequence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collection_entries SET removed=1,updated_sequence=? WHERE collection_id=? AND entry_id=?`, sequence, create.CollectionID, "cent_22222222222222222222222222222222"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collection_definitions SET definition_version=?,updated_sequence=? WHERE collection_id=?`, version, sequence, create.CollectionID); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"collection_quota", "collection_library_quota"} {
		where := "collection_id=?"
		arg := any(create.CollectionID)
		if table == "collection_library_quota" {
			where = "singleton=?"
			arg = 1
		}
		if _, err := tx.ExecContext(ctx, `UPDATE `+table+` SET active_entries=1,content_events=content_events+8191,content_receipts=content_receipts+8191,canonical_bytes=canonical_bytes+? WHERE `+where, addedBytes, arg); err != nil {
			t.Fatal(err)
		}
	}
	usage, err := readCollectionQuota(ctx, tx, create.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Collection == nil || usage.Collection.ContentEvents != 8192 || usage.Collection.CanonicalBytes+usage.Collection.ReservedBytes > 16<<20 {
		t.Fatal("sparse history fixture exceeded valid collection caps")
	}
	var rows, raw int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),sum(length(request)) FROM collection_events WHERE collection_id=?`, create.CollectionID).Scan(&rows, &raw); err != nil {
		t.Fatal(err)
	}
	if rows != 8192 || raw <= 0 || raw > 16<<20 {
		t.Fatal("sparse source workload not actually at row cap")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Every entry event references the hidden asset; the mixed original create is
	// hidden wholesale too. At the valid8192-row cap EOF is complete, not a source
	// overrun: the defensive row guard starts at8193 and raw guard above32MiB.
	body := collectionReadCall(t, d, reader, collectionReadRequest("history", create.CollectionID))
	result := collectionReadEnvelope(t, body)
	if len(result.Items) != 0 || result.MatchedCount != 0 || !result.Complete || result.Next != nil || bytes.Contains(body, []byte(otherAsset)) {
		t.Fatal("sparse hidden history fabricated work failure, incompleteness or disclosure")
	}
}

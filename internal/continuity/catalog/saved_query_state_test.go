package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/lelanddutcher/juicemount/internal/continuity"
	"testing"
)

func savedQueryReplaceTrigger(t *testing.T, s *Store, name string, body func()) {
	t.Helper()
	var ddl string
	if err := s.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=? AND type='trigger'`, name).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER ` + name); err != nil {
		t.Fatal(err)
	}
	body()
	if _, err := s.db.Exec(ddl); err != nil {
		t.Fatal(err)
	}
}

func TestSavedQueryStateRejectsDefinitionHistoryAndChildTampering(t *testing.T) {
	for _, scenario := range []string{"definition", "old_version", "old_updated", "child_actor", "child_digest", "historical_anchor", "quota", "family_entry"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, _, d, bearer, _ := savedQueryTestStore(t)
			ctx := context.Background()
			create := savedQueryTestCreate(t, 220, testAsset)
			first := savedQueryTestCall(t, d, bearer, create)
			blank := SavedQueryDefinition{1, "ranked_assets", "", []continuity.AssetID{}}
			replace := savedQueryTestEdit(t, 221, create.CollectionID, first.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{blank})
			replaced := savedQueryTestCall(t, d, bearer, replace)
			savedQueryTestCall(t, d, bearer, savedQueryTestEdit(t, 222, create.CollectionID, replaced.ResultVersion, "rename", RenameCollection{"After replacement"}))
			if err := checkSavedQueryState(ctx, s.db, testLibrary); err != nil {
				t.Fatalf("valid last definition followed by rename rejected: %v", err)
			}
			switch scenario {
			case "definition":
				data, _, _ := CanonicalSavedQueryDefinition(SavedQueryDefinition{1, "ranked_assets", "forged", []continuity.AssetID{}})
				if _, err := s.db.Exec(`UPDATE saved_query_definitions SET definition=? WHERE collection_id=?`, data, create.CollectionID); err != nil {
					t.Fatal(err)
				}
			case "old_version":
				if _, err := s.db.Exec(`UPDATE saved_query_definitions SET definition_version=? WHERE collection_id=?`, first.ResultVersion, create.CollectionID); err != nil {
					t.Fatal(err)
				}
			case "old_updated":
				if _, err := s.db.Exec(`UPDATE saved_query_definitions SET updated_sequence=created_sequence WHERE collection_id=?`, create.CollectionID); err != nil {
					t.Fatal(err)
				}
			case "child_actor", "child_digest":
				savedQueryReplaceTrigger(t, s, "collection_policy_events_no_update", func() {
					if scenario == "child_actor" {
						if _, err := s.db.Exec(`UPDATE collection_policy_events SET operator_subject_id=?,namespace_subject_id=? WHERE administration='collection_service'`, readerSubject, readerSubject); err != nil {
							t.Fatal(err)
						}
					} else {
						if _, err := s.db.Exec(`UPDATE collection_policy_events SET payload_sha256=? WHERE administration='collection_service'`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
							t.Fatal(err)
						}
					}
				})
			case "historical_anchor":
				// An absent anchor appears only in old create history, followed by a valid
				// current blank definition. Update parent/child bindings consistently so
				// structural anchor existence is the independent rejection.
				v, _ := savedQueryValue(create)
				v.(*CreateSavedQuery).Definition.ContextAssetIDs = []continuity.AssetID{"asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
				create.Value = collectionTxRaw(t, v)
				data, digest, err := CanonicalSavedQueryRequest(create)
				if err != nil {
					t.Fatal(err)
				}
				savedQueryReplaceTrigger(t, s, "collection_events_no_update", func() {
					if _, err := s.db.Exec(`UPDATE collection_events SET request=?,payload_sha256=? WHERE sequence=1`, data, digest); err != nil {
						t.Fatal(err)
					}
				})
				savedQueryReplaceTrigger(t, s, "collection_identities_identity", func() {
					if _, err := s.db.Exec(`UPDATE collection_identities SET creation_payload_sha256=? WHERE collection_id=?`, digest, create.CollectionID); err != nil {
						t.Fatal(err)
					}
				})
				var child int64
				if err := s.db.QueryRow(`SELECT generated_policy_sequence FROM collection_events WHERE sequence=1`).Scan(&child); err != nil {
					t.Fatal(err)
				}
				p, _, err := readCollectionPolicyRecord(ctx, s.db, testLibrary, child)
				if err != nil {
					t.Fatal(err)
				}
				p.Request.BindingPayloadSHA256 = digest
				request, hash, err := CanonicalCollectionPolicyRequest(p.Request)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, testLibrary, p.Request.ID, hash, p.RecordedUnixNano})
				if err != nil {
					t.Fatal(err)
				}
				savedQueryReplaceTrigger(t, s, "collection_policy_events_no_update", func() {
					if _, err := s.db.Exec(`UPDATE collection_policy_events SET request=?,payload_sha256=?,receipt=? WHERE sequence=?`, request, hash, receipt, child); err != nil {
						t.Fatal(err)
					}
				})
			case "quota":
				if _, err := s.db.Exec(`UPDATE collection_quota SET canonical_bytes=canonical_bytes+1 WHERE collection_id=?`, create.CollectionID); err != nil {
					t.Fatal(err)
				}
			case "family_entry":
				if _, err := s.db.Exec(`INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES('cent_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',?,?,?,0,1,1)`, testLibrary, create.CollectionID, testAsset); err == nil {
					t.Fatal("query entry trigger accepted manual member")
				}
				return
			}
			if err := d.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			opened, err := Open(s.path)
			if err == nil {
				opened.Close()
				t.Fatal("tampered schema6 state admitted")
			}
		})
	}
}

func TestSavedQueryStructuralActorGuardAndFinalInactiveHistory(t *testing.T) {
	s, o, a, d, bearer, _ := savedQueryTestStore(t)
	ctx := context.Background()
	create := savedQueryTestCreate(t, 240)
	savedQueryTestCall(t, d, bearer, create)
	subject, err := readSubject(ctx, s.db, testLibrary, readerSubject)
	if err != nil {
		t.Fatal(err)
	}
	value := subject.Value
	value.Enabled = false
	policyTest(t, o, a, "disable-query-reader", NewPolicyChange("update_subject", UpdateSubject{readerSubject, subject.Expected(), value}))
	before := savedQueryTestSnapshot(t, s, true)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	op := savedQueryTestCreate(t, 241)
	request, _, err := CanonicalSavedQueryRequest(op)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	record := SavedQueryRecord{2, op, readerSubject, "cv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", generatedCollectionPolicyID(op.ID), s.now().UnixNano()}
	receipt, _ := CanonicalSavedQueryReceipt(SavedQueryReceipt{2, op.ID, op.CollectionID, record.ResultVersion})
	called := false
	_, applyErr := s.applySavedQueryContentStructural(ctx, tx, record, request, receipt, nil, func(continuity.AssetID) error { called = true; return nil }, func(CollectionPolicyRequest) (int64, CollectionQuotaCounters, error) {
		called = true
		return 0, CollectionQuotaCounters{}, nil
	})
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(applyErr, ErrConflict) || called || !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
		t.Fatalf("disabled structural actor admitted or mutated: %v", applyErr)
	}
	// Disable the author only after a real accepted event. Structural admission
	// must preserve that event instead of applying today's Enabled bit to history.
	author, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	// Local-owner policy deliberately forbids disabling its own operational owner;
	// this fixture changes only the durable subject state, not historical events.
	if _, err := s.db.Exec(`UPDATE policy_subjects SET enabled=0,authorization_generation=authorization_generation+1 WHERE subject_id=? AND authorization_generation=?`, ownerSubject, author.AuthorizationGeneration); err != nil {
		t.Fatal(err)
	}
	if err := checkSavedQueryState(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("final disabled author invalidated old history: %v", err)
	}
}

func TestSavedQueryRetainedPolicyRejectsIdenticalActivePut(t *testing.T) {
	s, o, a, d, bearer, _ := savedQueryTestStore(t)
	ctx := context.Background()
	op := savedQueryTestCreate(t, 250)
	savedQueryTestCall(t, d, bearer, op)
	grant, err := readCollectionGrant(ctx, s.db, "cgr_000000000000000000000000000000fa")
	if err != nil {
		t.Fatal(err)
	}
	// Add another administrator so a real owner update can reduce this grant.
	collectionReadGrant(t, o, a, 251, op.CollectionID, readerSubject, []string{"collections.admin", "collections.edit", "collections.read"})
	value := grant.expected.Value
	value.Actions = []string{"collections.edit", "collections.read"}
	p := collectionTxPolicy(252, op.CollectionID, "collection", CollectionGrantChange{"put", grant.id, &grant.expected, &value})
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, p)); err != nil {
		t.Fatal(err)
	}
	// Forge only this owner event into an active identical put, with internally
	// matching bytes/hash/receipt. The retained-policy decoder must reject it.
	p.Changes[0].Value = &grant.expected.Value
	request, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	digest := domainDigest("juicemount.catalog.collection.policy.operation\x00\x02", request)
	var sequence, time int64
	if err := s.db.QueryRow(`SELECT sequence,recorded_unix_nano FROM collection_policy_events WHERE operation_id=?`, p.ID).Scan(&sequence, &time); err != nil {
		t.Fatal(err)
	}
	receipt, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, testLibrary, p.ID, digest, time})
	if err != nil {
		t.Fatal(err)
	}
	savedQueryReplaceTrigger(t, s, "collection_policy_events_no_update", func() {
		if _, err := s.db.Exec(`UPDATE collection_policy_events SET request=?,payload_sha256=?,receipt=? WHERE sequence=?`, request, digest, receipt, sequence); err != nil {
			t.Fatal(err)
		}
	})
	if _, _, err := readCollectionPolicyRecord(ctx, s.db, testLibrary, sequence); !errors.Is(err, ErrInvalid) {
		t.Fatalf("identical active put retained policy accepted: %v", err)
	}
}

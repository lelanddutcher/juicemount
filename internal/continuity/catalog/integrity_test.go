package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCatalogSQLConstraintsAndImmutableRows(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, initialOperation())
	child := testManifest(testAsset, strings.Repeat("1", 64), rootRevision)
	applyTest(t, s, testOperation("child", Change{RecordRevision: &child}))
	other := testManifest(otherAsset, testSHA)
	otherID, _ := other.RevisionID()
	applyTest(t, s, testOperation("other", Change{CreateAsset: &CreateAsset{AssetID: otherAsset}}, Change{RecordRevision: &other}))
	for name, statement := range map[string]string{
		"events update":           `UPDATE events SET recorded_unix_nano=0`,
		"events delete":           `DELETE FROM events`,
		"revision update":         `UPDATE revisions SET canonical=zeroblob(95)`,
		"revision delete":         `DELETE FROM revisions`,
		"parent update":           `UPDATE revision_parents SET asset_id='asset_101112131415161718191a1b1c1d1e1f'`,
		"parent delete":           `DELETE FROM revision_parents`,
		"catalog identity update": `UPDATE catalog SET library_id='lib_202122232425262728292a2b2c2d2e2f'`,
		"catalog delete":          `DELETE FROM catalog`,
		"ledger update":           `UPDATE schema_migrations SET version=1`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.db.Exec(statement); err == nil {
				t.Fatal("immutable SQL mutation succeeded")
			}
		})
	}
	if _, err := s.db.Exec(`UPDATE assets SET head=? WHERE asset_id=?`, otherID, testAsset); err == nil {
		t.Fatal("SQL accepted cross-asset head")
	}
	if _, err := s.db.Exec(`UPDATE assets SET label=? WHERE asset_id=?`, strings.Repeat("x", 4097), testAsset); err == nil {
		t.Fatal("SQL accepted oversized label")
	}
	if err := checkIntegrity(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogDetectsCorruptRevisionAndEventReadback(t *testing.T) {
	for _, kind := range []string{"canonical", "manifest", "parent edges", "event digest", "event request", "event receipt"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newTestStore(t)
			applyTest(t, s, initialOperation())
			child := testManifest(testAsset, strings.Repeat("1", 64), rootRevision)
			childID, _ := child.RevisionID()
			applyTest(t, s, testOperation("child", Change{RecordRevision: &child}))
			var err error
			switch kind {
			case "canonical":
				_, err = s.db.Exec(`DROP TRIGGER revisions_no_update`)
				if err == nil {
					_, err = s.db.Exec(`UPDATE revisions SET canonical=zeroblob(95) WHERE revision_id=?`, rootRevision)
				}
			case "manifest":
				_, err = s.db.Exec(`DROP TRIGGER revisions_no_update`)
				data, _ := json.Marshal(manifestWire(testManifest(testAsset, strings.Repeat("9", 64))))
				if err == nil {
					_, err = s.db.Exec(`UPDATE revisions SET manifest_json=? WHERE revision_id=?`, data, rootRevision)
				}
			case "parent edges":
				_, err = s.db.Exec(`DROP TRIGGER revision_parents_no_delete`)
				if err == nil {
					_, err = s.db.Exec(`DELETE FROM revision_parents WHERE revision_id=?`, childID)
				}
			default:
				_, err = s.db.Exec(`DROP TRIGGER events_no_update`)
				if err == nil {
					switch kind {
					case "event digest":
						_, err = s.db.Exec(`UPDATE events SET payload_sha256=? WHERE operation_id=?`, strings.Repeat("0", 64), "initial")
					case "event request":
						_, err = s.db.Exec(`UPDATE events SET request=? WHERE operation_id=?`, []byte(`{"future_kind":{}}`), "initial")
					case "event receipt":
						_, err = s.db.Exec(`UPDATE events SET receipt=? WHERE operation_id=?`, []byte(`{"version":2}`), "initial")
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(kind, "event ") {
				if _, err := s.GetEvent(context.Background(), testLibrary, "initial"); !errors.Is(err, ErrFormat) {
					t.Fatalf("corrupt event: %v", err)
				}
			} else {
				id := rootRevision
				if kind == "parent edges" {
					id = childID
				}
				if _, err := s.GetRevision(context.Background(), testLibrary, id); !errors.Is(err, ErrFormat) {
					t.Fatalf("corrupt revision: %v", err)
				}
			}
		})
	}
}

func TestCatalogRejectsSchemaChangeBeforeMutation(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.db.Exec(`CREATE TABLE sqliteXautoindexYextra(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(context.Background(), initialOperation()); !errors.Is(err, ErrFormat) {
		t.Fatalf("changed schema admission: %v", err)
	}
	var assets, events int
	if err := s.db.QueryRow(`SELECT count(*) FROM assets`).Scan(&assets); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if assets != 0 || events != 0 {
		t.Fatalf("changed schema mutated: assets=%d events=%d", assets, events)
	}
}

func TestCatalogListAssetsKeepsAtomicPairConsistent(t *testing.T) {
	s, path := newTestStore(t)
	applyTest(t, s, testOperation("pair", Change{CreateAsset: &CreateAsset{AssetID: testAsset, Label: "0"}}, Change{CreateAsset: &CreateAsset{AssetID: otherAsset, Label: "0"}}))
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		count int
		err   error
	}
	written, read := make(chan result, 1), make(chan result, 1)
	go func() {
		<-start
		for i := 1; i <= 64; i++ {
			op := testOperation(fmt.Sprintf("pair-%d", i),
				Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: fmt.Sprint(i - 1), Label: fmt.Sprint(i)}},
				Change{RenameAsset: &RenameAsset{AssetID: otherAsset, ExpectedLabel: fmt.Sprint(i - 1), Label: fmt.Sprint(i)}})
			if _, err := s.Apply(ctx, op); err != nil {
				written <- result{i - 1, err}
				return
			}
		}
		written <- result{64, nil}
	}()
	go func() {
		<-start
		for i := 0; i < 256; i++ {
			assets, err := reader.ListAssets(ctx, testLibrary, "", 2)
			if err != nil {
				read <- result{i, err}
				return
			}
			if len(assets) != 2 || assets[0].Label != assets[1].Label || assets[0].UpdatedSequence != assets[1].UpdatedSequence {
				read <- result{i, fmt.Errorf("mixed atomic pair: %+v", assets)}
				return
			}
		}
		read <- result{256, nil}
	}()
	close(start)
	// Join both actors before reporting failure; their completed work is explicit.
	w, r := <-written, <-read
	if w.err != nil || r.err != nil || w.count != 64 || r.count != 256 {
		t.Fatalf("writer %+v, reader %+v", w, r)
	}
	assets, err := reader.ListAssets(context.Background(), testLibrary, "", 2)
	if err != nil || len(assets) != 2 || assets[0].Label != "64" || assets[1].Label != "64" {
		t.Fatalf("final pair: %+v, %v", assets, err)
	}
}

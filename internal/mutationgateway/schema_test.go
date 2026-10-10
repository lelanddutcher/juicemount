package mutationgateway

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func TestCustodyReceiptProjectionPreservesLegacyJSON(t *testing.T) {
	r := &mutationauthority.Receipt{Volume: "v", ID: "op", Session: "s", Epoch: 7, Digest: "d", ExpectedRevision: "before", Revision: "after", Evidence: "proof", CommittedAtUnixNS: 123, Kind: mutationauthority.CustodyOperation}
	old := `{"Volume":"v","ID":"op","Session":"s","Epoch":7,"Digest":"d","ExpectedRevision":"before","Revision":"after","Evidence":"proof","CommittedAtUnixNS":123}`
	if hash(custodyCommitBytes(r)) != "d6a6c0e0c6bc496ef06298cccde2ea5bcea7c14d90fee150e699d4c39541ac6c" {
		t.Fatal("legacy receipt hash changed")
	}
	if got := custodyCommitBytes(r); string(got) != old {
		t.Fatalf("legacy receipt changed: %s", got)
	}
}

func TestLegacyCustodyUpgradePreservesReceiptAndReadback(t *testing.T) {
	cfg := testConfig(t)
	g := testGateway(t, cfg)
	ctx := context.Background()
	l, err := g.journal.Claim(ctx, g.volume, strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u := testUpload(wireLease(l), "old-custody")
	if _, err = g.journal.Execute(ctx, g.request(u.Chunk), stageBackend{g: g, chunk: u.Chunk, data: u.Data}); err != nil {
		t.Fatal(err)
	}
	before, data, err := g.custody(ctx, u.Chunk)
	if err != nil || !bytes.Equal(data, u.Data) {
		t.Fatal(before, err)
	}
	if err = g.Close(); err != nil {
		t.Fatal(err)
	}
	// Restore the independent v0 shape while retaining the exact staged artifact,
	// operation receipt fields, owner epoch and old custody format marker.
	db, err := sql.Open("sqlite", filepath.Join(cfg.Directory, "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`DROP TABLE media_publication_retention`, `DROP TABLE media_publications`, `DROP TABLE media_retention`, `DROP TABLE media_object_attempts`, `DROP TABLE media_slices`, `DROP INDEX media_stage_inode`, `DROP TABLE media_stages`, `ALTER TABLE operations DROP COLUMN file_id`, `ALTER TABLE operations DROP COLUMN kind`, `PRAGMA user_version=0`} {
		if _, err = db.Exec(stmt); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testGateway(t, cfg)
	after, data, err := reopened.custody(ctx, u.Chunk)
	if err != nil || !reflect.DeepEqual(before, after) || !bytes.Equal(data, u.Data) {
		t.Fatalf("custody changed: before=%+v after=%+v err=%v", before, after, err)
	}
	op, err := reopened.journal.Inspect(ctx, cfg.Volume, format+":"+u.Chunk.ID)
	if err != nil || op.Kind != mutationauthority.CustodyOperation || op.FileID != "" {
		t.Fatal(op, err)
	}
	// No old live lease is resurrected by the migration.
	if _, err = reopened.journal.Renew(ctx, l, time.Minute); !errors.Is(err, mutationauthority.ErrLeaseExpired) {
		t.Fatal(err)
	}
}

func TestCustodyStartupRejectsTypedMediaInEveryState(t *testing.T) {
	for _, state := range []mutationauthority.State{mutationauthority.Prepared, mutationauthority.Committed, mutationauthority.NoEffect} {
		t.Run(string(state), func(t *testing.T) {
			cfg := testConfig(t)
			g := testGateway(t, cfg)
			ctx := context.Background()
			l, err := g.journal.Claim(ctx, g.volume, strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// The old custody-shaped fields deliberately look valid; kind/file identity
			// must still prevent media adoption, including already terminal rows.
			r := mutationauthority.Request{Lease: l, ID: format + ":media", Digest: hash([]byte("media")), ExpectedRevision: format, Kind: mutationauthority.MediaPublishOperation, FileID: "incarnation"}
			if err = g.Close(); err != nil {
				t.Fatal(err)
			}
			// Current public APIs refuse generic media settlement. Seed an exact
			// historical row only after releasing this fixture's journal ownership.
			db, err := sql.Open("sqlite", filepath.Join(cfg.Directory, "authority.db"))
			if err != nil {
				t.Fatal(err)
			}
			revision, evidence, committed := "", "", int64(0)
			if state == mutationauthority.Committed {
				revision, evidence, committed = "revision", "proof", 123
			} else if state == mutationauthority.NoEffect {
				evidence = "proof"
			}
			_, err = db.Exec(`INSERT INTO operations(volume,id,session,epoch,digest,expected_revision,kind,file_id,state,revision,evidence,committed_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
				r.Volume, r.ID, r.Session, r.Epoch, r.Digest, r.ExpectedRevision, r.Kind, r.FileID, state, revision, evidence, committed)
			closeErr := db.Close()
			if err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			opened, err := Open(cfg)
			if opened != nil {
				opened.Close()
			}
			if !errors.Is(err, mutationauthority.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
}

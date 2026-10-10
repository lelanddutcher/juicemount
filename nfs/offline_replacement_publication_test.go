//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

func reopenReplacementPublicationFixture(t *testing.T, x *boundCacheFixture, root string) (*metadata.Store, *SpoolStore) {
	t.Helper()
	if !x.jfs.handler.StopHandler() {
		t.Fatal("original handler did not stop")
	}
	if x.session != nil {
		x.session.Stop()
	}
	if x.rc != nil {
		x.rc.Stop()
	}
	if err := x.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(x.db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(root, 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	store.SetSpoolStore(spool.Meta())
	return store, spool
}

func TestOfflineReplacementPublicationIntentSurvivesRestart(t *testing.T) {
	x, spool, root := newOfflineReplacementFixture(t, true)
	body := []byte("accepted offline image remains readable while publication is pending")
	const temp = "media/.journal-save.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.jfs.Rename(temp, "media/clip.mov"); err != nil {
		t.Fatal(err)
	}
	row, _ := assertOfflineReplacementRow(t, spool, body)
	queued, err := spool.Meta().ReplacementPublication(row)
	if err != nil || queued == nil || queued.Phase != "queued" || queued.StagePath != "" {
		t.Fatalf("queued publication: %+v %v", queued, err)
	}
	image, w, err := spool.openAcceptedReplacement(row)
	if err != nil {
		t.Fatal(err)
	}
	intent, beginErr := x.store.BeginSpoolReplacementPublication(row.ID, queued.Expected, func() error { return image.VerifyImage(w.Image) })
	image.Close()
	if beginErr != nil || intent == nil || intent.Phase != "intent" || intent.StagePath == "" {
		t.Fatalf("begin publication: %+v %v", intent, beginErr)
	}
	// Beginning publication grants no filesystem effect. The exact stage name
	// is reserved durably before any future create/upload can start.
	if _, err := os.Lstat(filepath.Join(x.fuseRoot, intent.StagePath)); !os.IsNotExist(err) {
		t.Fatalf("begin created backend stage: %v", err)
	}
	assertOfflineReplacementRead(t, x.jfs, body)
	second := []byte("next save must not retire an active publication")
	const next = "media/.journal-second.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, next, second, len(second))
	if err := x.jfs.Rename(next, "media/clip.mov"); err == nil {
		t.Fatal("successor retired an active publication")
	}
	// Both accepted data and the refused successor's source remain in custody.
	assertOfflineReplacementRead(t, x.jfs, body)
	nextRow, err := spool.Meta().LookupByPath(next)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(nextRow.SpoolFile); err != nil || !bytes.Equal(got, second) {
		t.Fatalf("refused successor lost bytes: %v", err)
	}
	current, err := spool.Meta().Get(row.ID)
	if err != nil || current.RetiredBy != 0 {
		t.Fatalf("active publication was retired: %+v %v", current, err)
	}
	// Finalize the separate temporary file so ordinary boot handling has no
	// interrupted writer to classify; the replacement itself remains READY.
	nextEntry, _ := spool.LookupActive(next)
	if nextEntry == nil {
		t.Fatal("successor source index absent")
	}
	if err := nextEntry.Finalize(); err != nil {
		t.Fatal(err)
	}
	store, recovered := reopenReplacementPublicationFixture(t, x, root)
	if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	recoveredRow, err := recovered.Meta().Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := recovered.Meta().ReplacementPublication(recoveredRow)
	if err != nil || !reflect.DeepEqual(got, intent) {
		t.Fatalf("journal did not survive reopen: %+v %v", got, err)
	}
	handler := NewHandler(store, x.fuseRoot)
	t.Cleanup(func() { handler.StopHandler() })
	handler.SetSpool(recovered, nil)
	assertOfflineReplacementRead(t, &juiceFS{handler: handler}, body)
	assertOfflineOriginalBacking(t, filepath.Join(x.fuseRoot, "media/clip.mov"))
}

func TestOfflineReplacementJournalFailurePreservesBootCustody(t *testing.T) {
	for _, fault := range []string{"missing", "malformed"} {
		t.Run(fault, func(t *testing.T) {
			x, spool, root := newOfflineReplacementFixture(t, true)
			body := []byte("durable bytes must survive a damaged publication journal")
			const temp = "media/.journal-corrupt.tmp"
			simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
			x.session.SetOnline(false)
			pin.SetOffline(true)
			if err := x.jfs.Rename(temp, "media/clip.mov"); err != nil {
				t.Fatal(err)
			}
			row, _ := assertOfflineReplacementRow(t, spool, body)
			var err error
			if fault == "missing" {
				_, err = x.store.DB().Exec(`DELETE FROM spool_replacement_publications WHERE spool_id=?`, row.ID)
			} else {
				_, err = x.store.DB().Exec(`UPDATE spool_replacement_publications SET payload=? WHERE spool_id=?`, []byte("malformed"), row.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, recovered := reopenReplacementPublicationFixture(t, x, root)
			recovered.used.Store(123)
			if _, err := recovered.RecoverOnBoot(context.Background()); err == nil {
				t.Fatal("boot accepted damaged required journal")
			}
			if recovered.used.Load() != 123 {
				t.Fatal("failed boot reset capacity before journal validation")
			}
			if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("failed boot lost accepted bytes: %v", err)
			}
			assertOfflineOriginalBacking(t, filepath.Join(x.fuseRoot, "media/clip.mov"))
		})
	}
}

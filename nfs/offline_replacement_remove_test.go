//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

func TestOfflineReplacementRemoveRetainsAcceptedSave(t *testing.T) {
	x, spool, _ := newOfflineReplacementFixture(t, true)
	const target = "media/clip.mov"
	const temp = "media/.clip-removal-attempt.tmp"
	body := []byte("accepted atomic save stays on this Mac through a refused delete")
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "setting", "d2", "i3", "c3_0", "c3_1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := x.jfs.Rename(temp, target); err != nil {
		t.Fatalf("accept offline save: %v", err)
	}
	row, witness := assertOfflineReplacementRow(t, spool, body)
	publication, err := spool.Meta().ReplacementPublication(row)
	if err != nil || publication == nil || publication.Phase != metadata.ReplacementPublicationQueued {
		t.Fatalf("accepted queued publication %+v: %v", publication, err)
	}
	indexEntry, ok := spool.LookupActive(target)
	if !ok || indexEntry.ID() != row.ID {
		t.Fatal("accepted image missing from local index")
	}
	used, _ := spool.Capacity()
	if used != int64(len(body)) {
		t.Fatalf("accepted charge = %d, want %d", used, len(body))
	}
	mirror := x.store.LookupByPath(target)
	if mirror == nil {
		t.Fatal("original mirror missing")
	}
	backing := filepath.Join(x.fuseRoot, target)
	backingInfo, err := os.Lstat(backing)
	if err != nil {
		t.Fatal(err)
	}

	assertRetained := func(where string) {
		t.Helper()
		gotRow, gotWitness := assertOfflineReplacementRow(t, spool, body)
		gotPublication, err := spool.Meta().ReplacementPublication(gotRow)
		if err != nil || !reflect.DeepEqual(gotRow, row) || !reflect.DeepEqual(gotWitness, witness) ||
			!reflect.DeepEqual(gotPublication, publication) {
			t.Fatalf("%s changed accepted row, witness or queued publication: row=%+v publication=%+v err=%v", where, gotRow, gotPublication, err)
		}
		if entry, ok := spool.LookupActive(target); !ok || entry != indexEntry {
			t.Fatalf("%s changed accepted local index", where)
		}
		if charge, _ := spool.Capacity(); charge != used {
			t.Fatalf("%s changed accepted charge: %d -> %d", where, used, charge)
		}
		if image, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(image, body) {
			t.Fatalf("%s changed private image: %v", where, err)
		}
		if got := x.store.LookupByPath(target); !reflect.DeepEqual(got, mirror) {
			t.Fatalf("%s changed destination mirror: %+v", where, got)
		}
		assertOfflineOriginalBacking(t, backing)
		if got, err := os.Lstat(backing); err != nil || !os.SameFile(got, backingInfo) {
			t.Fatalf("%s changed original backing identity: %v", where, err)
		}
	}

	// A caller retry while this exact namespace scope is owned is a pending
	// delete, never an API to discard the already accepted local save.
	held := x.jfs.handler.namespace.begin([]string{target})
	if held == nil {
		t.Fatal("could not hold accepted destination admission")
	}
	released := false
	defer func() {
		if !released {
			held.finish(nil)
		}
	}()
	for _, name := range []string{target, "media"} {
		started := time.Now()
		if err := x.jfs.Remove(name); !errors.Is(err, errFUSETimeout) || time.Since(started) > time.Second {
			t.Fatalf("Remove(%q) under held admission = %v after %s", name, err, time.Since(started))
		}
	}
	assertRetained("held admission")
	held.finish(nil)
	released = true
	awaitNamespaceIdle(t, &x.jfs.handler.namespace)

	for _, name := range []string{target, "media"} {
		if err := x.jfs.Remove(name); !errors.Is(err, metadata.ErrMirrorConflict) {
			t.Fatalf("Remove(%q) after admission release = %v, want accepted-save guard", name, err)
		}
	}
	assertRetained("queued publication")
	for _, directory := range []string{".", "media"} {
		info, err := x.jfs.Stat(directory)
		if err != nil || !info.IsDir() {
			t.Fatalf("directory %q unavailable after refusal: %v", directory, err)
		}
	}
	assertOfflineReplacementRead(t, x.jfs, body)
}

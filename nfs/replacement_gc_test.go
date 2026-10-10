//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// TestReplacementDeferredGCThroughBG exercises the SDK's pending-file deletion
// path after replacement. It intentionally does not exercise the hourly mount
// scheduler or Manager's separate background-job orchestration.
func TestReplacementDeferredGCThroughBG(t *testing.T) {
	const oldTracking = "3:65536"
	body := bytes.Repeat([]byte("new-live-gc-"), 18000)
	f, rt := newReplacementFreshFixture(t, body)
	oldObject := filepath.Join(rt.objectRoot, "chunks", "0", "0", "77_0_65536")
	if err := os.MkdirAll(filepath.Dir(oldObject), 0700); err != nil {
		t.Fatal(err)
	}
	oldFile, err := os.OpenFile(oldObject, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := oldFile.Write(f.x.want); err != nil || n != len(f.x.want) {
		_ = oldFile.Close()
		t.Fatalf("seed old object bytes=%d err=%v", n, err)
	}
	if err := oldFile.Sync(); err != nil {
		_ = oldFile.Close()
		t.Fatal(err)
	}
	if err := oldFile.Close(); err != nil {
		t.Fatal(err)
	}
	// Publication and the later offline reopen require a disk-resident SDK
	// generation; the initial constructor is deliberately memory-only.
	rt = replaceReplacementRuntime(t, f, rt)

	beforeCapacity, _ := f.spool.Capacity()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f.run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := cleanupPhase(t, f.spool, f.row.ID); got != metadata.ReplacementPublicationCleaned {
		t.Fatalf("replacement cleanup phase %s", got)
	}
	if f.factories != 1 || f.backend.renames != 1 {
		t.Fatalf("publication factory=%d rename=%d", f.factories, f.backend.renames)
	}
	current, err := f.backend.Stat("clip.mov")
	if err != nil || current.Inode == 3 || current.Length != uint64(len(body)) {
		t.Fatalf("published destination=%+v err=%v", current, err)
	}
	raw := f.x.rdb
	rctx := context.Background()
	if n, err := raw.Exists(rctx, "i3").Result(); err != nil || n != 0 {
		t.Fatalf("old inode remains: n=%d err=%v", n, err)
	}
	oldGraph, err := raw.LRange(rctx, "c3_0", 0, -1).Result()
	if err != nil || len(oldGraph) != 1 || len(oldGraph[0]) != 24 || binary.BigEndian.Uint64([]byte(oldGraph[0])[4:12]) != 77 {
		t.Fatalf("old graph=%d slices err=%v", len(oldGraph), err)
	}
	tracking, err := raw.ZRange(rctx, "delfiles", 0, -1).Result()
	if err != nil || !reflect.DeepEqual(tracking, []string{oldTracking}) {
		t.Fatalf("pending files=%v err=%v", tracking, err)
	}
	if got, err := os.ReadFile(oldObject); err != nil || !bytes.Equal(got, f.x.want) {
		t.Fatalf("old object bytes=%d err=%v", len(got), err)
	}
	newGraph, err := raw.LRange(rctx, fmt.Sprintf("c%d_0", current.Inode), 0, -1).Result()
	if err != nil || len(newGraph) != 1 || len(newGraph[0]) != 24 {
		t.Fatalf("published graph=%d slices err=%v", len(newGraph), err)
	}
	newSlice := []byte(newGraph[0])
	newID, newSize := binary.BigEndian.Uint64(newSlice[4:12]), binary.BigEndian.Uint32(newSlice[12:16])
	newObject := filepath.Join(rt.objectRoot, "chunks", fmt.Sprint(newID/1000000), fmt.Sprint(newID/1000), fmt.Sprintf("%d_0_%d", newID, newSize))
	if got, err := os.ReadFile(newObject); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("published raw object bytes=%d err=%v", len(got), err)
	}
	liveAttrKey := fmt.Sprintf("i%d", current.Inode)
	liveAttr, err := raw.Get(rctx, liveAttrKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	liveDentry, err := raw.HGet(rctx, "d4", "clip.mov").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	leaseBefore, err := raw.HGetAll(rctx, respproxy.LeaseKeyPrefix+"4").Result()
	if err != nil || leaseBefore["holder"] != "mac-test" || leaseBefore["epoch"] == "" {
		t.Fatalf("replacement owner lease=%v err=%v", leaseBefore, err)
	}
	checkLiveAuthority := func() {
		t.Helper()
		attr, err := raw.Get(rctx, liveAttrKey).Bytes()
		if err != nil || !bytes.Equal(attr, liveAttr) {
			t.Fatalf("live inode changed: %v", err)
		}
		dentry, err := raw.HGet(rctx, "d4", "clip.mov").Bytes()
		if err != nil || !bytes.Equal(dentry, liveDentry) {
			t.Fatalf("live dentry changed: %v", err)
		}
		lease, err := raw.HGetAll(rctx, respproxy.LeaseKeyPrefix+"4").Result()
		if err != nil || lease["holder"] != leaseBefore["holder"] || lease["epoch"] != leaseBefore["epoch"] || lease["host"] != leaseBefore["host"] {
			t.Fatalf("owner lease changed: before=%v after=%v err=%v", leaseBefore, lease, err)
		}
	}
	refBefore, err := raw.HGetAll(rctx, "sliceRef").Result()
	if err != nil {
		t.Fatal(err)
	}

	// A real Mac SDK scans the old pending-file record, but the enforcing
	// proxy denies its orphaned c3_0/sliceRef transaction. The scan's nil
	// return is not a deletion oracle: doDeleteFileData only logs failures.
	macConf := jfsmeta.DefaultConf()
	macConf.MaxDeletes, macConf.NoBGJob = 1, true
	macMeta := jfsmeta.NewClient(rt.url, macConf)
	t.Cleanup(func() {
		if err := macMeta.Shutdown(); err != nil {
			t.Errorf("Mac GC metadata shutdown: %v", err)
		}
	})
	if format, err := macMeta.Load(true); err != nil || format == nil || format.UUID != f.x.volumeUUID {
		t.Fatalf("Mac metadata format=%+v err=%v", format, err)
	}
	macDeleted := 0
	macMeta.OnMsg(jfsmeta.DeleteSlice, func(...interface{}) error { macDeleted++; return fmt.Errorf("Mac unexpectedly reached DeleteSlice") })
	macSeen := 0
	macDenied := func(s respproxy.Snapshot) int64 {
		var n int64
		for _, d := range s.Decisions {
			if d.Principal == "mac-test" && d.Kind == respproxy.KindMac && d.Outcome == respproxy.DecDeny && d.Reason == respproxy.ReasonUnknownKey {
				n += d.Count
			}
		}
		return n
	}
	beforeMacScan := rt.proxy.Stats.Snapshot()
	macScanStart := time.Now()
	if err := macMeta.ScanDeletedObject(jfsmeta.Background(), nil, nil, nil, func(ino jfsmeta.Ino, size uint64, _ int64) (bool, error) {
		if ino != 3 || size != 65536 {
			return false, fmt.Errorf("unexpected pending file %d:%d", ino, size)
		}
		macSeen++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if macSeen != 1 || macDeleted != 0 {
		t.Fatalf("Mac scan seen=%d slice deletes=%d", macSeen, macDeleted)
	}
	afterMacScan := rt.proxy.Stats.Snapshot()
	if macDenied(afterMacScan) <= macDenied(beforeMacScan) {
		t.Fatalf("Mac pending-file scan did not produce a proxy UNKNOWN_KEY denial: before=%d after=%d", macDenied(beforeMacScan), macDenied(afterMacScan))
	}
	matchedGCUnit := false
	for _, d := range afterMacScan.Recent {
		if !d.Time.Before(macScanStart) && d.Principal == "mac-test" && d.Kind == respproxy.KindMac && d.Outcome == respproxy.DecDeny && d.Reason == respproxy.ReasonUnknownKey && strings.Contains(d.Unit, "DEL c3_0") && strings.Contains(d.Unit, "HINCRBY sliceRef") {
			matchedGCUnit = true
			break
		}
	}
	if !matchedGCUnit {
		t.Fatalf("Mac UNKNOWN_KEY denial did not identify old-chunk GC transaction (recent=%d)", len(afterMacScan.Recent))
	}
	checkOldPresent := func() {
		t.Helper()
		graph, err := raw.LRange(rctx, "c3_0", 0, -1).Result()
		if err != nil || !reflect.DeepEqual(graph, oldGraph) {
			t.Fatalf("Mac changed old graph: %v", err)
		}
		pending, err := raw.ZRange(rctx, "delfiles", 0, -1).Result()
		if err != nil || !reflect.DeepEqual(pending, tracking) {
			t.Fatalf("Mac changed tracking: %v err=%v", pending, err)
		}
		refs, err := raw.HGetAll(rctx, "sliceRef").Result()
		if err != nil || !reflect.DeepEqual(refs, refBefore) {
			t.Fatalf("Mac changed slice refs: %v err=%v", refs, err)
		}
		if got, err := os.ReadFile(oldObject); err != nil || !bytes.Equal(got, f.x.want) {
			t.Fatalf("Mac changed old object: bytes=%d err=%v", len(got), err)
		}
	}
	checkOldPresent()
	checkLiveAuthority()

	// Preserve both existing Mac principals exactly when admitting one BG
	// principal; Swap must not revoke the active replacement/lease session.
	bgHash, err := respproxy.HashPassword("gc-bg-secret", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	principals, err := respproxy.ParsePrincipals([]byte(fmt.Sprintf(
		`{"principals":[{"name":"mac-test","kind":"mac","host":"test-host","argon2id":%q},{"name":"mac-peer","kind":"mac","host":"peer-host","argon2id":%q},{"name":"gc-bg","kind":"bg","argon2id":%q}]}`,
		rt.authHash, rt.authHash, bgHash)))
	if err != nil {
		t.Fatal(err)
	}
	rt.proxy.Auth.Swap(principals)
	bgURL, err := url.Parse(rt.url)
	if err != nil {
		t.Fatal(err)
	}
	bgURL.User = url.UserPassword("gc-bg", "gc-bg-secret")
	bgConf := jfsmeta.DefaultConf()
	bgConf.MaxDeletes, bgConf.NoBGJob = 1, true
	bgMeta := jfsmeta.NewClient(bgURL.String(), bgConf)
	t.Cleanup(func() {
		if err := bgMeta.Shutdown(); err != nil {
			t.Errorf("BG GC metadata shutdown: %v", err)
		}
	})
	if format, err := bgMeta.Load(true); err != nil || format == nil || format.UUID != f.x.volumeUUID {
		t.Fatalf("BG metadata format=%+v err=%v", format, err)
	}
	type deletedSlice struct {
		id    uint64
		size  uint32
		prior []byte
		ref   string
		err   error
	}
	var mu sync.Mutex
	var deleted []deletedSlice
	bgMeta.OnMsg(jfsmeta.DeleteSlice, func(args ...interface{}) error {
		id, size := args[0].(uint64), args[1].(uint32)
		prior, readErr := os.ReadFile(oldObject)
		ref, refErr := raw.HGet(rctx, "sliceRef", fmt.Sprintf("k%d_%d", id, size)).Result()
		removeErr := rt.store.Remove(id, int(size))
		mu.Lock()
		deleted = append(deleted, deletedSlice{id: id, size: size, prior: prior, ref: ref, err: firstReplacementGCError(readErr, refErr, removeErr)})
		mu.Unlock()
		return removeErr
	})
	bgSeen := 0
	bgScan := func() error {
		return bgMeta.ScanDeletedObject(jfsmeta.Background(), nil, nil, nil, func(ino jfsmeta.Ino, size uint64, _ int64) (bool, error) {
			if ino != 3 || size != 65536 {
				return false, fmt.Errorf("unexpected BG pending file %d:%d", ino, size)
			}
			bgSeen++
			return true, nil
		})
	}
	if err := bgScan(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	proof := append([]deletedSlice(nil), deleted...)
	mu.Unlock()
	if bgSeen != 1 || len(proof) != 1 {
		t.Fatalf("BG slice callback: pending=%d callbacks=%d", bgSeen, len(proof))
	}
	if proof[0].id != 77 || proof[0].size != 65536 || proof[0].ref != "-1" || proof[0].err != nil || !bytes.Equal(proof[0].prior, f.x.want) {
		t.Fatalf("BG slice callback: id=%d size=%d ref=%q error=%v prior_bytes=%d", proof[0].id, proof[0].size, proof[0].ref, proof[0].err, len(proof[0].prior))
	}
	checkCleaned := func() {
		t.Helper()
		for _, key := range []string{"i3", "c3_0"} {
			if n, err := raw.Exists(rctx, key).Result(); err != nil || n != 0 {
				t.Fatalf("old key %s remains: n=%d err=%v", key, n, err)
			}
		}
		if n, err := raw.HExists(rctx, "sliceRef", "k77_65536").Result(); err != nil || n {
			t.Fatalf("old slice ref remains: %v err=%v", n, err)
		}
		if n, err := raw.ZScore(rctx, "delfiles", oldTracking).Result(); err != redis.Nil {
			t.Fatalf("old tracking remains: score=%f err=%v", n, err)
		}
		if _, err := os.Lstat(oldObject); !os.IsNotExist(err) {
			t.Fatalf("old object remains: %v", err)
		}
		graph, err := raw.LRange(rctx, fmt.Sprintf("c%d_0", current.Inode), 0, -1).Result()
		if err != nil || !reflect.DeepEqual(graph, newGraph) {
			t.Fatalf("published graph changed: %v", err)
		}
		if got, err := os.ReadFile(newObject); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("published object changed: bytes=%d err=%v", len(got), err)
		}
		checkLiveAuthority()
	}
	checkCleaned()
	if err := bgScan(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	callbacks := len(deleted)
	mu.Unlock()
	if bgSeen != 1 || callbacks != 1 {
		t.Fatalf("repeat sweep seen=%d callbacks=%d", bgSeen, callbacks)
	}
	checkCleaned()
	f.d.leasesPtr.Load().Stop()
	assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, beforeCapacity-f.row.Size, false)
}

func firstReplacementGCError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

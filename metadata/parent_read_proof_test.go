package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

func privateParentProofRedis(t *testing.T) (*RedisClient, *redis.Client) {
	t.Helper()
	binaryPath := os.Getenv("JM_PARENT_PROOF_REDIS_SERVER")
	if !filepath.IsAbs(binaryPath) {
		t.Skip("set JM_PARENT_PROOF_REDIS_SERVER to an owned Redis binary")
	}
	base := os.Getenv("TMPDIR")
	if !filepath.IsAbs(base) {
		t.Fatal("private TMPDIR required")
	}
	dir, err := os.MkdirTemp(base, "pr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "r.sock")
	if len(socket) >= 104 {
		t.Fatal("private Redis socket path too long")
	}
	cmd := exec.Command(binaryPath, "--port", "0", "--unixsocket", socket,
		"--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", dir)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("owned Redis did not reap")
		}
	})
	rdb := redis.NewClient(&redis.Options{Network: "unix", Addr: socket, MaxRetries: -1,
		ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = rdb.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err = rdb.Ping(ctx).Err()
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("owned Redis did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rc := &RedisClient{}
	rc.rdb.Store(rdb)
	return rc, rdb
}

func proofEdge(typ byte, inode uint64) []byte {
	b := make([]byte, 9)
	b[0] = typ
	binary.BigEndian.PutUint64(b[1:], inode)
	return b
}

func proofAttr(typ uint8, parent, size uint64, ns uint32) []byte {
	return (&meta.Attr{Typ: typ, Parent: meta.Ino(parent), Length: size,
		Mtime: 1780007368, Mtimensec: ns}).Marshal()
}

func seedProofPath(t *testing.T, rdb *redis.Client, parent, leaf uint64) {
	t.Helper()
	ctx := context.Background()
	commands := []func() error{
		func() error { return rdb.Set(ctx, "setting", `{"UUID":"proof-volume"}`, 0).Err() },
		func() error { return rdb.Set(ctx, "i1", proofAttr(meta.TypeDirectory, 1, 0, 0), 0).Err() },
		func() error { return rdb.HSet(ctx, "d1", "media", proofEdge(meta.TypeDirectory, parent)).Err() },
		func() error {
			return rdb.Set(ctx, "i"+strconv.FormatUint(parent, 10), proofAttr(meta.TypeDirectory, 1, 0, 7), 0).Err()
		},
		func() error {
			return rdb.HSet(ctx, "d"+strconv.FormatUint(parent, 10), "video.mov", proofEdge(meta.TypeFile, leaf)).Err()
		},
		func() error {
			return rdb.Set(ctx, "i"+strconv.FormatUint(leaf, 10), proofAttr(meta.TypeFile, parent, 4096, 490386000), 0).Err()
		},
	}
	for i, command := range commands {
		if err := command(); err != nil {
			t.Fatalf("seed command %d: %v", i, err)
		}
	}
}

func proofContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), time.Second)
}

func TestParentReadProofFullWidthAndReplacement(t *testing.T) {
	rc, rdb := privateParentProofRedis(t)
	parent, leaf := uint64(1<<54+9), uint64(1<<54+10)
	seedProofPath(t, rdb, parent, leaf)
	read := func(ids []uint64) (RedisPathProof, error) {
		ctx, cancel := proofContext(t)
		defer cancel()
		return rc.ReadParentPathProof(ctx, "proof-volume", "media/video.mov", ids)
	}
	first, err := read([]uint64{parent, leaf})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Nodes) != 2 || first.Nodes[0].Inode != parent || first.Nodes[1].Inode != leaf ||
		first.Nodes[1].ParentInode != parent || first.Nodes[1].Size != 4096 || first.Nodes[1].MtimeNsec != 490386000 {
		t.Fatalf("full-width proof mismatch: %+v", first)
	}
	if first.RedisOps != 13 || first.RequestBytes <= 0 || first.ResponseBytes <= 0 || first.ResponseBytes > maxReadProofReplyBytes ||
		len(first.Root.RawAttr) != 72 || len(first.Nodes[1].RawEdge) != 9 || len(first.Nodes[1].RawAttr) != 72 {
		t.Fatalf("bounded raw proof accounting missing: %+v", first)
	}
	// Old parent + old leaf still exist under their old inode keys, with the
	// same content attrs. Only the root edge changes. The old tuple MUST fail.
	newParent, newLeaf := parent+100, leaf+100
	seedProofPath(t, rdb, newParent, newLeaf)
	if _, err := read([]uint64{parent, leaf}); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("old parent admitted: %v", err)
	}
	second, err := read([]uint64{newParent, newLeaf})
	if err != nil || second.Nodes[1].Inode != newLeaf {
		t.Fatalf("new parent proof: %+v %v", second, err)
	}
	// Replace only the leaf; a same-size/fine-mtime successor still changes
	// the exact edge and cannot satisfy the old expected leaf inode.
	thirdLeaf := newLeaf + 1
	if err := rdb.HSet(context.Background(), "d"+strconv.FormatUint(newParent, 10), "video.mov", proofEdge(meta.TypeFile, thirdLeaf)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), "i"+strconv.FormatUint(thirdLeaf, 10), proofAttr(meta.TypeFile, newParent, 4096, 490386000), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read([]uint64{newParent, newLeaf}); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("old leaf admitted: %v", err)
	}
	if _, err := read([]uint64{newParent, thirdLeaf}); err != nil {
		t.Fatal(err)
	}
}

func TestParentReadProofRefusalsAndResetLimit(t *testing.T) {
	rc, rdb := privateParentProofRedis(t)
	parent, leaf := uint64(1<<54+99), uint64(1<<54+100)
	seedProofPath(t, rdb, parent, leaf)
	read := func() (RedisPathProof, error) {
		ctx, cancel := proofContext(t)
		defer cancel()
		return rc.ReadParentPathProof(ctx, "proof-volume", "media/video.mov", []uint64{parent, leaf})
	}
	first, err := read()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := rdb.Set(ctx, "setting", `{"UUID":"another-volume"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("changed UUID admitted: %v", err)
	}
	if err := rdb.Set(ctx, "setting", `{"UUID":"proof-volume"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "setting", strings.Repeat("x", 65537), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("oversized setting admitted: %v", err)
	}
	if err := rdb.Set(ctx, "setting", `{"UUID":"proof-volume"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(leaf, 10), "short", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("truncated attr admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(leaf, 10), proofAttr(meta.TypeFile, parent, 4096, 490386000), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(leaf, 10), strings.Repeat("x", 65537), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("oversized attr admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(leaf, 10), proofAttr(meta.TypeFile, parent, 4096, 490386000), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "d"+strconv.FormatUint(parent, 10), "video.mov", strings.Repeat("x", 65537)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("oversized edge admitted: %v", err)
	}
	if err := rdb.HSet(ctx, "d"+strconv.FormatUint(parent, 10), "video.mov", proofEdge(meta.TypeFile, leaf)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Del(ctx, "d1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "d1", "wrongtype", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("WRONGTYPE admitted: %v", err)
	}
	if err := rdb.Del(ctx, "d1").Err(); err != nil {
		t.Fatal(err)
	}
	seedProofPath(t, rdb, parent, leaf)
	if err := rdb.Del(ctx, "i1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("missing root admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i1", proofAttr(meta.TypeFile, 1, 0, 0), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("wrong root type admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i1", proofAttr(meta.TypeDirectory, 0, 0, 0), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("wrong root parent admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i1", proofAttr(meta.TypeDirectory, 1, 0, 0), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(parent, 10), proofAttr(meta.TypeDirectory, 777, 0, 7), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("wrong parent attr admitted: %v", err)
	}
	if err := rdb.Set(ctx, "i"+strconv.FormatUint(parent, 10), proofAttr(meta.TypeDirectory, 1, 0, 7), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "d1", "media", proofEdge(meta.TypeSymlink, parent)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("symlink ancestor admitted: %v", err)
	}
	if err := rdb.HSet(ctx, "d1", "media", proofEdge(meta.TypeDirectory, parent)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HDel(ctx, "d"+strconv.FormatUint(parent, 10), "video.mov").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("missing leaf admitted: %v", err)
	}
	// A same-UUID dataset reset and restore can reproduce the same tuple.
	// This API deliberately does not call that a reusable FD incarnation.
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	seedProofPath(t, rdb, parent, leaf)
	second, err := read()
	if err != nil {
		t.Fatal(err)
	}
	if first.FormatSHA1 != second.FormatSHA1 || !reflect.DeepEqual(first.Root, second.Root) || !reflect.DeepEqual(first.Nodes, second.Nodes) {
		t.Fatalf("reset-limit fixture changed tuple unexpectedly: first=%+v second=%+v", first, second)
	}
	cancelled, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	if _, err := rc.ReadParentPathProof(cancelled, "proof-volume", "media/video.mov", []uint64{parent, leaf}); err == nil {
		t.Fatal("cancelled proof succeeded")
	}
}

func TestParentReadProofInputBounds(t *testing.T) {
	cases := []struct {
		name string
		path string
		ids  []uint64
	}{
		{"absolute", "/media/file", []uint64{2, 3}}, {"dot", "media/../file", []uint64{2, 3}},
		{"depth", "file", []uint64{2, 3}}, {"zero", "file", []uint64{0}},
		{"nul", "bad\x00name", []uint64{2}}, {"empty", "", nil},
		{"root-repeat", "media/file", []uint64{1, 3}},
		{"ancestor-repeat", "a/b/file", []uint64{2, 2, 3}},
		{"leaf-repeat", "a/file", []uint64{2, 2}},
		{"trash", "file", []uint64{uint64(meta.TrashInode)}},
		{"depth-over", strings.Repeat("a/", maxReadProofDepth) + "file", make([]uint64, maxReadProofDepth+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readProofParts(tc.path, tc.ids); !errors.Is(err, ErrReadPathProof) {
				t.Fatalf("%q ids=%v: %v", tc.path, tc.ids, err)
			}
		})
	}
	if _, err := readProofParts("media/file", []uint64{1<<54 + 9, 1<<54 + 10}); err != nil {
		t.Fatal(fmt.Errorf("valid full-width IDs refused: %w", err))
	}
}

func TestParentReadProofInflightDeadline(t *testing.T) {
	rc, rdb := privateParentProofRedis(t)
	parent, leaf := uint64(1<<54+200), uint64(1<<54+201)
	seedProofPath(t, rdb, parent, leaf)
	if err := rdb.Do(context.Background(), "CLIENT", "PAUSE", "300", "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	started := time.Now()
	_, err := rc.ReadParentPathProof(ctx, "proof-volume", "media/video.mov", []uint64{parent, leaf})
	elapsed := time.Since(started)
	cancel()
	if !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("paused in-flight EVAL was not refused: %v", err)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("paused in-flight EVAL exceeded deadline budget: %v", elapsed)
	}
	// The only owned process is this fixture Redis. After pause expires, a
	// terminal PING proves the client remains usable; cleanup reaps the server.
	terminal, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := rdb.Ping(terminal).Err(); err != nil {
		t.Fatalf("owned Redis did not recover after pause: %v", err)
	}
}

func TestParentReadProofDeepAncestry(t *testing.T) {
	rc, rdb := privateParentProofRedis(t)
	ctx := context.Background()
	if err := rdb.Set(ctx, "setting", `{"UUID":"proof-volume"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "i1", proofAttr(meta.TypeDirectory, 1, 0, 0), 0).Err(); err != nil {
		t.Fatal(err)
	}
	names := make([]string, maxReadProofDepth)
	ids := make([]uint64, maxReadProofDepth)
	parent := uint64(1)
	for i := range names {
		names[i], ids[i] = fmt.Sprintf("n%02d", i), uint64(i+2)
		typ := byte(meta.TypeDirectory)
		if i == len(names)-1 {
			typ = meta.TypeFile
		}
		if err := rdb.HSet(ctx, "d"+strconv.FormatUint(parent, 10), names[i], proofEdge(typ, ids[i])).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rdb.Set(ctx, "i"+strconv.FormatUint(ids[i], 10), proofAttr(typ, parent, 4096, 7), 0).Err(); err != nil {
			t.Fatal(err)
		}
		parent = ids[i]
	}
	name := strings.Join(names, "/")
	read := func() error {
		deadline, cancel := proofContext(t)
		defer cancel()
		_, err := rc.ReadParentPathProof(deadline, "proof-volume", name, ids)
		return err
	}
	if err := read(); err != nil {
		t.Fatalf("maximum-depth ancestry refused: %v", err)
	}
	if err := rdb.Set(ctx, "i17", proofAttr(meta.TypeDirectory, 999, 4096, 7), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := read(); !errors.Is(err, ErrReadPathProof) {
		t.Fatalf("corrupt middle ancestor admitted: %v", err)
	}
}

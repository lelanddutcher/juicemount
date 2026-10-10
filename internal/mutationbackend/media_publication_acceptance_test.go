//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

type publicationReady struct {
	Stage                           mutationauthority.MediaStage
	Parent, Replacement, RaceParent uint64
}

func TestSDKConditionalPublicationAndRecovery(t *testing.T) {
	for _, mode := range []string{"nested", "root", "lost", "lost-root", "intent-crash", "linked-crash", "stale-before", "stale-intent", "occupied-before", "occupied-intent", "racing-parent", "quota"} {
		t.Run(mode, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "none", DirStats: true}
			setting := f.SeedExisting(t, &format)
			socket := f.Socket
			var ackHeld <-chan error
			var drop func()
			var dropped <-chan struct{}
			var held <-chan struct{}
			var release chan<- struct{}
			lost := strings.HasPrefix(mode, "lost")
			if lost {
				socket = filepath.Join(f.Dir, "a.sock")
				ackHeld, drop, dropped = linkAckRelay(t, socket, f.Socket, true)
			}
			if mode == "racing-parent" {
				socket = filepath.Join(f.Dir, "p.sock")
				held, release = linkExecRelay(t, socket, f.Socket)
			}
			cfg := SDKConfig{Redis: RedisConfig{Socket: socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting)}
			body, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(f.Dir, "sdk.json")
			if err := os.WriteFile(config, body, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.Context, 40*time.Second)
			defer cancel()
			read, write, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKPublicationWriterChild$")
			cmd.Env = append(os.Environ(), "JM_PUBLICATION_CONFIG="+config, "JM_PUBLICATION_MODE="+mode)
			cmd.ExtraFiles = []*os.File{write}
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				_ = write.Close()
				t.Fatal(err)
			}
			_ = write.Close()
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			reaped := false
			defer func() {
				if !reaped {
					_ = cmd.Process.Kill()
					exit := <-done
					t.Logf("writer joined after failure: exit=%v stdout=%q stderr-bytes=%d", exit, stdout.String(), stderr.Len())
				}
			}()
			var ready publicationReady
			if err := json.NewDecoder(read).Decode(&ready); err != nil {
				t.Fatal("missing publication checkpoint", err)
			}
			if lost {
				select {
				case err := <-ackHeld:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("lost reply barrier not reached")
				}
				admin := f.Admin()
				entry, err := admin.HGet(ctx, fmt.Sprint("d", ready.Parent), "published.mov").Bytes()
				defer admin.Close()
				if err != nil || len(entry) != 9 || binary.BigEndian.Uint64(entry[1:]) != ready.Stage.Inode {
					t.Fatal("EXEC did not commit exact public link", err)
				}
				want := []int64{ready.Stage.Intent.Size, 4096, 1}
				if ready.Parent == 1 {
					want = []int64{2 * ready.Stage.Intent.Size, 4 * 4096, 4}
				}
				for i, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
					got, err := admin.HGet(ctx, key, strconv.FormatUint(ready.Parent, 10)).Int64()
					if err != nil || got != want[i] {
						t.Fatalf("committed accounting before reply loss differs: %s=%d want=%d err=%v", key, got, want[i], err)
					}
				}
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "racing-parent" {
				select {
				case <-held:
				case <-ctx.Done():
					t.Fatal("guarded EXEC not held")
				}
				admin := f.Admin()
				entry := binary.BigEndian.AppendUint64([]byte{meta.TypeDirectory}, ready.Replacement)
				err := admin.HSet(ctx, fmt.Sprint("d", ready.RaceParent), "inner", entry).Err()
				_ = admin.Close()
				if err != nil {
					t.Fatal(err)
				}
				close(release)
			}
			waitErr := <-done
			reaped = true
			crash := mode == "intent-crash" || mode == "linked-crash"
			var exit *exec.ExitError
			if ctx.Err() != nil || stderr.Len() != 0 || (lost && waitErr == nil) || (crash && (!errors.As(waitErr, &exit) || exit.ExitCode() != 57)) || (!lost && !crash && waitErr != nil) {
				t.Fatalf("writer exit differs: %v ctx=%v stdout=%q stderr-bytes=%d", waitErr, ctx.Err(), stdout.String(), stderr.Len())
			}
			if lost {
				drop()
				select {
				case <-dropped:
				case <-ctx.Done():
					t.Fatal("reply relay did not close")
				}
			}
			if !lost && !crash && stdout.String() != "PASS\n" {
				t.Fatalf("unexpected writer diagnostics %q", stdout.String())
			}
			before := linkAckObjectSnapshot(t, filepath.Join(f.Dir, "objects"), ready.Stage)
			// Verify two independent restarts: persisted certificates alone are
			// never enough, and absolute accounting repair must be idempotent.
			for restart := 0; restart < 2; restart++ {
				recoverCmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKPublicationRecoveryChild$")
				recoverCmd.Env = append(os.Environ(), "JM_PUBLICATION_CONFIG="+config, "JM_PUBLICATION_MODE="+mode)
				var out, errout bytes.Buffer
				recoverCmd.Stdout, recoverCmd.Stderr = &out, &errout
				if err := recoverCmd.Run(); err != nil || errout.Len() != 0 || out.String() != "{\"verified\":true}\nPASS\n" {
					t.Fatalf("restart %d failed: %v stdout=%q stderr-bytes=%d", restart, err, out.String(), errout.Len())
				}
			}
			after := linkAckObjectSnapshot(t, filepath.Join(f.Dir, "objects"), ready.Stage)
			for key, hash := range before {
				if after[key] != hash {
					t.Fatal("stage bytes changed")
				}
			}
			t.Logf("%s writer=%d joined; two fenced recoveries preserve exact stage bytes and observed/private classification", mode, cmd.Process.Pid)
		})
	}
}

func TestSDKPublicationWriterChild(t *testing.T) {
	config := os.Getenv("JM_PUBLICATION_CONFIG")
	if config == "" {
		return
	}
	mode := os.Getenv("JM_PUBLICATION_MODE")
	cfg, err := ReadSDKConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(filepath.Dir(config), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	mkdir := func(name string) meta.Ino {
		var ino meta.Ino
		var a meta.Attr
		if eno := r.metadata.Mkdir(fctx, meta.RootInode, name, 0700, 0077, 0, &ino, &a); eno != 0 {
			t.Fatal(eno)
		}
		return ino
	}
	parent, replacement := mkdir("target"), mkdir("replacement")
	raceParent := uint64(parent)
	if mode == "racing-parent" {
		var inner meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Mkdir(fctx, parent, "inner", 0700, 0077, 0, &inner, &attr); eno != 0 {
			t.Fatal(eno)
		}
		parent = inner
	}
	lease, err := j.Claim(ctx, "volume", "publisher", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("exact original publication bytes")
	target := NewNameStageTarget{Path: "/target/published.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "target", Inode: uint64(parent)}}}
	if mode == "racing-parent" {
		target = NewNameStageTarget{Path: "/target/inner/published.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "target", Inode: raceParent}, {Name: "inner", Inode: uint64(parent)}}}
	}
	if mode == "root" || mode == "lost-root" {
		parent = meta.RootInode
		target = NewNameStageTarget{Path: "/published.mov", Parents: []DirectoryIdentity{{Inode: 1}}}
	}
	spec := MediaStageRequest{Volume: "volume", Session: lease.Session, Epoch: lease.Epoch, ID: "publication", FileID: "origin-file", ExpectedRevision: "origin-absent", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("custody"))}
	stage, err := r.StageMediaForNewName(ctx, spec, target, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	moveParent := func() {
		var ino meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Rename(fctx, meta.RootInode, "target", meta.RootInode, "moved", meta.RenameNoReplace, &ino, &attr); eno != 0 {
			t.Fatal(eno)
		}
		mkdir("target")
	}
	occupy := func() {
		file, eno := r.filesystem.Create(fctx, target.Path, 0600, 0077)
		if eno != 0 {
			t.Fatal(eno)
		}
		if eno := file.Close(fctx); eno != 0 {
			t.Fatal(eno)
		}
	}
	if mode == "stale-before" {
		moveParent()
	}
	if mode == "occupied-before" {
		occupy()
	}
	if mode == "quota" {
		if err := r.reader.HSet(ctx, "dirQuota", parent.String(), "configured").Err(); err != nil {
			t.Fatal(err)
		}
	}
	r.media.hook = func(phase string) {
		if phase == "publication-intent" {
			p, err := j.InspectPublication(ctx, "volume", spec.ID)
			if err != nil || p == nil || p.Observed || p.Snapshot != mutationauthority.StageSnapshotDigest(stage) {
				t.Fatal("missing immutable pre-effect intent", err)
			}
			if mode == "intent-crash" {
				os.Exit(57)
			}
			if mode == "stale-intent" {
				moveParent()
			}
			if mode == "occupied-intent" {
				occupy()
			}
		}
		if phase == "publication-linked" && mode == "linked-crash" {
			// Model stale derivative totals on a published namespace; recovery
			// must replace exact totals without replaying the public link.
			for _, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
				if err := r.reader.HSet(ctx, key, parent.String(), "0").Err(); err != nil {
					t.Fatal(err)
				}
			}
			os.Exit(57)
		}
	}
	// For the root fixture, wait for the known private-stage baseline without
	// invoking lazy statistic repair. The held-reply oracle checks the +1
	// public link before any successor can repair missing accounting.
	if mode == "lost-root" {
		deadline := time.Now().Add(3 * time.Second)
		for {
			ready := true
			for i, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
				got, err := r.reader.HGet(ctx, key, "1").Int64()
				if err != nil || got != []int64{stage.Intent.Size, 3 * 4096, 3}[i] {
					ready = false
				}
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("private root baseline did not flush")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	report := os.NewFile(3, "publication-checkpoint")
	defer report.Close()
	if err := json.NewEncoder(report).Encode(publicationReady{stage, uint64(parent), uint64(replacement), raceParent}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := r.PublishMediaForNewName(ctx, lease, spec.ID)
	fail := strings.HasPrefix(mode, "stale") || strings.HasPrefix(mode, "occupied") || mode == "racing-parent" || mode == "quota"
	if fail {
		if err == nil {
			t.Fatal("guarded conflict accepted")
		}
		return
	}
	if err != nil || !checkpoint.Observed || checkpoint.Snapshot != mutationauthority.StageSnapshotDigest(stage) {
		t.Fatal("publication failed", err)
	}
	if _, err := r.PublishMediaForNewName(ctx, lease, spec.ID); !errors.Is(err, mutationauthority.ErrUncertain) {
		t.Fatal("same-process replay accepted", err)
	}
}

func TestSDKPublicationRecoveryChild(t *testing.T) {
	config := os.Getenv("JM_PUBLICATION_CONFIG")
	if config == "" {
		return
	}
	mode := os.Getenv("JM_PUBLICATION_MODE")
	cfg, err := ReadSDKConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(filepath.Dir(config), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	before, err := j.InspectStage(ctx, "volume", "publication")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Claim(ctx, "volume", "before-fence", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
		t.Fatal("persisted certificates bypassed fresh fence", err)
	}
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	if mode == "quota" {
		if err == nil {
			t.Fatal("unqualified quota admitted")
		}
		if _, err := j.Claim(ctx, "volume", "after-refusal", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
			t.Fatal("quota refusal settled operation", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"verified": true}); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	wantPublic := mode == "nested" || mode == "root" || strings.HasPrefix(mode, "lost") || mode == "linked-crash"
	after, err := j.InspectStage(ctx, "volume", "publication")
	if err != nil || mutationauthority.StageSnapshotDigest(before) != mutationauthority.StageSnapshotDigest(after) {
		t.Fatal("recovery changed immutable stage", err)
	}
	if err := j.RequireRecoveredStage(ctx, after.Request); err != nil {
		t.Fatal("missing current recovery certificate", err)
	}
	if wantPublic {
		if err := j.RequireRetainedStage(ctx, after.Request); err == nil {
			t.Fatal("published stage treated as private")
		}
	}
	p, err := j.InspectPublication(ctx, "volume", "publication")
	if err != nil || (wantPublic && (p == nil || !p.Observed)) || (!wantPublic && p != nil && p.Observed) {
		t.Fatal("publication classification differs", err)
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	var inode meta.Ino
	var attr meta.Attr
	if eno := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(after.Intent.Path, "/"), &inode, &attr, false); eno != 0 || uint64(inode) != after.Inode || attr.Nlink != map[bool]uint32{false: 1, true: 2}[wantPublic] {
		t.Fatal("private stage identity differs", eno)
	}
	e, err := publicationExpectation(after)
	if err != nil {
		t.Fatal(err)
	}
	parent := e.Parents[len(e.Parents)-1].Inode
	if wantPublic {
		if eno := r.metadata.Lookup(fctx, parent, e.Name, &inode, &attr, false); eno != 0 || uint64(inode) != after.Inode || attr.Nlink != 2 {
			t.Fatal("public inode differs", eno)
		}
		file, eno := r.filesystem.Open(fctx, func() string { var b mediaStageEnvelope; _ = strictJSON(after.Intent.Body, &b); return b.Target.Path }(), meta.MODE_MASK_R)
		if eno != 0 {
			t.Fatal(eno)
		}
		body := make([]byte, after.Intent.Size+1)
		n, readErr := file.Pread(fctx, body, 0)
		closeErr := file.Close(fctx)
		if (readErr != nil && readErr != io.EOF) || int64(n) != after.Intent.Size || digest(body[:n]) != after.Intent.SHA256 || closeErr != 0 {
			t.Fatal("public fresh-handle bytes differ")
		}
		wantLength, wantSpace, wantInodes := after.Intent.Size, int64(4096), int64(1)
		if parent == meta.RootInode {
			wantLength *= 2
			wantSpace = 4 * 4096
			wantInodes = 4
		} // target + replacement + private + public
		for i, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
			value, err := r.reader.HGet(ctx, key, parent.String()).Result()
			if err != nil || value != strconv.FormatInt([]int64{wantLength, wantSpace, wantInodes}[i], 10) {
				t.Fatal("recovered target accounting differs", key, value, err)
			}
		}
	} else {
		entry, err := r.reader.HGet(ctx, fmt.Sprint("d", parent), e.Name).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			t.Fatal("captured target read failed", err)
		}
		if err == nil && (len(entry) != 9 || binary.BigEndian.Uint64(entry[1:]) == after.Inode) {
			t.Fatal("refused operation exposed stage or malformed target")
		}
		current := meta.RootInode
		for _, component := range e.Parents[1:] {
			if eno := r.metadata.Lookup(fctx, current, component.Name, &inode, &attr, false); eno != 0 {
				t.Fatal("current ancestor lookup failed", eno)
			}
			current = inode
		}
		if eno := r.metadata.Lookup(fctx, current, e.Name, &inode, &attr, false); eno == 0 && uint64(inode) == after.Inode {
			t.Fatal("refused operation retargeted replacement ancestor")
		} else if eno != 0 && eno != syscall.ENOENT {
			t.Fatal("current target lookup failed", eno)
		}
	}
	lease, err := j.Claim(ctx, "volume", "successor", time.Minute)
	if err != nil || lease.Epoch <= after.Request.Epoch {
		t.Fatal("qualified recovery blocked safe successor", err)
	}
	op, err := j.Inspect(ctx, "volume", "publication")
	if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
		t.Fatal("publication manufactured durability receipt", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"verified": true}); err != nil {
		t.Fatal(err)
	}
}

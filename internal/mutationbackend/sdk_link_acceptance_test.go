//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

type sdkLinkReport struct {
	Errno                                           uint32
	Destination, StageInode, TargetParent, Occupant uint64
	SourceUnchanged, ParentUnchanged, MapUnchanged  bool
	Nlink                                           uint32
	ParentCounts                                    map[string]string
}

// These are real pinned-SDK Link calls, not a proposed media receipt. A
// rejected link must not leave a public name or partially change its inode.
func TestSDKLinkFailureHasNoNamespaceEffects(t *testing.T) {
	for _, scenario := range []string{"wrong-type", "not-integer", "overflow", "second-increment-overflow", "nlink-overflow", "leading-zero", "plus-sign", "negative", "negative-zero", "cross-parent-source-bad", "cross-parent-target-bad", "concurrent-map", "valid", "valid-hash", "cross-parent", "already-linked", "occupied"} {
		t.Run(scenario, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "none", DirStats: true}
			setting := f.SeedExisting(t, &format)
			cfg := SDKConfig{Redis: RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting)}
			body, _ := json.Marshal(cfg)
			config := filepath.Join(f.Dir, "sdk.json")
			if err := os.WriteFile(config, body, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.Context, 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKLinkChild$")
			cmd.Env = append(os.Environ(), "JM_LINK_CONFIG="+config, "JM_LINK_SCENARIO="+scenario)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil || ctx.Err() != nil || stderr.Len() != 0 {
				t.Fatalf("SDK child failed: %v context=%v stderr-bytes=%d diagnostic=%s", err, ctx.Err(), stderr.Len(), stdout.String())
			}
			var report sdkLinkReport
			decoder := json.NewDecoder(&stdout)
			if err := decoder.Decode(&report); err != nil {
				t.Fatal("missing SDK report", err)
			}
			trailing, err := io.ReadAll(io.MultiReader(decoder.Buffered(), &stdout))
			if err != nil || string(bytes.TrimSpace(trailing)) != "PASS" {
				t.Fatalf("unexpected SDK child diagnostics: %q, error=%v", trailing, err)
			}
			switch scenario {
			case "valid", "valid-hash", "cross-parent", "already-linked":
				wantNlink, rootCount := uint32(2), "2"
				if scenario == "already-linked" {
					wantNlink, rootCount = 3, "3"
				}
				if scenario == "cross-parent" {
					rootCount = "1"
				}
				if report.Errno != 0 || report.Destination != report.StageInode || report.Nlink != wantNlink || report.ParentCounts["1"] != rootCount {
					t.Fatalf("valid hard link failed: %+v", report)
				}
				if scenario == "cross-parent" && (report.TargetParent <= 1 || report.ParentCounts[fmt.Sprint(report.TargetParent)] != "1") {
					t.Errorf("cross-parent accounting differs: %+v", report)
				}
				if scenario == "valid-hash" && report.ParentCounts["777"] != "3" {
					t.Errorf("unrelated parent counter changed: %+v", report)
				}
			case "occupied":
				if report.Errno != uint32(syscall.EEXIST) || report.Occupant == 0 || report.Destination != report.Occupant || report.Destination == report.StageInode || !report.SourceUnchanged || !report.ParentUnchanged || !report.MapUnchanged {
					t.Errorf("occupied destination was not preserved: %+v", report)
				}
			default:
				if report.Errno == 0 || report.Destination != 0 || !report.SourceUnchanged || !report.ParentUnchanged || !report.MapUnchanged {
					t.Errorf("rejected Link changed namespace: %+v", report)
				}
			}
			t.Logf("scenario=%s SDK child=%d reaped; errno=%d destination=%d source-unchanged=%t", scenario, cmd.Process.Pid, report.Errno, report.Destination, report.SourceUnchanged)
		})
	}
}

func TestSDKLinkChild(t *testing.T) {
	config := os.Getenv("JM_LINK_CONFIG")
	if config == "" {
		return
	}
	cfg, err := ReadSDKConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(filepath.Dir(config), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	scenario := os.Getenv("JM_LINK_SCENARIO")
	realSocket := cfg.Redis.Socket
	var held <-chan struct{}
	var release chan<- struct{}
	if scenario == "concurrent-map" {
		cfg.Redis.Socket = filepath.Join(filepath.Dir(config), "p.sock")
		held, release = linkExecRelay(t, cfg.Redis.Socket, realSocket)
	}
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := j.Claim(ctx, "volume", "link-session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("operation-owned stage bytes")
	spec := MediaStageRequest{Volume: "volume", Session: lease.Session, Epoch: lease.Epoch, ID: "link-stage", FileID: "file-incarnation", ExpectedRevision: "private-stage-only", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("owned custody fixture"))}
	stage, err := r.StageMedia(ctx, spec, bytes.NewReader(payload))
	if err != nil || !stage.Closed {
		t.Fatal("stage failed", err)
	}
	parentKey, inodeKey := fmt.Sprintf("p%d", stage.Inode), fmt.Sprintf("i%d", stage.Inode)
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	parent := meta.RootInode
	var occupant meta.Ino
	if strings.HasPrefix(scenario, "cross-parent") {
		var attr meta.Attr
		if eno := r.metadata.Mkdir(fctx, meta.RootInode, "target", 0700, 0077, 0, &parent, &attr); eno != 0 {
			t.Fatal("target mkdir", eno)
		}
	}
	switch scenario {
	case "wrong-type":
		err = r.reader.Set(ctx, parentKey, "invalid-parent-map", 0).Err()
	case "leading-zero":
		err = r.reader.HSet(ctx, parentKey, "1", "01").Err()
	case "plus-sign":
		err = r.reader.HSet(ctx, parentKey, "1", "+1").Err()
	case "negative":
		err = r.reader.HSet(ctx, parentKey, "1", "-1").Err()
	case "negative-zero":
		err = r.reader.HSet(ctx, parentKey, "1", "-0").Err()
	case "cross-parent-source-bad":
		err = r.reader.HSet(ctx, parentKey, "1", "bad-source").Err()
	case "cross-parent-target-bad":
		err = r.reader.HSet(ctx, parentKey, parent.String(), "bad-target").Err()
	case "valid-hash":
		err = r.reader.HSet(ctx, parentKey, "1", "0", "777", "3").Err()
	case "already-linked":
		var attr meta.Attr
		if eno := r.metadata.Link(fctx, meta.Ino(stage.Inode), meta.RootInode, "prior.mov", &attr); eno != 0 {
			t.Fatal("first link failed", eno)
		}
	case "occupied":
		otherSpec := spec
		otherSpec.ID = "other-stage"
		other := []byte("original destination bytes")
		otherSpec.Size, otherSpec.SHA256 = int64(len(other)), digest(other)
		staged, err := r.StageMedia(ctx, otherSpec, bytes.NewReader(other))
		if err != nil {
			t.Fatal("occupied stage", err)
		}
		occupant = meta.Ino(staged.Inode)
		var attr meta.Attr
		if eno := r.metadata.Link(fctx, occupant, parent, "published.mov", &attr); eno != 0 {
			t.Fatal("occupied link", eno)
		}
	case "not-integer":
		err = r.reader.HSet(ctx, parentKey, "1", "invalid-count").Err()
	case "overflow":
		err = r.reader.HSet(ctx, parentKey, "1", "9223372036854775807").Err()
	case "second-increment-overflow":
		err = r.reader.HSet(ctx, parentKey, "1", "9223372036854775806").Err()
	case "nlink-overflow":
		var raw []byte
		raw, err = r.reader.Get(ctx, inodeKey).Bytes()
		if err == nil {
			var attr meta.Attr
			attr.Unmarshal(raw)
			attr.Nlink = ^uint32(0)
			err = r.reader.Set(ctx, inodeKey, attr.Marshal(), 0).Err()
		}
	}
	if err != nil {
		t.Fatal("fault preparation failed", err)
	}
	get := func(key string) []byte {
		b, err := r.reader.Get(ctx, key).Bytes()
		if err != nil {
			t.Fatal("raw metadata read", err)
		}
		return b
	}
	mapSnapshot := func() string {
		typ, err := r.reader.Type(ctx, parentKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		if typ == "string" {
			return "string:" + string(get(parentKey))
		}
		counts, err := r.reader.HGetAll(ctx, parentKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(counts)
		return typ + ":" + string(b)
	}
	sourceBefore, parentBefore, mapBefore := get(inodeKey), get("i"+parent.String()), mapSnapshot()
	stageName := strings.TrimPrefix(stage.Intent.Path, "/")
	stageEntryBefore, err := r.reader.HGet(ctx, "d1", stageName).Bytes()
	if err != nil {
		t.Fatal("stage dentry snapshot", err)
	}
	var mutationDone chan struct{}
	var mutationResult error
	if scenario == "concurrent-map" {
		options := *r.reader.Options()
		options.Addr = realSocket
		// The SDK dialer captures its original socket; replace it for this
		// separate, explicitly owned mutator connection.
		options.Dialer = nil
		mutator := redis.NewClient(&options)
		mutationDone = make(chan struct{})
		go func() {
			defer close(mutationDone)
			defer mutator.Close()
			select {
			case <-held:
				cctx, cancel := context.WithTimeout(ctx, time.Second)
				err := mutator.Set(cctx, parentKey, "concurrent-parent-map", 0).Err()
				cancel()
				close(release)
				mutationResult = err
			case <-time.After(5 * time.Second):
				close(release)
				mutationResult = fmt.Errorf("Link never reached held EXEC")
			}
		}()
		// Link must preserve the concurrent writer's value, not revert it.
		mapBefore = "string:concurrent-parent-map"
	}
	var attr meta.Attr
	eno := r.metadata.Link(fctx, meta.Ino(stage.Inode), parent, "published.mov", &attr)
	if mutationDone != nil {
		<-mutationDone
		if mutationResult != nil {
			t.Fatal("concurrent mutation", mutationResult)
		}
	}
	var dest meta.Ino
	var destAttr meta.Attr
	if lookup := r.metadata.Lookup(fctx, parent, "published.mov", &dest, &destAttr, false); lookup == syscall.ENOENT {
		dest = 0
	} else if lookup != 0 {
		t.Fatal("destination lookup failed", lookup)
	}
	sourceAfter := get(inodeKey)
	attr.Unmarshal(sourceAfter)
	counts, err := r.reader.HGetAll(ctx, parentKey).Result()
	if err != nil && scenario != "wrong-type" && scenario != "concurrent-map" {
		t.Fatal(err)
	}
	// This diagnostic never authorizes a publication receipt or retirement.
	// Fresh file handles must still read the original complete stage bytes.
	readback := func(path string, want []byte) {
		file, eno := r.filesystem.Open(fctx, path, meta.MODE_MASK_R)
		if eno != 0 {
			t.Fatal("fresh open", eno)
		}
		got := make([]byte, len(want)+1)
		n, err := file.Pread(fctx, got, 0)
		if err != nil && err != io.EOF || n != len(want) || !bytes.Equal(got[:n], want) {
			t.Fatal("fresh handle readback differs", n, err)
		}
		if eno := file.Close(fctx); eno != 0 {
			t.Fatal("readback close", eno)
		}
	}
	readback(stage.Intent.Path, payload)
	if scenario == "occupied" {
		readback("/published.mov", []byte("original destination bytes"))
	}
	stageEntryAfter, err := r.reader.HGet(ctx, "d1", stageName).Bytes()
	if err != nil || !bytes.Equal(stageEntryBefore, stageEntryAfter) {
		t.Fatal("stage dentry changed", err)
	}
	var original meta.Ino
	var originalAttr meta.Attr
	if lookup := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(stage.Intent.Path, "/"), &original, &originalAttr, false); lookup != 0 || uint64(original) != stage.Inode {
		t.Fatalf("original stage link lost: errno=%v inode=%d expected=%d", lookup, original, stage.Inode)
	}
	op, err := j.Inspect(ctx, "volume", spec.ID)
	if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
		t.Fatal("diagnostic manufactured publication receipt", err)
	}
	rawDest, rawErr := r.reader.HGet(ctx, "d"+parent.String(), "published.mov").Bytes()
	if dest == 0 {
		if rawErr != redis.Nil {
			t.Fatal("destination lookup did not prove absence", rawErr)
		}
	} else if rawErr != nil || len(rawDest) != 9 || binary.BigEndian.Uint64(rawDest[1:]) != uint64(dest) {
		t.Fatal("raw destination differs from SDK lookup", rawErr)
	}
	report := sdkLinkReport{Errno: uint32(eno), Destination: uint64(dest), StageInode: stage.Inode, TargetParent: uint64(parent), Occupant: uint64(occupant), SourceUnchanged: bytes.Equal(sourceBefore, sourceAfter), ParentUnchanged: bytes.Equal(parentBefore, get("i"+parent.String())), MapUnchanged: mapBefore == mapSnapshot(), Nlink: attr.Nlink, ParentCounts: counts}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		t.Fatal(err)
	}
}

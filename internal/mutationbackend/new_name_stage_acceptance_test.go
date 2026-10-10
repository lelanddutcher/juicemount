//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func TestSDKNewNameStageIntentAndRecovery(t *testing.T) {
	for _, scenario := range []string{"nested", "root", "parent-before", "parent-after-intent", "occupied", "symlink-parent", "legacy-rebind", "changed-target"} {
		t.Run(scenario, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "none", DirStats: true}
			setting := f.SeedExisting(t, &format)
			cfg := SDKConfig{Redis: RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting)}
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
			run := func(mode string) {
				ctx, cancel := context.WithTimeout(f.Context, 25*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKNewNameStageChild$")
				cmd.Env = append(os.Environ(), "JM_NEW_NAME_CONFIG="+config, "JM_NEW_NAME_SCENARIO="+scenario, "JM_NEW_NAME_MODE="+mode)
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				if err := cmd.Run(); err != nil || ctx.Err() != nil || stderr.Len() != 0 {
					t.Fatalf("owned SDK child failed: %v ctx=%v stderr-bytes=%d diagnostic=%s", err, ctx.Err(), stderr.Len(), stdout.String())
				}
				if stdout.String() != "{\"verified\":true}\nPASS\n" {
					t.Fatalf("unexpected child output: %q", stdout.String())
				}
				t.Logf("%s %s SDK child=%d joined", scenario, mode, cmd.Process.Pid)
			}
			run("prepare")
			if scenario != "parent-before" && scenario != "occupied" && scenario != "symlink-parent" {
				run("recover")
			}
		})
	}
}

func TestSDKNewNameStageChild(t *testing.T) {
	config := os.Getenv("JM_NEW_NAME_CONFIG")
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
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	payload := []byte("immutable new-name stage contents")
	scenario := os.Getenv("JM_NEW_NAME_SCENARIO")
	verifyStage := func(s mutationauthority.MediaStage) {
		if !s.Closed || r.media.validateRetainedLayout(s) != nil {
			t.Fatal("closed private stage layout invalid")
		}
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(s.Intent.Path, "/"), &inode, &attr, false); eno != 0 || uint64(inode) != s.Inode || attr.Nlink != 1 || attr.Parent != meta.RootInode {
			t.Fatal("private root stage identity changed", eno)
		}
		file, eno := r.filesystem.Open(fctx, s.Intent.Path, meta.MODE_MASK_R)
		if eno != 0 {
			t.Fatal("fresh stage open", eno)
		}
		got := make([]byte, len(payload)+1)
		n, err := file.Pread(fctx, got, 0)
		if (err != nil && err != io.EOF) || n != len(payload) || !bytes.Equal(got[:n], payload) {
			t.Fatal("fresh stage bytes changed", n, err)
		}
		if eno := file.Close(fctx); eno != 0 {
			t.Fatal(eno)
		}
		op, err := j.Inspect(ctx, "volume", "new-name")
		if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
			t.Fatal("stage manufactured a media receipt", err)
		}
		var body mediaStageEnvelope
		if strictJSON(s.Intent.Body, &body) != nil {
			t.Fatal("invalid canonical stage body")
		}
		if scenario == "legacy-rebind" {
			if body.Version != "media-stage-v1" || body.Target != nil {
				t.Fatal("legacy stage promoted")
			}
		} else {
			if body.Version != "media-stage-v2-new-name" || body.Target == nil {
				t.Fatal("missing v2 target")
			}
			parent := meta.Ino(body.Target.Parents[len(body.Target.Parents)-1].Inode)
			if eno := r.metadata.Lookup(fctx, parent, path.Base(body.Target.Path), &inode, &attr, false); eno != syscall.ENOENT {
				t.Fatal("staging published target", eno)
			}
		}
	}
	if os.Getenv("JM_NEW_NAME_MODE") == "recover" {
		s, err := j.InspectStage(ctx, "volume", "new-name")
		if err != nil {
			t.Fatal(err)
		}
		verifyStage(s)
		if err := j.RequireRetainedStage(ctx, s.Request); err != nil {
			t.Fatal("fresh fence did not retain exact stage", err)
		}
		lease, err := j.Claim(ctx, "volume", "successor", time.Minute)
		if err != nil || lease.Epoch <= s.Request.Epoch {
			t.Fatal("private stage recovery blocked safe succession", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"verified": true}); err != nil {
			t.Fatal(err)
		}
		return
	}
	lease, err := j.Claim(ctx, "volume", "origin", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mkdir := func(parent meta.Ino, name string) meta.Ino {
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Mkdir(fctx, parent, name, 0700, 0077, 0, &inode, &attr); eno != 0 {
			t.Fatal("fixture mkdir", eno)
		}
		return inode
	}
	parent := mkdir(meta.RootInode, "parent")
	child := mkdir(parent, "child")
	target := NewNameStageTarget{Path: "/parent/child/file.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "parent", Inode: uint64(parent)}, {Name: "child", Inode: uint64(child)}}}
	if scenario == "root" {
		target = NewNameStageTarget{Path: "/file.mov", Parents: []DirectoryIdentity{{Inode: 1}}}
	}
	replaceParent := func() {
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Rename(fctx, meta.RootInode, "parent", meta.RootInode, "moved", meta.RenameNoReplace, &inode, &attr); eno != 0 {
			t.Fatal("fixture parent move", eno)
		}
		if scenario == "symlink-parent" {
			if eno := r.metadata.Symlink(fctx, meta.RootInode, "parent", "moved", &inode, &attr); eno != 0 {
				t.Fatal("fixture symlink", eno)
			}
		} else {
			replacement := mkdir(meta.RootInode, "parent")
			mkdir(replacement, "child")
		}
	}
	if scenario == "parent-before" || scenario == "symlink-parent" {
		replaceParent()
	}
	if scenario == "occupied" {
		file, eno := r.filesystem.Create(fctx, target.Path, 0600, 0077)
		if eno != 0 {
			t.Fatal("fixture occupant", eno)
		}
		if eno := file.Close(fctx); eno != 0 {
			t.Fatal(eno)
		}
	}
	spec := MediaStageRequest{Volume: "volume", Session: lease.Session, Epoch: lease.Epoch, ID: "new-name", FileID: "origin-file", ExpectedRevision: "origin-absent", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("custody"))}
	intentObserved := false
	r.media.hook = func(phase string) {
		if phase != "intent" {
			return
		}
		intentObserved = true
		s, err := j.InspectStage(ctx, "volume", spec.ID)
		if err != nil || s.Inode != 0 {
			t.Fatal("intent did not precede create", err)
		}
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(s.Intent.Path, "/"), &inode, &attr, false); eno != syscall.ENOENT {
			t.Fatal("stage existed before recorded intent", eno)
		}
		if scenario != "legacy-rebind" {
			var body mediaStageEnvelope
			if strictJSON(s.Intent.Body, &body) != nil || body.Target == nil || !reflect.DeepEqual(*body.Target, target) {
				t.Fatal("origin target not recorded before effects")
			}
		}
		if scenario == "parent-after-intent" {
			replaceParent()
		}
	}
	var staged mutationauthority.MediaStage
	if scenario == "legacy-rebind" {
		staged, err = r.StageMedia(ctx, spec, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		_, err = r.StageMediaForNewName(ctx, spec, target, bytes.NewReader(payload))
		if !errors.Is(err, mutationauthority.ErrChangedRequest) {
			t.Fatal("legacy stage adopted new target", err)
		}
	} else {
		staged, err = r.StageMediaForNewName(ctx, spec, target, bytes.NewReader(payload))
	}
	if scenario == "parent-before" || scenario == "occupied" || scenario == "symlink-parent" {
		if err == nil || intentObserved {
			t.Fatal("conflict reached stage effects")
		}
		if _, err := j.Inspect(ctx, "volume", spec.ID); !errors.Is(err, mutationauthority.ErrNoSuchOperation) {
			t.Fatal("refused stage left operation", err)
		}
		var inode meta.Ino
		var attr meta.Attr
		stageName := ".juicemount-stage-" + digest([]byte(spec.Volume+"\x00"+spec.ID))
		if eno := r.metadata.Lookup(fctx, meta.RootInode, stageName, &inode, &attr, false); eno != syscall.ENOENT {
			t.Fatal("refused stage created private inode", eno)
		}
	} else {
		if scenario != "legacy-rebind" && err != nil {
			t.Fatal(err)
		}
		if !intentObserved {
			t.Fatal("missing pre-effect intent observation")
		}
		verifyStage(staged)
		if scenario == "changed-target" {
			changed := target
			changed.Path = "/parent/child/other.mov"
			if _, err := r.StageMediaForNewName(ctx, spec, changed, bytes.NewReader(payload)); !errors.Is(err, mutationauthority.ErrChangedRequest) {
				t.Fatal("retargeted retry accepted", err)
			}
			after, err := j.InspectStage(ctx, "volume", spec.ID)
			if err != nil || mutationauthority.StageSnapshotDigest(after) != mutationauthority.StageSnapshotDigest(staged) {
				t.Fatal("retargeting changed original stage", err)
			}
		}
		if scenario == "parent-after-intent" && r.checkNewNameTarget(ctx, target) == nil {
			t.Fatal("replacement unexpectedly matches origin chain")
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"verified": true}); err != nil {
		t.Fatal(err)
	}
}

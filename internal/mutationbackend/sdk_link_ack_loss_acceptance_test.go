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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

type linkAckReady struct {
	Stage  mutationauthority.MediaStage
	Parent uint64
	Worker string
}

// A PASS preserves an unsafe counterexample: losing the successful EXEC reply
// leaves exact public/link state but misses the later SDK directory accounting.
// It does not qualify publication or authorize a media receipt.
func TestSDKLinkLostAcknowledgmentAccounting(t *testing.T) {
	for _, mode := range []string{"acknowledged", "lost"} {
		t.Run(mode, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "none", DirStats: true}
			setting := f.SeedExisting(t, &format)
			proxy := filepath.Join(f.Dir, "a.sock")
			held, drop, dropped := linkAckRelay(t, proxy, f.Socket, mode == "lost")
			cfg := SDKConfig{Redis: RedisConfig{Socket: proxy, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting)}
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(f.Dir, "sdk.json")
			if err := os.WriteFile(config, data, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.Context, 20*time.Second)
			defer cancel()
			readyRead, readyWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer readyRead.Close()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKLinkAckWriterChild$")
			cmd.Env = append(os.Environ(), "JM_ACK_CONFIG="+config, "JM_ACK_MODE="+mode)
			cmd.ExtraFiles = []*os.File{readyWrite}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer stdin.Close()
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				_ = readyWrite.Close()
				t.Fatal(err)
			}
			_ = readyWrite.Close()
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			reaped := false
			defer func() {
				if !reaped {
					_ = cmd.Process.Kill()
					exitErr := <-done
					t.Logf("joined writer after failed checkpoint: exit=%v stdout=%q stderr-bytes=%d", exitErr, stdout.String(), stderr.Len())
				}
			}()
			decoder := json.NewDecoder(readyRead)
			var ready linkAckReady
			if err := decoder.Decode(&ready); err != nil {
				t.Fatal("missing private stage checkpoint", err)
			}
			if mode == "lost" {
				select {
				case err := <-held:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("EXEC reply not intercepted")
				}
			} else {
				var ack bool
				if err := decoder.Decode(&ack); err != nil || !ack {
					t.Fatal("missing persisted accounting acknowledgment", err)
				}
			}
			// Independently confirm the committed state while the writer is still
			// alive; use raw Redis reads so a lazy SDK repair cannot hide the gap.
			verify := func() {
				admin := f.Admin()
				defer admin.Close()
				get := func(key string) []byte {
					b, err := admin.Get(f.Context, key).Bytes()
					if err != nil {
						t.Fatal("raw inode read", err)
					}
					return b
				}
				var attr meta.Attr
				attr.Unmarshal(get(fmt.Sprint("i", ready.Stage.Inode)))
				if attr.Nlink != 2 || attr.Parent != 0 || attr.Length != uint64(ready.Stage.Intent.Size) {
					t.Fatal("committed inode state differs")
				}
				for _, entry := range []struct {
					parent uint64
					name   string
				}{{1, strings.TrimPrefix(ready.Stage.Intent.Path, "/")}, {ready.Parent, "published.mov"}} {
					b, err := admin.HGet(f.Context, fmt.Sprint("d", entry.parent), entry.name).Bytes()
					if err != nil || len(b) != 9 || b[0] != meta.TypeFile || binary.BigEndian.Uint64(b[1:]) != ready.Stage.Inode {
						t.Fatal("exact committed dentry differs", err)
					}
				}
				counts, err := admin.HGetAll(f.Context, fmt.Sprint("p", ready.Stage.Inode)).Result()
				if err != nil || !reflect.DeepEqual(counts, map[string]string{"1": "1", fmt.Sprint(ready.Parent): "1"}) {
					t.Fatal("parent link counts differ", err)
				}
				want := []string{"0", "0", "0"}
				if mode == "acknowledged" {
					want = []string{fmt.Sprint(ready.Stage.Intent.Size), "4096", "1"}
				}
				for i, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
					value, err := admin.HGet(f.Context, key, fmt.Sprint(ready.Parent)).Result()
					if err != nil || value != want[i] {
						t.Fatalf("raw %s differs: got=%q want=%q error=%v", key, value, want[i], err)
					}
				}
			}
			verify()
			objects := linkAckObjectSnapshot(t, filepath.Join(f.Dir, "objects"), ready.Stage)
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal("kill owned SDK", err)
			}
			waitErr := <-done
			reaped = true
			if waitErr == nil || ctx.Err() != nil || stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("writer lifecycle differs: exit=%v context=%v stdout=%q stderr-bytes=%d", waitErr, ctx.Err(), stdout.String(), stderr.Len())
			}
			if mode == "lost" {
				drop()
				select {
				case <-dropped:
				case <-ctx.Done():
					t.Fatal("held response connection not joined")
				}
			}
			verify()
			// The same config/journal in a fresh process must fence old writers,
			// then refuse this no-longer-private stage without inventing success.
			recoverCmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKLinkAckRecoveryChild$")
			recoverCmd.Env = append(os.Environ(), "JM_ACK_CONFIG="+config)
			var out, errout bytes.Buffer
			recoverCmd.Stdout, recoverCmd.Stderr = &out, &errout
			if err := recoverCmd.Run(); err != nil || ctx.Err() != nil || errout.Len() != 0 || out.String() != "{\"blocked\":true}\nPASS\n" {
				t.Fatalf("recovery child failed: %v context=%v stdout=%q stderr-bytes=%d", err, ctx.Err(), out.String(), errout.Len())
			}
			verify()
			admin := f.Admin()
			oldUser, err := admin.Do(f.Context, "ACL", "GETUSER", ready.Worker).Result()
			_ = admin.Close()
			if !errors.Is(err, redis.Nil) || oldUser != nil {
				t.Fatal("old worker identity survived fence", err)
			}
			if !reflect.DeepEqual(objects, linkAckObjectSnapshot(t, filepath.Join(f.Dir, "objects"), ready.Stage)) {
				t.Fatal("provider objects changed across fence")
			}
			t.Logf("%s: writer=%d killed/reaped after observed commit; recovery=%d reaped; namespace and parent counts exact, raw target accounting %v", mode, cmd.Process.Pid, recoverCmd.Process.Pid, map[string]string{"acknowledged": "complete", "lost": "stale zero"}[mode])
		})
	}
}

func linkAckObjectSnapshot(t *testing.T, root string, stage mutationauthority.MediaStage) map[string]string {
	t.Helper()
	out := map[string]string{}
	// Startup adds fresh retained admission probes. Compare only the exact
	// recorded stage objects, and independently check their manifest hashes.
	for _, object := range stage.Objects {
		p := filepath.Join(object.Bucket, object.Key)
		if object.Provider != "file" || !object.Verified || filepath.Clean(object.Bucket) != root || !strings.HasPrefix(p, root+string(os.PathSeparator)) {
			t.Fatal("unexpected stage object identity")
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal("recorded stage object missing", err)
		}
		if int64(len(b)) != object.Size || digest(b) != object.SHA256 {
			t.Fatal("recorded stage object changed")
		}
		out[object.Key] = digest(b)
	}
	if len(out) == 0 {
		t.Fatal("empty stage object manifest")
	}
	return out
}

func TestSDKLinkAckWriterChild(t *testing.T) {
	config := os.Getenv("JM_ACK_CONFIG")
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
	var parent meta.Ino
	var attr meta.Attr
	if eno := r.metadata.Mkdir(fctx, meta.RootInode, "target", 0700, 0077, 0, &parent, &attr); eno != 0 {
		t.Fatal(eno)
	}
	lease, err := j.Claim(ctx, "volume", "writer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("unchanged bytes across lost link acknowledgment")
	spec := MediaStageRequest{Volume: "volume", Session: lease.Session, Epoch: lease.Epoch, ID: "lost-ack", FileID: "original-file", ExpectedRevision: "origin-absent", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("custody"))}
	target := NewNameStageTarget{Path: "/target/published.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "target", Inode: uint64(parent)}}}
	stage, err := r.StageMediaForNewName(ctx, spec, target, bytes.NewReader(payload))
	if err != nil || !stage.Closed {
		t.Fatal("stage failed", err)
	}
	for _, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
		value, err := r.reader.HGet(ctx, key, parent.String()).Result()
		if err != nil || value != "0" {
			t.Fatal("empty target accounting not initialized", key, err)
		}
	}
	processes, err := j.BackendProcesses(ctx, "volume")
	if err != nil || len(processes) != 1 {
		t.Fatal("missing writer identity", err)
	}
	file, eno := r.filesystem.Open(fctx, stage.Intent.Path, meta.MODE_MASK_R)
	if eno != 0 {
		t.Fatal(eno)
	}
	got := make([]byte, len(payload)+1)
	n, readErr := file.Pread(fctx, got, 0)
	closeErr := file.Close(fctx)
	if (readErr != nil && readErr != io.EOF) || n != len(payload) || !bytes.Equal(got[:n], payload) || closeErr != 0 {
		t.Fatal("private payload readback failed")
	}
	report := os.NewFile(3, "owned-checkpoint")
	defer report.Close()
	encoder := json.NewEncoder(report)
	if err := encoder.Encode(linkAckReady{stage, uint64(parent), processes[0].Username}); err != nil {
		t.Fatal(err)
	}
	if eno := r.metadata.Link(fctx, meta.Ino(stage.Inode), parent, "published.mov", &attr); eno != 0 {
		t.Fatal("Link returned before writer kill", eno)
	}
	if os.Getenv("JM_ACK_MODE") == "lost" {
		t.Fatal("withheld Link unexpectedly returned")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		complete := true
		for i, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
			value, err := r.reader.HGet(ctx, key, parent.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			complete = complete && value == []string{fmt.Sprint(len(payload)), "4096", "1"}[i]
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("acknowledged accounting did not persist")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := encoder.Encode(true); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestSDKLinkAckRecoveryChild(t *testing.T) {
	config := os.Getenv("JM_ACK_CONFIG")
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
	before, err := j.InspectStage(ctx, "volume", "lost-ack")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSDK(ctx, j, "volume", *cfg); err == nil || !strings.Contains(err.Error(), "stage recovery") {
		t.Fatal("changed stage not refused at post-fence recovery", err)
	}
	processes, err := j.BackendProcesses(ctx, "volume")
	if err != nil || len(processes) != 2 || processes[1].Generation <= processes[0].Generation || processes[1].RedisRunID != processes[0].RedisRunID {
		t.Fatal("successor generation missing", err)
	}
	after, err := j.InspectStage(ctx, "volume", "lost-ack")
	if err != nil || mutationauthority.StageSnapshotDigest(before) != mutationauthority.StageSnapshotDigest(after) {
		t.Fatal("stage journal changed", err)
	}
	op, err := j.Inspect(ctx, "volume", "lost-ack")
	if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
		t.Fatal("unproven publication settled", err)
	}
	if _, err := j.Claim(ctx, "volume", "successor", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
		t.Fatal("uncertain publication admitted successor", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"blocked": true}); err != nil {
		t.Fatal(err)
	}
}

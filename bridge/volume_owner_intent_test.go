package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func TestVolumeOwnerIntentDurableSelectionAndCreatorPipe(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	cfg := ServerConfig{RedisURL: "redis://selected:6379", FUSEPath: filepath.Join(scope.Root, "fuse")}
	i, created, err := createOwnerIntent(scope, cfg)
	if err != nil || !created {
		t.Fatalf("first intent: %v", err)
	}
	got, created, err := createOwnerIntent(scope, cfg)
	if err != nil || created || got != i {
		t.Fatalf("original intent not reused: %v", err)
	}
	changed := cfg
	changed.RedisURL = "redis://foreign:6379"
	if _, _, err := createOwnerIntent(scope, changed); err == nil {
		t.Fatal("foreign config rebound durable intent")
	}
	if _, bound, err := authorizeOriginalOwner(scope, "", nil); err == nil || bound {
		t.Fatal("retained file alone authorized a fresh child")
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := write.WriteString(i.Instance); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	if selected, bound, err := authorizeOriginalOwner(scope, "3", read); err != nil || !bound || selected != i {
		t.Fatalf("original creator descriptor did not bind selected child: %v", err)
	}
}

func TestVolumeOwnerIntentRejectsSymlinkTrailingAndInvalidBootstrap(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	cfg := ServerConfig{RedisURL: "redis://selected:6379"}
	i, _, err := createOwnerIntent(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := ownerIntentPath(scope.Root)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{}")
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readOwnerIntent(scope); err == nil {
		t.Fatal("trailing intent data accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(scope.Root, "unrelated")
	if err := os.WriteFile(outside, []byte("unrelated"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := readOwnerIntent(scope); err == nil {
		t.Fatal("symlink intent followed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := createOwnerIntent(scope, cfg); err != nil {
		t.Fatal(err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = write.WriteString(strings.Repeat("0", len(i.Instance)))
	_ = write.Close()
	if _, bound, err := authorizeOriginalOwner(scope, "3", read); err == nil || bound {
		t.Fatal("wrong inherited nonce adopted retained intent")
	}
}

func TestVolumeOwnerIntentIDsFenceFirstMutation(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Disabled admission controls may create a worker. Join its original
	// handle even when the intended assertion exits this test early.
	defer func() {
		o.mu.Lock()
		var originals []*volumeOwnerOperation
		for _, op := range o.ops {
			originals = append(originals, op)
		}
		o.mu.Unlock()
		for _, op := range originals {
			select {
			case <-op.done:
			case <-time.After(time.Second):
				t.Error("original intent operation not joined")
			}
		}
	}()
	o.intentBound = true
	o.intent = volumeOwnerIntent{ClaimID: "11111111111111111111111111111111", MountID: "22222222222222222222222222222222",
		StartupProbeID: "33333333333333333333333333333333"}
	o.config = "selected"
	request := func(method string, ints []int) servicecontrol.Request {
		strings := []string{o.instance, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", o.config}
		if method == "claim" {
			strings[2] = `{"redis_url":"redis://selected"}`
		}
		return servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: method,
			Strings: strings, Ints: ints}
	}
	for _, req := range []servicecontrol.Request{request("claim", []int{0, 0}), request("mount", nil), request("probe-startup", nil)} {
		if resp := o.dispatch(req); resp.Error == "" || len(o.ops) != 0 {
			t.Fatalf("wrong first %s operation ID admitted", req.Method)
		}
	}
}

func TestVolumeOwnerCommandIsIndependentProcessGroupAndPrivateFile(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	cmd, logFile, err := selectedOwnerCommand(scope, "/bin/true", read)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid || len(cmd.ExtraFiles) != 1 ||
		cmd.Stdout != logFile || cmd.Stderr != logFile {
		t.Fatal("owner child would die with serving job group or inherited output pipe")
	}
}

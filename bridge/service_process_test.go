//go:build !serviceclient

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type servingFixture struct{ Addr, Export string }

// A real separate serving process owns the production NFS handler/listener and
// local SQLite store. No kernel mount, Redis, FUSE, launchd or GUI is touched.
func TestServingProcessFixture(t *testing.T) {
	role := os.Getenv("JM_SERVICE_FIXTURE_ROLE")
	if role == "" {
		return
	}
	dir := os.Getenv("JM_SERVICE_FIXTURE_DIR")
	path := filepath.Join(dir, "s")
	if role == "controller" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		result, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: 1, Build: version.Commit, Method: "set-offline", Ints: []int{1}})
		if err != nil || result.Error != "" {
			t.Fatalf("controller: %+v %v", result, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "controller-ready"), []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, os.Stdin) // Parent owns the pipe; killed before EOF.
		return
	}
	if role != "engine" {
		t.Fatal("invalid fixture role")
	}
	store, err := metadata.Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	srv := jmnfs.NewServer(jmnfs.Config{ListenAddr: "127.0.0.1:0", FUSEPath: filepath.Join(dir, "unused-fuse")}, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		srv.Stop()
		if !srv.Handler().StopHandler() {
			t.Error("handler did not stop")
		}
	}()
	globalServer, globalStore = srv, store
	control, err := servicecontrol.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- control.Serve(func(req servicecontrol.Request) servicecontrol.Response {
			if req.Method != "set-offline" {
				return dispatchServingRequest(req)
			}
			if err := os.WriteFile(filepath.Join(dir, "operation-entered"), []byte("entered"), 0600); err != nil {
				return servicecontrol.Response{Error: err.Error()}
			}
			deadline := time.Now().Add(8 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(dir, "release-operation")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					return servicecontrol.Response{Error: "fixture release timed out"}
				}
				time.Sleep(time.Millisecond)
			}
			result := dispatchServingRequest(req)
			if err := os.WriteFile(filepath.Join(dir, "operation-completed"), []byte("completed"), 0600); err != nil {
				return servicecontrol.Response{Error: err.Error()}
			}
			return result
		})
	}()
	ready, _ := json.Marshal(servingFixture{srv.Addr(), srv.ExportPath()})
	if err := os.WriteFile(filepath.Join(dir, "engine-ready"), ready, 0600); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	_ = control.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("control listener did not stop")
	}
}

func TestServingProcessSurvivesControllerDeathWithSameNFSHandle(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "sp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	start := func(role string) (*exec.Cmd, io.WriteCloser, <-chan struct{}) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestServingProcessFixture$", "-test.v")
		cmd.Env = append(os.Environ(), "JM_SERVICE_FIXTURE_ROLE="+role, "JM_SERVICE_FIXTURE_DIR="+dir)
		log, err := os.Create(filepath.Join(dir, role+".log"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { log.Close() })
		cmd.Stdout, cmd.Stderr = log, log
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var waitErr error
		go func() { waitErr = cmd.Wait(); close(done) }()
		t.Cleanup(func() {
			stdin.Close()
			select {
			case <-done:
				if role == "engine" && waitErr != nil {
					data, _ := os.ReadFile(filepath.Join(dir, role+".log"))
					t.Errorf("engine exit: %v %s", waitErr, data)
				}
			case <-time.After(4 * time.Second):
				cmd.Process.Kill()
				<-done
				t.Errorf("%s fixture required kill", role)
			}
		})
		return cmd, stdin, done
	}
	_, enginePipe, _ := start("engine")
	waitFile := func(name string) []byte {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if data, err := os.ReadFile(filepath.Join(dir, name)); err == nil && len(data) != 0 {
				return data
			}
			time.Sleep(10 * time.Millisecond)
		}
		logs, _ := os.ReadFile(filepath.Join(dir, "engine.log"))
		t.Fatalf("fixture %s not ready: %s", name, logs)
		return nil
	}
	var info servingFixture
	if err := json.Unmarshal(waitFile("engine-ready"), &info); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", info.Addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	xid := uint32(0)
	call := func(program, procedure uint32, args any) []byte {
		t.Helper()
		xid++
		var payload bytes.Buffer
		for _, word := range []uint32{xid, 0} {
			_ = binary.Write(&payload, binary.BigEndian, word)
		}
		if err := xdr.Write(&payload, rpc.Header{Rpcvers: 2, Prog: program, Vers: 3, Proc: procedure, Cred: rpc.AuthNull, Verf: rpc.AuthNull}); err != nil {
			t.Fatal(err)
		}
		if err := xdr.Write(&payload, args); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		frame := make([]byte, 4+payload.Len())
		binary.BigEndian.PutUint32(frame, uint32(payload.Len())|0x80000000)
		copy(frame[4:], payload.Bytes())
		if _, err := conn.Write(frame); err != nil {
			t.Fatal(err)
		}
		var header [4]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			t.Fatal(err)
		}
		n := binary.BigEndian.Uint32(header[:])
		if n>>31 != 1 || n&0x7fffffff > 1<<20 {
			t.Fatal("invalid RPC frame")
		}
		body := make([]byte, n&0x7fffffff)
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatal(err)
		}
		if len(body) < 28 || binary.BigEndian.Uint32(body[:4]) != xid || binary.BigEndian.Uint32(body[4:8]) != 1 || binary.BigEndian.Uint32(body[8:12]) != 0 || binary.BigEndian.Uint32(body[12:16]) != 0 || binary.BigEndian.Uint32(body[16:20]) != 0 || binary.BigEndian.Uint32(body[20:24]) != 0 || binary.BigEndian.Uint32(body[24:28]) != 0 {
			t.Fatalf("RPC/NFS failure %x", body)
		}
		return body[28:]
	}
	mounted := call(100005, 1, []byte(info.Export))
	if len(mounted) < 4 {
		t.Fatal("missing root handle")
	}
	size := int(binary.BigEndian.Uint32(mounted[:4]))
	if size != 28 || len(mounted) < 4+size {
		t.Fatal("unexpected export handle")
	}
	handle := append([]byte(nil), mounted[4:4+size]...)
	getattr := func() {
		t.Helper()
		body := call(100003, 1, handle)
		if len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 2 {
			t.Fatal("root is not a directory")
		}
	}
	getattr()
	controller, _, controllerDone := start("controller")
	_ = waitFile("operation-entered")
	if _, err := os.Stat(filepath.Join(dir, "controller-ready")); !os.IsNotExist(err) {
		t.Fatal("controller already received reply before kill")
	}
	getattr() // NFS remains responsive while an admitted control operation is held.
	if err := controller.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-controllerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("controller kill not reaped")
	}
	if err := os.WriteFile(filepath.Join(dir, "release-operation"), []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	_ = waitFile("operation-completed")
	// The same TCP connection and the exact pre-crash handle must still work.
	getattr()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := servicecontrol.Call(ctx, filepath.Join(dir, "s"), servicecontrol.Request{Version: 1, Build: version.Commit, Method: "is-offline"})
	if err != nil || got.Error != "" || got.Int != 1 {
		t.Fatalf("new controller lost service state: %+v %v", got, err)
	}
	conn.Close()
	enginePipe.Close()
	t.Log(fmt.Sprintf("controller killed; same NFS socket and 28-byte handle remained valid at %s", info.Addr))
}

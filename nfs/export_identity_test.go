package nfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func TestExportIdentityRejectsOtherServerBeforeStoreAccess(t *testing.T) {
	store, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.InsertToCache(metadata.MakeEntry("file", false, 3, time.Now(), 42))
	a := NewServer(Config{}, store)
	b := NewServer(Config{}, store)
	defer a.Handler().StopHandler()
	defer b.Handler().StopHandler()
	if a.ExportPath() == b.ExportPath() || len(a.ExportPath()) != 36 {
		t.Fatal("fresh servers must have different canonical exports")
	}
	root, file := a.Handler().ToHandle(nil, nil), a.Handler().ToHandle(nil, []string{"file"})
	for _, handle := range [][]byte{root, file} {
		if _, _, err := a.Handler().FromHandle(handle); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := b.Handler().FromHandle(b.Handler().ToHandle(nil, []string{"file"})); err != nil {
		t.Fatal(err)
	}
	// With no store at all, any accidental lookup/recovery would panic. Both
	// servers were constructed over the same populated store/inode above.
	b.handler.store = nil
	legacy := make([]byte, 8)
	binary.BigEndian.PutUint64(legacy, 42)
	wrongVersion := append([]byte(nil), b.Handler().ToHandle(nil, nil)...)
	binary.BigEndian.PutUint32(wrongVersion, 2)
	for name, handle := range map[string][]byte{
		"other-root": root, "other-file": file, "legacy": legacy,
		"nil": nil, "short": root[:4], "truncated": root[:len(root)-1],
		"extended": append(append([]byte(nil), root...), 0), "version": wrongVersion,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := b.Handler().FromHandle(handle)
			var status *nfslib.NFSStatusError
			if !errors.As(err, &status) || status.NFSStatus != nfslib.NFSStatusStale {
				t.Fatalf("got %v, want STALE", err)
			}
		})
	}
}

func TestExportIdentitySurvivesSameServerRestart(t *testing.T) {
	store, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	initServerTestSpoolSchema(t, store)
	srv := NewServer(Config{ListenAddr: "127.0.0.1:0"}, store)
	defer srv.Handler().StopHandler()
	defer srv.Stop()
	export, root := srv.ExportPath(), srv.Handler().ToHandle(nil, nil)
	for i := 0; i < 2; i++ {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		if srv.ExportPath() != export || !bytes.Equal(root, srv.Handler().ToHandle(nil, nil)) {
			t.Fatal("same Server changed identity")
		}
		if _, _, err := srv.Handler().FromHandle(root); err != nil {
			t.Fatal(err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

type exportFailedEntropy struct{}

func (exportFailedEntropy) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestExportIdentityInitializationFailsClosed(t *testing.T) {
	for name, identity := range map[string]exportIdentity{
		"uninitialized": {}, "entropy-error": newExportIdentity(exportFailedEntropy{}),
		"short-entropy": newExportIdentity(strings.NewReader("short")),
		"zero-entropy":  newExportIdentity(bytes.NewReader(make([]byte, 16))),
	} {
		t.Run(name, func(t *testing.T) {
			h := &JuiceMountHandler{export: identity}
			srv := &Server{handler: h, config: Config{ListenAddr: "127.0.0.1:0"}}
			if err := srv.Start(); err == nil {
				t.Fatal("invalid identity published listener")
			}
			if srv.IsRunning() || srv.Addr() != "" || srv.ExportPath() != "" {
				t.Fatal("invalid server published identity")
			}
			if h.ToHandle(nil, nil) != nil {
				t.Fatal("invalid identity published root handle")
			}
			status, fs, _ := h.Mount(nil, nil, nfslib.MountRequest{Dirpath: []byte("/")})
			if status == nfslib.MountStatusOk || fs != nil {
				t.Fatal("invalid identity accepted MOUNT")
			}
		})
	}
}

// Exercise the actual encoded RPC boundary, including the denied-MOUNT union.
// There is deliberately no backing Store: invalid handles must fail before it.
func TestExportIdentityEncodedRPC(t *testing.T) {
	h := NewHandler(nil, "")
	defer h.StopHandler()
	other := NewHandler(nil, "")
	defer other.StopHandler()
	server := nfslib.NewManagedServer(h)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = listener.Close()
		if !server.Quiesce(time.Second) {
			t.Error("RPC server did not quiesce")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Serve did not return")
		}
	})
	for name, path := range map[string]string{
		"exact": h.ExportPath(), "root": "/", "stale": other.ExportPath(),
		"empty": "", "trailing-slash": h.ExportPath() + "/", "dot-alias": "/./" + h.ExportPath()[1:],
		"upper": strings.ToUpper(h.ExportPath()), "nul": h.ExportPath() + "\x00",
	} {
		t.Run(name, func(t *testing.T) {
			body := exportRPC(t, conn, 100005, 1, []byte(path))
			status := binary.BigEndian.Uint32(body)
			if name == "exact" {
				if status != 0 {
					t.Fatalf("exact MOUNT denied: %d", status)
				}
				handle, err := xdr.ReadOpaque(bytes.NewReader(body[4:]))
				if err != nil || !bytes.Equal(handle, h.ToHandle(nil, nil)) {
					t.Fatalf("root handle mismatch: %x %v", handle, err)
				}
			} else if status != uint32(nfslib.MountStatusErrAcces) || len(body) != 4 {
				t.Fatalf("denied MOUNT must emit status only: %x", body)
			}
		})
	}
	legacy := make([]byte, 8)
	binary.BigEndian.PutUint64(legacy, 1)
	for name, handle := range map[string][]byte{"other-root": other.ToHandle(nil, nil), "other-inode": other.export.handle(42), "legacy-root": legacy} {
		t.Run(name, func(t *testing.T) {
			body := exportRPC(t, conn, 100003, uint32(nfslib.NFSProcedureGetAttr), handle)
			if len(body) != 4 || binary.BigEndian.Uint32(body) != uint32(nfslib.NFSStatusStale) {
				t.Fatalf("wrong export GETATTR did not return STALE: %x", body)
			}
		})
	}
}

func exportRPC(t *testing.T, conn net.Conn, program, proc uint32, opaque []byte) []byte {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var request bytes.Buffer
	// xid, CALL, RPC v2, program, v3, procedure, AUTH_NULL credential/verifier.
	for _, word := range []uint32{1, 0, 2, program, 3, proc, 0, 0, 0, 0} {
		if err := binary.Write(&request, binary.BigEndian, word); err != nil {
			t.Fatal(err)
		}
	}
	if err := xdr.Write(&request, opaque); err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4+request.Len())
	binary.BigEndian.PutUint32(frame, uint32(request.Len())|0x80000000)
	copy(frame[4:], request.Bytes())
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	var reply []byte
	for {
		var marker uint32
		if err := binary.Read(conn, binary.BigEndian, &marker); err != nil {
			t.Fatal(err)
		}
		size := marker & 0x7fffffff
		if size > 1<<20 {
			t.Fatal("unbounded RPC reply")
		}
		fragment := make([]byte, size)
		if _, err := io.ReadFull(conn, fragment); err != nil {
			t.Fatal(err)
		}
		reply = append(reply, fragment...)
		if marker&0x80000000 != 0 {
			break
		}
	}
	if len(reply) < 28 || binary.BigEndian.Uint32(reply) != 1 || binary.BigEndian.Uint32(reply[4:]) != 1 || !bytes.Equal(reply[8:24], make([]byte, 16)) {
		t.Fatalf("unexpected RPC reply header: %x", reply)
	}
	return reply[24:]
}

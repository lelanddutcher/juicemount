package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type bindingWriteArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
	How    uint32
	Data   []byte
}

func bindingRPCAddress(t *testing.T, handler nfslib.Handler) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := nfslib.NewManagedServer(handler)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() {
		_ = l.Close()
		if !s.Quiesce(time.Second) {
			t.Error("RPC server did not quiesce")
		}
	})
	return l.Addr().String()
}

func bindingRPCStatus(address string, procedure nfslib.NFSProcedure, args any) (uint32, error) {
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	var call bytes.Buffer
	// ONC RPC v2, NFS v3, AUTH_NONE credential and verifier.
	if err := binary.Write(&call, binary.BigEndian, []uint32{7, 0, 2, 100003, 3, uint32(procedure), 0, 0, 0, 0}); err != nil {
		return 0, err
	}
	if err := xdr.Write(&call, args); err != nil {
		return 0, err
	}
	var record bytes.Buffer
	_ = binary.Write(&record, binary.BigEndian, uint32(call.Len())|0x80000000)
	record.Write(call.Bytes())
	if _, err := io.Copy(c, &record); err != nil {
		return 0, err
	}
	var marker uint32
	if err := binary.Read(c, binary.BigEndian, &marker); err != nil {
		return 0, err
	}
	if marker&0x80000000 == 0 || marker&0x7fffffff > 1<<20 {
		return 0, fmt.Errorf("unexpected RPC record %#x", marker)
	}
	reply := make([]byte, marker&0x7fffffff)
	if _, err := io.ReadFull(c, reply); err != nil {
		return 0, err
	}
	if len(reply) < 28 || binary.BigEndian.Uint32(reply[4:]) != 1 || binary.BigEndian.Uint32(reply[8:]) != 0 || binary.BigEndian.Uint32(reply[16:]) != 0 || binary.BigEndian.Uint32(reply[20:]) != 0 {
		return 0, fmt.Errorf("unexpected RPC envelope %x", reply)
	}
	return binary.BigEndian.Uint32(reply[24:]), nil
}

func TestWriteBindingRPCActiveMutationRetriesAndPoisonFails(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	seedFile(t, jfs, store, root, "take.mov", patternedPayload(8192, 23))
	handle := jfs.handler.ToHandle(jfs, []string{"take.mov"})
	address := bindingRPCAddress(t, jfs.handler)
	paused, resume := make(chan struct{}), make(chan struct{})
	jfs.handler.testAfterInPlaceMutation = func(_, op string) {
		if op == "write" {
			close(paused)
			<-resume
		}
	}
	type result struct {
		status uint32
		err    error
	}
	done := make(chan result, 1)
	go func() {
		s, err := bindingRPCStatus(address, nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Count: 4, Data: []byte("data")})
		done <- result{s, err}
	}()
	select {
	case <-paused:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("first write never entered mutation")
	}
	readArgs := struct {
		Handle []byte
		Offset uint64
		Count  uint32
	}{handle, 0, 4}
	attrArgs := struct{ Handle []byte }{handle}
	for _, tc := range []struct {
		name string
		proc nfslib.NFSProcedure
		args any
	}{
		{"write", nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Count: 4, Data: []byte("next")}},
		{"read", nfslib.NFSProcedureRead, readArgs},
		{"getattr", nfslib.NFSProcedureGetAttr, attrArgs},
	} {
		status, err := bindingRPCStatus(address, tc.proc, tc.args)
		if err != nil || status != uint32(nfslib.NFSStatusJukebox) {
			t.Errorf("active %s status=%d err=%v, want JUKEBOX", tc.name, status, err)
		}
	}
	close(resume)
	first := <-done
	if first.err != nil || first.status != uint32(nfslib.NFSStatusOk) {
		t.Fatalf("first write status=%d err=%v", first.status, first.err)
	}
	jfs.handler.testAfterInPlaceMutation = nil
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_rpc_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'RPC publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		proc nfslib.NFSProcedure
		args any
	}{
		{"faulted-write", nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Count: 4, Data: []byte("fail")}},
		{"poisoned-write", nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Count: 4, Data: []byte("more")}},
		{"read", nfslib.NFSProcedureRead, readArgs},
		{"getattr", nfslib.NFSProcedureGetAttr, attrArgs},
		{"commit", nfslib.NFSProcedureCommit, readArgs},
	} {
		status, err := bindingRPCStatus(address, tc.proc, tc.args)
		if err != nil || status != uint32(nfslib.NFSStatusIO) {
			t.Errorf("poisoned %s status=%d err=%v, want IO", tc.name, status, err)
		}
	}
}

type closeAdmissionHandler struct{ *JuiceMountHandler }

func (h closeAdmissionHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	fs, names, err := h.JuiceMountHandler.FromHandle(handle)
	return closeAdmissionFS{Filesystem: fs, handler: h.JuiceMountHandler}, names, err
}

type closeAdmissionFS struct {
	billy.Filesystem
	handler *JuiceMountHandler
}

func (fs closeAdmissionFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return closeAdmissionFile{File: f, handler: fs.handler, name: name}, nil
}

type closeAdmissionFile struct {
	billy.File
	handler *JuiceMountHandler
	name    string
}

func (f closeAdmissionFile) WriteAt(p []byte, off int64) (int, error) {
	return f.File.(io.WriterAt).WriteAt(p, off)
}
func (f closeAdmissionFile) Close() error {
	op := f.handler.namespace.begin([]string{f.name})
	if op == nil {
		return fmt.Errorf("test could not reserve close collision")
	}
	defer op.finish(nil)
	return f.File.Close()
}

func TestWriteBindingRPCCloseAdmissionCollisionRetries(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "take.mov", []byte("original"))
	handle := jfs.handler.ToHandle(jfs, []string{"take.mov"})
	address := bindingRPCAddress(t, closeAdmissionHandler{jfs.handler})
	status, err := bindingRPCStatus(address, nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Count: 4, Data: []byte("data")})
	if err != nil || status != uint32(nfslib.NFSStatusJukebox) {
		t.Fatalf("Close collision status=%d err=%v, want JUKEBOX", status, err)
	}
}

func TestWriteBindingTruncateWithoutWriteMode(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "take.mov", []byte("original"))
	f, err := jfs.OpenFile("take.mov", os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if len(onDisk(t, root, "take.mov")) != 0 || store.LookupByPath("take.mov").Size != 0 {
		t.Fatal("O_TRUNC without write mode bypassed exact publication")
	}
}

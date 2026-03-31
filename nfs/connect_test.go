package nfs

import (
	"net"
	"testing"
	"time"
)

func TestNFSTCPConnect(t *testing.T) {
	srv, _ := setupTestServer(t)
	addr := srv.Addr()

	// Verify we can TCP connect to the server
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("TCP connect: %v", err)
	}
	defer conn.Close()

	// Check that TCP_NODELAY is set
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// We can't directly check TCP_NODELAY on the client side,
		// but verify the connection is alive
		_ = tcpConn.SetDeadline(time.Now().Add(1 * time.Second))
	}

	t.Logf("TCP connection to NFS server at %s successful", addr)
}

func TestNFSHandlerPathResolution(t *testing.T) {
	_, store := setupTestServer(t)

	handler := NewHandler(store, testFUSEPath)

	// Test root handle
	rootHandle := handler.ToHandle(&juiceFS{handler: handler}, []string{})
	if len(rootHandle) != 8 {
		t.Fatalf("root handle length = %d, want 8", len(rootHandle))
	}

	// Root should resolve back
	_, path, err := handler.FromHandle(rootHandle)
	if err != nil {
		t.Fatalf("FromHandle(root): %v", err)
	}
	if len(path) != 0 {
		t.Fatalf("root path = %v, want empty", path)
	}

	// Test a known file handle
	children, _ := store.ListChildren(".")
	if len(children) == 0 {
		t.Skip("no entries")
	}

	e := children[0]
	handle := handler.ToHandle(&juiceFS{handler: handler}, splitPath(e.Path))
	if len(handle) != 8 {
		t.Fatalf("handle length = %d, want 8", len(handle))
	}

	// Should resolve back to the same path
	_, resolvedPath, err := handler.FromHandle(handle)
	if err != nil {
		t.Fatalf("FromHandle: %v", err)
	}

	got := ""
	if len(resolvedPath) > 0 {
		got = resolvedPath[len(resolvedPath)-1]
	}
	if got != e.Name {
		t.Fatalf("resolved name = %q, want %q", got, e.Name)
	}

	t.Logf("Handle round-trip for %q: OK (inode=%d)", e.Path, e.Inode)
}

func TestJuiceFSStatAndReadDir(t *testing.T) {
	_, store := setupTestServer(t)

	handler := NewHandler(store, testFUSEPath)
	jfs := &juiceFS{handler: handler}

	// Stat root
	info, err := jfs.Stat("")
	if err != nil {
		t.Fatalf("Stat root: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("root should be a directory")
	}

	// ReadDir root
	entries, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	t.Logf("Root has %d entries", len(entries))
	if len(entries) == 0 {
		t.Fatal("expected entries in root")
	}

	// Stat a child
	child := entries[0]
	info2, err := jfs.Stat(child.Name())
	if err != nil {
		t.Fatalf("Stat child %q: %v", child.Name(), err)
	}
	t.Logf("Child %q: size=%d dir=%v", info2.Name(), info2.Size(), info2.IsDir())
}

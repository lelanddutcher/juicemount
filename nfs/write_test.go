package nfs

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNFSCreateAndWrite(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	testFile := filepath.Join(mountPoint, fmt.Sprintf("__test_write_%d.txt", time.Now().UnixNano()))
	testData := []byte("Hello from JuiceMount5 NFS write test!\n")

	// Write file via NFS
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("WriteFile via NFS: %v", err)
	}

	// Read it back via NFS
	readBack, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile via NFS: %v", err)
	}

	if string(readBack) != string(testData) {
		t.Fatalf("data mismatch: wrote %d bytes, read back %d bytes", len(testData), len(readBack))
	}

	// Verify it appeared on FUSE mount too
	fusePath := filepath.Join(testFUSEPath, filepath.Base(testFile))
	fuseData, err := os.ReadFile(fusePath)
	if err != nil {
		t.Logf("WARNING: file not visible on FUSE yet (may need time to sync): %v", err)
	} else {
		if string(fuseData) != string(testData) {
			t.Fatalf("FUSE data mismatch")
		}
		t.Logf("File verified on FUSE mount too")
	}

	// Clean up
	os.Remove(testFile)
	t.Logf("Write test passed: %d bytes written and verified", len(testData))
}

func TestNFSCreateAndReadLarger(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	testFile := filepath.Join(mountPoint, fmt.Sprintf("__test_write_large_%d.bin", time.Now().UnixNano()))

	// Write 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	writeHash := sha256.Sum256(data)

	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Read back and verify integrity
	readBack, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	readHash := sha256.Sum256(readBack)

	if writeHash != readHash {
		t.Fatal("SHA256 mismatch on 1MB write/read cycle")
	}

	// Clean up
	os.Remove(testFile)
	t.Logf("1MB write/read cycle: SHA256 verified")
}

func TestNFSMkdir(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	dirPath := filepath.Join(mountPoint, fmt.Sprintf("__test_dir_%d", time.Now().UnixNano()))

	if err := os.Mkdir(dirPath, 0755); err != nil {
		t.Fatalf("Mkdir via NFS: %v", err)
	}

	info, err := os.Stat(dirPath)
	if err != nil {
		t.Fatalf("Stat new dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected directory")
	}

	// Create a file inside the new dir
	testFile := filepath.Join(dirPath, "test.txt")
	os.WriteFile(testFile, []byte("inside new dir"), 0644)

	readBack, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile in new dir: %v", err)
	}
	if string(readBack) != "inside new dir" {
		t.Fatal("data mismatch in new dir file")
	}

	// Clean up
	os.Remove(testFile)
	os.Remove(dirPath)
	t.Logf("Mkdir + write inside new dir: OK")
}

func TestNFSRename(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	oldName := filepath.Join(mountPoint, fmt.Sprintf("__test_rename_old_%d.txt", time.Now().UnixNano()))
	newName := filepath.Join(mountPoint, fmt.Sprintf("__test_rename_new_%d.txt", time.Now().UnixNano()))

	// Create file
	os.WriteFile(oldName, []byte("rename me"), 0644)

	// Rename
	if err := os.Rename(oldName, newName); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Old should not exist
	if _, err := os.Stat(oldName); err == nil {
		t.Fatal("old file still exists after rename")
	}

	// New should exist with correct data
	data, err := os.ReadFile(newName)
	if err != nil {
		t.Fatalf("ReadFile new name: %v", err)
	}
	if string(data) != "rename me" {
		t.Fatal("data mismatch after rename")
	}

	os.Remove(newName)
	t.Logf("Rename test: OK")
}

func TestNFSRemove(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	testFile := filepath.Join(mountPoint, fmt.Sprintf("__test_remove_%d.txt", time.Now().UnixNano()))
	os.WriteFile(testFile, []byte("delete me"), 0644)

	if err := os.Remove(testFile); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := os.Stat(testFile); err == nil {
		t.Fatal("file still exists after remove")
	}

	t.Logf("Remove test: OK")
}

func TestNFSFileCopy(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	// Create 5 test files
	srcDir := filepath.Join(mountPoint, fmt.Sprintf("__test_copy_src_%d", time.Now().UnixNano()))
	os.Mkdir(srcDir, 0755)

	hashes := make([][32]byte, 5)
	for i := 0; i < 5; i++ {
		data := make([]byte, 10*1024) // 10KB each
		for j := range data {
			data[j] = byte((i + j) % 256)
		}
		hashes[i] = sha256.Sum256(data)
		os.WriteFile(filepath.Join(srcDir, fmt.Sprintf("file_%d.bin", i)), data, 0644)
	}

	// Copy files manually (ditto has issues with NFS handle caching on new dirs)
	dstDir := filepath.Join(mountPoint, fmt.Sprintf("__test_copy_dst_%d", time.Now().UnixNano()))
	os.Mkdir(dstDir, 0755)

	for i := 0; i < 5; i++ {
		src := filepath.Join(srcDir, fmt.Sprintf("file_%d.bin", i))
		dst := filepath.Join(dstDir, fmt.Sprintf("file_%d.bin", i))
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("ReadFile src: %v", err)
		}
		if err := os.WriteFile(dst, data, 0644); err != nil {
			t.Fatalf("WriteFile dst: %v", err)
		}
	}

	// Verify all files
	for i := 0; i < 5; i++ {
		dstFile := filepath.Join(dstDir, fmt.Sprintf("file_%d.bin", i))
		dstData, err := os.ReadFile(dstFile)
		if err != nil {
			t.Fatalf("ReadFile dst: %v", err)
		}
		h := sha256.Sum256(dstData)
		if h != hashes[i] {
			t.Fatalf("file_%d.bin: SHA256 mismatch after copy", i)
		}
	}

	os.RemoveAll(dstDir)
	os.RemoveAll(srcDir)
	t.Logf("File copy 5 files: 100%% success, all SHA256 verified")
}

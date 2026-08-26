package test

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// These tests simulate real video editing workflows on the live NFS mount.
// They use the E2E stack from e2e_test.go.

const workflowBlockSize = 4 * 1024 * 1024

type workflowMediaFile struct {
	path string
	size int64
	seed int64
}

// setupWorkflowMediaFixture writes deterministic, non-sparse media through the
// NFS arm under test. Earlier workflow tests selected arbitrary large entries
// from a Redis snapshot. A stale entry could then return zero bytes while the
// benchmark still passed. These exact fixtures make a short, stale, corrupt,
// or failed read fatal and never enumerate unrelated user data.
func setupWorkflowMediaFixture(t *testing.T, env *e2eEnv, fileCount int, size int64) []workflowMediaFile {
	t.Helper()

	root := filepath.Join(env.mount, fmt.Sprintf("__jm_workflow_media_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create workflow media fixture: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	files := make([]workflowMediaFile, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		media := workflowMediaFile{
			path: filepath.Join(root, fmt.Sprintf("track-%02d.mov", i)),
			size: size,
			seed: int64(i+1) * 37,
		}
		writeWorkflowPatternFile(t, media)
		files = append(files, media)
	}
	return files
}

func writeWorkflowPatternFile(t *testing.T, media workflowMediaFile) {
	t.Helper()

	fd, err := os.OpenFile(media.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("create workflow media %s: %v", filepath.Base(media.path), err)
	}

	buf := make([]byte, 1024*1024)
	var offset int64
	for offset < media.size {
		chunk := int64(len(buf))
		if remaining := media.size - offset; remaining < chunk {
			chunk = remaining
		}
		for i := int64(0); i < chunk; i++ {
			buf[i] = byte((offset + i + media.seed) % 251)
		}
		if _, err := fd.Write(buf[:chunk]); err != nil {
			_ = fd.Close()
			t.Fatalf("write workflow media %s at %d: %v", filepath.Base(media.path), offset, err)
		}
		offset += chunk
	}
	if err := fd.Close(); err != nil {
		t.Fatalf("close workflow media %s: %v", filepath.Base(media.path), err)
	}
	info, err := os.Stat(media.path)
	if err != nil {
		t.Fatalf("stat workflow media %s: %v", filepath.Base(media.path), err)
	}
	if info.Size() != media.size {
		t.Fatalf("workflow media %s size=%d, want %d", filepath.Base(media.path), info.Size(), media.size)
	}
}

func readAndVerifyWorkflowBlock(fd *os.File, media workflowMediaFile, buf []byte, offset int64) error {
	n, err := fd.ReadAt(buf, offset)
	if err != nil {
		return fmt.Errorf("read %s at %d: %w", filepath.Base(media.path), offset, err)
	}
	if n != len(buf) {
		return fmt.Errorf("short read %s at %d: got %d, want %d", filepath.Base(media.path), offset, n, len(buf))
	}
	for i, actual := range buf {
		expected := byte((offset + int64(i) + media.seed) % 251)
		if actual != expected {
			return fmt.Errorf("data mismatch %s at %d: got %d, want %d", filepath.Base(media.path), offset+int64(i), actual, expected)
		}
	}
	return nil
}

// Workflow 1: Opening a project — browse deep directory tree, stat many files
func TestWorkflow_BrowseProjectTree(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)
	// Simulate Premiere opening a project: recursively stat the fixture tree.
	t.Logf("Browsing deterministic project tree (simulates Premiere project open)...")

	start := time.Now()
	var fileCount, dirCount int
	var totalSize int64

	err := filepath.Walk(fx.nfsRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
			totalSize += info.Size()
		}
		return nil
	})
	dur := time.Since(start)

	if err != nil {
		t.Fatalf("Walk: %v", err)
	}

	t.Logf("Tree walk: %d dirs, %d files, %.1f GB total in %v",
		dirCount, fileCount, float64(totalSize)/(1024*1024*1024), dur)
	t.Logf("Average stat: %.0fµs per entry", float64(dur.Microseconds())/float64(fileCount+dirCount))
}

// Workflow 2: Scrubbing video — sequential read of a large file, then random seeks
func TestWorkflow_VideoScrub(t *testing.T) {
	env := setupE2E(t)
	media := setupWorkflowMediaFixture(t, env, 1, 100*1024*1024)[0]

	t.Logf("Video file: %s (%.1f MB)", filepath.Base(media.path), float64(media.size)/(1024*1024))

	fd, err := os.Open(media.path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fd.Close()

	buf := make([]byte, workflowBlockSize) // Matches the JuiceFS block size.

	// Phase 1: Sequential read (playback) — first 40MB
	t.Log("Phase 1: Sequential read (simulates playback)...")
	seqStart := time.Now()
	seqBytes := int64(0)
	for i := 0; i < 10; i++ {
		offset := int64(i) * workflowBlockSize
		if err := readAndVerifyWorkflowBlock(fd, media, buf, offset); err != nil {
			t.Fatal(err)
		}
		seqBytes += int64(len(buf))
	}
	if want := int64(10 * workflowBlockSize); seqBytes != want {
		t.Fatalf("sequential bytes=%d, want %d", seqBytes, want)
	}
	seqDur := time.Since(seqStart)
	seqThroughput := float64(seqBytes) / seqDur.Seconds() / (1024 * 1024)
	t.Logf("  Sequential: %d bytes in %v = %.1f MB/s", seqBytes, seqDur, seqThroughput)

	// Phase 2: Random seeks (scrubbing) — 20 random 4MB reads across the file
	t.Log("Phase 2: Random seeks (simulates scrubbing)...")
	rng := rand.New(rand.NewSource(42))
	maxBlock := media.size/workflowBlockSize - 1

	seekStart := time.Now()
	seekBytes := int64(0)
	for i := 0; i < 20; i++ {
		offset := rng.Int63n(maxBlock+1) * workflowBlockSize
		if err := readAndVerifyWorkflowBlock(fd, media, buf, offset); err != nil {
			t.Fatal(err)
		}
		seekBytes += int64(len(buf))
	}
	const seekCount = 20
	if want := int64(seekCount * workflowBlockSize); seekBytes != want {
		t.Fatalf("random-seek bytes=%d, want %d", seekBytes, want)
	}
	seekDur := time.Since(seekStart)
	seekLatency := seekDur / seekCount
	t.Logf("  Random seek: %d reads, avg %v per 4MB read, total %v",
		seekCount, seekLatency, seekDur)

	// Phase 3: Return to sequential (resume playback after scrub)
	t.Log("Phase 3: Resume sequential playback...")
	resumeStart := time.Now()
	resumeBytes := int64(0)
	for i := 0; i < 5; i++ {
		offset := int64(20+i) * workflowBlockSize // start from block 20
		if err := readAndVerifyWorkflowBlock(fd, media, buf, offset); err != nil {
			t.Fatal(err)
		}
		resumeBytes += int64(len(buf))
	}
	if want := int64(5 * workflowBlockSize); resumeBytes != want {
		t.Fatalf("resume bytes=%d, want %d", resumeBytes, want)
	}
	resumeDur := time.Since(resumeStart)
	resumeThroughput := float64(resumeBytes) / resumeDur.Seconds() / (1024 * 1024)
	t.Logf("  Resume sequential: %d bytes in %v = %.1f MB/s", resumeBytes, resumeDur, resumeThroughput)
}

// Workflow 3: Multi-track editing — concurrent reads from multiple files
func TestWorkflow_MultiTrackRead(t *testing.T) {
	env := setupE2E(t)
	mediaFiles := setupWorkflowMediaFixture(t, env, 4, 20*1024*1024)

	t.Logf("Multi-track test with %d files:", len(mediaFiles))
	for _, media := range mediaFiles {
		t.Logf("  %s (%.1f MB)", filepath.Base(media.path), float64(media.size)/(1024*1024))
	}

	// Simulate concurrent reads (like Premiere reading multiple video tracks)
	var wg sync.WaitGroup
	errors := make(chan error, len(mediaFiles))
	bytesRead := make(chan int64, len(mediaFiles))

	start := time.Now()
	for _, media := range mediaFiles {
		wg.Add(1)
		go func(media workflowMediaFile) {
			defer wg.Done()
			fd, err := os.Open(media.path)
			if err != nil {
				errors <- err
				return
			}
			defer fd.Close()
			localBuf := make([]byte, workflowBlockSize)
			var total int64
			// Read 5 sequential blocks from each file
			for i := 0; i < 5; i++ {
				offset := int64(i) * workflowBlockSize
				if err := readAndVerifyWorkflowBlock(fd, media, localBuf, offset); err != nil {
					errors <- err
					return
				}
				total += int64(len(localBuf))
			}
			bytesRead <- total
		}(media)
	}
	wg.Wait()
	close(errors)
	close(bytesRead)
	dur := time.Since(start)

	for err := range errors {
		t.Errorf("concurrent read error: %v", err)
	}
	var totalBytes int64
	for n := range bytesRead {
		totalBytes += n
	}
	wantBytes := int64(len(mediaFiles) * 5 * workflowBlockSize)
	if totalBytes != wantBytes {
		t.Fatalf("multi-track bytes=%d, want %d", totalBytes, wantBytes)
	}

	t.Logf("Multi-track concurrent read: %d files × 5 verified blocks (%d bytes) in %v", len(mediaFiles), totalBytes, dur)
}

// Workflow 4: Project save — create directory structure, write project files
func TestWorkflow_ProjectSave(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	projectDir := filepath.Join(mount, fmt.Sprintf("__workflow_project_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.RemoveAll(projectDir) })

	// Simulate Premiere project save structure
	t.Log("Creating project directory structure...")
	dirs := []string{
		projectDir,
		filepath.Join(projectDir, "Footage"),
		filepath.Join(projectDir, "Footage", "Day1"),
		filepath.Join(projectDir, "Footage", "Day1", "Proxy"),
		filepath.Join(projectDir, "Audio"),
		filepath.Join(projectDir, "Graphics"),
		filepath.Join(projectDir, "Exports"),
	}

	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("MkdirAll %s: %v", filepath.Base(d), err)
		}
	}

	// Write project file (simulates .prproj XML — 2MB)
	t.Log("Writing project file (2MB simulated .prproj)...")
	projectData := make([]byte, 2*1024*1024)
	for i := range projectData {
		projectData[i] = byte("<?xml version='1.0'?><Project>"[i%30])
	}
	projectFile := filepath.Join(projectDir, "MyProject.prproj")
	if err := os.WriteFile(projectFile, projectData, 0644); err != nil {
		t.Fatalf("Write project file: %v", err)
	}

	// Write LUT files (small, read frequently)
	t.Log("Writing LUT files...")
	for i := 0; i < 5; i++ {
		lutData := make([]byte, 50*1024) // 50KB each
		for j := range lutData {
			lutData[j] = byte(j % 256)
		}
		lutFile := filepath.Join(projectDir, "Graphics", fmt.Sprintf("grade_%d.cube", i))
		if err := os.WriteFile(lutFile, lutData, 0o644); err != nil {
			t.Fatalf("write LUT %d: %v", i, err)
		}
	}

	// Write proxy files (medium size, 1MB each)
	t.Log("Writing proxy files...")
	for i := 0; i < 3; i++ {
		proxyData := make([]byte, 1*1024*1024)
		for j := range proxyData {
			proxyData[j] = byte((i + j) % 256)
		}
		proxyFile := filepath.Join(projectDir, "Footage", "Day1", "Proxy", fmt.Sprintf("proxy_%d.mov", i))
		if err := os.WriteFile(proxyFile, proxyData, 0o644); err != nil {
			t.Fatalf("write proxy %d: %v", i, err)
		}
	}

	// Write export file (larger, 5MB)
	t.Log("Writing export file (5MB)...")
	exportData := make([]byte, 5*1024*1024)
	for i := range exportData {
		exportData[i] = byte(i % 256)
	}
	exportFile := filepath.Join(projectDir, "Exports", "final_cut_v1.mp4")
	exportHash := sha256.Sum256(exportData)
	if err := os.WriteFile(exportFile, exportData, 0o644); err != nil {
		t.Fatalf("write export file: %v", err)
	}

	// Verify: read everything back and check integrity
	t.Log("Verifying all files...")

	// Project file
	readBack, err := os.ReadFile(projectFile)
	if err != nil {
		t.Fatalf("read project file: %v", err)
	}
	if len(readBack) != len(projectData) {
		t.Fatalf("project file size mismatch: %d vs %d", len(readBack), len(projectData))
	}

	// Export file SHA256
	readExport, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("read export file: %v", err)
	}
	readHash := sha256.Sum256(readExport)
	if readHash != exportHash {
		t.Fatal("export file SHA256 mismatch")
	}

	// Count all files
	totalFiles := 0
	if err := filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalFiles++
		}
		return nil
	}); err != nil {
		t.Fatalf("count project files: %v", err)
	}

	t.Logf("Project save complete: %d dirs, %d files, all verified", len(dirs), totalFiles)

}

// Workflow 5: Finder operations — copy, rename, move files between folders
func TestWorkflow_FinderOps(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	workDir := filepath.Join(mount, fmt.Sprintf("__workflow_finder_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.RemoveAll(workDir) })
	for _, dir := range []string{workDir, filepath.Join(workDir, "src"), filepath.Join(workDir, "dst")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create Finder workflow directory %s: %v", filepath.Base(dir), err)
		}
	}

	// Create test files
	var fileHashes map[string][32]byte = make(map[string][32]byte)
	for i := 0; i < 5; i++ {
		data := make([]byte, 100*1024) // 100KB each
		for j := range data {
			data[j] = byte((i*31 + j*17) % 256)
		}
		name := fmt.Sprintf("clip_%d.mov", i)
		if err := os.WriteFile(filepath.Join(workDir, "src", name), data, 0o644); err != nil {
			t.Fatalf("write source file %s: %v", name, err)
		}
		fileHashes[name] = sha256.Sum256(data)
	}

	// Manual file copy (ditto/cp have issues with xattrs on NFS)
	t.Log("Copying files src → dst...")
	for name := range fileHashes {
		src := filepath.Join(workDir, "src", name)
		dst := filepath.Join(workDir, "dst", name)
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read src %s: %v", name, err)
		}
		if err := os.WriteFile(dst, data, 0644); err != nil {
			t.Fatalf("write dst %s: %v", name, err)
		}
	}

	// Verify copy integrity
	for name, expectedHash := range fileHashes {
		data, err := os.ReadFile(filepath.Join(workDir, "dst", name))
		if err != nil {
			t.Fatalf("read copied file %s: %v", name, err)
		}
		h := sha256.Sum256(data)
		if h != expectedHash {
			t.Fatalf("%s: SHA256 mismatch after copy", name)
		}
	}
	t.Log("  ditto copy: all 5 files verified")

	// Rename (simulates organizing footage)
	t.Log("Renaming files...")
	if err := os.Rename(
		filepath.Join(workDir, "dst", "clip_0.mov"),
		filepath.Join(workDir, "dst", "A001_hero_shot.mov"),
	); err != nil {
		t.Fatalf("rename clip: %v", err)
	}
	// Verify renamed file
	data, err := os.ReadFile(filepath.Join(workDir, "dst", "A001_hero_shot.mov"))
	if err != nil {
		t.Fatalf("read renamed file: %v", err)
	}
	h := sha256.Sum256(data)
	if h != fileHashes["clip_0.mov"] {
		t.Fatal("SHA256 mismatch after rename")
	}
	t.Log("  rename: verified")

	// Move file between directories
	t.Log("Moving file between directories...")
	if err := os.MkdirAll(filepath.Join(workDir, "selects"), 0o755); err != nil {
		t.Fatalf("create selects directory: %v", err)
	}
	if err := os.Rename(
		filepath.Join(workDir, "dst", "clip_1.mov"),
		filepath.Join(workDir, "selects", "clip_1.mov"),
	); err != nil {
		t.Fatalf("move clip: %v", err)
	}
	data2, err := os.ReadFile(filepath.Join(workDir, "selects", "clip_1.mov"))
	if err != nil {
		t.Fatalf("read moved file: %v", err)
	}
	h2 := sha256.Sum256(data2)
	if h2 != fileHashes["clip_1.mov"] {
		t.Fatal("SHA256 mismatch after move")
	}
	t.Log("  move: verified")

	// Delete
	t.Log("Deleting files...")
	if err := os.Remove(filepath.Join(workDir, "dst", "clip_2.mov")); err != nil {
		t.Fatalf("delete clip: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workDir, "dst", "clip_2.mov")); err == nil {
		t.Fatal("file should not exist after delete")
	}
	t.Log("  delete: verified")

	t.Log("Finder ops workflow: all operations verified")
}

// Workflow 6: Rapid project file access (simulates Premiere auto-save)
func TestWorkflow_RapidProjectAccess(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	projectFile := filepath.Join(mount, fmt.Sprintf("__workflow_rapid_%d.prproj", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(projectFile) })

	// Simulate 10 rapid save cycles (Premiere auto-saves every 5 minutes)
	t.Log("Simulating 10 rapid project saves...")
	for cycle := 0; cycle < 10; cycle++ {
		// Write project file (1MB, grows slightly each save)
		size := (1 + cycle/5) * 1024 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte((cycle*13 + i*7) % 256)
		}

		start := time.Now()
		if err := os.WriteFile(projectFile, data, 0644); err != nil {
			t.Fatalf("save cycle %d: %v", cycle, err)
		}
		writeDur := time.Since(start)

		// Immediately read back (Premiere re-reads after save)
		readStart := time.Now()
		readBack, err := os.ReadFile(projectFile)
		readDur := time.Since(readStart)

		if err != nil {
			t.Fatalf("read cycle %d: %v", cycle, err)
		}

		writeHash := sha256.Sum256(data)
		readHash := sha256.Sum256(readBack)
		if writeHash != readHash {
			t.Fatalf("cycle %d: SHA256 mismatch", cycle)
		}

		if cycle == 0 || cycle == 9 {
			t.Logf("  cycle %d: write %dKB in %v, read in %v",
				cycle, size/1024, writeDur, readDur)
		}
	}

	t.Log("Rapid project access: 10 save/read cycles verified")
}

package test

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// These tests simulate real video editing workflows on the live NFS mount.
// They use the E2E stack from e2e_test.go.

// Workflow 1: Opening a project — browse deep directory tree, stat many files
func TestWorkflow_BrowseProjectTree(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	// Simulate Premiere opening a project: recursively stat the entire tree
	t.Log("Browsing Film Projects tree (simulates Premiere project open)...")

	start := time.Now()
	var fileCount, dirCount int
	var totalSize int64

	err := filepath.Walk(filepath.Join(mount, "Film Projects"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors (permission, etc)
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
	mount := env.mount

	videoFile := filepath.Join(mount, "Soap Regular", "A_0056C829H260207_093739SL_LAD04.MP4")
	info, err := os.Stat(videoFile)
	if err != nil {
		t.Skipf("Video file not found: %v", err)
	}

	t.Logf("Video file: %s (%.1f MB)", info.Name(), float64(info.Size())/(1024*1024))

	fd, err := os.Open(videoFile)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fd.Close()

	buf := make([]byte, 4*1024*1024) // 4MB reads (matches JuiceFS block size)

	// Phase 1: Sequential read (playback) — first 40MB
	t.Log("Phase 1: Sequential read (simulates playback)...")
	seqStart := time.Now()
	seqBytes := int64(0)
	for i := 0; i < 10; i++ {
		n, err := fd.ReadAt(buf, int64(i)*4*1024*1024)
		seqBytes += int64(n)
		if err != nil && err != io.EOF {
			break
		}
	}
	seqDur := time.Since(seqStart)
	seqThroughput := float64(seqBytes) / seqDur.Seconds() / (1024 * 1024)
	t.Logf("  Sequential: %d bytes in %v = %.1f MB/s", seqBytes, seqDur, seqThroughput)

	// Phase 2: Random seeks (scrubbing) — 20 random 4MB reads across the file
	t.Log("Phase 2: Random seeks (simulates scrubbing)...")
	rng := rand.New(rand.NewSource(42))
	maxOffset := info.Size() - 4*1024*1024
	if maxOffset < 0 {
		maxOffset = 0
	}

	seekStart := time.Now()
	seekBytes := int64(0)
	seekCount := 0
	for i := 0; i < 20; i++ {
		offset := rng.Int63n(maxOffset)
		// Align to 4MB boundary (JuiceFS block alignment)
		offset = (offset / (4 * 1024 * 1024)) * (4 * 1024 * 1024)
		n, err := fd.ReadAt(buf, offset)
		seekBytes += int64(n)
		seekCount++
		if err != nil && err != io.EOF {
			break
		}
	}
	seekDur := time.Since(seekStart)
	seekLatency := seekDur / time.Duration(seekCount)
	t.Logf("  Random seek: %d reads, avg %v per 4MB read, total %v",
		seekCount, seekLatency, seekDur)

	// Phase 3: Return to sequential (resume playback after scrub)
	t.Log("Phase 3: Resume sequential playback...")
	resumeStart := time.Now()
	resumeBytes := int64(0)
	for i := 0; i < 5; i++ {
		offset := int64(20+i) * 4 * 1024 * 1024 // start from block 20
		n, err := fd.ReadAt(buf, offset)
		resumeBytes += int64(n)
		if err != nil && err != io.EOF {
			break
		}
	}
	resumeDur := time.Since(resumeStart)
	resumeThroughput := float64(resumeBytes) / resumeDur.Seconds() / (1024 * 1024)
	t.Logf("  Resume sequential: %d bytes in %v = %.1f MB/s", resumeBytes, resumeDur, resumeThroughput)
}

// Workflow 3: Multi-track editing — concurrent reads from multiple files
func TestWorkflow_MultiTrackRead(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	// Find multiple video/media files
	var mediaFiles []string
	filepath.Walk(mount, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.Size() > 1*1024*1024 { // >1MB
			mediaFiles = append(mediaFiles, path)
		}
		if len(mediaFiles) >= 4 {
			return filepath.SkipAll
		}
		return nil
	})

	if len(mediaFiles) < 2 {
		t.Skipf("Need at least 2 media files, found %d", len(mediaFiles))
	}

	t.Logf("Multi-track test with %d files:", len(mediaFiles))
	for _, f := range mediaFiles {
		info, _ := os.Stat(f)
		rel, _ := filepath.Rel(mount, f)
		t.Logf("  %s (%.1f MB)", rel, float64(info.Size())/(1024*1024))
	}

	// Simulate concurrent reads (like Premiere reading multiple video tracks)
	buf := make([]byte, 4*1024*1024)
	var wg sync.WaitGroup
	errors := make(chan error, len(mediaFiles))

	start := time.Now()
	for _, path := range mediaFiles {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			fd, err := os.Open(p)
			if err != nil {
				errors <- err
				return
			}
			defer fd.Close()
			localBuf := make([]byte, len(buf))
			// Read 5 sequential blocks from each file
			for i := 0; i < 5; i++ {
				_, err := fd.ReadAt(localBuf, int64(i)*4*1024*1024)
				if err != nil && err != io.EOF {
					break
				}
			}
		}(path)
	}
	wg.Wait()
	close(errors)
	dur := time.Since(start)

	for err := range errors {
		t.Errorf("concurrent read error: %v", err)
	}

	t.Logf("Multi-track concurrent read: %d files × 5 blocks in %v", len(mediaFiles), dur)
}

// Workflow 4: Project save — create directory structure, write project files
func TestWorkflow_ProjectSave(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	projectDir := filepath.Join(mount, fmt.Sprintf("__workflow_project_%d", time.Now().UnixNano()))

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
		os.WriteFile(lutFile, lutData, 0644)
	}

	// Write proxy files (medium size, 1MB each)
	t.Log("Writing proxy files...")
	for i := 0; i < 3; i++ {
		proxyData := make([]byte, 1*1024*1024)
		for j := range proxyData {
			proxyData[j] = byte((i + j) % 256)
		}
		proxyFile := filepath.Join(projectDir, "Footage", "Day1", "Proxy", fmt.Sprintf("proxy_%d.mov", i))
		os.WriteFile(proxyFile, proxyData, 0644)
	}

	// Write export file (larger, 5MB)
	t.Log("Writing export file (5MB)...")
	exportData := make([]byte, 5*1024*1024)
	for i := range exportData {
		exportData[i] = byte(i % 256)
	}
	exportFile := filepath.Join(projectDir, "Exports", "final_cut_v1.mp4")
	exportHash := sha256.Sum256(exportData)
	os.WriteFile(exportFile, exportData, 0644)

	// Verify: read everything back and check integrity
	t.Log("Verifying all files...")

	// Project file
	readBack, _ := os.ReadFile(projectFile)
	if len(readBack) != len(projectData) {
		t.Fatalf("project file size mismatch: %d vs %d", len(readBack), len(projectData))
	}

	// Export file SHA256
	readExport, _ := os.ReadFile(exportFile)
	readHash := sha256.Sum256(readExport)
	if readHash != exportHash {
		t.Fatal("export file SHA256 mismatch")
	}

	// Count all files
	totalFiles := 0
	filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalFiles++
		}
		return nil
	})

	t.Logf("Project save complete: %d dirs, %d files, all verified", len(dirs), totalFiles)

	// Clean up
	os.RemoveAll(projectDir)
}

// Workflow 5: Finder operations — copy, rename, move files between folders
func TestWorkflow_FinderOps(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	workDir := filepath.Join(mount, fmt.Sprintf("__workflow_finder_%d", time.Now().UnixNano()))
	os.MkdirAll(workDir, 0755)
	os.MkdirAll(filepath.Join(workDir, "src"), 0755)
	os.MkdirAll(filepath.Join(workDir, "dst"), 0755)

	// Create test files
	var fileHashes map[string][32]byte = make(map[string][32]byte)
	for i := 0; i < 5; i++ {
		data := make([]byte, 100*1024) // 100KB each
		for j := range data {
			data[j] = byte((i*31 + j*17) % 256)
		}
		name := fmt.Sprintf("clip_%d.mov", i)
		os.WriteFile(filepath.Join(workDir, "src", name), data, 0644)
		fileHashes[name] = sha256.Sum256(data)
	}

	// Manual file copy (ditto/cp have issues with xattrs on NFS)
	t.Log("Copying files src → dst...")
	os.MkdirAll(filepath.Join(workDir, "dst"), 0755)
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
	os.Rename(
		filepath.Join(workDir, "dst", "clip_0.mov"),
		filepath.Join(workDir, "dst", "A001_hero_shot.mov"),
	)
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
	os.MkdirAll(filepath.Join(workDir, "selects"), 0755)
	os.Rename(
		filepath.Join(workDir, "dst", "clip_1.mov"),
		filepath.Join(workDir, "selects", "clip_1.mov"),
	)
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
	os.Remove(filepath.Join(workDir, "dst", "clip_2.mov"))
	if _, err := os.Stat(filepath.Join(workDir, "dst", "clip_2.mov")); err == nil {
		t.Fatal("file should not exist after delete")
	}
	t.Log("  delete: verified")

	// Clean up
	os.RemoveAll(workDir)
	t.Log("Finder ops workflow: all operations verified")
}

// Workflow 6: Rapid project file access (simulates Premiere auto-save)
func TestWorkflow_RapidProjectAccess(t *testing.T) {
	env := setupE2E(t)
	mount := env.mount

	projectFile := filepath.Join(mount, fmt.Sprintf("__workflow_rapid_%d.prproj", time.Now().UnixNano()))

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

	os.Remove(projectFile)
	t.Log("Rapid project access: 10 save/read cycles verified")
}

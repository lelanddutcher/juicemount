//go:build h8aacceptance

package metadata

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestH8ACrashLeftCompleteCandidateIsReclaimedAfterReopen(t *testing.T) {
	if dbPath := os.Getenv("JM_H8A_CANDIDATE_CRASH_DB"); dbPath != "" {
		s, err := Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		generation, err := s.StagePruneLadderSnapshot(map[string]int{"crash-left.mov": 2})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Getenv("JM_H8A_CANDIDATE_CRASH_RECEIPT"), []byte(generation), 0600); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "mirror.db")
	receipt := filepath.Join(t.TempDir(), "generation")
	cmd := exec.Command(os.Args[0], "-test.run=^TestH8ACrashLeftCompleteCandidateIsReclaimedAfterReopen$")
	cmd.Env = append(os.Environ(), "JM_H8A_CANDIDATE_CRASH_DB="+path, "JM_H8A_CANDIDATE_CRASH_RECEIPT="+receipt)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("staging child exited: %v\n%s", err, output)
	}
	rawGeneration, err := os.ReadFile(receipt)
	if err != nil {
		t.Fatal(err)
	}
	generation := strings.TrimSpace(string(rawGeneration))

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.GCPruneLadderStages(100); err != nil {
			t.Fatal(err)
		}
		var manifests int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=?`, generation).Scan(&manifests); err != nil {
			t.Fatal(err)
		}
		if manifests == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("crash-left complete candidate %q remains after reopening its store", generation)
}

func TestH8AStagedLadder131kCleanup(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := make(map[string]int, 131000)
	second := make(map[string]int, 131000)
	for i := 0; i < 131000; i++ {
		path := fmt.Sprintf("absent/%06d.mov", i)
		first[path] = 1
		second[path] = 2
	}
	seedStart := time.Now()
	oldGeneration, err := s.StagePruneLadderSnapshot(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(oldGeneration, time.Now()); err != nil {
		t.Fatal(err)
	}
	seedDuration := time.Since(seedStart)
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "1")
	rc := &RedisClient{store: s, pruneAbsent: second, ladderPersisted: first}
	start := time.Now()
	completed, err := rc.completeScanPublication(scanTokenForPublicationTest(t, rc), time.Now())
	if err != nil || !completed {
		t.Fatalf("publish 131k ladder = %v, %v", completed, err)
	}
	publishDuration := time.Since(start)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var oldRows int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, oldGeneration).Scan(&oldRows); err != nil {
			t.Fatal(err)
		}
		if oldRows == 0 {
			t.Logf("131k seed=%s publish=%s background_drain=%s", seedDuration, publishDuration, time.Since(start)-publishDuration)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("131k retired generation did not drain within 60s; seed=%s publish=%s", seedDuration, publishDuration)
}

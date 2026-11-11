package nfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

type offlineRefusalLine struct {
	Level      string `json:"level"`
	Message    string `json:"msg"`
	Op         string `json:"op"`
	Why        string `json:"why"`
	Path       string `json:"path"`
	Auto       bool   `json:"auto_offline"`
	AutoReason string `json:"auto_reason"`
	Suppressed int64  `json:"suppressed_since_last"`
}

func offlineRefusalLines(t *testing.T, raw []byte) []offlineRefusalLine {
	t.Helper()
	var out []offlineRefusalLine
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var l offlineRefusalLine
		if err := json.Unmarshal(line, &l); err != nil {
			continue
		}
		if l.Message == "offline gate refused request (ENXIO to the app)" {
			out = append(out, l)
		}
	}
	return out
}

// TestOfflineOpenRefusalLogsAtInfoRateLimited is the NFS-level check for the
// 2026-11-11 lab gap: a reachability flip engaged auto-offline, the open gate
// refused an un-pinned file with ENXIO, and nothing reached the Info log. The
// refusal must now log once at Info with the gate and the auto-offline reason,
// later refusals inside the window must be counted rather than logged, and the
// next Info line after the window must carry that count.
func TestOfflineOpenRefusalLogsAtInfoRateLimited(t *testing.T) {
	p96OfflineState(t)
	jfs, _, root := p96LstatFixture(t)
	if err := os.WriteFile(filepath.Join(root, "cold.mov"), []byte("body"), 0600); err != nil {
		t.Fatal(err)
	}

	var logs lockedLog
	if err := jmlog.Init(jmlog.Config{Stderr: &logs, Level: jmlog.LevelInfo}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jmlog.Init(jmlog.Config{Stderr: os.Stderr}) })

	var clockMu sync.Mutex
	clock := time.Unix(1_800_000_000, 0)
	offlineRefusalLog.mu.Lock()
	offlineRefusalLog.last, offlineRefusalLog.suppressed = time.Time{}, 0
	offlineRefusalLog.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	offlineRefusalLog.mu.Unlock()
	t.Cleanup(func() {
		offlineRefusalLog.mu.Lock()
		offlineRefusalLog.last, offlineRefusalLog.suppressed, offlineRefusalLog.now = time.Time{}, 0, nil
		offlineRefusalLog.mu.Unlock()
	})

	const reason = "probe failed 2x — backend unreachable"
	pin.SetAutoOffline(true, reason)

	open := func() {
		t.Helper()
		f, err := jfs.OpenFile("cold.mov", os.O_RDONLY, 0)
		if err == nil {
			_ = f.Close()
			t.Fatal("offline open of an un-pinned, uncached file succeeded")
		}
		if !errors.Is(err, pin.ErrOfflineNotAvailable) {
			t.Fatalf("offline open err=%v, want ErrOfflineNotAvailable (NXIO)", err)
		}
	}

	open()
	open()
	open()
	lines := offlineRefusalLines(t, logs.Bytes())
	if len(lines) != 1 {
		t.Fatalf("got %d Info refusal lines inside one window, want 1: %+v", len(lines), lines)
	}
	first := lines[0]
	if first.Level != "INFO" || first.Op != "open" || first.Path != "cold.mov" || !first.Auto || first.AutoReason != reason || first.Why == "" {
		t.Fatalf("first refusal line %+v lacks gate, path or auto-offline reason", first)
	}

	clockMu.Lock()
	clock = clock.Add(offlineRefusalLogEvery)
	clockMu.Unlock()
	open()
	lines = offlineRefusalLines(t, logs.Bytes())
	if len(lines) != 2 {
		t.Fatalf("got %d Info refusal lines after the window, want 2", len(lines))
	}
	if lines[1].Suppressed != 2 {
		t.Fatalf("suppressed_since_last=%d, want 2", lines[1].Suppressed)
	}
}

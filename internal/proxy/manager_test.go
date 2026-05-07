package proxy

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newTempManager creates a Manager with a throwaway cache dir.
func newTempManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(dir, 1)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Stop() })
	return m
}

func TestManagerCacheKeyDeterministic(t *testing.T) {
	m := newTempManager(t)
	tmp := filepath.Join(t.TempDir(), "x.r3d")
	if err := os.WriteFile(tmp, []byte("dummy r3d content"), 0o644); err != nil {
		t.Fatal(err)
	}

	k1, err := m.CacheKeyFor(tmp)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := m.CacheKeyFor(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Errorf("cache key not deterministic: %q vs %q", k1, k2)
	}
	if len(k1) != 32 {
		t.Errorf("cache key length = %d, want 32", len(k1))
	}
}

func TestManagerCacheKeyChangesOnMtime(t *testing.T) {
	m := newTempManager(t)
	tmp := filepath.Join(t.TempDir(), "x.r3d")
	if err := os.WriteFile(tmp, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	k1, _ := m.CacheKeyFor(tmp)

	// Wait + rewrite to change mtime
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(tmp, []byte("v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	k2, _ := m.CacheKeyFor(tmp)

	if k1 == k2 {
		t.Errorf("expected cache key to change on mtime; got same key %q", k1)
	}
}

func TestManagerGetUnknownCodec(t *testing.T) {
	m := newTempManager(t)
	tmp := filepath.Join(t.TempDir(), "x.txt")
	os.WriteFile(tmp, []byte("hello"), 0o644)

	r := m.Get(tmp)
	if r.Status != StatusNotProxyable {
		t.Errorf("Get(.txt) status = %v, want NotProxyable", r.Status)
	}
}

func TestManagerGetReturnsGeneratingForRAW(t *testing.T) {
	m := newTempManager(t)
	tmp := filepath.Join(t.TempDir(), "x.r3d")
	os.WriteFile(tmp, []byte("not a real r3d but the codec detection only sees the ext"), 0o644)

	r := m.Get(tmp)
	// Should be Generating (worker enqueued) — even though the actual ffmpeg
	// run will fail because the bytes aren't real R3D, the Get call is async
	// and returns immediately.
	if r.Status != StatusGenerating && r.Status != StatusReady {
		t.Errorf("Get(fake .r3d) initial status = %v, want Generating or Ready", r.Status)
	}
	if r.Codec != CodecR3D {
		t.Errorf("codec = %v, want R3D", r.Codec)
	}

	// Wait for the worker to finish (ffmpeg will fail on fake bytes, but it
	// needs to finish and clean up before the t.TempDir cleanup runs).
	// Up to 10s — should be ~100ms in practice since ffmpeg fails fast.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Stats()
		if s.TotalGenerated+s.TotalFailed > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestManagerGetCoalescesConcurrent(t *testing.T) {
	m := newTempManager(t)
	tmp := filepath.Join(t.TempDir(), "x.r3d")
	os.WriteFile(tmp, []byte("dummy"), 0o644)

	// Fire 10 concurrent Gets for the same file. Only one job should be enqueued.
	results := make([]Result, 10)
	done := make(chan int, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			results[idx] = m.Get(tmp)
			done <- idx
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	// All should report the same proxy path
	first := results[0].ProxyPath
	for i, r := range results {
		if r.ProxyPath != first {
			t.Errorf("result[%d] proxy path %q != first %q", i, r.ProxyPath, first)
		}
	}
}

func TestManagerStats(t *testing.T) {
	m := newTempManager(t)
	s := m.Stats()
	if s.Workers < 1 {
		t.Errorf("Workers = %d, want >= 1", s.Workers)
	}
	if s.CacheDir == "" {
		t.Error("CacheDir empty")
	}
	if s.TotalGenerated != 0 || s.TotalCached != 0 || s.TotalFailed != 0 {
		t.Errorf("expected zero counters initially; got %+v", s)
	}
}

func TestProxySpecBuildArgs(t *testing.T) {
	spec := DefaultSpec("/in.r3d", "/out.mp4", CodecR3D)
	args := spec.buildArgs()

	// Should include hardware encoder
	hasEncoder := false
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-c:v" && args[i+1] == "h264_videotoolbox" {
			hasEncoder = true
			break
		}
	}
	if !hasEncoder {
		t.Errorf("expected -c:v h264_videotoolbox in args; got %v", args)
	}

	// Should include scale filter
	hasScale := false
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-vf" && args[i+1] == "scale=1280:-2" {
			hasScale = true
			break
		}
	}
	if !hasScale {
		t.Error("expected -vf scale=1280:-2 in args")
	}

	// Should NOT include audio
	for _, a := range args {
		if a == "-c:a" {
			t.Error("audio codec specified but should be -an only")
		}
	}

	// First non-flag should be the source path
	hasInput := false
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-i" && args[i+1] == "/in.r3d" {
			hasInput = true
			break
		}
	}
	if !hasInput {
		t.Error("expected -i /in.r3d in args")
	}
}

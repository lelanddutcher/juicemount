package health

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestControlFileResponsiveWithinUsesOnlyLocalControlPath(t *testing.T) {
	var got string
	ok := controlFileResponsiveWithin("/mount", time.Second, func(path string) ([]byte, error) {
		got = path
		return nil, errors.New("prompt error still proves the daemon answered")
	})
	if !ok {
		t.Fatal("prompt control-file error was classified as an unresponsive session")
	}
	if want := filepath.Join("/mount", ".config"); got != want {
		t.Fatalf("control probe path = %q, want %q", got, want)
	}
}

func TestControlFileResponsiveWithinTimesOut(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := time.Now()
	ok := controlFileResponsiveWithin("/mount", 10*time.Millisecond, func(string) ([]byte, error) {
		<-release
		return nil, nil
	})
	if ok {
		t.Fatal("blocked control-file read was classified as responsive")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("control probe exceeded its bound: %s", elapsed)
	}
}

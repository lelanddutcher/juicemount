package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNFSMountResponsiveForcesUniqueLookup(t *testing.T) {
	var got string
	ok := nfsMountResponsiveWith("/Volumes/zpool", time.Second, func(path string) (os.FileInfo, error) {
		got = path
		return nil, os.ErrNotExist
	})
	if !ok {
		t.Fatal("ENOENT is a successful server response")
	}
	if filepath.Dir(got) != "/Volumes/zpool" || !strings.HasPrefix(filepath.Base(got), ".jm-live-probe-") {
		t.Fatalf("probe path %q is not a unique child of the mount", got)
	}
}

func TestNFSMountResponsiveRejectsIOError(t *testing.T) {
	ok := nfsMountResponsiveWith("/Volumes/zpool", time.Second, func(string) (os.FileInfo, error) {
		return nil, errors.New("input/output error")
	})
	if ok {
		t.Fatal("I/O failure must not be treated as a reusable mount")
	}
}

func TestNFSMountResponsiveIsBounded(t *testing.T) {
	release := make(chan struct{})
	started := time.Now()
	ok := nfsMountResponsiveWith("/Volumes/zpool", 5*time.Millisecond, func(string) (os.FileInfo, error) {
		<-release
		return nil, os.ErrNotExist
	})
	close(release)
	if ok {
		t.Fatal("timed-out probe must not be treated as responsive")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("probe exceeded its bound: %v", elapsed)
	}
}

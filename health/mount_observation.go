package health

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type healthMountDeps struct {
	platform   string
	executable func() (string, error)
	snapshot   func(context.Context, string) (mounttable.Snapshot, error)
	output     func(context.Context) ([]byte, error)
}

func productionHealthMountDeps() healthMountDeps {
	return healthMountDeps{runtime.GOOS, os.Executable, mounttable.SnapshotFromHelper, mounttable.Output}
}

// healthMountPresent is presence evidence only. Neither the mount source nor
// its FSID gives this monitor authority to retire or replace an instance.
func healthMountPresent(path, kind string, timeout time.Duration) (bool, error) {
	return healthMountPresentWith(path, kind, timeout, productionHealthMountDeps())
}

// The explicit dependency seam exercises the real platform selection without
// mutable package-wide observer state. Production always supplies the sibling
// bundled helper and SnapshotFromHelper's original-child/shared-gate contract.
func healthMountPresentWith(path, kind string, timeout time.Duration, deps healthMountDeps) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if deps.platform != "darwin" {
		out, err := deps.output(ctx)
		if err != nil {
			return false, fmt.Errorf("mount table query failed within %s: %w", timeout, err)
		}
		if kind == "nfs" {
			return strings.Contains(string(out), " on "+path+" ("), nil
		}
		// Preserve the other-platform FUSE text predicate exactly.
		return strings.Contains(string(out), path), nil
	}
	executable, err := deps.executable()
	if err != nil {
		return false, fmt.Errorf("native mount helper executable unavailable: %w", err)
	}
	helper := filepath.Join(filepath.Dir(executable), "juicemount-mount-probe")
	snapshot, err := deps.snapshot(ctx, helper)
	if err != nil {
		return false, fmt.Errorf("native mount snapshot failed within %s: %w", timeout, err)
	}
	return exactHealthMount(snapshot, path, kind)
}

func exactHealthMount(s mounttable.Snapshot, path, kind string) (bool, error) {
	if s.Version != 1 || strings.TrimSpace(s.BootID) == "" || s.Entries == nil {
		return false, errors.New("incomplete native mount snapshot")
	}
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		(kind != "nfs" && kind != "macfuse") {
		return false, errors.New("invalid health mount target")
	}
	found := false
	for _, entry := range s.Entries {
		if entry.Path == "" || !filepath.IsAbs(entry.Path) ||
			filepath.Clean(entry.Path) != entry.Path ||
			strings.TrimSpace(entry.Source) == "" || strings.TrimSpace(entry.Type) == "" {
			return false, errors.New("incomplete native mount entry")
		}
		if !strings.EqualFold(entry.Path, path) {
			continue
		}
		if entry.Path != path || found || entry.Type != kind || entry.FSID == [2]int32{} {
			return false, errors.New("ambiguous or foreign native mount at health target")
		}
		found = true
	}
	return found, nil
}

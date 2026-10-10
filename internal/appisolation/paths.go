package appisolation

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Paths is the explicit state namespace used by JuiceMount-controlled cache,
// FUSE diagnostics and offline recovery. HOME remains the account home.
type Paths struct {
	CacheBase     string
	FUSELog       string
	OfflineIntent string
	StallDumps    string
}

// ResolvePaths is cheap enough for cache operations: CurrentControl checks
// only the control root, marker and tmp, not a possibly wedged mounted child.
// Ordinary mode returns no paths so each caller preserves its old default.
func ResolvePaths() (Paths, bool, error) {
	scope, isolated, err := CurrentControl()
	if err != nil || !isolated {
		return Paths{}, isolated, err
	}
	p := Paths{
		CacheBase:     filepath.Join(scope.Root, ".juicefs", "cache"),
		FUSELog:       filepath.Join(scope.Root, ".juicefs", "juicefs.log"),
		OfflineIntent: filepath.Join(scope.Root, ".juicemount", "offline-intent"),
		StallDumps:    filepath.Join(scope.Root, "Library", "Logs", "JuiceMount", "stall-dumps"),
	}
	for _, item := range []struct {
		path string
		dir  bool
	}{
		{p.CacheBase, true}, {p.FUSELog, false},
		{p.OfflineIntent, false}, {p.StallDumps, true},
	} {
		if err := scope.RequirePrivateChild(item.path, item.dir); err != nil {
			return Paths{}, true, err
		}
	}
	if configured, ok := os.LookupEnv("JM_CACHE_DIR"); ok && configured != "" {
		volume := configured
		if filepath.Base(volume) == "chunks" && filepath.Base(filepath.Dir(volume)) == "raw" {
			volume = filepath.Dir(filepath.Dir(volume))
		}
		if filepath.Clean(configured) != configured || !filepath.IsAbs(configured) || filepath.Dir(volume) != p.CacheBase || filepath.Base(volume) == "." {
			return Paths{}, true, errors.New("isolated cache override conflicts with the private FUSE cache base")
		}
		if err := scope.RequirePrivateChild(configured, true); err != nil {
			return Paths{}, true, err
		}
	}
	if dump := os.Getenv("JM_INFLIGHT_DUMP_DIR"); dump != "" && dump != p.StallDumps {
		return Paths{}, true, errors.New("isolated stall dump override conflicts with the private dump directory")
	}
	return p, true, nil
}

// RequirePrivateChild rejects planted links, wrong owners and permissions on
// existing private state components. Missing components remain creatable only
// beneath a checked ancestor; creation still belongs to the caller.
func (s Scope) RequirePrivateChild(path string, directory bool) error {
	if err := s.RequireChild(path); err != nil {
		return err
	}
	for child := path; child != s.Root; child = filepath.Dir(child) {
		info, err := os.Lstat(child)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		wantDir := child != path || directory
		if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm()&0022 != 0 ||
			(wantDir && !info.IsDir()) || (!wantDir && !info.Mode().IsRegular()) {
			return fmt.Errorf("private state %s has unsafe owner, type or permissions", strings.TrimPrefix(child, s.Root+string(filepath.Separator)))
		}
	}
	return nil
}

// ValidateCachePath checks the actual selected descendant, not just the base.
// It is a no-op in ordinary mode and performs no remote or mounted-path work.
func ValidateCachePath(path string, directory bool) error {
	scope, isolated, err := CurrentControl()
	if err != nil || !isolated {
		return err
	}
	base := filepath.Join(scope.Root, ".juicefs", "cache")
	rel, err := filepath.Rel(base, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("isolated cache path escapes private cache base")
	}
	return scope.RequirePrivateChild(path, directory)
}

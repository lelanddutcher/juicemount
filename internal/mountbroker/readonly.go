package mountbroker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"golang.org/x/sys/unix"
)

var ErrJournalBusy = errors.New("mount journal retirement still in flight")

// LocalJournalDirectory names the existing per-user journal without creating
// it. Recovery observation must never provision or claim a mount transaction.
func LocalJournalDirectory() (string, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", err
	}
	if isolated {
		return filepath.Join(scope.Root, "mount-transactions"), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "JuiceMount", "mount-transactions"), nil
}

// ReadRecordReadonly holds the existing shared journal lock while validating
// the canonical leaf and syncing its directory. It never creates a lock or
// rewrites the record. Busy/uncertain custody is an error, never absence.
func ReadRecordReadonly(directory, mountPath string) (*Record, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || !filepath.IsAbs(mountPath) || filepath.Clean(mountPath) != mountPath {
		return nil, errors.New("invalid journal path")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	if err := privateFile(dir, true); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(strings.ToLower(mountPath)))
	name := hex.EncodeToString(sum[:]) + ".json"
	j := &journal{dir: dir, name: name}
	lock, err := privatefile.OpenAt(dir, name+".lock", os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		record, readErr := j.read()
		if readErr != nil {
			return nil, readErr
		}
		if record != nil {
			return nil, errors.New("mount journal exists without custody lock")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := privateFile(lock, false); err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return nil, ErrJournalBusy
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	record, err := j.read()
	if err != nil {
		return nil, err
	}
	if record != nil && record.State == Retired {
		if err := dir.Sync(); err != nil {
			return nil, err
		}
	}
	return record, nil
}

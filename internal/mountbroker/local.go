package mountbroker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// LocalCreator uses a private per-user journal shared by GUI and CLI. The
// bundled observer runs outside the host process. This is creation evidence;
// it does not upgrade this user's file into a privileged broker receipt.
func LocalCreator() (Creator, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return Creator{}, err
	}
	var directory string
	if isolated {
		directory = filepath.Join(scope.Root, "mount-transactions")
		if err := scope.RequireChild(directory); err != nil {
			return Creator{}, err
		}
	} else {
		base, err := os.UserConfigDir()
		if err != nil {
			return Creator{}, err
		}
		directory = filepath.Join(base, "JuiceMount", "mount-transactions")
	}
	if err := ensureDirectory(directory); err != nil {
		return Creator{}, err
	}
	executable, err := os.Executable()
	if err != nil {
		return Creator{}, err
	}
	helper := filepath.Join(filepath.Dir(executable), "juicemount-mount-probe")
	return Creator{Directory: directory, Observe: func(parent context.Context) (mounttable.Snapshot, error) {
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		defer cancel()
		return mounttable.SnapshotFromHelper(ctx, helper)
	}}, nil
}

// Flush newly created directory entries from the leaf through the first
// existing parent before any intent can authorize starting a mount command.
func ensureDirectory(path string) error {
	err := os.Mkdir(path, 0700)
	if errors.Is(err, os.ErrNotExist) {
		if err := ensureDirectory(filepath.Dir(path)); err != nil {
			return err
		}
		err = os.Mkdir(path, 0700)
	}
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

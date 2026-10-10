package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
)

// Tests may supply an already-private short socket namespace without changing
// the account's HOME. Production always derives this from servicecontrol.
var selectedOwnerServiceSocketPath = servicecontrol.SocketPath

// selectedOwnerScope is only the namespace for owner intent, lifetime, and
// control records. Normal serving retains its existing data paths and HOME.
// The service listener creates its private parent before Start; an unexpected
// or insecure parent is a HOLD, never permission to use the engine's FUSE.
func selectedOwnerScope() (appisolation.Scope, bool, error) {
	return selectedOwnerScopeWith(appisolation.Current)
}

// selectedOwnerControlScope locates only durable control records. It cannot
// be used for owner selection, Start, or mount admission.
func selectedOwnerControlScope() (appisolation.Scope, bool, error) {
	return selectedOwnerScopeWith(appisolation.CurrentControl)
}

func selectedOwnerScopeWith(current func() (appisolation.Scope, bool, error)) (appisolation.Scope, bool, error) {
	scope, isolated, err := current()
	if err != nil || isolated {
		return scope, isolated, err // malformed opt-in never falls back
	}
	servicePath, err := selectedOwnerServiceSocketPath()
	if err != nil {
		return appisolation.Scope{}, false, err
	}
	root := filepath.Dir(servicePath)
	if root == "" || root == "." || root == "/" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return appisolation.Scope{}, false, errors.New("owner control namespace is not canonical")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return appisolation.Scope{}, false, errors.New("owner control namespace has missing or linked components")
	}
	st, err := os.Lstat(root)
	if err != nil {
		return appisolation.Scope{}, false, err
	}
	uid, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 || int(uid.Uid) != os.Geteuid() {
		return appisolation.Scope{}, false, errors.New("owner control namespace is not an owned private directory")
	}
	return appisolation.Scope{Root: root}, false, nil
}

func selectedOwnerMode() (bool, error) {
	if !servingProcessMode.Load() {
		return false, nil // local CLI keeps its existing manager and Link path
	}
	_, _, err := selectedOwnerScope()
	return err == nil, err
}

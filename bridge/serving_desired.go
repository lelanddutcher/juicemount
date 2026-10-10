package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/lelanddutcher/juicemount/internal/version"
	"golang.org/x/sys/unix"
)

const servingDesiredLeaf = "serving-desired.json"
const servingDesiredLimit = 64 << 10

type servingDesiredRecord struct {
	Version   int                 `json:"version"`
	Build     string              `json:"build"`
	Root      string              `json:"root"`
	Revision  uint64              `json:"revision"`
	Desired   string              `json:"desired"` // pending, running, stopped
	Config    ServerConfig        `json:"config"`
	Owner     volumeOwnerIntent   `json:"owner"`
	NFSPhase  string              `json:"nfs_phase"` // none, pending, bound
	NFSRecord *mountbroker.Record `json:"nfs_record,omitempty"`
}

// This mutex never takes globalLifecycleMu. NFS completion may publish while
// Stop holds that lock and waits for the original mount flight to join.
var servingDesiredMu sync.Mutex
var servingDesiredActiveRevision uint64

func readServingDesired() (servingDesiredRecord, error) {
	scope, _, err := selectedOwnerControlScope()
	if err != nil {
		return servingDesiredRecord{}, err
	}
	if err := scope.RequireChild(filepath.Join(scope.Root, servingDesiredLeaf)); err != nil {
		return servingDesiredRecord{}, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return servingDesiredRecord{}, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, servingDesiredLeaf, os.O_RDONLY, 0)
	if err != nil {
		return servingDesiredRecord{}, err
	}
	defer f.Close()
	if err := privateOwnerRecord(f, servingDesiredLimit); err != nil {
		return servingDesiredRecord{}, err
	}
	data, err := io.ReadAll(io.LimitReader(f, servingDesiredLimit+1))
	if err != nil || len(data) > servingDesiredLimit {
		return servingDesiredRecord{}, errors.New("desired record unreadable or too large")
	}
	var r servingDesiredRecord
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return servingDesiredRecord{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return servingDesiredRecord{}, errors.New("desired record trailing data")
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(data, canonical) {
		return servingDesiredRecord{}, errors.New("desired record is not canonical")
	}
	// A record from another build is refused unless it owns nothing: a durable
	// Stop with no NFS phase and no owner. Refusing that case made every app
	// update unable to Start until the file was removed by hand (H-13); the
	// next write restamps it with this build. Anything that owns a mount
	// still requires the exact build.
	staleStopped := r.Build != version.Commit && r.Build != "" && r.Desired == "stopped" &&
		r.NFSPhase == "none" && r.NFSRecord == nil && r.Owner == (volumeOwnerIntent{})
	if r.Version != 1 || (r.Build != version.Commit && !staleStopped) || r.Root != scope.Root || r.Revision == 0 ||
		(r.Desired != "pending" && r.Desired != "running" && r.Desired != "recovering" && r.Desired != "stopped") ||
		(r.NFSPhase != "none" && r.NFSPhase != "pending" && r.NFSPhase != "bound") {
		return servingDesiredRecord{}, errors.New("desired record identity or state unavailable")
	}
	if r.Desired == "running" || r.Desired == "recovering" {
		if err := validateOwnerIntent(r.Owner, scope.Root); err != nil {
			return servingDesiredRecord{}, err
		}
	}
	if r.NFSPhase == "bound" && (r.NFSRecord == nil || r.NFSRecord.OperationID == "") {
		return servingDesiredRecord{}, errors.New("bound NFS operation unavailable")
	}
	return r, nil
}

// Caller owns servingDesiredMu. An uncertain write is a HOLD, including
// sync/rename failures; no caller may acknowledge an effect after that error.
func writeServingDesired(r servingDesiredRecord) error {
	scope, _, err := selectedOwnerControlScope()
	if err != nil {
		return err
	}
	if err := scope.RequireChild(filepath.Join(scope.Root, servingDesiredLeaf)); err != nil {
		return err
	}
	r.Version, r.Build, r.Root = 1, version.Commit, scope.Root
	r.Config = persistableServerConfig(r.Config) // never a secret on disk
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > servingDesiredLimit {
		return errors.New("desired record too large")
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return err
	}
	defer dir.Close()
	if old, err := privatefile.OpenAt(dir, servingDesiredLeaf, os.O_RDONLY, 0); err == nil {
		check := privateOwnerRecord(old, servingDesiredLimit)
		_ = old.Close()
		if check != nil {
			return check
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	leaf := fmt.Sprintf(".serving-desired-%d-%d", os.Getpid(), r.Revision)
	f, err := privatefile.OpenAt(dir, leaf, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(int(dir.Fd()), leaf, 0)
	if err := privateOwnerRecord(f, 0); err != nil {
		_ = f.Close()
		return err
	}
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := unix.Renameat(int(dir.Fd()), leaf, int(dir.Fd()), servingDesiredLeaf); err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	return nil
}

// Called with globalLifecycleMu held before Start side effects. The requested
// config is the typed pre-default value, never a runtime-rewritten endpoint.
func beginServingDesired(cfg ServerConfig) (uint64, error) {
	if !servingProcessMode.Load() {
		return 0, nil
	}
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	old, err := readServingDesired()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if old.Revision == ^uint64(0) {
		return 0, errors.New("desired revision exhausted")
	}
	r := servingDesiredRecord{Revision: old.Revision + 1, Desired: "pending", Config: cfg, NFSPhase: "none"}
	if err := writeServingDesired(r); err != nil {
		return 0, err
	}
	servingDesiredActiveRevision = r.Revision
	return r.Revision, nil
}

func updateServingDesired(revision uint64, mutate func(*servingDesiredRecord) error) error {
	if revision == 0 {
		return nil
	}
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	r, err := readServingDesired()
	if err != nil {
		return err
	}
	if r.Revision != revision || r.Desired == "stopped" {
		return errors.New("desired revision superseded")
	}
	if err := mutate(&r); err != nil {
		return err
	}
	return writeServingDesired(r)
}

func confirmServingDesired(revision uint64) error {
	return updateServingDesired(revision, func(r *servingDesiredRecord) error {
		if err := validateOwnerIntent(r.Owner, r.Root); err != nil {
			return err
		}
		if r.Desired != "pending" && r.Desired != "recovering" {
			return errors.New("serving Start was not pending")
		}
		r.Desired = "running"
		return nil
	})
}

// A replacement attempt is durable before it can attach to the owner. An
// interrupted replacement remains held; startup never repeats it blindly.
func admitServingRecovery(expected servingDesiredRecord) (servingDesiredRecord, error) {
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	r, err := readServingDesired()
	if err != nil {
		return servingDesiredRecord{}, err
	}
	if r.Revision != expected.Revision || r.Desired != "running" || r.Config != expected.Config || r.Owner != expected.Owner || r.NFSPhase != expected.NFSPhase {
		return servingDesiredRecord{}, errors.New("serving recovery superseded")
	}
	r.Desired = "recovering"
	if err := writeServingDesired(r); err != nil {
		return servingDesiredRecord{}, err
	}
	servingDesiredActiveRevision = r.Revision
	return r, nil
}

func stopServingDesired() (uint64, error) {
	if !servingProcessMode.Load() {
		return 0, nil
	}
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	old, err := readServingDesired()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if old.Revision == ^uint64(0) {
		return 0, errors.New("desired revision exhausted")
	}
	old.Revision++
	if old.NFSPhase == "" {
		old.NFSPhase = "none"
	}
	old.Desired = "stopped"
	if err := writeServingDesired(old); err != nil {
		return 0, err
	}
	servingDesiredActiveRevision = 0
	return old.Revision, nil
}

func boundServingNFSRevision() uint64 {
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	return servingDesiredActiveRevision
}

func stoppedServingRevision(revision uint64) bool {
	if revision == 0 {
		return false
	}
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	r, err := readServingDesired()
	return err == nil && r.Revision == revision && r.Desired == "stopped"
}

func markServingNFSPending(revision uint64) error {
	if revision == 0 && servingProcessMode.Load() {
		return errors.New("serving NFS has no admitted desired revision")
	}
	return updateServingDesired(revision, func(r *servingDesiredRecord) error {
		r.NFSPhase, r.NFSRecord = "pending", nil
		return nil
	})
}

func markServingNFSBound(revision uint64, record *mountbroker.Record) error {
	if revision == 0 && !servingProcessMode.Load() {
		return nil
	}
	if record == nil || record.OperationID == "" {
		return errors.New("NFS arm did not yield original operation")
	}
	return updateServingDesired(revision, func(r *servingDesiredRecord) error {
		if r.NFSPhase != "pending" {
			return errors.New("NFS operation was not durably pending")
		}
		copy := *record
		r.NFSPhase, r.NFSRecord = "bound", &copy
		return nil
	})
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"

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
	r, err := readServingDesiredRecord(scope, servingDesiredLeaf)
	if err != nil {
		return servingDesiredRecord{}, err
	}
	if r.Build != version.Commit {
		if r.Desired != "stopped" {
			return servingDesiredRecord{}, errors.New("legacy desired state is not stopped")
		}
		// Preserve H-13's never-owned stopped record. A previously mounted
		// record needs terminal proof, not merely a stopped label.
		if r.Owner != (volumeOwnerIntent{}) || r.NFSPhase != "none" || r.NFSRecord != nil {
			l, err := acquireOwnerLifetime(scope)
			if err != nil {
				return servingDesiredRecord{}, err
			}
			defer l.close()
			if err := validateRetiredServingRecord(l, r, r.Build); err != nil {
				return servingDesiredRecord{}, err
			}
		}
	} else if r.Desired == "running" || r.Desired == "recovering" {
		if err := validateOwnerIntent(r.Owner, scope.Root); err != nil {
			return servingDesiredRecord{}, err
		}
	}
	return r, nil
}

// Read-only canonical parser shared with immutable predecessor archives.
func readServingDesiredRecord(scope appisolation.Scope, leaf string) (servingDesiredRecord, error) {
	if err := scope.RequireChild(filepath.Join(scope.Root, leaf)); err != nil {
		return servingDesiredRecord{}, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return servingDesiredRecord{}, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, leaf, os.O_RDONLY, 0)
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
	if r.Version != 1 || r.Build == "" || r.Root != scope.Root || r.Revision == 0 ||
		(r.Desired != "pending" && r.Desired != "running" && r.Desired != "recovering" && r.Desired != "stopped") ||
		(r.NFSPhase != "none" && r.NFSPhase != "pending" && r.NFSPhase != "bound") {
		return servingDesiredRecord{}, errors.New("desired record identity or state unavailable")
	}
	if r.NFSPhase == "none" && r.NFSRecord != nil {
		return servingDesiredRecord{}, errors.New("unbound desired record retains contradictory NFS authority")
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
	// Repeated Stop of an already retired legacy intent keeps its provenance.
	// Only a fresh owner-free Start supersedes it with this build.
	writeBuild := version.Commit
	if r.Owner != (volumeOwnerIntent{}) && r.Owner.Build != version.Commit {
		if r.Desired != "stopped" || r.Build != r.Owner.Build {
			return errors.New("legacy owner cannot become current active intent")
		}
		writeBuild = r.Build
	}
	r.Version, r.Build, r.Root = 1, writeBuild, scope.Root
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
	previous, previousErr := readServingDesiredRecord(scope, servingDesiredLeaf)
	if previousErr == nil && previous.Build != version.Commit && previous.Owner != (volumeOwnerIntent{}) {
		fence, err := acquireOwnerLifetime(scope)
		if err != nil {
			return err
		}
		defer fence.close()
		if err := validateRetiredServingRecord(fence, previous, previous.Build); err != nil {
			return err
		}
		if r.Build == previous.Build {
			unchanged := r
			unchanged.Revision = previous.Revision
			if r.Revision != previous.Revision+1 || !reflect.DeepEqual(unchanged, previous) {
				return errors.New("legacy stopped provenance changed")
			}
		} else {
			if r.Desired != "pending" || r.Owner != (volumeOwnerIntent{}) || r.NFSPhase != "none" || r.NFSRecord != nil || r.Revision != previous.Revision+1 {
				return errors.New("legacy predecessor requires a fresh Start")
			}
			if err := archiveRetiredServingPredecessor(fence, previous); err != nil {
				return err
			}
		}
	} else if previousErr != nil && !errors.Is(previousErr, os.ErrNotExist) {
		return previousErr
	}
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

func retiredServingDesiredLeaf(i volumeOwnerIntent) string {
	return "serving-retired-" + i.Instance + ".json"
}

// The caller retains this fence through its separately approved service
// handoff. This function does not mutate records, exit or unregister a service.
// Return unchanged provenance; never reacquire the caller's lifetime flock.
func validateRetiredServingForUpgrade(l *ownerLifetime, expectedBuild string) (servingDesiredRecord, error) {
	if l == nil || l.lock == nil || l.dir == nil {
		return servingDesiredRecord{}, errors.New("upgrade lifetime fence unavailable")
	}
	r, err := readServingDesiredRecord(l.scope, servingDesiredLeaf)
	if err != nil {
		return servingDesiredRecord{}, err
	}
	if err := validateRetiredServingRecord(l, r, expectedBuild); err != nil {
		return servingDesiredRecord{}, err
	}
	return r, nil
}

func validateRetiredServingRecord(l *ownerLifetime, r servingDesiredRecord, expectedBuild string) error {
	if l == nil || l.lock == nil || l.dir == nil || expectedBuild == "" || r.Build != expectedBuild || r.Root != l.scope.Root || r.Desired != "stopped" || r.NFSPhase == "pending" {
		return errors.New("upgrade lacks exact stopped predecessor and lifetime fence")
	}
	if r.Owner == (volumeOwnerIntent{}) {
		if r.NFSPhase != "none" || r.NFSRecord != nil {
			return errors.New("unowned predecessor has NFS authority")
		}
		if _, err := readOwnerIntentRecord(l.scope); !errors.Is(err, os.ErrNotExist) {
			return errors.New("unowned predecessor has selected or unknown owner")
		}
	} else {
		digest, err := servingConfigDigest(r.Config)
		if err != nil || digest != r.Owner.ConfigDigest || r.Owner.Build != r.Build {
			return errors.New("legacy owner config or build changed")
		}
		if err := confirmOwnerRetiredUnderLifetime(l, r.Owner, expectedBuild); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	retired, err := priorServingShutdownNFSRetired(ctx, r)
	if err != nil || !retired {
		return errors.New("legacy canonical NFS retirement or absence unconfirmed")
	}
	return nil
}

// Called only for a fresh Start while its exact predecessor's lifetime is
// fenced. Never replace an existing archive, including an uncertain partial
// publication. Stop revisions before that Start do not create an archive.
func archiveRetiredServingPredecessor(l *ownerLifetime, previous servingDesiredRecord) error {
	if l == nil || l.lock == nil || l.dir == nil || previous.Owner == (volumeOwnerIntent{}) {
		return errors.New("retired predecessor archive lacks lifetime fence")
	}
	b, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	leaf := retiredServingDesiredLeaf(previous.Owner)
	f, err := privatefile.OpenAt(l.dir, leaf, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		old, readErr := privatefile.OpenAt(l.dir, leaf, os.O_RDONLY, 0)
		if readErr != nil {
			return readErr
		}
		defer old.Close()
		if err := privateOwnerRecord(old, servingDesiredLimit); err != nil {
			return err
		}
		got, err := io.ReadAll(io.LimitReader(old, servingDesiredLimit+1))
		if err != nil || !bytes.Equal(got, b) {
			return errors.New("immutable stopped predecessor differs")
		}
		if err := old.Sync(); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		n, writeErr := f.Write(b)
		if writeErr == nil && n != len(b) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = f.Sync()
		}
		if closeErr := f.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			return writeErr
		}
	}
	return l.dir.Sync()
}

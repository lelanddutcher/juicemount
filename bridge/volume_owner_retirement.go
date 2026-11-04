package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	"golang.org/x/sys/unix"
)

// This stable leaf is never unlinked or replaced. Unlike the control socket
// lock, it remains held through durable terminal publication after handlers join.
const ownerLifetimeLeaf = "volume-owner-lifetime.lock"

type ownerLifetime struct {
	scope         appisolation.Scope
	dir, lock     *os.File
	syncDirectory func() error // nil uses the real pinned directory; per-instance fault seam
}

func (l *ownerLifetime) syncPublicationDirectory() error {
	if l.syncDirectory != nil {
		return l.syncDirectory()
	}
	return l.dir.Sync()
}

type ownerRetirement struct {
	Version    int               `json:"version"`
	Intent     volumeOwnerIntent `json:"intent"`
	ShutdownID string            `json:"shutdown_id"`
}

func ownerShutdownID(i volumeOwnerIntent) string {
	b, _ := json.Marshal(i)
	sum := sha256.Sum256(append([]byte("JuiceMount/volume-owner/shutdown/v1\x00"), b...))
	return hex.EncodeToString(sum[:16])
}

func ownerRetirementLeaf(i volumeOwnerIntent) string {
	return "volume-owner-retired-" + i.Instance + ".json"
}

func privateOwnerRecord(f *os.File, limit int64) error {
	s, err := f.Stat()
	if err != nil {
		return err
	}
	u, ok := s.Sys().(*syscall.Stat_t)
	if !ok || !s.Mode().IsRegular() || s.Mode().Perm() != 0600 || u.Nlink != 1 || int(u.Uid) != os.Geteuid() || s.Size() > limit {
		return errors.New("owner record is not private, regular and singly linked")
	}
	return nil
}

func acquireOwnerLifetime(scope appisolation.Scope) (*ownerLifetime, error) {
	if err := scope.RequireChild(filepath.Join(scope.Root, ownerLifetimeLeaf)); err != nil {
		return nil, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return nil, err
	}
	f, err := privatefile.OpenAt(dir, ownerLifetimeLeaf, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		_ = dir.Close()
		return nil, err
	}
	fail := func(err error) (*ownerLifetime, error) { _ = f.Close(); _ = dir.Close(); return nil, err }
	if err := privateOwnerRecord(f, 0); err != nil {
		return fail(err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("original owner lifetime remains held: %w", err))
	}
	return &ownerLifetime{scope: scope, dir: dir, lock: f}, nil
}

func (l *ownerLifetime) close() {
	if l == nil {
		return
	}
	if l.lock != nil {
		_ = l.lock.Close()
		l.lock = nil
	}
	if l.dir != nil {
		_ = l.dir.Close()
		l.dir = nil
	}
}

func readOwnerRetirement(scope appisolation.Scope, i volumeOwnerIntent) (ownerRetirement, error) {
	return readOwnerRetirementForBuild(scope, i, version.Commit)
}

func readOwnerRetirementForBuild(scope appisolation.Scope, i volumeOwnerIntent, build string) (ownerRetirement, error) {
	var got ownerRetirement
	if err := validateOwnerIntentForBuild(i, scope.Root, build); err != nil {
		return got, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return got, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, ownerRetirementLeaf(i), os.O_RDONLY, 0)
	if err != nil {
		return got, err
	}
	defer f.Close()
	if err := privateOwnerRecord(f, 8192); err != nil {
		return got, err
	}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&got); err != nil {
		return got, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return got, errors.New("owner retirement has trailing data")
	}
	if got.Version != 1 || got.Intent != i || got.ShutdownID != ownerShutdownID(i) {
		return got, errors.New("retirement belongs to another exact owner intent")
	}
	return got, nil
}

// atomicRecord never exposes partial JSON. A failed write leaves only a private
// staging leaf; a failed directory sync leaves uncertainty with the caller's
// lifetime lock still held. The caller must not release that fence on failure.
func (l *ownerLifetime) atomicRecord(leaf string, value any) error {
	if l == nil || l.lock == nil || l.dir == nil {
		return errors.New("owner lifetime fence unavailable")
	}
	id, err := randomOwnerID()
	if err != nil {
		return err
	}
	tmp := ".volume-owner-publish-" + id
	f, err := privatefile.OpenAt(l.dir, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err == nil {
		var n int
		n, err = f.Write(b)
		if err == nil && n != len(b) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err := unix.Renameat(int(l.dir.Fd()), tmp, int(l.dir.Fd()), leaf); err != nil {
		return err
	}
	return l.syncPublicationDirectory()
}

// Only the original owner calls this after terminal admission, all physical
// resources and operations settle, and the control transport joins. Repetition
// may finish durability of the same exact record, never authorize new work.
func (l *ownerLifetime) publishRetired(i volumeOwnerIntent, shutdownID string) error {
	if l == nil || l.lock == nil || shutdownID != ownerShutdownID(i) {
		return errors.New("invalid terminal owner publication")
	}
	current, err := readOwnerIntent(l.scope)
	if err != nil || current != i {
		return errors.New("selected owner intent changed before terminal publication")
	}
	if _, err := readOwnerRetirement(l.scope, i); err == nil {
		f, err := privatefile.OpenAt(l.dir, ownerRetirementLeaf(i), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		err = f.Sync()
		_ = f.Close()
		if err != nil {
			return err
		}
		return l.syncPublicationDirectory()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return l.atomicRecord(ownerRetirementLeaf(i), ownerRetirement{Version: 1, Intent: i, ShutdownID: shutdownID})
}

// Resource-terminal proof for a replacement engine. It deliberately does not
// claim exec.Cmd.Wait: only the creating engine can join that original handle.
func confirmOwnerRetired(i volumeOwnerIntent) error {
	scope := appisolation.Scope{Root: i.Root}
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		return err
	}
	defer l.close()
	return confirmOwnerRetiredUnderLifetime(l, i, version.Commit)
}

// An explicit old build proves retirement only. It cannot claim an owner or
// attach to an old socket, and does not relax the active current-build guard.
func confirmOwnerRetiredUnderLifetime(l *ownerLifetime, i volumeOwnerIntent, build string) error {
	if l == nil || l.lock == nil || l.dir == nil || l.scope.Root != i.Root {
		return errors.New("owner retirement lifetime fence unavailable")
	}
	if err := validateOwnerIntentForBuild(i, l.scope.Root, build); err != nil {
		return err
	}
	current, err := readOwnerIntentRecord(l.scope)
	if err != nil || current != i {
		return errors.New("owner intent changed while confirming retirement")
	}
	_, err = readOwnerRetirementForBuild(l.scope, i, build)
	return err
}

// Called with servingOwnerSelection locked. A replacement engine has no
// original child handle; it relies on resource-terminal proof, never a fake Wait.
func joinSelectedOwnerRetirement(i volumeOwnerIntent) error {
	if servingOwnerSelection.childResult == nil {
		return nil
	}
	if servingOwnerSelection.intent != i {
		return errors.New("original owner waiter belongs to another selection")
	}
	select {
	case <-servingOwnerSelection.childResult.done:
		if servingOwnerSelection.childResult.err != nil {
			return fmt.Errorf("original owner exit failed: %w", servingOwnerSelection.childResult.err)
		}
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("original owner waiter has not joined")
	}
}

func (f *ownerFacade) retire() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// A previous request may already have drained control. The durable exact
	// record plus lifetime fence is the replacement engine's reconciliation.
	if err := confirmOwnerRetired(f.intent); err != nil {
		resp, callErr := servicecontrol.Call(ctx, f.client.path, servicecontrol.Request{
			Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "shutdown",
			Strings: []string{f.intent.Instance, ownerShutdownID(f.intent), f.intent.ConfigDigest}})
		if callErr == nil && resp.Error != "" {
			return errors.New(resp.Error)
		}
		for {
			if err := confirmOwnerRetired(f.intent); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return errors.New("original owner terminal publication or lifetime settlement remains pending")
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	servingOwnerSelection.Lock()
	defer servingOwnerSelection.Unlock()
	if err := joinSelectedOwnerRetirement(f.intent); err != nil {
		return err
	}
	if servingOwnerSelection.selected && servingOwnerSelection.intent != f.intent {
		return errors.New("serving owner selection changed during terminal settlement")
	}
	servingOwnerSelection.intent = volumeOwnerIntent{}
	servingOwnerSelection.selected = false
	servingOwnerSelection.child, servingOwnerSelection.childResult = nil, nil
	return nil
}

// Existing active intent is always returned unchanged. A new identity requires
// both the exact predecessor's durable terminal record and its lifetime fence.
func createOwnerIntent(scope appisolation.Scope, cfg ServerConfig) (volumeOwnerIntent, bool, error) {
	old, readErr := readOwnerIntent(scope)
	if readErr == nil {
		if _, err := readOwnerRetirement(scope, old); errors.Is(err, os.ErrNotExist) {
			return createOwnerIntentUnfenced(scope, cfg)
		} else if err != nil {
			return volumeOwnerIntent{}, false, err
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return createHistoricalRetiredOwnerSuccessor(scope, cfg, readErr)
	}
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	defer l.close()
	current, err := readOwnerIntent(scope)
	if errors.Is(err, os.ErrNotExist) && errors.Is(readErr, os.ErrNotExist) {
		return createOwnerIntentUnfenced(scope, cfg)
	}
	if err != nil || readErr != nil || current != old {
		return volumeOwnerIntent{}, false, errors.New("selected intent changed during successor selection")
	}
	if _, err := readOwnerRetirement(scope, old); err != nil {
		return volumeOwnerIntent{}, false, err
	}
	next, err := intendedOwner(cfg, scope)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	if err := l.atomicRecord(filepath.Base(ownerIntentPath(scope.Root)), next); err != nil {
		return volumeOwnerIntent{}, false, fmt.Errorf("successor publication uncertain; retain selected intent: %w", err)
	}
	return next, true, nil
}

// Historical succession requires both the archived stopped predecessor and a
// current fresh Start. Recheck every terminal proof under the SAME fence that
// publishes the successor. Never adopt a live old-build owner.
func createHistoricalRetiredOwnerSuccessor(scope appisolation.Scope, cfg ServerConfig, originalErr error) (volumeOwnerIntent, bool, error) {
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	defer l.close()
	old, err := readOwnerIntentRecord(scope)
	if err != nil || old.Build == version.Commit {
		return volumeOwnerIntent{}, false, originalErr
	}
	current, err := readServingDesiredRecord(scope, servingDesiredLeaf)
	if err != nil || current.Build != version.Commit || current.Desired != "pending" ||
		current.Owner != (volumeOwnerIntent{}) || current.NFSPhase != "none" || current.NFSRecord != nil {
		return volumeOwnerIntent{}, false, errors.New("legacy succession lacks current fresh Start admission")
	}
	wantDigest, err := servingConfigDigest(cfg)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	currentDigest, err := servingConfigDigest(current.Config)
	if err != nil || currentDigest != wantDigest {
		return volumeOwnerIntent{}, false, errors.New("fresh Start configuration changed before legacy succession")
	}
	previous, err := readServingDesiredRecord(scope, retiredServingDesiredLeaf(old))
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	if previous.Owner != old || current.Revision <= previous.Revision {
		return volumeOwnerIntent{}, false, errors.New("legacy stopped archive belongs to another admission")
	}
	if err := validateRetiredServingRecord(l, previous, old.Build); err != nil {
		return volumeOwnerIntent{}, false, err
	}
	next, err := intendedOwner(cfg, scope)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	if err := l.atomicRecord(filepath.Base(ownerIntentPath(scope.Root)), next); err != nil {
		return volumeOwnerIntent{}, false, fmt.Errorf("successor publication uncertain; retain selected intent: %w", err)
	}
	return next, true, nil
}

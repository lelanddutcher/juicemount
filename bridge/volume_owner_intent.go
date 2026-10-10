package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// The intent is selected and fsynced before owner birth. An engine that loses
// its reply must inspect this exact instance and original IDs. Absence of the
// socket with an existing intent is HOLD, never permission to spawn another.
type volumeOwnerIntent struct {
	Version        int    `json:"version"`
	Build          string `json:"build"`
	Root           string `json:"root"`
	Instance       string `json:"instance"`
	ConfigDigest   string `json:"config_digest"`
	ClaimID        string `json:"claim_id"`
	MountID        string `json:"mount_id"`
	StartupProbeID string `json:"startup_probe_id"`
}

var servingOwnerSelection struct {
	sync.Mutex
	intent      volumeOwnerIntent
	selected    bool
	child       *exec.Cmd
	childResult *ownerChildResult
}

type ownerChildResult struct {
	done chan struct{}
	err  error // published before done closes; never waited a second time
}

func ownerIntentPath(root string) string { return filepath.Join(root, "volume-owner-intent.json") }

// ExtraFiles descriptors arrive in blocking mode. NewFile only registers an
// inherited descriptor with Go's poller when O_NONBLOCK is already set, so set
// it before constructing the File that must enforce the bootstrap deadline.
// The pipe type is a format check; the transferred nonce still proves selection.
func originalOwnerBootstrap(fd string) (*os.File, error) {
	if fd == "" {
		return nil, nil
	}
	if fd != "3" {
		return nil, errors.New("unsupported owner bootstrap descriptor")
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(3, &st); err != nil {
		return nil, err
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		return nil, errors.New("owner bootstrap descriptor is not a pipe")
	}
	if err := syscall.SetNonblock(3, true); err != nil {
		return nil, err
	}
	return os.NewFile(3, "original-owner-bootstrap"), nil
}

func randomOwnerID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func intendedOwner(cfg ServerConfig, scope appisolation.Scope) (volumeOwnerIntent, error) {
	digest, err := servingConfigDigest(cfg)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	i := volumeOwnerIntent{Version: 1, Build: version.Commit, Root: scope.Root,
		ConfigDigest: digest}
	for _, dst := range []*string{&i.Instance, &i.ClaimID, &i.MountID, &i.StartupProbeID} {
		*dst, err = randomOwnerID()
		if err != nil {
			return volumeOwnerIntent{}, err
		}
	}
	return i, nil
}

func validateOwnerIntent(i volumeOwnerIntent, root string) error {
	if i.Version != 1 || i.Build != version.Commit || i.Root != root || len(i.ConfigDigest) != 64 {
		return errors.New("owner intent build, root, or configuration unavailable")
	}
	if b, err := hex.DecodeString(i.ConfigDigest); err != nil || len(b) != 32 || hex.EncodeToString(b) != i.ConfigDigest {
		return errors.New("owner intent has invalid selected configuration digest")
	}
	seen := make(map[string]struct{})
	for _, id := range []string{i.Instance, i.ClaimID, i.MountID, i.StartupProbeID} {
		b, err := hex.DecodeString(id)
		if err != nil || len(b) != 16 || hex.EncodeToString(b) != id {
			return errors.New("owner intent has invalid original identity")
		}
		if _, duplicate := seen[id]; duplicate {
			return errors.New("owner intent reuses an original operation identity")
		}
		seen[id] = struct{}{}
	}
	return nil
}

// A retained file selects which original process may exist; it cannot mint a
// successor. Only the creator's inherited descriptor transfers its nonce.
func authorizeOriginalOwner(scope appisolation.Scope, bootstrapFD string, pipe io.ReadCloser) (volumeOwnerIntent, bool, error) {
	intent, err := readOwnerIntent(scope)
	if errors.Is(err, os.ErrNotExist) && bootstrapFD == "" {
		return volumeOwnerIntent{}, false, nil
	}
	if err != nil {
		return volumeOwnerIntent{}, false, fmt.Errorf("selected owner intent unavailable: %w", err)
	}
	if bootstrapFD != "3" || pipe == nil {
		return volumeOwnerIntent{}, false, errors.New("retained owner intent cannot authorize a replacement child")
	}
	defer pipe.Close()
	if deadline, ok := pipe.(interface{ SetReadDeadline(time.Time) error }); ok {
		if err := deadline.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return volumeOwnerIntent{}, false, err
		}
	} else {
		return volumeOwnerIntent{}, false, errors.New("bootstrap descriptor cannot be bounded")
	}
	var nonce [32]byte
	if _, err := io.ReadFull(pipe, nonce[:]); err != nil {
		return volumeOwnerIntent{}, false, err
	}
	var trailing [1]byte
	if n, err := pipe.Read(trailing[:]); n != 0 || err != io.EOF {
		return volumeOwnerIntent{}, false, errors.New("original owner bootstrap has trailing data")
	}
	if string(nonce[:]) != intent.Instance {
		return volumeOwnerIntent{}, false, errors.New("original creator did not transfer selected owner nonce")
	}
	return intent, true, nil
}

func readOwnerIntent(scope appisolation.Scope) (volumeOwnerIntent, error) {
	path := ownerIntentPath(scope.Root)
	if err := scope.RequireChild(path); err != nil {
		return volumeOwnerIntent{}, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, filepath.Base(path), os.O_RDONLY, 0)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || sys.Nlink != 1 || int(sys.Uid) != os.Geteuid() || st.Size() > 4096 {
		return volumeOwnerIntent{}, errors.New("owner intent is not a private singly linked record")
	}
	var i volumeOwnerIntent
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&i); err != nil {
		return volumeOwnerIntent{}, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return volumeOwnerIntent{}, errors.New("owner intent has trailing data")
	}
	if err := validateOwnerIntent(i, scope.Root); err != nil {
		return volumeOwnerIntent{}, err
	}
	return i, nil
}

func createOwnerIntentUnfenced(scope appisolation.Scope, cfg ServerConfig) (volumeOwnerIntent, bool, error) {
	if i, err := readOwnerIntent(scope); err == nil {
		want, makeErr := intendedOwner(cfg, scope)
		if makeErr != nil {
			return volumeOwnerIntent{}, false, makeErr
		}
		if i.ConfigDigest != want.ConfigDigest {
			return volumeOwnerIntent{}, false, errors.New("selected owner config changed; retained intent cannot be rebound")
		}
		return i, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return volumeOwnerIntent{}, false, err
	}
	i, err := intendedOwner(cfg, scope)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	path := ownerIntentPath(scope.Root)
	dir, err := os.Open(scope.Root)
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, filepath.Base(path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return volumeOwnerIntent{}, false, errors.New("owner intent raced; inspect original, never replace")
	}
	if err != nil {
		return volumeOwnerIntent{}, false, err
	}
	data, _ := json.Marshal(i)
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return volumeOwnerIntent{}, false, fmt.Errorf("owner intent write uncertain, original retained: %w", writeErr)
	}
	err = dir.Sync()
	if err != nil {
		return volumeOwnerIntent{}, false, fmt.Errorf("owner intent directory sync uncertain, original retained: %w", err)
	}
	return i, true, nil
}

func selectVolumeOwner(cfg ServerConfig) (volumeOwnerIntent, error) {
	scope, _, err := selectedOwnerScope()
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	if err := validateIsolatedServerConfig(cfg); err != nil {
		return volumeOwnerIntent{}, err
	}
	servingOwnerSelection.Lock()
	defer servingOwnerSelection.Unlock()
	if servingOwnerSelection.childResult != nil {
		prior := servingOwnerSelection.intent
		if _, err := readOwnerRetirement(scope, prior); err == nil {
			if err := confirmOwnerRetired(prior); err != nil {
				return volumeOwnerIntent{}, err
			}
			if err := joinSelectedOwnerRetirement(prior); err != nil {
				return volumeOwnerIntent{}, err
			}
		}
	}
	i, created, err := createOwnerIntent(scope, cfg)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	if servingOwnerSelection.selected && servingOwnerSelection.intent != i {
		if !created {
			return volumeOwnerIntent{}, errors.New("owner selection changed in this engine")
		}
		if _, err := readOwnerRetirement(scope, servingOwnerSelection.intent); err != nil {
			return volumeOwnerIntent{}, err
		}
		servingOwnerSelection.selected = false
		servingOwnerSelection.child, servingOwnerSelection.childResult = nil, nil
	}
	path, err := ownerSocketPath()
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	if created {
		executable, err := os.Executable()
		if err != nil {
			return volumeOwnerIntent{}, err
		}
		readPipe, writePipe, err := os.Pipe()
		if err != nil {
			return volumeOwnerIntent{}, err
		}
		defer readPipe.Close()
		cmd, logFile, err := selectedOwnerCommand(scope, executable, readPipe)
		if err != nil {
			_ = writePipe.Close()
			return volumeOwnerIntent{}, err
		}
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			_ = writePipe.Close()
			return volumeOwnerIntent{}, fmt.Errorf("selected owner launch uncertain; intent retained: %w", err)
		}
		_ = logFile.Close() // child owns its inherited descriptor, not engine pipes
		result := &ownerChildResult{done: make(chan struct{})}
		go func() { result.err = cmd.Wait(); close(result.done) }() // sole Wait for this exact child
		servingOwnerSelection.child, servingOwnerSelection.childResult = cmd, result
		servingOwnerSelection.intent = i
		_, sendErr := io.WriteString(writePipe, i.Instance)
		if closeErr := writePipe.Close(); sendErr == nil {
			sendErr = closeErr
		}
		if sendErr != nil {
			return volumeOwnerIntent{}, fmt.Errorf("selected child bootstrap uncertain; intent retained: %w", sendErr)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		resp, callErr := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "inspect", Strings: []string{i.Instance, i.ConfigDigest}})
		cancel()
		if callErr == nil && resp.Error == "" {
			var got volumeOwnerReceipt
			if json.Unmarshal([]byte(resp.Text), &got) == nil && got.Instance == i.Instance && got.Build == i.Build &&
				got.IsolationRoot == i.Root && got.ConfigDigest == i.ConfigDigest {
				servingOwnerSelection.intent, servingOwnerSelection.selected = i, true
				return i, nil
			}
			return volumeOwnerIntent{}, errors.New("selected owner socket returned another identity")
		}
		if !created || time.Now().After(deadline) {
			return volumeOwnerIntent{}, errors.New("selected owner unavailable; original intent retained, no replacement launch")
		}
		select {
		case <-servingOwnerSelection.childResult.done:
			return volumeOwnerIntent{}, fmt.Errorf("original selected owner exited before ready: %v; intent retained", servingOwnerSelection.childResult.err)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// inspectSelectedOwner is the replacement engine's read-only gate. It never
// launches a process from a persisted intent; an unavailable original is HOLD.
func inspectSelectedOwner(cfg ServerConfig) (volumeOwnerIntent, error) {
	scope, _, err := selectedOwnerScope()
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	i, err := readOwnerIntent(scope)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	digest, err := servingConfigDigest(cfg)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	if i.ConfigDigest != digest {
		return volumeOwnerIntent{}, errors.New("selected owner config changed")
	}
	path, err := ownerSocketPath()
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "inspect", Strings: []string{i.Instance, i.ConfigDigest}})
	if err != nil {
		return volumeOwnerIntent{}, fmt.Errorf("selected original owner unavailable; no replacement launch: %w", err)
	}
	if resp.Error != "" {
		return volumeOwnerIntent{}, errors.New(resp.Error)
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return volumeOwnerIntent{}, err
	}
	if r.Instance != i.Instance || r.Build != i.Build || r.ConfigDigest != i.ConfigDigest || r.IsolationRoot != i.Root {
		return volumeOwnerIntent{}, errors.New("selected original owner identity changed")
	}
	return i, nil
}

func selectedOwnerCommand(scope appisolation.Scope, executable string, readPipe *os.File) (*exec.Cmd, *os.File, error) {
	if readPipe == nil {
		return nil, nil, errors.New("owner bootstrap descriptor missing")
	}
	cmd := exec.Command(executable, "--volume-owner")
	cmd.Env = append(os.Environ(), "JM_VOLUME_OWNER_BOOTSTRAP_FD=3")
	cmd.ExtraFiles = []*os.File{readPipe}
	// launchd kills a deceased job's process group. The owner must survive a
	// serving engine crash in a distinct group with no inherited output pipe.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return nil, nil, err
	}
	logFile, err := privatefile.OpenAt(dir, "volume-owner.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	_ = dir.Close()
	if err != nil {
		return nil, nil, err
	}
	st, statErr := logFile.Stat()
	if statErr != nil {
		_ = logFile.Close()
		return nil, nil, statErr
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		_ = logFile.Close()
		return nil, nil, errors.New("owner log is not an owned private regular file")
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	return cmd, logFile, nil
}

package respproxy

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// PrincipalReloader publishes an applied receipt only for a request whose
// exact principals-file bytes were validated and installed by this process.
// The caller owns Tick serialization and invokes the constructor only after
// acquiring the proxy singleton.
type PrincipalReloader struct {
	principalsPath string
	dir            string
	auth           *Authenticator
	swap           func(*Principals) // defaults to auth.Swap; replaceable by tests
	lastApplied    string
}

func NewPrincipalReloader(principalsPath string, auth *Authenticator) (*PrincipalReloader, error) {
	if principalsPath == "" || auth == nil {
		return nil, errors.New("principal reloader needs a principals path and authenticator")
	}
	dir := filepath.Dir(principalsPath)
	for _, name := range []string{"reload.ack", "reload.applied"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("invalidate prior %s receipt: %w", name, err)
		}
	}
	return &PrincipalReloader{principalsPath: principalsPath, dir: dir, auth: auth, swap: auth.Swap}, nil
}

// Tick snapshots at most one request and one principals file. A newer request
// arriving during Swap waits for the next Tick; this one can acknowledge only
// the captured line. Heartbeat is refreshed even when validation fails.
func (r *PrincipalReloader) Tick() (err error) {
	defer func() {
		heartbeat := []byte(strconv.FormatInt(time.Now().Unix(), 10) + "\n")
		if e := writeReloadFile(filepath.Join(r.dir, "reload.heartbeat"), heartbeat); e != nil {
			err = errors.Join(err, fmt.Errorf("refresh reload heartbeat: %w", e))
		}
	}()
	line, err := readReloadRequest(filepath.Join(r.dir, "reload.request"))
	if err != nil || line == "" {
		return err
	}
	if line != r.lastApplied {
		ps, digest, loadErr := loadPrincipalsWithDigest(r.principalsPath)
		if loadErr != nil {
			return loadErr
		}
		if digest != strings.Fields(line)[0] {
			return errors.New("reload request does not match principals bytes")
		}
		r.swap(ps)
	}
	if err := writeReloadFile(filepath.Join(r.dir, "reload.applied"), []byte(line+"\n")); err != nil {
		return fmt.Errorf("publish applied principal receipt: %w", err)
	}
	r.lastApplied = line
	if err := writeReloadFile(filepath.Join(r.dir, "reload.ack"), []byte(line+"\n")); err != nil {
		return fmt.Errorf("publish legacy reload ack: %w", err)
	}
	return nil
}

func readReloadRequest(path string) (line string, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			line, err = "", closeErr
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() {
		return "", errors.New("reload request is not a regular file")
	}
	const maxRequestBytes = 4096
	data, err := io.ReadAll(io.LimitReader(f, maxRequestBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxRequestBytes {
		return "", errors.New("reload request exceeds 4096 bytes")
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 && len(fields) != 3 {
		return "", errors.New("reload request needs a digest, time, and optional legacy nonce")
	}
	if !reloadLowerHex(fields[0], 64) {
		return "", errors.New("reload request has an invalid SHA256")
	}
	when, parseErr := strconv.ParseUint(fields[1], 10, 64)
	if parseErr != nil || when == 0 {
		return "", errors.New("reload request has an invalid positive time")
	}
	if len(fields) == 3 && !reloadLowerHex(fields[2], 16) {
		return "", errors.New("reload request has an invalid nonce")
	}
	return strings.Join(fields, " "), nil
}

func reloadLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}

func writeReloadFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".reload-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

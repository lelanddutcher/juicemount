package mounttable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	snapshotVersion    = 1
	maxSnapshotBytes   = 4 << 20
	maxDiagnosticBytes = 16 << 10
)

// MountEntry is one observed kernel mount. A matching source is not proof
// that JuiceMount owns the kernel instance or may detach it.
type MountEntry struct {
	Path   string   `json:"path"`
	Source string   `json:"source"`
	Type   string   `json:"type"`
	FSID   [2]int32 `json:"fsid"`
}

// Snapshot is a complete mount-table observation from the standalone helper.
// BootID distinguishes boots; neither it nor FSID authorizes unmounting.
type Snapshot struct {
	Version int          `json:"version"`
	BootID  string       `json:"boot_id"`
	Entries []MountEntry `json:"entries"`
}

// Share the existing mount-table process gate: neither the old mount(8)
// observer nor this helper may pile a second kernel observer on a stuck one.
// A timed-out helper releases the gate only after its child is reaped.
var snapshotHelperGate = queryGate

type boundedSnapshotOutput struct {
	buf      bytes.Buffer
	limit    int
	overflow chan struct{}
	once     sync.Once
}

func (b *boundedSnapshotOutput) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		keep := len(p)
		if keep > remaining {
			keep = remaining
		}
		_, _ = b.buf.Write(p[:keep])
	}
	if len(p) > remaining && b.overflow != nil {
		b.once.Do(func() { close(b.overflow) })
	}
	// Continue draining the pipe until the caller kills the oversized child.
	return len(p), nil
}

// SnapshotFromHelper runs only the exact supplied executable, with no shell,
// arguments, or in-process kernel mount-table syscall. An incomplete output
// or a helper that cannot settle is an error, never an empty snapshot.
func SnapshotFromHelper(ctx context.Context, helperPath string) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, errors.New("mount snapshot: nil context")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("mount snapshot: %w", err)
	}
	if helperPath == "" || !filepath.IsAbs(helperPath) {
		return Snapshot{}, fmt.Errorf("mount snapshot: helper path must be absolute")
	}
	select {
	case snapshotHelperGate <- struct{}{}:
	case <-ctx.Done():
		return Snapshot{}, fmt.Errorf("mount snapshot: prior helper still running (%s): %w", blockedObservation(), ctx.Err())
	}
	op := beginObservation(ctx, snapshotHelperGate, "native snapshot", helperPath)
	if err := ctx.Err(); err != nil {
		finishObservation(op, snapshotHelperGate, err, false)
		return Snapshot{}, fmt.Errorf("mount snapshot: %w", err)
	}

	stdout := &boundedSnapshotOutput{limit: maxSnapshotBytes, overflow: make(chan struct{})}
	stderr := &boundedSnapshotOutput{limit: maxDiagnosticBytes}
	cmd := exec.Command(helperPath)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		finishObservation(op, snapshotHelperGate, err, false)
		return Snapshot{}, fmt.Errorf("mount snapshot: start helper: %w", err)
	}
	launchedObservation(op, cmd)
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		finishObservation(op, snapshotHelperGate, err, true)
		done <- err
	}()
	select {
	case <-ctx.Done():
		timeoutObservation(op, ctx.Err())
		_ = cmd.Process.Kill()
		return Snapshot{}, fmt.Errorf("mount snapshot: helper deadline: %w", ctx.Err())
	case <-stdout.overflow:
		timeoutObservation(op, fmt.Errorf("output exceeds %d bytes", maxSnapshotBytes))
		_ = cmd.Process.Kill()
		return Snapshot{}, fmt.Errorf("mount snapshot: helper output exceeds %d bytes", maxSnapshotBytes)
	case err := <-done:
		if err != nil {
			return Snapshot{}, fmt.Errorf("mount snapshot: helper failed: %w: %s", err, strings.TrimSpace(stderr.buf.String()))
		}
		select {
		case <-stdout.overflow:
			return Snapshot{}, fmt.Errorf("mount snapshot: helper output exceeds %d bytes", maxSnapshotBytes)
		default:
		}
		return parseSnapshot(stdout.buf.Bytes())
	}
}

type snapshotWire struct {
	Version *int              `json:"version"`
	BootID  *string           `json:"boot_id"`
	Entries *[]mountEntryWire `json:"entries"`
}

type mountEntryWire struct {
	Path   *string  `json:"path"`
	Source *string  `json:"source"`
	Type   *string  `json:"type"`
	FSID   *[]int32 `json:"fsid"`
}

func parseSnapshot(data []byte) (Snapshot, error) {
	if len(data) == 0 || len(data) > maxSnapshotBytes {
		return Snapshot{}, errors.New("mount snapshot: empty or oversized helper output")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Snapshot{}, fmt.Errorf("mount snapshot: ambiguous JSON: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var wire snapshotWire
	if err := dec.Decode(&wire); err != nil {
		return Snapshot{}, fmt.Errorf("mount snapshot: decode: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Snapshot{}, errors.New("mount snapshot: trailing data")
	}
	if wire.Version == nil || *wire.Version != snapshotVersion || wire.BootID == nil ||
		strings.TrimSpace(*wire.BootID) == "" || wire.Entries == nil {
		return Snapshot{}, errors.New("mount snapshot: missing or invalid snapshot header")
	}
	snapshot := Snapshot{Version: *wire.Version, BootID: *wire.BootID, Entries: make([]MountEntry, 0, len(*wire.Entries))}
	for i, entry := range *wire.Entries {
		if entry.Path == nil || *entry.Path == "" || entry.Source == nil || *entry.Source == "" ||
			entry.Type == nil || *entry.Type == "" || entry.FSID == nil || len(*entry.FSID) != 2 {
			return Snapshot{}, fmt.Errorf("mount snapshot: incomplete entry %d", i)
		}
		snapshot.Entries = append(snapshot.Entries, MountEntry{
			Path: *entry.Path, Source: *entry.Source, Type: *entry.Type,
			FSID: [2]int32{(*entry.FSID)[0], (*entry.FSID)[1]},
		})
	}
	return snapshot, nil
}

// encoding/json otherwise accepts duplicate keys and keeps the last value.
// A mount receipt must not depend on which decoder or field ordering wins.
func rejectDuplicateJSONKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := consumeUniqueJSONValue(dec, 0); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func consumeUniqueJSONValue(dec *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting exceeds snapshot schema")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeUniqueJSONValue(dec, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := consumeUniqueJSONValue(dec, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = dec.Token() // matching '}' or ']'
	return err
}

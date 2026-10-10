//go:build darwin

package mounttable

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	maxSnapshotEntries    = 8192
	maxSnapshotRetries    = 8
	snapshotCapacitySlack = 16
)

func snapshotCString(raw []byte) string {
	if end := bytes.IndexByte(raw, 0); end >= 0 {
		raw = raw[:end]
	}
	return string(raw)
}

// CaptureSnapshot is for the standalone mount-probe executable only. Darwin's
// getfsstat can park inside the kernel even with MNT_NOWAIT, so the app must
// call SnapshotFromHelper instead of calling this function in-process.
func CaptureSnapshot() (Snapshot, error) {
	bootID, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return Snapshot{}, fmt.Errorf("mount snapshot: boot session UUID: %w", err)
	}
	bootID = strings.TrimSpace(strings.Trim(bootID, "\x00"))
	if bootID == "" {
		return Snapshot{}, errors.New("mount snapshot: empty boot session UUID")
	}
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return Snapshot{}, fmt.Errorf("mount snapshot: count: %w", err)
	}
	if count <= 0 || count > maxSnapshotEntries-snapshotCapacitySlack {
		return Snapshot{}, fmt.Errorf("mount snapshot: invalid or excessive mount count %d", count)
	}
	capacity := count + snapshotCapacitySlack
	if capacity < 32 {
		capacity = 32
	}
	for attempt := 0; attempt < maxSnapshotRetries && capacity <= maxSnapshotEntries; attempt++ {
		rows := make([]unix.Statfs_t, capacity)
		n, err := unix.Getfsstat(rows, unix.MNT_NOWAIT)
		if err != nil {
			return Snapshot{}, fmt.Errorf("mount snapshot: capture: %w", err)
		}
		if n <= 0 {
			return Snapshot{}, fmt.Errorf("mount snapshot: invalid returned mount count %d", n)
		}
		if n >= len(rows) {
			capacity *= 2
			continue
		}
		snapshot := Snapshot{Version: snapshotVersion, BootID: bootID, Entries: make([]MountEntry, 0, n)}
		for i, row := range rows[:n] {
			entry := MountEntry{
				Path: snapshotCString(row.Mntonname[:]), Source: snapshotCString(row.Mntfromname[:]),
				Type: snapshotCString(row.Fstypename[:]), FSID: row.Fsid.Val,
			}
			if entry.Path == "" || entry.Source == "" || entry.Type == "" {
				return Snapshot{}, fmt.Errorf("mount snapshot: incomplete kernel entry %d", i)
			}
			snapshot.Entries = append(snapshot.Entries, entry)
		}
		return snapshot, nil
	}
	return Snapshot{}, errors.New("mount snapshot: mount table grew beyond bounded capacity")
}

//go:build darwin

package mounttable

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/sys/unix"
)

// Output returns a non-refreshing Darwin mount-table snapshot in mount(8)'s
// ordinary text form. MNT_NOWAIT is load-bearing: mount(8) uses a refreshing
// getfsstat query that can park forever in an uninterruptible kernel call when
// a dead NFS or macFUSE entry is present. CommandContext cannot bound Wait in
// that state, which used to hang JuiceMount startup while trying to recover
// the very stale mount that caused the query to wedge.
func Output(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("mount table: nil context")
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("mount table: query canceled: %w", ctx.Err())
	default:
	}

	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, fmt.Errorf("mount table: getfsstat count: %w", err)
	}
	// Leave room for mounts added between the sizing call and the snapshot.
	stats := make([]unix.Statfs_t, count+16)
	n, err := unix.Getfsstat(stats, unix.MNT_NOWAIT)
	if err != nil {
		return nil, fmt.Errorf("mount table: getfsstat snapshot: %w", err)
	}
	if n > len(stats) {
		return nil, fmt.Errorf("mount table: grew from %d to %d entries during snapshot", count, n)
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("mount table: query canceled: %w", ctx.Err())
	default:
	}
	return darwinSnapshotOutput(stats[:n]), nil
}

func darwinSnapshotOutput(stats []unix.Statfs_t) []byte {
	var out bytes.Buffer
	for i := range stats {
		from := unix.ByteSliceToString(stats[i].Mntfromname[:])
		on := unix.ByteSliceToString(stats[i].Mntonname[:])
		fsType := unix.ByteSliceToString(stats[i].Fstypename[:])
		fmt.Fprintf(&out, "%s on %s (%s)\n", from, on, fsType)
	}
	return out.Bytes()
}

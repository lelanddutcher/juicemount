package nfs

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

// hashSpoolFile streams a SHA-256 over the named file. Used by the
// drainer (slice B) to re-hash from disk when:
//   - the streaming SHA was invalidated by out-of-order WriteAt, OR
//   - the drainer wants to verify the file's at-rest content matches
//     what the streaming SHA recorded (defense against bit flip)
//
// Returns the hash and the byte count read. Uses a 1 MiB buffer; the
// io.CopyBuffer is intentional so we don't churn the GC on a default
// 32 KiB buffer for multi-GB files.
func hashSpoolFile(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("hashSpoolFile: open: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	n, err := io.CopyBuffer(h, f, buf)
	if err != nil {
		return nil, n, fmt.Errorf("hashSpoolFile: read: %w", err)
	}
	return h.Sum(nil), n, nil
}

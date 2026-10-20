package continuity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

var (
	ErrContentTooLarge = errors.New("continuity content exceeds byte limit")
	ErrContentMismatch = errors.New("continuity content does not match descriptor")
)

// Content describes complete logical bytes, including zeros supplied for holes.
// A syntactically valid digest is a claim until verified against actual bytes.
type Content struct {
	SHA256 string
	Size   int64
}

func (c Content) Validate() error {
	if c.Size < 0 {
		return fmt.Errorf("continuity content: negative size")
	}
	if _, err := decodeHex(c.SHA256, sha256.Size); err != nil {
		return fmt.Errorf("continuity content digest: %w", err)
	}
	return nil
}

// HashContent hashes every byte through successful EOF, reading at most one
// byte beyond maxBytes. On any error it returns an unusable zero descriptor.
// The caller must separately establish an immutable source and its durability.
func HashContent(r io.Reader, maxBytes int64) (Content, error) {
	if r == nil {
		return Content{}, fmt.Errorf("continuity content: nil reader")
	}
	if maxBytes < 0 {
		return Content{}, fmt.Errorf("continuity content: negative limit")
	}
	h := sha256.New()
	var buf [32 * 1024]byte
	var size int64
	emptyReads := 0
	for {
		remaining := maxBytes - size
		readSize := len(buf)
		if remaining < int64(readSize) {
			// Add one only after bounding remaining, including at MaxInt64.
			readSize = int(remaining) + 1
		}
		n, err := r.Read(buf[:readSize])
		if n < 0 || n > readSize {
			return Content{}, fmt.Errorf("continuity content: invalid read count %d", n)
		}
		if err != nil && err != io.EOF {
			return Content{}, fmt.Errorf("continuity content read: %w", err)
		}
		if int64(n) > remaining {
			return Content{}, ErrContentTooLarge
		}
		if n > 0 {
			size += int64(n) // n <= maxBytes-size, so this cannot overflow.
			_, _ = h.Write(buf[:n])
			emptyReads = 0
		} else if err == nil {
			emptyReads++
			if emptyReads >= 100 {
				return Content{}, io.ErrNoProgress
			}
		}
		if err == io.EOF {
			return Content{SHA256: hex.EncodeToString(h.Sum(nil)), Size: size}, nil
		}
	}
}

// VerifyContent validates expected, then compares the entire stream's length
// and digest. It reads at most expected.Size+1 bytes without overflowing int64.
// A longer stream returns ErrContentTooLarge; short or changed bytes return
// ErrContentMismatch. Read errors are propagated, never replaced by a digest.
func VerifyContent(r io.Reader, expected Content) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	actual, err := HashContent(r, expected.Size)
	if err != nil {
		return err
	}
	if actual != expected {
		return ErrContentMismatch
	}
	return nil
}

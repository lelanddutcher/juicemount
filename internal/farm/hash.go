package farm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/zeebo/xxh3"
)

// sampleWindow is how many bytes we read from each of the head and tail when
// content-hashing. A sampled hash (size + head + tail) is the right tradeoff for
// large media: full-file hashing a multi-GB camera original to stamp a thumbnail
// is wasteful, and media is effectively immutable once written (cameras/NLEs
// write a new file, they don't edit bytes in place). The size term catches
// truncation/append; head+tail catch header/trailer rewrites (e.g. moov atom
// relocation). For the slices where a stale derivative would actively mislead
// (AI embeddings, proxies), a full-file hash can be layered later behind the
// same source_hash field — the wire contract is identical.
const sampleWindow = 1 << 20 // 1 MiB

// SampleHash returns the xxh3 (64-bit, 16 hex chars — matching the contract's
// source_hash style) of size‖head‖tail. The producer writes this same value as
// both the asset's source_hash AND each derivative's hash, so a consumer's
// hash==source_hash gate is exact by construction.
func SampleHash(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := xxh3.New()
	var sz [8]byte
	binary.LittleEndian.PutUint64(sz[:], uint64(size))
	_, _ = h.Write(sz[:])

	head := sampleWindow
	if int64(head) > size {
		head = int(size)
	}
	if head > 0 {
		buf := make([]byte, head)
		if _, err := io.ReadFull(f, buf); err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		_, _ = h.Write(buf)
	}
	// Tail only when it wouldn't overlap the head (file bigger than 2 windows).
	if size > int64(2*sampleWindow) {
		if _, err := f.Seek(-int64(sampleWindow), io.SeekEnd); err == nil {
			buf := make([]byte, sampleWindow)
			if _, err := io.ReadFull(f, buf); err == nil {
				_, _ = h.Write(buf)
			}
		}
	}
	return fmt.Sprintf("%016x", h.Sum64()), nil
}

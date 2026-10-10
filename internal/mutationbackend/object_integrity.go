package mutationbackend

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"github.com/juicedata/juicefs/pkg/object"
)

// Larger than the maximum supported 16 MiB chunk block plus compression and
// encryption framing. Bound input and verification even if a provider ignores
// range requests. This gate sits below encryption, so ciphertext is checked.
const maxRawObjectBytes int64 = 32 << 20

var errObjectIntegrity = errors.New("SDK object upload could not be verified")
var errObjectReclamation = errors.New("SDK object reclamation is not authorized")

// verifiedObjects gates chunk PUT completion on exact uncached provider bytes.
// It is not an operation manifest, version assertion, or durability receipt.
// A failed verification may follow a successful PUT: retain uncertain custody;
// never delete the object or infer that no backend effect occurred.
type objectRecorder interface {
	Begin(string, int64, string) (func() error, error)
}

type verifiedObjects struct {
	object.ObjectStorage
	recorder objectRecorder
}

// The SDK aborts a failed upload by deleting its block keys, even with metadata
// background deletion disabled. No exact-object reclamation authority exists
// for this gateway yet. Retain every object and report refusal; a PUT error or
// uncertain Fsync never authorizes cleanup, including after process recovery.
func (v *verifiedObjects) Delete(string, ...object.AttrGetter) error {
	return errObjectReclamation
}

func (v *verifiedObjects) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	body, size, expected, err := hashObjectInput(in)
	if err != nil {
		return errObjectIntegrity
	}
	creator, ok := v.ObjectStorage.(object.SupportCreateOnly)
	if !ok {
		return errObjectIntegrity
	}
	var verified func() error
	if v.recorder != nil {
		verified, err = v.recorder.Begin(key, size, hex.EncodeToString(expected))
		if err != nil {
			return errObjectIntegrity
		}
	}
	if err := creator.PutIfAbsent(key, body, getters...); err != nil {
		return errObjectIntegrity
	}
	// Use the raw provider path, not chunk/file caches or the encryption reader
	// (which buffers the whole ciphertext before applying its requested limit).
	reader, err := v.ObjectStorage.Get(key, 0, -1)
	if err != nil || reader == nil {
		if reader != nil {
			_ = reader.Close()
		}
		return errObjectIntegrity
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(reader, size+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || n != size || !bytes.Equal(h.Sum(nil), expected) {
		return errObjectIntegrity
	}
	if verified != nil && verified() != nil {
		return errObjectIntegrity
	}
	return nil
}

// All other object mutation entry points are denied, including methods promoted
// by the embedded provider. Only the bounded, recorded Put path is admitted.
func (v *verifiedObjects) Create() error             { return errObjectReclamation }
func (v *verifiedObjects) Copy(string, string) error { return errObjectReclamation }
func (v *verifiedObjects) CreateMultipartUpload(string) (*object.MultipartUpload, error) {
	return nil, errObjectReclamation
}
func (v *verifiedObjects) UploadPart(string, string, int, []byte) (*object.Part, error) {
	return nil, errObjectReclamation
}
func (v *verifiedObjects) UploadPartCopy(string, string, int, string, int64, int64) (*object.Part, error) {
	return nil, errObjectReclamation
}
func (v *verifiedObjects) CompleteUpload(string, string, []*object.Part) error {
	return errObjectReclamation
}
func (v *verifiedObjects) AbortUpload(string, string) {}

func hashObjectInput(in io.Reader) (io.Reader, int64, []byte, error) {
	if in == nil {
		return nil, 0, nil, errObjectIntegrity
	}
	if seeker, ok := in.(io.ReadSeeker); ok {
		start, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil || start < 0 {
			return nil, 0, nil, errObjectIntegrity
		}
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(seeker, maxRawObjectBytes+1))
		if err != nil || n > maxRawObjectBytes {
			return nil, 0, nil, errObjectIntegrity
		}
		restored, err := seeker.Seek(start, io.SeekStart)
		if err != nil || restored != start {
			return nil, 0, nil, errObjectIntegrity
		}
		// A fresh section prevents a provider retry from seeking into bytes that
		// preceded the caller's original offset. Avoid copying the usual SDK reader.
		if at, ok := in.(io.ReaderAt); ok {
			return io.NewSectionReader(at, start, n), n, h.Sum(nil), nil
		}
		// A seeker without ReaderAt cannot safely expose its original coordinate
		// system to a provider that may rewind. Buffer only this bounded suffix.
	}
	b, err := io.ReadAll(io.LimitReader(in, maxRawObjectBytes+1))
	if err != nil || int64(len(b)) > maxRawObjectBytes {
		return nil, 0, nil, errObjectIntegrity
	}
	sum := sha256.Sum256(b)
	return bytes.NewReader(b), int64(len(b)), sum[:], nil
}

func (v *verifiedObjects) SetStorageClass(class string) error {
	setter, ok := v.ObjectStorage.(object.SupportStorageClass)
	if !ok {
		return errObjectIntegrity
	}
	return setter.SetStorageClass(class)
}

func (v *verifiedObjects) Shutdown() { object.Shutdown(v.ObjectStorage) }

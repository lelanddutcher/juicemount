// Package nativeclient defines bounded local copy-custody records. Decoding a
// record establishes syntax only; ValidateChain establishes its local links.
// Neither operation proves current server authority or live file identity.
package nativeclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

const RecordSlotBytes = 4096
const MetadataReservationBytes = 6 * RecordSlotBytes

var ErrInvalidRecord = errors.New("invalid native copy record")

type Intent struct {
	Version                  int                           `json:"version"`
	Endpoint                 string                        `json:"endpoint"`
	LibraryID                continuity.LibraryID          `json:"library_id"`
	OperationID              string                        `json:"operation_id"`
	CopyID                   string                        `json:"copy_id"`
	SelectRequest            catalog.DeliverySelectRequest `json:"select_request"`
	Basename                 string                        `json:"basename"`
	MaxBytes                 int64                         `json:"max_bytes"`
	MetadataReservationBytes int64                         `json:"metadata_reservation_bytes"`
}

type SelectionRecord struct {
	Version                 int                       `json:"version"`
	IntentSHA256            string                    `json:"intent_sha256"`
	Selection               catalog.DeliverySelection `json:"selection"`
	LogicalReservationBytes int64                     `json:"logical_reservation_bytes"`
}

type Checkpoint struct {
	Version      int                  `json:"version"`
	IntentSHA256 string               `json:"intent_sha256"`
	Root         privatefile.Identity `json:"root"`
	Directory    privatefile.Identity `json:"directory"`
	Lock         privatefile.Identity `json:"lock"`
	File         privatefile.Identity `json:"file"`
	PrefixLength int64                `json:"prefix_length"`
	PrefixSHA256 string               `json:"prefix_sha256"`
}

type Prepared struct {
	Version         int                     `json:"version"`
	IntentSHA256    string                  `json:"intent_sha256"`
	Header          delivery.TransferHeader `json:"header"`
	End             delivery.TransferEnd    `json:"end"`
	Root            privatefile.Identity    `json:"root"`
	Directory       privatefile.Identity    `json:"directory"`
	Lock            privatefile.Identity    `json:"lock"`
	File            privatefile.Identity    `json:"file"`
	TargetDirectory privatefile.Identity    `json:"target_directory"`
}

type Verified struct {
	Version        int                  `json:"version"`
	IntentSHA256   string               `json:"intent_sha256"`
	PreparedSHA256 string               `json:"prepared_sha256"`
	CopyID         string               `json:"copy_id"`
	FinalFile      privatefile.Identity `json:"final_file"`
}

type Released struct {
	Version      int    `json:"version"`
	IntentSHA256 string `json:"intent_sha256"`
	BasisKind    string `json:"basis_kind"`
	BasisSHA256  string `json:"basis_sha256"`
	State        string `json:"state"`
}

func validOperationID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validDigest(s string) bool { return mutationauthority.ImageHashValid(s) }
func validCopyID(s string) bool { return mutationauthority.ImageIDValid(s, "copy_") }

func validIdentity(id privatefile.Identity, directory bool) bool {
	if id.Device == 0 || id.Inode == 0 || id.Size < 0 || id.Size > mutationauthority.MaxImageBytes {
		return false
	}
	if directory {
		return id.Mode == 0o40700 && id.Size == 0
	}
	return id.Mode == 0o100600
}

func validSelection(s catalog.DeliverySelection) bool {
	return s.Version == 1 && s.LibraryID.Validate() == nil && mutationauthority.ImageIDValid(s.SelectionID, "dsel_") && validOperationID(s.OperationID) && s.AssetID.Validate() == nil && s.RevisionID.Validate() == nil && mutationauthority.ImageIDValid(s.SourceID, "dsrc_") && s.SourceVersion >= 1 && s.Content.Size >= 0 && s.Content.Size <= mutationauthority.MaxImageBytes && s.Content.Validate() == nil && s.ExpiresUnix > 0 && !s.Canceled
}

func validateIntent(r Intent, allowLoopbackHTTP bool) error {
	_, _, err := catalog.CanonicalDeliverySelectRequest(r.SelectRequest)
	if r.Version != 1 || r.LibraryID.Validate() != nil || !validOperationID(r.OperationID) || !validCopyID(r.CopyID) || r.MaxBytes < 0 || r.MaxBytes > mutationauthority.MaxImageBytes || r.MetadataReservationBytes != MetadataReservationBytes || r.SelectRequest.LibraryID != r.LibraryID || r.SelectRequest.ID != r.OperationID || r.SelectRequest.MaxBytes != r.MaxBytes || err != nil || ValidateBasename(r.Basename) != nil {
		return ErrInvalidRecord
	}
	if _, err := ParseEndpoint(r.Endpoint, allowLoopbackHTTP); err != nil {
		return ErrInvalidRecord
	}
	return nil
}

func validateSelectionRecord(r SelectionRecord) error {
	if r.Version != 1 || !validDigest(r.IntentSHA256) || !validSelection(r.Selection) || r.LogicalReservationBytes != r.Selection.Content.Size {
		return ErrInvalidRecord
	}
	return nil
}

func validateCheckpoint(r Checkpoint) error {
	if r.Version != 1 || !validDigest(r.IntentSHA256) || !validIdentity(r.Root, true) || !validIdentity(r.Directory, true) || !validIdentity(r.Lock, false) || r.Lock.Size != 0 || !validIdentity(r.File, false) || r.PrefixLength != r.File.Size || !validDigest(r.PrefixSHA256) {
		return ErrInvalidRecord
	}
	if r.PrefixLength == 0 && r.PrefixSHA256 != emptyDigest() {
		return ErrInvalidRecord
	}
	return nil
}

func validatePrepared(r Prepared) error {
	if r.Version != 1 || !validDigest(r.IntentSHA256) || !validIdentity(r.Root, true) || !validIdentity(r.Directory, true) || !validIdentity(r.Lock, false) || r.Lock.Size != 0 || !validIdentity(r.File, false) || !validIdentity(r.TargetDirectory, true) || r.File.Size != r.Header.Selection.Content.Size {
		return ErrInvalidRecord
	}
	if _, err := delivery.CanonicalTransferHeader(r.Header); err != nil {
		return ErrInvalidRecord
	}
	if _, err := delivery.CanonicalTransferEnd(r.End); err != nil || r.End.RequestSHA256 != r.Header.RequestSHA256 || r.End.AttemptID != r.Header.Attempt.ID || r.End.Offset != r.Header.Offset || r.End.Length != r.Header.Length {
		return ErrInvalidRecord
	}
	return nil
}

func validateVerified(r Verified) error {
	if r.Version != 1 || !validDigest(r.IntentSHA256) || !validDigest(r.PreparedSHA256) || !validCopyID(r.CopyID) || !validIdentity(r.FinalFile, false) {
		return ErrInvalidRecord
	}
	return nil
}

func validateReleased(r Released) error {
	if r.Version != 1 || !validDigest(r.IntentSHA256) || !validDigest(r.BasisSHA256) || r.State != "released" || (r.BasisKind != "checkpoint" && r.BasisKind != "prepared" && r.BasisKind != "verified") {
		return ErrInvalidRecord
	}
	return nil
}

func emptyDigest() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}

func recordDigest(kind string, payload []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte("juicemount.continuity.native." + kind + "\x00\x01"))
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func encodeRecord[T any](kind string, value T, validate func(T) error) ([]byte, string, error) {
	if validate(value) != nil {
		return nil, "", ErrInvalidRecord
	}
	payload, err := json.Marshal(value)
	if err != nil || len(payload)+1 > RecordSlotBytes {
		return nil, "", ErrInvalidRecord
	}
	line := append(payload, '\n')
	return line, recordDigest(kind, payload), nil
}

func decodeRecord[T any](kind string, line []byte, validate func(T) error) (T, string, error) {
	var value T
	if len(line) < 2 || len(line) > RecordSlotBytes || line[len(line)-1] != '\n' || mutationauthority.DecodeImageCanonical(line[:len(line)-1], &value) != nil {
		var zero T
		return zero, "", ErrInvalidRecord
	}
	encoded, digest, err := encodeRecord(kind, value, validate)
	if err != nil || !bytes.Equal(encoded, line) {
		var zero T
		return zero, "", ErrInvalidRecord
	}
	return value, digest, nil
}

func EncodeIntent(r Intent, allowLoopbackHTTP bool) ([]byte, string, error) {
	return encodeRecord("intent", r, func(v Intent) error { return validateIntent(v, allowLoopbackHTTP) })
}
func DecodeIntent(b []byte, allowLoopbackHTTP bool) (Intent, string, error) {
	return decodeRecord("intent", b, func(v Intent) error { return validateIntent(v, allowLoopbackHTTP) })
}
func EncodeSelection(r SelectionRecord) ([]byte, string, error) {
	return encodeRecord("selection", r, validateSelectionRecord)
}
func DecodeSelection(b []byte) (SelectionRecord, string, error) {
	return decodeRecord("selection", b, validateSelectionRecord)
}
func EncodeCheckpoint(r Checkpoint) ([]byte, string, error) {
	return encodeRecord("checkpoint", r, validateCheckpoint)
}
func DecodeCheckpoint(b []byte) (Checkpoint, string, error) {
	return decodeRecord("checkpoint", b, validateCheckpoint)
}
func EncodePrepared(r Prepared) ([]byte, string, error) {
	return encodeRecord("prepared", r, validatePrepared)
}
func DecodePrepared(b []byte) (Prepared, string, error) {
	return decodeRecord("prepared", b, validatePrepared)
}
func EncodeVerified(r Verified) ([]byte, string, error) {
	return encodeRecord("verified", r, validateVerified)
}
func DecodeVerified(b []byte) (Verified, string, error) {
	return decodeRecord("verified", b, validateVerified)
}
func EncodeReleased(r Released) ([]byte, string, error) {
	return encodeRecord("released", r, validateReleased)
}
func DecodeReleased(b []byte) (Released, string, error) {
	return decodeRecord("released", b, validateReleased)
}

// Chain describes records already read from a local journal. ValidateChain
// checks their immutable references and pinned request, not which record is
// current, whether the journal was durably written, or live server/file rights.
type Chain struct {
	Intent     Intent
	Selection  *SelectionRecord
	Checkpoint *Checkpoint
	Prepared   *Prepared
	Verified   *Verified
	Released   *Released
}

func ValidateChain(chain Chain, allowLoopbackHTTP bool) error {
	_, intentDigest, err := EncodeIntent(chain.Intent, allowLoopbackHTTP)
	if err != nil {
		return ErrInvalidRecord
	}
	if chain.Checkpoint != nil && chain.Selection == nil || chain.Prepared != nil && chain.Checkpoint == nil || chain.Verified != nil && chain.Prepared == nil {
		return ErrInvalidRecord
	}
	var checkpointDigest, preparedDigest, verifiedDigest string
	if r := chain.Selection; r != nil {
		if _, _, err = EncodeSelection(*r); err != nil || r.IntentSHA256 != intentDigest || r.Selection.LibraryID != chain.Intent.LibraryID || r.Selection.OperationID != chain.Intent.OperationID || r.Selection.AssetID != chain.Intent.SelectRequest.AssetID || r.Selection.ExpiresUnix != chain.Intent.SelectRequest.ExpiresUnix || r.Selection.Content.Size > chain.Intent.MaxBytes || (chain.Intent.SelectRequest.Mode == "exact" && r.Selection.RevisionID != chain.Intent.SelectRequest.RevisionID) {
			return ErrInvalidRecord
		}
	}
	if r := chain.Checkpoint; r != nil {
		_, checkpointDigest, err = EncodeCheckpoint(*r)
		selected := chain.Selection.Selection.Content
		if err != nil || r.IntentSHA256 != intentDigest || r.PrefixLength > selected.Size || r.File.Size > selected.Size || r.PrefixLength == selected.Size && r.PrefixSHA256 != selected.SHA256 {
			return ErrInvalidRecord
		}
	}
	if r := chain.Prepared; r != nil {
		_, preparedDigest, err = EncodePrepared(*r)
		if err != nil || r.IntentSHA256 != intentDigest || chain.Selection == nil || r.Header.Selection != chain.Selection.Selection || r.Header.Attempt.OperationID != chain.Intent.OperationID {
			return ErrInvalidRecord
		}
		request := delivery.TransferRequest{Version: 1, LibraryID: chain.Intent.LibraryID, SelectionID: r.Header.Selection.SelectionID, OperationID: chain.Intent.OperationID, Offset: r.Header.Offset, PrefixSHA256: r.Header.PrefixSHA256}
		_, requestDigest, err := delivery.CanonicalTransferRequest(request)
		if err != nil || requestDigest != r.Header.RequestSHA256 {
			return ErrInvalidRecord
		}
		if c := chain.Checkpoint; c != nil && (r.Root != c.Root || r.Directory != c.Directory || r.Lock != c.Lock || r.File.Device != c.File.Device || r.File.Inode != c.File.Inode || r.File.UID != c.File.UID || r.File.Mode != c.File.Mode || r.File.Size < c.PrefixLength) {
			return ErrInvalidRecord
		}
	}
	if r := chain.Verified; r != nil {
		_, verifiedDigest, err = EncodeVerified(*r)
		if err != nil || r.IntentSHA256 != intentDigest || chain.Prepared == nil || r.PreparedSHA256 != preparedDigest || r.CopyID != chain.Intent.CopyID || r.FinalFile != chain.Prepared.File {
			return ErrInvalidRecord
		}
	}
	if r := chain.Released; r != nil {
		if _, _, err = EncodeReleased(*r); err != nil || r.IntentSHA256 != intentDigest {
			return ErrInvalidRecord
		}
		var basis string
		switch {
		case chain.Verified != nil && r.BasisKind == "verified":
			basis = verifiedDigest
		case chain.Prepared != nil && chain.Verified == nil && r.BasisKind == "prepared":
			basis = preparedDigest
		case chain.Checkpoint != nil && chain.Prepared == nil && r.BasisKind == "checkpoint":
			basis = checkpointDigest
		}
		if basis == "" || r.BasisSHA256 != basis {
			return ErrInvalidRecord
		}
	}
	return nil
}

package mutationauthority

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	MaxImageBytes         int64 = 1 << 30
	MaxImageChunks              = 128
	MaxImageEnvelope            = 512 << 10
	MaxImages             int64 = 1024
	MaxImageLogicalBytes  int64 = 16 << 30
	MaxImageMetadataBytes int64 = 64 << 20
)

var (
	ErrImageQuota    = errors.New("image authority quota exhausted")
	ErrImageInactive = errors.New("image authority inactive or disabled")
	ErrImageConflict = errors.New("image grant generation changed")
)

// ImageConfig is immutable after explicit initialization. Initialize never
// replaces an existing identity, quota, activation, reservation or grant.
type ImageConfig struct {
	Version          int64  `json:"version"`
	StoreID          string `json:"store_id"`
	Initialize       bool   `json:"initialize"`
	MaxImages        int64  `json:"max_images"`
	MaxLogicalBytes  int64  `json:"max_logical_bytes"`
	MaxMetadataBytes int64  `json:"max_metadata_bytes"`
}

type ImageContent struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type ImageChunk struct {
	Session string `json:"session"`
	Epoch   int64  `json:"epoch"`
	ID      string `json:"id"`
	FileID  string `json:"file_id"`
	Offset  int64  `json:"offset"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// This persisted projection deliberately matches the historical custody wire,
// without changing that wire or the original operation receipt projection.
type ImageCustodyReceipt struct {
	Kind              string     `json:"kind"`
	Volume            string     `json:"volume"`
	Chunk             ImageChunk `json:"chunk"`
	ArtifactSHA256    string     `json:"artifact_sha256"`
	ArtifactSize      int64      `json:"artifact_size"`
	RequestSHA256     string     `json:"request_sha256"`
	ReceiptID         string     `json:"receipt_id"`
	CommittedAtUnixNS int64      `json:"committed_at_unix_ns"`
}
type ImageSeal struct {
	Version       int64                 `json:"version"`
	StoreID       string                `json:"store_id"`
	Volume        string                `json:"volume"`
	LibraryID     string                `json:"library_id"`
	ImageID       string                `json:"image_id"`
	OperationID   string                `json:"operation_id"`
	Incarnation   string                `json:"incarnation"`
	Producer      string                `json:"producer"`
	InitialReader string                `json:"initial_reader"`
	Content       ImageContent          `json:"content"`
	Receipts      []ImageCustodyReceipt `json:"receipts"`
}
type ImageRef struct {
	Version   int64  `json:"version"`
	StoreID   string `json:"store_id"`
	LibraryID string `json:"library_id"`
	ImageID   string `json:"image_id"`
}
type ImagePartView struct {
	Index  int64  `json:"index"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type ImageView struct {
	Version     int64           `json:"version"`
	StoreID     string          `json:"store_id"`
	Volume      string          `json:"volume"`
	LibraryID   string          `json:"library_id"`
	ImageID     string          `json:"image_id"`
	Incarnation string          `json:"incarnation"`
	ImageDigest string          `json:"image_digest"`
	Content     ImageContent    `json:"content"`
	Activation  int64           `json:"activation"`
	Generation  int64           `json:"generation"`
	Chunks      []ImagePartView `json:"chunks"`
}
type ImageRead struct {
	Version     int64  `json:"version"`
	StoreID     string `json:"store_id"`
	LibraryID   string `json:"library_id"`
	ImageID     string `json:"image_id"`
	ImageDigest string `json:"image_digest"`
	Activation  int64  `json:"activation"`
	Generation  int64  `json:"generation"`
	Index       int64  `json:"index"`
}
type ImageReaderChange struct {
	Version            int64   `json:"version"`
	StoreID            string  `json:"store_id"`
	ImageID            string  `json:"image_id"`
	OperationID        string  `json:"operation_id"`
	ExpectedGeneration int64   `json:"expected_generation"`
	Reader             *string `json:"reader"`
}
type ImageSealReceipt struct {
	Version     int64  `json:"version"`
	StoreID     string `json:"store_id"`
	ImageID     string `json:"image_id"`
	OperationID string `json:"operation_id"`
	ImageDigest string `json:"image_digest"`
	Activation  int64  `json:"activation"`
	Generation  int64  `json:"generation"`
}
type ImageReaderReceipt struct {
	Version       int64   `json:"version"`
	StoreID       string  `json:"store_id"`
	ImageID       string  `json:"image_id"`
	OperationID   string  `json:"operation_id"`
	RequestDigest string  `json:"request_digest"`
	Activation    int64   `json:"activation"`
	Generation    int64   `json:"generation"`
	Reader        *string `json:"reader"`
}
type ImageRecord struct {
	Seal       ImageSeal
	Digest     string
	State      string
	Receipt    *ImageSealReceipt
	Reader     *string
	Activation int64
	Generation int64
}

func ImageIDValid(s, prefix string) bool {
	if len(s) != len(prefix)+32 || len(s) <= len(prefix) || s[:len(prefix)] != prefix {
		return false
	}
	tail := s[len(prefix):]
	if !imageHex(tail, 16) {
		return false
	}
	return tail != "00000000000000000000000000000000"
}
func imageHex(s string, n int) bool {
	if len(s) != n*2 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func ImageHashValid(s string) bool { return imageHex(s, 32) }
func imageIdentifier(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// DecodeImageCanonical admits exactly the typed encoder's bytes. Equality
// rejects duplicate/missing/unknown members, nulls, alternate numbers, escapes,
// key order, whitespace and trailing data, including in nested receipts.
func DecodeImageCanonical(b []byte, v any) error {
	if len(b) == 0 || len(b) > MaxImageEnvelope || !utf8.Valid(b) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return ErrInvalid
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonical, b) {
		return ErrInvalid
	}
	return nil
}
func imageDigest(domain string, b []byte) string {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write([]byte{0, 1})
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}
func (s ImageSeal) Canonical() ([]byte, string, error) {
	if s.Version != 1 || !ImageIDValid(s.StoreID, "ims_") || !imageIdentifier(s.Volume) || continuity.LibraryID(s.LibraryID).Validate() != nil ||
		!ImageIDValid(s.ImageID, "img_") || !ImageIDValid(s.OperationID, "iop_") || !imageIdentifier(s.Incarnation) || !ImageHashValid(s.Producer) || !ImageHashValid(s.InitialReader) ||
		s.Content.Size < 0 || s.Content.Size > MaxImageBytes || !ImageHashValid(s.Content.SHA256) || s.Receipts == nil || len(s.Receipts) > MaxImageChunks {
		return nil, "", ErrInvalid
	}
	var offset int64
	seen := map[string]bool{}
	for i, r := range s.Receipts {
		c := r.Chunk
		if r.Kind != "custody-v1" || r.Volume != s.Volume || len(c.Session) != 129 || c.Session[:64] != s.Producer || c.Session[64] != '.' || !ImageHashValid(c.Session[65:]) ||
			c.Epoch <= 0 || !imageIdentifier(c.ID) || c.FileID != s.Incarnation || c.Offset != offset || c.Size <= 0 || c.Size > 8<<20 || !ImageHashValid(c.SHA256) ||
			!ImageHashValid(r.ArtifactSHA256) || !ImageHashValid(r.RequestSHA256) || !ImageHashValid(r.ReceiptID) || r.CommittedAtUnixNS <= 0 || seen[c.ID] {
			return nil, "", ErrInvalid
		}
		if i > 0 && (c.Session != s.Receipts[0].Chunk.Session || c.Epoch != s.Receipts[0].Chunk.Epoch) {
			return nil, "", ErrInvalid
		}
		seen[c.ID] = true
		offset += c.Size
		if offset > s.Content.Size {
			return nil, "", ErrInvalid
		}
	}
	if offset != s.Content.Size || (s.Content.Size == 0 && s.Content.SHA256 != DigestBytes(nil)) {
		return nil, "", ErrInvalid
	}
	b, err := json.Marshal(s)
	if err != nil || len(b) > MaxImageEnvelope {
		return nil, "", ErrInvalid
	}
	return b, imageDigest("juicemount.gateway.image", b), nil
}
func (r ImageRef) Validate() error {
	if r.Version != 1 || !ImageIDValid(r.StoreID, "ims_") || continuity.LibraryID(r.LibraryID).Validate() != nil || !ImageIDValid(r.ImageID, "img_") {
		return ErrInvalid
	}
	return nil
}
func (r ImageReaderChange) Canonical() ([]byte, string, error) {
	if r.Version != 1 || !ImageIDValid(r.StoreID, "ims_") || !ImageIDValid(r.ImageID, "img_") || !ImageIDValid(r.OperationID, "irg_") || r.ExpectedGeneration < 1 || r.ExpectedGeneration == int64(^uint64(0)>>1) || r.Reader != nil && !ImageHashValid(*r.Reader) {
		return nil, "", ErrInvalid
	}
	b, err := json.Marshal(r)
	return b, imageDigest("juicemount.gateway.image-reader", b), err
}
func (r ImageRecord) View() ImageView {
	parts := make([]ImagePartView, len(r.Seal.Receipts))
	for i, p := range r.Seal.Receipts {
		parts[i] = ImagePartView{int64(i), p.Chunk.Offset, p.Chunk.Size, p.Chunk.SHA256}
	}
	s := r.Seal
	return ImageView{1, s.StoreID, s.Volume, s.LibraryID, s.ImageID, s.Incarnation, r.Digest, s.Content, r.Activation, r.Generation, parts}
}
func (c ImageConfig) Validate() error {
	if c.Version != 1 || !ImageIDValid(c.StoreID, "ims_") || c.MaxImages < 1 || c.MaxImages > MaxImages || c.MaxLogicalBytes < 0 || c.MaxLogicalBytes > MaxImageLogicalBytes || c.MaxMetadataBytes < 1 || c.MaxMetadataBytes > MaxImageMetadataBytes {
		return fmt.Errorf("%w: image configuration", ErrInvalid)
	}
	return nil
}
func (v ImageView) Validate() error {
	if (ImageRef{v.Version, v.StoreID, v.LibraryID, v.ImageID}).Validate() != nil || !imageIdentifier(v.Volume) || !imageIdentifier(v.Incarnation) || !ImageHashValid(v.ImageDigest) || v.Activation < 1 || v.Generation < 1 || v.Content.Size < 0 || v.Content.Size > MaxImageBytes || !ImageHashValid(v.Content.SHA256) || v.Chunks == nil || len(v.Chunks) > MaxImageChunks {
		return ErrInvalid
	}
	var offset int64
	for i, p := range v.Chunks {
		if p.Index != int64(i) || p.Offset != offset || p.Size < 1 || p.Size > 8<<20 || !ImageHashValid(p.SHA256) {
			return ErrInvalid
		}
		offset += p.Size
		if offset > v.Content.Size {
			return ErrInvalid
		}
	}
	if offset != v.Content.Size || v.Content.Size == 0 && v.Content.SHA256 != DigestBytes(nil) {
		return ErrInvalid
	}
	return nil
}

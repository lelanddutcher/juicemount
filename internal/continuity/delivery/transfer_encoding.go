package delivery

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

const (
	MaxTransferRequestBytes = 1024
	MaxTransferHeaderBytes  = 4096
	MaxTransferEndBytes     = 2048
	MaxTransferChunkBytes   = 1 << 20
)

var ErrInvalidTransfer = errors.New("invalid delivery transfer")
var ErrTransferClosed = errors.New("delivery transfer closed")
var ErrTransferIncomplete = errors.New("delivery transfer incomplete")

// TransferRequest's cursor never creates a new selection or materialization.
type TransferRequest struct {
	Version      int                  `json:"version"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	SelectionID  string               `json:"selection_id"`
	OperationID  string               `json:"operation_id"`
	Offset       int64                `json:"offset"`
	PrefixSHA256 string               `json:"prefix_sha256"`
}

type TransferHeader struct {
	Version              int                            `json:"version"`
	RequestSHA256        string                         `json:"request_sha256"`
	AuthorityIncarnation string                         `json:"authority_incarnation"`
	Selection            catalog.DeliverySelection      `json:"selection"`
	Attempt              catalog.MaterializationAttempt `json:"attempt"`
	Offset               int64                          `json:"offset"`
	Length               int64                          `json:"length"`
	PrefixSHA256         string                         `json:"prefix_sha256"`
}

type TransferEnd struct {
	Version       int    `json:"version"`
	RequestSHA256 string `json:"request_sha256"`
	AttemptID     string `json:"attempt_id"`
	Offset        int64  `json:"offset"`
	Length        int64  `json:"length"`
	Result        string `json:"result"`
}

func transferOperationID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func emptyPrefixSHA256() string {
	h := sha256.Sum256(nil)
	return hex.EncodeToString(h[:])
}
func transferCursor(offset int64, prefix string) bool {
	return offset >= 0 && offset <= mutationauthority.MaxImageBytes && mutationauthority.ImageHashValid(prefix) && (offset != 0 || prefix == emptyPrefixSHA256())
}
func CanonicalTransferRequest(r TransferRequest) ([]byte, string, error) {
	if r.Version != 1 || r.LibraryID.Validate() != nil || !mutationauthority.ImageIDValid(r.SelectionID, "dsel_") || !transferOperationID(r.OperationID) || !transferCursor(r.Offset, r.PrefixSHA256) {
		return nil, "", ErrInvalidTransfer
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > MaxTransferRequestBytes {
		return nil, "", ErrInvalidTransfer
	}
	h := sha256.New()
	_, _ = h.Write([]byte("juicemount.continuity.native-transfer\x00\x01"))
	_, _ = h.Write(b)
	return b, hex.EncodeToString(h.Sum(nil)), nil
}
func DecodeTransferRequest(b []byte) (TransferRequest, error) {
	var r TransferRequest
	if len(b) == 0 || len(b) > MaxTransferRequestBytes || mutationauthority.DecodeImageCanonical(b, &r) != nil {
		return TransferRequest{}, ErrInvalidTransfer
	}
	canonical, _, err := CanonicalTransferRequest(r)
	if err != nil || !bytes.Equal(canonical, b) {
		return TransferRequest{}, ErrInvalidTransfer
	}
	return r, nil
}
func CanonicalTransferHeader(h TransferHeader) ([]byte, error) {
	s, a := h.Selection, h.Attempt
	if h.Version != 1 || !mutationauthority.ImageHashValid(h.RequestSHA256) || !mutationauthority.ImageIDValid(h.AuthorityIncarnation, "auth_") || s.Version != 1 || s.LibraryID.Validate() != nil || !mutationauthority.ImageIDValid(s.SelectionID, "dsel_") || !transferOperationID(s.OperationID) || s.AssetID.Validate() != nil || s.RevisionID.Validate() != nil || !mutationauthority.ImageIDValid(s.SourceID, "dsrc_") || s.SourceVersion < 1 || s.Content.Validate() != nil || s.Content.Size > mutationauthority.MaxImageBytes || s.ExpiresUnix <= 0 || s.Canceled || !mutationauthority.ImageIDValid(a.ID, "dmat_") || a.SelectionID != s.SelectionID || !transferOperationID(a.OperationID) || a.State != "COMPLETED" || a.Content != s.Content || !transferCursor(h.Offset, h.PrefixSHA256) || h.Offset > s.Content.Size || h.Length != s.Content.Size-h.Offset {
		return nil, ErrInvalidTransfer
	}
	b, err := json.Marshal(h)
	if err != nil || len(b) > MaxTransferHeaderBytes {
		return nil, ErrInvalidTransfer
	}
	return b, nil
}
func DecodeTransferHeader(b []byte) (TransferHeader, error) {
	var h TransferHeader
	if len(b) == 0 || len(b) > MaxTransferHeaderBytes || mutationauthority.DecodeImageCanonical(b, &h) != nil {
		return TransferHeader{}, ErrInvalidTransfer
	}
	canonical, err := CanonicalTransferHeader(h)
	if err != nil || !bytes.Equal(canonical, b) {
		return TransferHeader{}, ErrInvalidTransfer
	}
	return h, nil
}
func CanonicalTransferEnd(e TransferEnd) ([]byte, error) {
	if e.Version != 1 || !mutationauthority.ImageHashValid(e.RequestSHA256) || !mutationauthority.ImageIDValid(e.AttemptID, "dmat_") || e.Offset < 0 || e.Length < 0 || e.Offset > mutationauthority.MaxImageBytes || e.Length > mutationauthority.MaxImageBytes-e.Offset || e.Result != "server_stream_complete" {
		return nil, ErrInvalidTransfer
	}
	b, err := json.Marshal(e)
	if err != nil || len(b) > MaxTransferEndBytes {
		return nil, ErrInvalidTransfer
	}
	return b, nil
}
func DecodeTransferEnd(b []byte) (TransferEnd, error) {
	var e TransferEnd
	if len(b) == 0 || len(b) > MaxTransferEndBytes || mutationauthority.DecodeImageCanonical(b, &e) != nil {
		return TransferEnd{}, ErrInvalidTransfer
	}
	canonical, err := CanonicalTransferEnd(e)
	if err != nil || !bytes.Equal(canonical, b) {
		return TransferEnd{}, ErrInvalidTransfer
	}
	return e, nil
}

// ValidateTransferResponse compares all cursor and pin links; canonical decoders
// alone cannot establish that a valid response belongs to this request.
func ValidateTransferResponse(r TransferRequest, expected catalog.DeliverySelection, h TransferHeader, e *TransferEnd) error {
	_, digest, err := CanonicalTransferRequest(r)
	if err != nil {
		return err
	}
	if _, err := CanonicalTransferHeader(h); err != nil {
		return err
	}
	if h.Selection != expected || h.RequestSHA256 != digest || h.Selection.LibraryID != r.LibraryID || h.Selection.SelectionID != r.SelectionID || h.Attempt.OperationID != r.OperationID || h.Offset != r.Offset || h.PrefixSHA256 != r.PrefixSHA256 {
		return ErrInvalidTransfer
	}
	if e != nil {
		if _, err := CanonicalTransferEnd(*e); err != nil {
			return err
		}
		if e.RequestSHA256 != digest || e.AttemptID != h.Attempt.ID || e.Offset != h.Offset || e.Length != h.Length {
			return ErrInvalidTransfer
		}
	}
	return nil
}

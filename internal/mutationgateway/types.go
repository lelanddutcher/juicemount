// Package mutationgateway provides authenticated, immutable upload custody.
// Custody is not media publication: no API here retires a client spool row.
package mutationgateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

const (
	MaxChunkBytes    = 8 << 20
	format           = "custody-v1"
	maxArtifactBytes = MaxChunkBytes + 4096
	maxWireBytes     = 12 << 20
)

var (
	ErrIntegrity    = errors.New("custody artifact integrity could not be established")
	ErrUnauthorized = errors.New("client does not own this operation")
	identifier      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// Chunk identifies immutable bytes within one client file incarnation. FileID
// is not a public path. Rewriting an offset requires a new operation ID.
type Chunk struct {
	Session string `json:"session"`
	Epoch   int64  `json:"epoch"`
	ID      string `json:"id"`
	FileID  string `json:"file_id"`
	Offset  int64  `json:"offset"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type Upload struct {
	Chunk Chunk  `json:"chunk"`
	Data  []byte `json:"data"`
}

type Lease struct {
	Volume      string `json:"volume"`
	Session     string `json:"session"`
	Epoch       int64  `json:"epoch"`
	ExpiresAtNS int64  `json:"expires_at_ns"`
}

func wireLease(l mutationauthority.Lease) Lease {
	return Lease{Volume: l.Volume, Session: l.Session, Epoch: l.Epoch, ExpiresAtNS: l.ExpiresAtNS}
}

func (l Lease) authority() mutationauthority.Lease {
	return mutationauthority.Lease{Volume: l.Volume, Session: l.Session, Epoch: l.Epoch, ExpiresAtNS: l.ExpiresAtNS}
}

// CustodyReceipt proves only the immutable stage effect named by Chunk. It
// deliberately has no media revision or conversion to a publication receipt.
// Consumers must verify readback before any future chunk custody transfer.
type CustodyReceipt struct {
	Kind              string `json:"kind"`
	Volume            string `json:"volume"`
	Chunk             Chunk  `json:"chunk"`
	ArtifactSHA256    string `json:"artifact_sha256"`
	ArtifactSize      int64  `json:"artifact_size"`
	RequestSHA256     string `json:"request_sha256"`
	ReceiptID         string `json:"receipt_id"`
	CommittedAtUnixNS int64  `json:"committed_at_unix_ns"`
}

type artifactHeader struct {
	Kind   string `json:"kind"`
	Volume string `json:"volume"`
	Chunk  Chunk  `json:"chunk"`
}

func hash(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

func validHash(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func validSession(s string) bool {
	return len(s) == 129 && s[64] == '.' && validHash(s[:64]) && validHash(s[65:])
}

func (c Chunk) valid() bool {
	return validSession(c.Session) && c.Epoch > 0 && identifier.MatchString(c.ID) &&
		identifier.MatchString(c.FileID) && c.Offset >= 0 && c.Size > 0 &&
		c.Size <= MaxChunkBytes && c.Offset <= int64(^uint64(0)>>1)-c.Size && validHash(c.SHA256)
}

func (g *Gateway) request(c Chunk) mutationauthority.Request {
	b, _ := json.Marshal(artifactHeader{Kind: format, Volume: g.volume, Chunk: c})
	return mutationauthority.Request{
		Lease: mutationauthority.Lease{Volume: g.volume, Session: c.Session, Epoch: c.Epoch},
		ID:    format + ":" + c.ID, Digest: hash(b), ExpectedRevision: format, Kind: mutationauthority.CustodyOperation,
	}
}

func sameRequest(a, b mutationauthority.Request) bool {
	return a.Volume == b.Volume && a.Session == b.Session && a.Epoch == b.Epoch &&
		a.ID == b.ID && a.Digest == b.Digest && a.ExpectedRevision == b.ExpectedRevision && a.Kind == b.Kind && a.FileID == b.FileID
}

// The v1 wire receipt ID hashes the original nine-field journal projection.
// Keep its names/order/encoding stable across journal schema migrations.
func custodyCommitBytes(r *mutationauthority.Receipt) []byte {
	b, _ := json.Marshal(struct {
		Volume            string
		ID                string
		Session           string
		Epoch             int64
		Digest            string
		ExpectedRevision  string
		Revision          string
		Evidence          string
		CommittedAtUnixNS int64
	}{r.Volume, r.ID, r.Session, r.Epoch, r.Digest, r.ExpectedRevision, r.Revision, r.Evidence, r.CommittedAtUnixNS})
	return b
}

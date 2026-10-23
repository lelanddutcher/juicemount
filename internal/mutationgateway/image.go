package mutationgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

type ImageConfig = mutationauthority.ImageConfig
type ImageContent = mutationauthority.ImageContent
type ImageSeal = mutationauthority.ImageSeal
type ImageRef = mutationauthority.ImageRef
type ImageView = mutationauthority.ImageView
type ImageRead = mutationauthority.ImageRead
type ImageReaderChange = mutationauthority.ImageReaderChange
type ImageSealReceipt = mutationauthority.ImageSealReceipt
type ImageReaderReceipt = mutationauthority.ImageReaderReceipt
type ImageCustodyReceipt = mutationauthority.ImageCustodyReceipt

type ImageSealStatus struct {
	Version     int64             `json:"version"`
	StoreID     string            `json:"store_id"`
	ImageID     string            `json:"image_id"`
	ImageDigest string            `json:"image_digest"`
	State       string            `json:"state"`
	Receipt     *ImageSealReceipt `json:"receipt"`
}

// ImageCustody preserves the historical receipt projection when binding
// previously committed custody into a new immutable image.
func ImageCustody(r CustodyReceipt) ImageCustodyReceipt {
	c := r.Chunk
	return ImageCustodyReceipt{r.Kind, r.Volume, mutationauthority.ImageChunk{c.Session, c.Epoch, c.ID, c.FileID, c.Offset, c.Size, c.SHA256}, r.ArtifactSHA256, r.ArtifactSize, r.RequestSHA256, r.ReceiptID, r.CommittedAtUnixNS}
}
func imageChunk(c mutationauthority.ImageChunk) Chunk {
	return Chunk{c.Session, c.Epoch, c.ID, c.FileID, c.Offset, c.Size, c.SHA256}
}
func imageRef(s ImageSeal) ImageRef { return ImageRef{1, s.StoreID, s.LibraryID, s.ImageID} }

func ReadImagesConfig(path string) (*ImageConfig, error) {
	if !filepath.IsAbs(path) {
		return nil, mutationauthority.ErrInvalid
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	f, err := privatefile.OpenAt(parent, filepath.Base(path), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if regularPrivate(f) != nil {
		return nil, mutationauthority.ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil {
		return nil, err
	}
	if len(b) > 8192 {
		return nil, mutationauthority.ErrInvalid
	}
	var c ImageConfig
	if mutationauthority.DecodeImageCanonical(b, &c) != nil || c.Validate() != nil {
		return nil, mutationauthority.ErrInvalid
	}
	return &c, nil
}

// imageStream retains at most one original artifact at a time, comparing its
// complete fresh receipt before feeding any part into the full-image hash.
type imageStream struct {
	ctx      context.Context
	g        *Gateway
	receipts []ImageCustodyReceipt
	index    int
	current  *bytes.Reader
}

func (r *imageStream) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	for {
		if r.current != nil {
			n, err := r.current.Read(p)
			if err == io.EOF {
				r.current = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		if r.index == len(r.receipts) {
			return 0, io.EOF
		}
		expected := r.receipts[r.index]
		r.index++
		actual, data, err := r.g.custody(r.ctx, imageChunk(expected.Chunk))
		if err != nil {
			return 0, err
		}
		if ImageCustody(actual) != expected {
			return 0, ErrIntegrity
		}
		r.current = bytes.NewReader(data)
	}
}
func (g *Gateway) sealImage(ctx context.Context, principal string, s ImageSeal, authorized func() error) (ImageSealReceipt, error) {
	if _, _, err := s.Canonical(); err != nil {
		return ImageSealReceipt{}, err
	}
	if s.Volume != g.volume || s.Producer != principal {
		return ImageSealReceipt{}, ErrUnauthorized
	}
	for _, r := range s.Receipts {
		if !owns(principal, r.Chunk.Session) {
			return ImageSealReceipt{}, ErrUnauthorized
		}
	}
	g.imageMu.Lock()
	if g.imageVerifying == nil {
		g.imageVerifying = map[string]bool{}
	}
	if g.imageVerifying[s.ImageID] {
		g.imageMu.Unlock()
		return ImageSealReceipt{}, mutationauthority.ErrInFlight
	}
	g.imageVerifying[s.ImageID] = true
	g.imageMu.Unlock()
	defer func() { g.imageMu.Lock(); delete(g.imageVerifying, s.ImageID); g.imageMu.Unlock() }()
	record, err := g.journal.ReserveImage(ctx, s)
	if err != nil {
		return ImageSealReceipt{}, err
	}
	if record.Receipt != nil {
		if err = authorized(); err != nil {
			return ImageSealReceipt{}, err
		}
		return *record.Receipt, nil
	}
	if g.imageHook != nil {
		g.imageHook("reserved")
	}
	stream := &imageStream{ctx: ctx, g: g, receipts: s.Receipts}
	if err = continuity.VerifyContent(stream, continuity.Content{Size: s.Content.Size, SHA256: s.Content.SHA256}); err != nil {
		return ImageSealReceipt{}, errors.Join(ErrIntegrity, err)
	}
	if g.imageHook != nil {
		g.imageHook("verified")
	}
	if err = authorized(); err != nil {
		return ImageSealReceipt{}, err
	}
	return g.journal.SealImage(ctx, imageRef(s), record.Digest)
}
func (g *Gateway) imageStatus(ctx context.Context, principal string, ref ImageRef) (ImageView, error) {
	record, err := g.journal.InspectImage(ctx, ref)
	if errors.Is(err, mutationauthority.ErrNoSuchOperation) {
		return ImageView{}, ErrUnauthorized
	}
	if err != nil {
		return ImageView{}, err
	}
	if record.Reader == nil || *record.Reader != principal {
		return ImageView{}, ErrUnauthorized
	}
	if record.State != "SEALED" {
		return ImageView{}, mutationauthority.ErrUncertain
	}
	return record.View(), nil
}
func (g *Gateway) imageRead(ctx context.Context, principal string, request ImageRead, authorized func() error) ([]byte, ImageView, error) {
	ref := ImageRef{request.Version, request.StoreID, request.LibraryID, request.ImageID}
	if ref.Validate() != nil || !validHash(request.ImageDigest) || request.Activation < 1 || request.Generation < 1 || request.Index < 0 {
		return nil, ImageView{}, mutationauthority.ErrInvalid
	}
	record, err := g.journal.InspectImage(ctx, ref)
	if errors.Is(err, mutationauthority.ErrNoSuchOperation) {
		return nil, ImageView{}, ErrUnauthorized
	}
	if err != nil {
		return nil, ImageView{}, err
	}
	if record.Reader == nil || *record.Reader != principal {
		return nil, ImageView{}, ErrUnauthorized
	}
	if record.State != "SEALED" || record.Digest != request.ImageDigest || record.Activation != request.Activation || record.Generation != request.Generation || request.Index >= int64(len(record.Seal.Receipts)) {
		return nil, ImageView{}, mutationauthority.ErrImageConflict
	}
	expected := record.Seal.Receipts[request.Index]
	receipt, data, err := g.custody(ctx, imageChunk(expected.Chunk))
	if err != nil {
		return nil, ImageView{}, err
	}
	if ImageCustody(receipt) != expected {
		return nil, ImageView{}, ErrIntegrity
	}
	if g.imageHook != nil {
		g.imageHook("read-verified")
	}
	current, err := g.imageStatus(ctx, principal, ref)
	if err != nil {
		return nil, ImageView{}, err
	}
	if current.ImageDigest != record.Digest || current.Activation != request.Activation || current.Generation != request.Generation {
		return nil, ImageView{}, mutationauthority.ErrImageConflict
	}
	if err = authorized(); err != nil {
		return nil, ImageView{}, err
	}
	return data, current, nil
}
func imageJSON(v any) []byte { b, _ := json.Marshal(v); return b }

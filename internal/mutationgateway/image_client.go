package mutationgateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func (c *Client) imageCall(ctx context.Context, path string, input any, out any) error {
	resp, err := c.imageResponse(ctx, path, input)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, mutationauthority.MaxImageEnvelope+1))
	if err != nil {
		return err
	}
	if resp.Header.Get("Content-Type") != "application/json" || mutationauthority.DecodeImageCanonical(b, out) != nil {
		return ErrIntegrity
	}
	return nil
}
func (c *Client) imageResponse(ctx context.Context, path string, input any) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(imageJSON(input)))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("gateway HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return resp, nil
}
func (c *Client) SealImage(ctx context.Context, seal ImageSeal) (ImageSealReceipt, error) {
	_, digest, err := seal.Canonical()
	if err != nil {
		return ImageSealReceipt{}, err
	}
	var receipt ImageSealReceipt
	if err = c.imageCall(ctx, "/v1/images/seal", seal, &receipt); err != nil {
		return receipt, err
	}
	if receipt != (ImageSealReceipt{1, seal.StoreID, seal.ImageID, seal.OperationID, digest, 1, 1}) {
		return ImageSealReceipt{}, ErrIntegrity
	}
	return receipt, nil
}

// ImageSealStatus takes the original immutable envelope so a status response is
// checked against the exact binding, rather than a receipt-shaped document.
func (c *Client) ImageSealStatus(ctx context.Context, seal ImageSeal) (ImageSealStatus, error) {
	_, digest, err := seal.Canonical()
	if err != nil {
		return ImageSealStatus{}, err
	}
	var status ImageSealStatus
	if err = c.imageCall(ctx, "/v1/images/seal-status", imageRef(seal), &status); err != nil {
		return status, err
	}
	if status.Version != 1 || status.StoreID != seal.StoreID || status.ImageID != seal.ImageID || status.ImageDigest != digest {
		return ImageSealStatus{}, ErrIntegrity
	}
	if status.State == "RESERVED" && status.Receipt == nil {
		return status, nil
	}
	if status.State != "SEALED" || status.Receipt == nil || *status.Receipt != (ImageSealReceipt{1, seal.StoreID, seal.ImageID, seal.OperationID, digest, 1, 1}) {
		return ImageSealStatus{}, ErrIntegrity
	}
	return status, nil
}
func (c *Client) ImageStatus(ctx context.Context, ref ImageRef) (ImageView, error) {
	if ref.Validate() != nil {
		return ImageView{}, mutationauthority.ErrInvalid
	}
	var view ImageView
	if err := c.imageCall(ctx, "/v1/images/status", ref, &view); err != nil {
		return view, err
	}
	if view.Validate() != nil || view.Version != ref.Version || view.StoreID != ref.StoreID || view.LibraryID != ref.LibraryID || view.ImageID != ref.ImageID {
		return ImageView{}, ErrIntegrity
	}
	return view, nil
}
func (c *Client) RebindImageReader(ctx context.Context, change ImageReaderChange) (ImageReaderReceipt, error) {
	_, digest, err := change.Canonical()
	if err != nil {
		return ImageReaderReceipt{}, err
	}
	var receipt ImageReaderReceipt
	if err = c.imageCall(ctx, "/v1/images/reader", change, &receipt); err != nil {
		return receipt, err
	}
	expected := ImageReaderReceipt{1, change.StoreID, change.ImageID, change.OperationID, digest, 1, change.ExpectedGeneration + 1, change.Reader}
	if !bytes.Equal(imageJSON(receipt), imageJSON(expected)) {
		return ImageReaderReceipt{}, ErrIntegrity
	}
	return receipt, nil
}

// ImageReadback pins an online current view before requesting one exact chunk.
// Consumers assembling all chunks must separately verify full content size/SHA.
func (c *Client) ImageReadback(ctx context.Context, pinned ImageView, index int64) ([]byte, error) {
	if pinned.Validate() != nil || index < 0 || index >= int64(len(pinned.Chunks)) {
		return nil, mutationauthority.ErrInvalid
	}
	current, err := c.ImageStatus(ctx, ImageRef{1, pinned.StoreID, pinned.LibraryID, pinned.ImageID})
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(imageJSON(current), imageJSON(pinned)) {
		return nil, ErrIntegrity
	}
	request := ImageRead{1, pinned.StoreID, pinned.LibraryID, pinned.ImageID, pinned.ImageDigest, pinned.Activation, pinned.Generation, index}
	resp, err := c.imageResponse(ctx, "/v1/images/read", request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	part := pinned.Chunks[index]
	if resp.Header.Get("Content-Type") != "application/octet-stream" || resp.ContentLength != part.Size ||
		resp.Header.Get("X-JM-Image-Digest") != pinned.ImageDigest || resp.Header.Get("X-JM-Image-Activation") != strconv.FormatInt(pinned.Activation, 10) ||
		resp.Header.Get("X-JM-Image-Generation") != strconv.FormatInt(pinned.Generation, 10) || resp.Header.Get("X-JM-Image-Index") != strconv.FormatInt(index, 10) || resp.Header.Get("X-JM-Chunk-SHA256") != part.SHA256 {
		return nil, ErrIntegrity
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, part.Size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != part.Size || hash(b) != part.SHA256 {
		return nil, ErrIntegrity
	}
	return b, nil
}

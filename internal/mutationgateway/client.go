package mutationgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"crypto/tls"
	"crypto/x509"

	"github.com/lelanddutcher/juicemount/internal/mutationclient"
)

// Client supplies typed custody APIs only. It never publishes media, retires a
// spool row, punches local bytes, or retries an ambiguous mutation by itself.
type Client struct {
	base string
	http *http.Client
	image *mutationclient.Client
}

func NewClient(endpoint string, cert tls.Certificate, roots *x509.CertPool) (*Client, error) {
	image, err := mutationclient.NewClient(endpoint, cert, roots)
	if err != nil { return nil, err }
	base, transport := image.Transport()
	return &Client{base: base, http: transport, image: image}, nil
}

func (c *Client) Close() { c.http.CloseIdleConnections() }

func (c *Client) call(ctx context.Context, method, path string, input any) ([]byte, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	limit := int64(maxArtifactBytes)
	if resp.StatusCode != http.StatusOK {
		limit = 4096
	}
	out, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, fmt.Errorf("gateway response exceeds limit")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(out))
	}
	return out, nil
}

func (c *Client) Claim(ctx context.Context) (Lease, error) {
	var l Lease
	b, err := c.call(ctx, http.MethodPost, "/v1/claim", struct{}{})
	if err == nil {
		err = json.Unmarshal(b, &l)
	}
	return l, err
}

func (c *Client) Renew(ctx context.Context, l Lease) (Lease, error) {
	var next Lease
	b, err := c.call(ctx, http.MethodPost, "/v1/renew", l)
	if err == nil {
		err = json.Unmarshal(b, &next)
	}
	return next, err
}

func (c *Client) Upload(ctx context.Context, u Upload) (CustodyReceipt, error) {
	if !u.Chunk.valid() || int64(len(u.Data)) != u.Chunk.Size || hash(u.Data) != u.Chunk.SHA256 {
		return CustodyReceipt{}, fmt.Errorf("invalid upload")
	}
	b, err := c.call(ctx, http.MethodPut, "/v1/custody/upload", u)
	return decodeReceipt(b, u.Chunk, err)
}

func (c *Client) Status(ctx context.Context, chunk Chunk) (CustodyReceipt, error) {
	b, err := c.call(ctx, http.MethodPost, "/v1/custody/status", chunk)
	return decodeReceipt(b, chunk, err)
}

func decodeReceipt(b []byte, chunk Chunk, err error) (CustodyReceipt, error) {
	var receipt CustodyReceipt
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(b, &receipt); err != nil {
		return receipt, err
	}
	header, _ := json.Marshal(artifactHeader{Kind: format, Volume: receipt.Volume, Chunk: chunk})
	if receipt.Kind != format || !identifier.MatchString(receipt.Volume) || receipt.Chunk != chunk || !validHash(receipt.ArtifactSHA256) || receipt.CommittedAtUnixNS <= 0 ||
		!validHash(receipt.ReceiptID) || receipt.RequestSHA256 != hash(header) || receipt.ArtifactSize != 4+int64(len(header))+chunk.Size {
		return CustodyReceipt{}, ErrIntegrity
	}
	return receipt, nil
}

func (c *Client) Readback(ctx context.Context, receipt CustodyReceipt) ([]byte, error) {
	encoded, _ := json.Marshal(receipt)
	if _, err := decodeReceipt(encoded, receipt.Chunk, nil); err != nil || !receipt.Chunk.valid() {
		return nil, ErrIntegrity
	}
	// Confirm the exact committed receipt online; shape-valid client JSON is
	// not authority. Immutable readback then independently verifies its bytes.
	confirmed, err := c.Status(ctx, receipt.Chunk)
	if err != nil {
		return nil, err
	}
	if confirmed != receipt {
		return nil, ErrIntegrity
	}
	b, err := c.call(ctx, http.MethodPost, "/v1/custody/readback", receipt.Chunk)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != receipt.Chunk.Size || hash(b) != receipt.Chunk.SHA256 ||
		hash(artifactBytes(artifactHeader{Kind: format, Volume: receipt.Volume, Chunk: receipt.Chunk}, b)) != receipt.ArtifactSHA256 {
		return nil, ErrIntegrity
	}
	return b, nil
}

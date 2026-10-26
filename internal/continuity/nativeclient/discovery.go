package nativeclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const clientDiscoveryPageLimit = 1 << 20

func boundedDiscoveryBody(ctx context.Context, cancel context.CancelFunc, body io.Reader) ([]byte, error) {
	var result []byte
	var buffer [32 << 10]byte
	for {
		n, err := readStalled(ctx, cancel, body, buffer[:])
		if len(result)+n > clientDiscoveryPageLimit {
			return nil, ErrInvalidResponse
		}
		result = append(result, buffer[:n]...)
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrInvalidResponse
		}
		if n == 0 {
			return nil, ErrInvalidResponse
		}
	}
}

func decodeDiscoveryRemoteError(status int, b []byte) error {
	var value struct {
		Version int    `json:"version"`
		Error   string `json:"error"`
	}
	if decodeCanonicalResponse(b, &value) != nil || value.Version != 1 {
		return ErrInvalidResponse
	}
	valid := map[int][]string{
		400: {"invalid"}, 401: {"unauthenticated"}, 404: {"not_found"},
		409: {"restart_query", "conflict", "expired_request"},
		422: {"work_budget"}, 503: {"busy", "unavailable"}, 504: {"deadline"},
	}
	for _, code := range valid[status] {
		if value.Error == code {
			return &RemoteError{StatusCode: status, Code: code}
		}
	}
	return ErrInvalidResponse
}

// Discover requests only the existing v2 search or a retained opaque page.
// It uses the Client's pinned endpoint, library, OS-trusted transport and
// original close/cancel owner. A search result is metadata, not a byte pin.
func (c *Client) Discover(ctx context.Context, request catalog.DiscoveryRequest) (catalog.DiscoverySearchPage, error) {
	if ctx == nil || c == nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidClient
	}
	encoded, err := catalog.CanonicalNativeDiscoveryRequest(request, c.library)
	if err != nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidClient
	}
	authorization, err := c.enter()
	if err != nil {
		return catalog.DiscoverySearchPage{}, err
	}
	defer c.leave()
	requestCtx, cancel := c.requestContext(ctx, time.Now().Add(clientStallLimit))
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.endpoint.URL+"/api/continuity/v2/"+request.Kind, bytes.NewReader(encoded))
	if err != nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidClient
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		if requestCtx.Err() != nil {
			return catalog.DiscoverySearchPage{}, requestCtx.Err()
		}
		return catalog.DiscoverySearchPage{}, ErrTransport
	}
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()
	if !responseBase(resp, "application/json") {
		return catalog.DiscoverySearchPage{}, ErrInvalidResponse
	}
	if resp.StatusCode != http.StatusOK {
		body, err := boundedBody(requestCtx, cancel, resp.Body)
		if err != nil {
			return catalog.DiscoverySearchPage{}, err
		}
		closed = true
		if resp.Body.Close() != nil {
			return catalog.DiscoverySearchPage{}, ErrInvalidResponse
		}
		return catalog.DiscoverySearchPage{}, decodeDiscoveryRemoteError(resp.StatusCode, body)
	}
	if resp.ContentLength > clientDiscoveryPageLimit {
		return catalog.DiscoverySearchPage{}, ErrInvalidResponse
	}
	body, err := boundedDiscoveryBody(requestCtx, cancel, resp.Body)
	if err != nil {
		return catalog.DiscoverySearchPage{}, err
	}
	closed = true
	if resp.Body.Close() != nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidResponse
	}
	if requestCtx.Err() != nil {
		return catalog.DiscoverySearchPage{}, requestCtx.Err()
	}
	page, err := catalog.DecodeDiscoverySearchPage(body)
	if err != nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidResponse
	}
	return page, nil
}

// Search and Page keep the wire route choice explicit for helper callers.
func (c *Client) Search(ctx context.Context, request catalog.DiscoveryRequest) (catalog.DiscoverySearchPage, error) {
	if request.Kind != "search" {
		return catalog.DiscoverySearchPage{}, ErrInvalidClient
	}
	return c.Discover(ctx, request)
}

func (c *Client) Page(ctx context.Context, token string) (catalog.DiscoverySearchPage, error) {
	if c == nil {
		return catalog.DiscoverySearchPage{}, ErrInvalidClient
	}
	return c.Discover(ctx, catalog.DiscoveryRequest{Version: 2, Kind: "page", LibraryID: c.library, Token: token})
}

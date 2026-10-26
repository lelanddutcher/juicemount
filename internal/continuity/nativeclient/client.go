package nativeclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

const (
	clientMetadataLimit = 2048
	clientStallLimit    = 30 * time.Second
	clientTotalLimit    = 900 * time.Second
)

var ErrInvalidClient = errors.New("invalid native delivery client")
var ErrClientClosed = errors.New("native delivery client closing")
var ErrClientDraining = errors.New("native delivery client still draining")
var ErrInvalidResponse = errors.New("invalid native delivery response")
var ErrTransport = errors.New("native delivery transport failed")

// RemoteError contains only the fixed public error code, never a server body or
// bearer. Callers may reconcile the same operation after an uncertain transfer.
type RemoteError struct {
	StatusCode int
	Code       string
}

func (e *RemoteError) Error() string { return "native delivery: " + e.Code }

// Client binds every request to one configured endpoint and library. Its bearer
// is copied into memory and is never put in a URL, cookie jar, proxy, or log.
// Close cancels and joins active request/body owners; a timed-out Close remains
// draining and can be retried against the same original owners.
type Client struct {
	endpoint  Endpoint
	library   continuity.LibraryID
	http      *http.Client
	transport *http.Transport

	mu       sync.Mutex
	bearer   []byte
	active   int
	closing  bool
	drained  chan struct{}
	lifetime context.Context
	cancel   context.CancelFunc
}

func validBearer(b []byte) bool {
	if len(b) != 102 || string(b[:5]) != "jmc1_" || b[37] != '.' {
		return false
	}
	for i, ch := range b {
		if i >= 5 && i < 37 || i > 37 {
			if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
				return false
			}
		}
	}
	return true
}

// NewClient uses OS TLS roots for HTTPS. HTTP requires ParseEndpoint's explicit
// opt-in and canonical numeric loopback restriction. There is no TLS override.
func NewClient(rawEndpoint string, library continuity.LibraryID, bearer []byte, allowLoopbackHTTP bool) (*Client, error) {
	endpoint, err := ParseEndpoint(rawEndpoint, allowLoopbackHTTP)
	if err != nil || library.Validate() != nil || !validBearer(bearer) {
		return nil, ErrInvalidClient
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: clientStallLimit,
		DisableCompression:    true,
		IdleConnTimeout:       clientStallLimit,
	}
	return newClient(endpoint, library, bearer, &http.Client{
		Transport:     transport,
		Jar:           nil,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, transport), nil
}

func newClient(endpoint Endpoint, library continuity.LibraryID, bearer []byte, client *http.Client, transport *http.Transport) *Client {
	lifetime, cancel := context.WithCancel(context.Background())
	return &Client{endpoint: endpoint, library: library, bearer: append([]byte(nil), bearer...), http: client, transport: transport, drained: make(chan struct{}), lifetime: lifetime, cancel: cancel}
}

func (c *Client) enter() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return "", ErrClientClosed
	}
	c.active++
	return "Bearer " + string(c.bearer), nil
}

func (c *Client) leave() {
	c.mu.Lock()
	c.active--
	if c.closing && c.active == 0 {
		close(c.drained)
	}
	c.mu.Unlock()
}

func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidClient
	}
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		if c.active == 0 {
			close(c.drained)
		}
		c.cancel()
	}
	drained := c.drained
	c.mu.Unlock()
	select {
	case <-drained:
		// Prefer a proven join even when the caller's deadline has just fired.
	default:
		select {
		case <-drained:
		case <-ctx.Done():
			return errors.Join(ErrClientDraining, ctx.Err())
		}
	}
	c.mu.Lock()
	clear(c.bearer)
	c.bearer = nil
	c.mu.Unlock()
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return nil
}

func (c *Client) requestContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(parent, deadline)
	stop := context.AfterFunc(c.lifetime, cancel)
	return ctx, func() { stop(); cancel() }
}

func (c *Client) post(ctx context.Context, route string, body []byte, authorization string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint.URL+"/api/continuity/v1/delivery/"+route, bytes.NewReader(body))
	if err != nil {
		return nil, ErrInvalidClient
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Accept", "application/json")
	if route == "transfer" {
		req.Header.Set("Accept", "application/octet-stream")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrTransport
	}
	return resp, nil
}

func responseBase(resp *http.Response, contentType string) bool {
	return oneHeader(resp.Header, "Content-Type", contentType) && oneHeader(resp.Header, "Cache-Control", "no-store") && oneHeader(resp.Header, "X-Content-Type-Options", "nosniff") && len(resp.Header.Values("Content-Encoding")) == 0 && !resp.Uncompressed
}

func oneHeader(header http.Header, key, expected string) bool {
	values := header.Values(key)
	return len(values) == 1 && values[0] == expected
}

func readStalled(ctx context.Context, cancel context.CancelFunc, reader io.Reader, buffer []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	timer := time.AfterFunc(clientStallLimit, cancel)
	n, err := reader.Read(buffer)
	timer.Stop()
	if ctx.Err() != nil {
		return n, ctx.Err()
	}
	return n, err
}

func boundedBody(ctx context.Context, cancel context.CancelFunc, reader io.Reader) ([]byte, error) {
	var out []byte
	buffer := make([]byte, clientMetadataLimit+1)
	for len(out) <= clientMetadataLimit {
		maximum := clientMetadataLimit + 1 - len(out)
		n, err := readStalled(ctx, cancel, reader, buffer[:maximum])
		out = append(out, buffer[:n]...)
		if len(out) > clientMetadataLimit {
			return nil, ErrInvalidResponse
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil || n == 0 {
			return nil, ErrInvalidResponse
		}
	}
	return nil, ErrInvalidResponse
}

func decodeCanonicalResponse(body []byte, target any) error {
	if len(body) == 0 || len(body) > clientMetadataLimit || mutationauthority.DecodeImageCanonical(body, target) != nil {
		return ErrInvalidResponse
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, body) {
		return ErrInvalidResponse
	}
	return nil
}

func decodeRemoteError(status int, body []byte) error {
	var envelope struct {
		Version int    `json:"version"`
		Error   string `json:"error"`
	}
	if decodeCanonicalResponse(body, &envelope) != nil || envelope.Version != 1 {
		return ErrInvalidResponse
	}
	valid := map[int][]string{
		400: {"invalid"}, 401: {"unauthenticated"}, 404: {"unavailable", "denied"},
		409: {"conflict", "reconciliation_required"}, 429: {"admission_limit"},
		503: {"busy", "storage", "integrity"}, 504: {"deadline"},
	}
	for _, code := range valid[status] {
		if envelope.Error == code {
			return &RemoteError{StatusCode: status, Code: code}
		}
	}
	return ErrInvalidResponse
}

func (c *Client) metadata(ctx context.Context, route string, body []byte, heavy bool, target any) error {
	if ctx == nil || len(body) == 0 || len(body) > delivery.MaxTransferRequestBytes {
		return ErrInvalidClient
	}
	authorization, err := c.enter()
	if err != nil {
		return err
	}
	defer c.leave()
	limit := clientStallLimit
	if heavy {
		limit = clientTotalLimit
	}
	requestCtx, cancel := c.requestContext(ctx, time.Now().Add(limit))
	defer cancel()
	resp, err := c.post(requestCtx, route, body, authorization)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()
	if !responseBase(resp, "application/json") {
		return ErrInvalidResponse
	}
	encoded, err := boundedBody(requestCtx, cancel, resp.Body)
	if err != nil {
		return err
	}
	closed = true
	if err := resp.Body.Close(); err != nil {
		return ErrInvalidResponse
	}
	if resp.StatusCode != http.StatusOK {
		return decodeRemoteError(resp.StatusCode, encoded)
	}
	if err := requestCtx.Err(); err != nil {
		return err
	}
	return decodeCanonicalResponse(encoded, target)
}

func validInspection(s catalog.DeliveryInspection) bool {
	if !validSelection(s.OriginalSelection) {
		return false
	}
	switch s.RecordState.Terminal {
	case "none", "legacy", "committed":
	default:
		return false
	}
	switch s.RequesterDeliveryEligibility.Reason {
	case "eligible", "canceled", "expired", "selector_mismatch", "pin_changed", "budget", "source_or_pin_changed", "ineligible":
	default:
		return false
	}
	if s.RequesterDeliveryEligibility.Eligible != (s.RequesterDeliveryEligibility.Reason == "eligible") {
		return false
	}
	if s.RecordState.Canceled {
		return s.RecordState.Terminal != "none" && s.RequesterDeliveryEligibility.Reason == "canceled"
	}
	if s.RecordState.Terminal != "none" {
		return false
	}
	if s.RecordState.Expired {
		return s.RequesterDeliveryEligibility.Reason == "expired"
	}
	return s.RequesterDeliveryEligibility.Reason != "canceled" && s.RequesterDeliveryEligibility.Reason != "expired"
}

func (c *Client) Select(ctx context.Context, request catalog.DeliverySelectRequest) (catalog.DeliverySelection, error) {
	encoded, _, err := catalog.CanonicalDeliverySelectRequest(request)
	if err != nil || request.LibraryID != c.library || len(encoded) > 1024 {
		return catalog.DeliverySelection{}, ErrInvalidClient
	}
	var selection catalog.DeliverySelection
	if err := c.metadata(ctx, "select", encoded, false, &selection); err != nil {
		return catalog.DeliverySelection{}, err
	}
	if !validSelection(selection) || selection.LibraryID != c.library || selection.OperationID != request.ID || selection.AssetID != request.AssetID || selection.Content.Size > request.MaxBytes || selection.ExpiresUnix != request.ExpiresUnix || request.Mode == "exact" && selection.RevisionID != request.RevisionID {
		return catalog.DeliverySelection{}, ErrInvalidResponse
	}
	return selection, nil
}

func (c *Client) Inspect(ctx context.Context, request catalog.DeliveryInspectRequest) (catalog.DeliveryInspection, error) {
	encoded, err := catalog.CanonicalDeliveryInspectRequest(request)
	if err != nil || request.LibraryID != c.library {
		return catalog.DeliveryInspection{}, ErrInvalidClient
	}
	var result catalog.DeliveryInspection
	if err := c.metadata(ctx, "inspect", encoded, false, &result); err != nil {
		return catalog.DeliveryInspection{}, err
	}
	if !validInspection(result) || result.OriginalSelection.LibraryID != c.library || result.OriginalSelection.SelectionID != request.SelectionID {
		return catalog.DeliveryInspection{}, ErrInvalidResponse
	}
	return result, nil
}

func (c *Client) Lookup(ctx context.Context, request catalog.DeliveryLookupRequest) (catalog.DeliveryInspection, error) {
	encoded, err := catalog.CanonicalDeliveryLookupRequest(request)
	if err != nil || request.LibraryID != c.library {
		return catalog.DeliveryInspection{}, ErrInvalidClient
	}
	var result catalog.DeliveryInspection
	if err := c.metadata(ctx, "lookup", encoded, false, &result); err != nil {
		return catalog.DeliveryInspection{}, err
	}
	if !validInspection(result) || result.OriginalSelection.LibraryID != c.library || result.OriginalSelection.OperationID != request.SelectionOperationID {
		return catalog.DeliveryInspection{}, ErrInvalidResponse
	}
	return result, nil
}

func (c *Client) Cancel(ctx context.Context, request catalog.DeliveryCancelRequest) (catalog.DeliveryCancelResult, error) {
	encoded, digest, err := catalog.CanonicalDeliveryCancelRequest(request)
	if err != nil || request.LibraryID != c.library {
		return catalog.DeliveryCancelResult{}, ErrInvalidClient
	}
	var result catalog.DeliveryCancelResult
	if err := c.metadata(ctx, "cancel", encoded, false, &result); err != nil {
		return catalog.DeliveryCancelResult{}, err
	}
	receipt := result.OriginalReceipt
	status := result.CurrentStatus
	if _, err := catalog.CanonicalTerminalReceipt(receipt); err != nil || receipt.LibraryID != c.library || receipt.OperationID != request.ID || receipt.Kind != "selection_cancel" || receipt.SelectionID != request.SelectionID || receipt.PayloadSHA256 != digest || !validInspection(status) || status.OriginalSelection.LibraryID != c.library || status.OriginalSelection.SelectionID != request.SelectionID || !status.RecordState.Canceled || status.RecordState.Terminal != "committed" || status.RequesterDeliveryEligibility.Reason != "canceled" {
		return catalog.DeliveryCancelResult{}, ErrInvalidResponse
	}
	return result, nil
}

type reconcileRequest struct {
	Version     int                  `json:"version"`
	LibraryID   continuity.LibraryID `json:"library_id"`
	SelectionID string               `json:"selection_id"`
	OperationID string               `json:"operation_id"`
}
type reconcileResult struct {
	Version int                            `json:"version"`
	Attempt catalog.MaterializationAttempt `json:"attempt"`
}

func (c *Client) Reconcile(ctx context.Context, selectionID, operationID string) (catalog.MaterializationAttempt, error) {
	if !mutationauthority.ImageIDValid(selectionID, "dsel_") || !validOperationID(operationID) {
		return catalog.MaterializationAttempt{}, ErrInvalidClient
	}
	encoded, _ := json.Marshal(reconcileRequest{1, c.library, selectionID, operationID})
	var result reconcileResult
	if err := c.metadata(ctx, "reconcile", encoded, true, &result); err != nil {
		return catalog.MaterializationAttempt{}, err
	}
	a := result.Attempt
	if result.Version != 1 || !mutationauthority.ImageIDValid(a.ID, "dmat_") || a.SelectionID != selectionID || a.OperationID != operationID || a.State != "COMPLETED" || a.Content.Validate() != nil || a.Content.Size > mutationauthority.MaxImageBytes {
		return catalog.MaterializationAttempt{}, ErrInvalidResponse
	}
	return a, nil
}

// TransferProgress reports bytes accepted by dst. End is populated only after
// a valid final marker, exact EOF, and successful original Body.Close. On any
// error the receiver must treat its appended bytes as an unverified partial;
// it must rehash that actual prefix before a same-pin retry.
type TransferProgress struct {
	Header       delivery.TransferHeader
	End          delivery.TransferEnd
	BytesWritten int64
}

func transferField(values []string, maxDecoded int) ([]byte, error) {
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > base64.RawURLEncoding.EncodedLen(maxDecoded) {
		return nil, ErrInvalidResponse
	}
	b, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil || len(b) == 0 || len(b) > maxDecoded || base64.RawURLEncoding.EncodeToString(b) != values[0] {
		return nil, ErrInvalidResponse
	}
	return b, nil
}

func (c *Client) Transfer(ctx context.Context, request delivery.TransferRequest, expected catalog.DeliverySelection, dst io.Writer) (TransferProgress, error) {
	var progress TransferProgress
	encoded, _, err := delivery.CanonicalTransferRequest(request)
	if ctx == nil || dst == nil || err != nil || request.LibraryID != c.library || !validSelection(expected) || expected.LibraryID != c.library || expected.SelectionID != request.SelectionID || expected.OperationID != request.OperationID || request.Offset > expected.Content.Size {
		return progress, ErrInvalidClient
	}
	deadline := time.Now().Add(clientTotalLimit)
	if expiry := time.Unix(expected.ExpiresUnix, 0); expiry.Before(deadline) {
		deadline = expiry
	}
	if !deadline.After(time.Now()) {
		return progress, ErrInvalidClient
	}
	authorization, err := c.enter()
	if err != nil {
		return progress, err
	}
	defer c.leave()
	requestCtx, cancel := c.requestContext(ctx, deadline)
	defer cancel()
	resp, err := c.post(requestCtx, "transfer", encoded, authorization)
	if err != nil {
		return progress, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode != http.StatusOK {
		if !responseBase(resp, "application/json") {
			return progress, ErrInvalidResponse
		}
		body, err := boundedBody(requestCtx, cancel, resp.Body)
		if err != nil {
			return progress, err
		}
		closed = true
		if err := resp.Body.Close(); err != nil {
			return progress, ErrInvalidResponse
		}
		return progress, decodeRemoteError(resp.StatusCode, body)
	}
	if !responseBase(resp, "application/octet-stream") || resp.ContentLength != -1 || len(resp.TransferEncoding) > 1 || len(resp.TransferEncoding) == 1 && resp.TransferEncoding[0] != "chunked" {
		return progress, ErrInvalidResponse
	}
	if _, declared := resp.Trailer[http.CanonicalHeaderKey("X-JM-Transfer-End")]; !declared {
		return progress, ErrInvalidResponse
	}
	headerBytes, err := transferField(resp.Header.Values("X-JM-Delivery"), delivery.MaxTransferHeaderBytes)
	if err != nil {
		return progress, err
	}
	header, err := delivery.DecodeTransferHeader(headerBytes)
	if err != nil || delivery.ValidateTransferResponse(request, expected, header, nil) != nil {
		return progress, ErrInvalidResponse
	}
	progress.Header = header
	buffer := make([]byte, delivery.MaxTransferChunkBytes)
	sawEOF := false
	for progress.BytesWritten < header.Length {
		maximum := int64(len(buffer))
		if remaining := header.Length - progress.BytesWritten; remaining < maximum {
			maximum = remaining
		}
		n, readErr := readStalled(requestCtx, cancel, resp.Body, buffer[:int(maximum)])
		if n > 0 {
			written, writeErr := dst.Write(buffer[:n])
			if written < 0 || written > n {
				return progress, ErrInvalidResponse
			}
			progress.BytesWritten += int64(written)
			if writeErr != nil {
				return progress, writeErr
			}
			if written != n {
				return progress, io.ErrShortWrite
			}
			if err := requestCtx.Err(); err != nil {
				return progress, err
			}
		}
		if readErr == io.EOF && progress.BytesWritten == header.Length {
			sawEOF = true
			break
		}
		if readErr != nil || n == 0 {
			return progress, ErrInvalidResponse
		}
	}
	var one [1]byte
	for retries := 0; !sawEOF && retries < 3; retries++ {
		n, readErr := readStalled(requestCtx, cancel, resp.Body, one[:])
		if n != 0 || readErr != nil && readErr != io.EOF {
			return progress, ErrInvalidResponse
		}
		if readErr == io.EOF {
			break
		}
		if retries == 2 {
			return progress, ErrInvalidResponse
		}
	}
	endBytes, err := transferField(resp.Trailer.Values("X-JM-Transfer-End"), delivery.MaxTransferEndBytes)
	if err != nil {
		return progress, err
	}
	end, err := delivery.DecodeTransferEnd(endBytes)
	if err != nil || delivery.ValidateTransferResponse(request, expected, header, &end) != nil {
		return progress, ErrInvalidResponse
	}
	closed = true
	if err := resp.Body.Close(); err != nil {
		return progress, ErrInvalidResponse
	}
	if err := requestCtx.Err(); err != nil {
		return progress, err
	}
	progress.End = end
	return progress, nil
}

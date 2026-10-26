package manager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

const deliveryMetadataLimit = 1024

type deliveryReconcileRequest struct {
	Version     int                  `json:"version"`
	LibraryID   continuity.LibraryID `json:"library_id"`
	SelectionID string               `json:"selection_id"`
	OperationID string               `json:"operation_id"`
}

type deliveryReconcileResult struct {
	Version int                            `json:"version"`
	Attempt catalog.MaterializationAttempt `json:"attempt"`
}

func deliveryOperationID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, b := range []byte(s) {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-') {
			return false
		}
	}
	return true
}
func deliverySelectionID(s string) bool {
	if len(s) != 37 || !strings.HasPrefix(s, "dsel_") {
		return false
	}
	for _, b := range []byte(s[5:]) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return s[5:] != strings.Repeat("0", 32)
}
func decodeDeliveryReconcile(body []byte) (deliveryReconcileRequest, error) {
	var op deliveryReconcileRequest
	if len(body) == 0 || len(body) > deliveryMetadataLimit || mutationauthority.DecodeImageCanonical(body, &op) != nil || op.Version != 1 || op.LibraryID.Validate() != nil || !deliverySelectionID(op.SelectionID) || !deliveryOperationID(op.OperationID) {
		return op, delivery.ErrInvalidTransfer
	}
	canonical, err := json.Marshal(op)
	if err != nil || !bytes.Equal(canonical, body) {
		return op, delivery.ErrInvalidTransfer
	}
	return op, nil
}

func deliveryErrorCode(err error) string {
	switch {
	case errors.Is(err, delivery.ErrInvalidTransfer), errors.Is(err, catalog.ErrInvalid):
		return "invalid"
	case errors.Is(err, catalog.ErrUnauthenticated):
		return "unauthenticated"
	case errors.Is(err, catalog.ErrNotFound):
		return "unavailable"
	case errors.Is(err, catalog.ErrReconciliationRequired):
		return "reconciliation_required"
	case errors.Is(err, catalog.ErrConflict), errors.Is(err, catalog.ErrChangedRequest):
		return "conflict"
	case errors.Is(err, catalog.ErrLimit):
		return "admission_limit"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "deadline"
	default:
		return "storage"
	}
}
func writeDeliveryError(w http.ResponseWriter, code string) {
	status := http.StatusServiceUnavailable
	switch code {
	case "invalid":
		status = http.StatusBadRequest
	case "unauthenticated":
		status = http.StatusUnauthorized
	case "denied", "unavailable":
		status = http.StatusNotFound
	case "conflict", "reconciliation_required":
		status = http.StatusConflict
	case "admission_limit":
		status = http.StatusTooManyRequests
	case "deadline":
		status = http.StatusGatewayTimeout
	}
	continuityHeaders(w)
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"version":1,"error":"`+code+`"}`)
}

func deliveryResponseDeadline(controller *http.ResponseController, start time.Time) error {
	deadline := time.Now().Add(30 * time.Second)
	if total := start.Add(900 * time.Second); total.Before(deadline) {
		deadline = total
	}
	return controller.SetWriteDeadline(deadline)
}

func (c *Continuity) deliveryTransportAllowed(r *http.Request) bool {
	local, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	if !ok || local == nil {
		return false
	}
	c.mu.Lock()
	bound := c.deliveryHosts != nil && c.deliveryHosts[local.String()]
	c.mu.Unlock()
	if !bound || r.Host != local.String() || r.TLS != nil {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	peer, err := netip.ParseAddr(host)
	if err != nil || peer.Is4In6() || !peer.IsLoopback() || peer.Zone() != "" {
		return false
	}
	if len(r.Header.Values("Origin")) != 0 || len(r.Header.Values("Sec-Fetch-Site")) != 0 {
		return false
	}
	for name := range r.Header {
		lower := strings.ToLower(name)
		if lower == "forwarded" || strings.HasPrefix(lower, "x-forwarded-") || lower == "x-real-ip" || lower == "x-original-host" {
			return false
		}
	}
	return true
}

func (c *Continuity) serveDeliveryHTTP(w http.ResponseWriter, r *http.Request, base string) {
	start := time.Now()
	controller := http.NewResponseController(w)
	// These deadlines remain installed while net/http serializes trailers and
	// cleans up after the handler returns.
	if controller.SetReadDeadline(start.Add(2*time.Second)) != nil || controller.SetWriteDeadline(start.Add(4*time.Second)) != nil {
		panic(http.ErrAbortHandler)
	}
	if c.delivery == nil {
		writeDeliveryError(w, "unavailable")
		return
	}
	route := strings.TrimPrefix(r.URL.Path, base)
	switch route {
	case "select", "inspect", "lookup", "cancel", "reconcile", "transfer":
	default:
		writeDeliveryError(w, "unavailable")
		return
	}
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || !c.deliveryTransportAllowed(r) {
		writeDeliveryError(w, "invalid")
		return
	}
	bearer, ok := continuityBearer(r.Header.Values("Authorization"))
	if !ok {
		writeDeliveryError(w, "unauthenticated")
		return
	}
	defer clear(bearer)
	r.Header.Del("Authorization")
	limit := int64(deliveryMetadataLimit)
	heavy := route == "transfer" || route == "reconcile"
	if route == "transfer" {
		limit = delivery.MaxTransferRequestBytes
	}
	if !continuityContentType(r.Header) || r.ContentLength > limit {
		writeDeliveryError(w, "invalid")
		return
	}
	if !c.admitDelivery(heavy) {
		c.mu.Lock()
		bound := c.deliveryHosts != nil
		c.mu.Unlock()
		if bound {
			writeDeliveryError(w, "admission_limit")
		} else {
			writeDeliveryError(w, "unavailable")
		}
		return
	}
	defer c.leaveDelivery(heavy)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		if ctx.Err() != nil || time.Now().After(start.Add(2*time.Second)) {
			writeDeliveryError(w, "deadline")
		} else {
			writeDeliveryError(w, "invalid")
		}
		return
	}
	if err := ctx.Err(); err != nil {
		writeDeliveryError(w, "deadline")
		return
	}
	if route == "transfer" {
		request, err := delivery.DecodeTransferRequest(body)
		if err != nil {
			writeDeliveryError(w, "invalid")
			return
		}
		if err := controller.SetWriteDeadline(start.Add(900 * time.Second)); err != nil {
			panic(http.ErrAbortHandler)
		}
		c.streamDelivery(w, controller, ctx, bearer, request, start)
		return
	}
	var response any
	if !heavy {
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithDeadline(ctx, start.Add(2*time.Second))
		defer deadlineCancel()
	} else {
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithDeadline(ctx, start.Add(900*time.Second))
		defer deadlineCancel()
	}
	switch route {
	case "select":
		var op catalog.DeliverySelectRequest
		op, err = catalog.DecodeDeliverySelectRequest(body)
		if err == nil {
			response, err = c.delivery.Select(ctx, bearer, op)
		}
	case "inspect":
		var op catalog.DeliveryInspectRequest
		op, err = catalog.DecodeDeliveryInspectRequest(body)
		if err == nil {
			response, err = c.store.InspectDelivery(ctx, bearer, op)
		}
	case "lookup":
		var op catalog.DeliveryLookupRequest
		op, err = catalog.DecodeDeliveryLookupRequest(body)
		if err == nil {
			response, err = c.store.LookupDelivery(ctx, bearer, op)
		}
	case "cancel":
		var op catalog.DeliveryCancelRequest
		op, err = catalog.DecodeDeliveryCancelRequest(body)
		if err == nil {
			response, err = c.store.CancelDelivery(ctx, bearer, op)
		}
	case "reconcile":
		var op deliveryReconcileRequest
		op, err = decodeDeliveryReconcile(body)
		if err == nil && op.LibraryID != c.library {
			err = catalog.ErrNotFound
		}
		if err == nil {
			if deadlineErr := controller.SetWriteDeadline(start.Add(900 * time.Second)); deadlineErr != nil {
				panic(http.ErrAbortHandler)
			}
			var attempt catalog.MaterializationAttempt
			_, attempt, err = c.delivery.Reconcile(ctx, bearer, op.SelectionID, op.OperationID)
			if err == nil {
				response = deliveryReconcileResult{1, attempt}
			}
		}
	}
	if err != nil {
		if heavy {
			_ = deliveryResponseDeadline(controller, start)
		}
		writeDeliveryError(w, deliveryErrorCode(err))
		return
	}
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > 2048 {
		if heavy {
			_ = deliveryResponseDeadline(controller, start)
		}
		writeDeliveryError(w, "storage")
		return
	}
	if heavy {
		if err := deliveryResponseDeadline(controller, start); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	continuityHeaders(w)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(encoded); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (c *Continuity) streamDelivery(w http.ResponseWriter, controller *http.ResponseController, ctx context.Context, bearer []byte, request delivery.TransferRequest, start time.Time) {
	if request.LibraryID != c.deliveryLibrary() {
		_ = deliveryResponseDeadline(controller, start)
		writeDeliveryError(w, "unavailable")
		return
	}
	overall, cancel := context.WithDeadline(ctx, start.Add(900*time.Second))
	defer cancel()
	stream, err := c.delivery.OpenTransfer(overall, bearer, request)
	if err != nil {
		_ = deliveryResponseDeadline(controller, start)
		writeDeliveryError(w, deliveryErrorCode(err))
		return
	}
	defer stream.Close()
	header := stream.Header()
	encodedHeader, err := delivery.CanonicalTransferHeader(header)
	if err != nil {
		writeDeliveryError(w, "unavailable")
		return
	}
	final := start.Add(900 * time.Second)
	if expiry := time.Unix(header.Selection.ExpiresUnix, 0); expiry.Before(final) {
		final = expiry
	}
	writeDeadline := func() error {
		deadline := time.Now().Add(30 * time.Second)
		if final.Before(deadline) {
			deadline = final
		}
		return controller.SetWriteDeadline(deadline)
	}
	if err := writeDeadline(); err != nil {
		panic(http.ErrAbortHandler)
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-JM-Delivery", base64.RawURLEncoding.EncodeToString(encodedHeader))
	h.Set("Trailer", "X-JM-Transfer-End")
	h.Del("Content-Length")
	h.Del("Content-Encoding")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		panic(http.ErrAbortHandler)
	}
	var written int64
	for written < header.Length {
		maximum := delivery.MaxTransferChunkBytes
		if remaining := header.Length - written; remaining < int64(maximum) {
			maximum = int(remaining)
		}
		chunk, err := stream.ReadChunk(overall, maximum)
		if err != nil || len(chunk) == 0 || len(chunk) > maximum {
			panic(http.ErrAbortHandler)
		}
		if err := writeDeadline(); err != nil {
			panic(http.ErrAbortHandler)
		}
		n, err := w.Write(chunk)
		written += int64(n)
		if err != nil || n != len(chunk) {
			panic(http.ErrAbortHandler)
		}
		if err := controller.Flush(); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	end, err := stream.Complete(overall)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	encodedEnd, err := delivery.CanonicalTransferEnd(end)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if err := writeDeadline(); err != nil {
		panic(http.ErrAbortHandler)
	}
	h.Set("X-JM-Transfer-End", base64.RawURLEncoding.EncodeToString(encodedEnd))
}

func (c *Continuity) deliveryLibrary() continuity.LibraryID { return c.library }

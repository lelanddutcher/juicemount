package nativeclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func clientTestBearer() []byte {
	return []byte("jmc1_" + strings.Repeat("a", 32) + "." + strings.Repeat("b", 64))
}

func clientTestDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

type clientTestPin struct {
	selectRequest catalog.DeliverySelectRequest
	selection     catalog.DeliverySelection
	attempt       catalog.MaterializationAttempt
	request       delivery.TransferRequest
	header        delivery.TransferHeader
	end           delivery.TransferEnd
	payload       []byte
}

func newClientTestPin(payload []byte, offset int64) clientTestPin {
	v := maxVectors()
	selection := v.selection
	selection.Content = continuity.Content{SHA256: clientTestDigest(payload), Size: int64(len(payload))}
	selection.ExpiresUnix = time.Now().Add(5 * time.Minute).Unix()
	selectRequest := v.selectRequest
	selectRequest.Mode = "exact"
	selectRequest.MaxBytes = int64(len(payload))
	selectRequest.ExpiresUnix = selection.ExpiresUnix
	attempt := v.attempt
	attempt.Content = selection.Content
	request := v.transferRequest
	request.Offset = offset
	request.PrefixSHA256 = clientTestDigest(payload[:offset])
	_, requestHash, _ := delivery.CanonicalTransferRequest(request)
	header := delivery.TransferHeader{Version: 1, RequestSHA256: requestHash, AuthorityIncarnation: "auth_" + strings.Repeat("a", 32), Selection: selection, Attempt: attempt, Offset: offset, Length: int64(len(payload)) - offset, PrefixSHA256: request.PrefixSHA256}
	end := delivery.TransferEnd{Version: 1, RequestSHA256: requestHash, AttemptID: attempt.ID, Offset: offset, Length: header.Length, Result: "server_stream_complete"}
	return clientTestPin{selectRequest, selection, attempt, request, header, end, payload}
}

func clientTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func clientTestTransferHandler(t *testing.T, pin clientTestPin, variation string, prefix string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != prefix+"/api/continuity/v1/delivery/transfer" || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+string(clientTestBearer()) || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Cookie")) != 0 {
			t.Errorf("unexpected transfer request: path=%s method=%s", r.URL.Path, r.Method)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1025))
		if err != nil {
			t.Errorf("request body: %v", err)
			return
		}
		got, err := delivery.DecodeTransferRequest(body)
		if err != nil || got != pin.request {
			t.Errorf("request pin mismatch: %+v %v", got, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if variation == "early" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"version":1,"error":"admission_limit"}`))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		header := pin.header
		if variation == "wrongpin" {
			header.RequestSHA256 = strings.Repeat("f", 64)
		}
		headerJSON, err := delivery.CanonicalTransferHeader(header)
		if err != nil {
			t.Errorf("header construction: %v", err)
			return
		}
		headerValue := base64.RawURLEncoding.EncodeToString(headerJSON)
		if variation == "malformedheader" {
			headerValue += "="
		}
		w.Header().Set("X-JM-Delivery", headerValue)
		w.Header().Set("Trailer", "X-JM-Transfer-End")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		remaining := pin.payload[pin.request.Offset:]
		switch variation {
		case "truncated":
			if len(remaining) > 0 {
				remaining = remaining[:len(remaining)-1]
			}
		case "extra":
			remaining = append(append([]byte(nil), remaining...), '!')
		}
		_, _ = w.Write(remaining)
		if variation != "missingend" {
			endJSON, err := delivery.CanonicalTransferEnd(pin.end)
			if err != nil {
				t.Errorf("end construction: %v", err)
				return
			}
			w.Header().Set("X-JM-Transfer-End", base64.RawURLEncoding.EncodeToString(endJSON))
		}
	}
}

func TestNativeClientTransferEndEOFAndOriginalBodyClose(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payload     string
		offset      int64
		variation   string
		wantWritten int64
		wantRemote  bool
	}{
		{"full", "native-delivery-media", 0, "", 21, false},
		{"empty", "", 0, "", 0, false},
		{"resume", "native-delivery-media", 6, "", 15, false},
		{"truncated", "native-delivery-media", 0, "truncated", 20, false},
		{"extra", "native-delivery-media", 0, "extra", 21, false},
		{"missing_end", "native-delivery-media", 0, "missingend", 21, false},
		{"wrong_pin", "native-delivery-media", 0, "wrongpin", 0, false},
		{"malformed_header", "native-delivery-media", 0, "malformedheader", 0, false},
		{"early_quota", "native-delivery-media", 0, "early", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pin := newClientTestPin([]byte(tc.payload), tc.offset)
			prefix := "/manager"
			if tc.name == "full" {
				prefix = ""
			}
			server := httptest.NewServer(clientTestTransferHandler(t, pin, tc.variation, prefix))
			defer server.Close()
			client, err := NewClient(server.URL+prefix, pin.selection.LibraryID, clientTestBearer(), true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(context.Background()); err != nil {
					t.Errorf("client original owner join: %v", err)
				}
			}()
			var dst bytes.Buffer
			progress, err := client.Transfer(context.Background(), pin.request, pin.selection, &dst)
			if progress.BytesWritten != tc.wantWritten || int64(dst.Len()) != tc.wantWritten {
				t.Fatalf("stream length: progress=%d dst=%d want=%d err=%v", progress.BytesWritten, dst.Len(), tc.wantWritten, err)
			}
			if tc.variation == "" {
				if err != nil || progress.End != pin.end || !bytes.Equal(dst.Bytes(), pin.payload[tc.offset:]) {
					t.Fatalf("complete transfer: progress=%+v bytes=%q err=%v", progress, dst.Bytes(), err)
				}
				return
			}
			if tc.wantRemote {
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.StatusCode != 429 || remote.Code != "admission_limit" {
					t.Fatalf("typed early refusal: %v", err)
				}
			} else if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("invalid transfer accepted: %v", err)
			}
			if progress.End != (delivery.TransferEnd{}) {
				t.Fatalf("failed transfer asserted End: %+v", progress.End)
			}
		})
	}
}

func TestNativeClientCanonicalMetadataAndPin(t *testing.T) {
	pin := newClientTestPin([]byte("metadata"), 0)
	inspection := catalog.DeliveryInspection{
		OriginalSelection:            pin.selection,
		RecordState:                  catalog.DeliveryRecordState{Terminal: "none"},
		RequesterDeliveryEligibility: catalog.DeliveryEligibility{Eligible: true, Reason: "eligible"},
	}
	cancelRequest := catalog.DeliveryCancelRequest{Version: 1, LibraryID: pin.selection.LibraryID, ID: "cancel-one", SelectionID: pin.selection.SelectionID}
	_, cancelDigest, err := catalog.CanonicalDeliveryCancelRequest(cancelRequest)
	if err != nil {
		t.Fatal(err)
	}
	canceledInspection := inspection
	canceledInspection.RecordState = catalog.DeliveryRecordState{Canceled: true, Terminal: "committed"}
	canceledInspection.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "canceled"}
	cancelResult := catalog.DeliveryCancelResult{OriginalReceipt: catalog.TerminalReceipt{Version: 1, LibraryID: pin.selection.LibraryID, OperationID: cancelRequest.ID, Kind: "selection_cancel", PayloadSHA256: cancelDigest, ActorSubjectID: catalog.SubjectID("sub_" + strings.Repeat("a", 32)), AuthorityIncarnation: "auth_" + strings.Repeat("a", 32), RecordedUnixNano: time.Now().UnixNano(), SelectionID: cancelRequest.SelectionID}, CurrentStatus: canceledInspection}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+string(clientTestBearer()) || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Cookie")) != 0 {
			t.Errorf("metadata request headers/method: %s", r.Method)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		route := strings.TrimPrefix(r.URL.Path, "/api/continuity/v1/delivery/")
		var expected any
		switch route {
		case "select":
			expected = pin.selectRequest
		case "inspect":
			expected = catalog.DeliveryInspectRequest{Version: 1, LibraryID: pin.selection.LibraryID, SelectionID: pin.selection.SelectionID}
		case "lookup":
			expected = catalog.DeliveryLookupRequest{Version: 1, LibraryID: pin.selection.LibraryID, SelectionOperationID: pin.selection.OperationID}
		case "cancel":
			expected = cancelRequest
		case "reconcile":
			expected = reconcileRequest{1, pin.selection.LibraryID, pin.selection.SelectionID, pin.selection.OperationID}
		default:
			t.Errorf("unknown route: %s", r.URL.Path)
			return
		}
		requestBody, err := io.ReadAll(io.LimitReader(r.Body, 1025))
		if err != nil || !bytes.Equal(requestBody, clientTestJSON(t, expected)) {
			t.Errorf("noncanonical %s request: %q %v", route, requestBody, err)
			return
		}
		switch route {
		case "select":
			_, _ = w.Write(clientTestJSON(t, pin.selection))
		case "inspect", "lookup":
			_, _ = w.Write(clientTestJSON(t, inspection))
		case "cancel":
			_, _ = w.Write(clientTestJSON(t, cancelResult))
		case "reconcile":
			_, _ = w.Write(clientTestJSON(t, reconcileResult{1, pin.attempt}))
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, pin.selection.LibraryID, clientTestBearer(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client join: %v", err)
		}
	}()
	if selected, err := client.Select(context.Background(), pin.selectRequest); err != nil || selected != pin.selection {
		t.Fatalf("select: %+v %v", selected, err)
	}
	if got, err := client.Inspect(context.Background(), catalog.DeliveryInspectRequest{Version: 1, LibraryID: pin.selection.LibraryID, SelectionID: pin.selection.SelectionID}); err != nil || got != inspection {
		t.Fatalf("inspect: %+v %v", got, err)
	}
	if got, err := client.Lookup(context.Background(), catalog.DeliveryLookupRequest{Version: 1, LibraryID: pin.selection.LibraryID, SelectionOperationID: pin.selection.OperationID}); err != nil || got != inspection {
		t.Fatalf("lookup: %+v %v", got, err)
	}
	if got, err := client.Cancel(context.Background(), cancelRequest); err != nil || got != cancelResult {
		t.Fatalf("cancel: %+v %v", got, err)
	}
	if got, err := client.Reconcile(context.Background(), pin.selection.SelectionID, pin.selection.OperationID); err != nil || got != pin.attempt {
		t.Fatalf("reconcile: %+v %v", got, err)
	}
}

func TestNativeClientRejectsNoncanonicalAndForeignMetadata(t *testing.T) {
	pin := newClientTestPin([]byte("metadata"), 0)
	wrong := pin.selection
	wrong.OperationID = "foreign-operation"
	canonicalSelection := clientTestJSON(t, pin.selection)
	withForeignField := append(append([]byte(nil), canonicalSelection[:len(canonicalSelection)-1]...), []byte(`,"foreign":true}`)...)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"duplicate_version", []byte(`{"version":1,"version":1}`)},
		{"unknown_field", withForeignField},
		{"wrong_original_operation", clientTestJSON(t, wrong)},
		{"oversized_metadata", bytes.Repeat([]byte("x"), clientMetadataLimit+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, pin.selection.LibraryID, clientTestBearer(), true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(context.Background()); err != nil {
					t.Errorf("client join: %v", err)
				}
			}()
			if _, err := client.Select(context.Background(), pin.selectRequest); !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("invalid selection response accepted: %v", err)
			}
		})
	}
}

func clientTestFixedResponse(t *testing.T, library continuity.LibraryID, route string, value any) *Client {
	t.Helper()
	body := clientTestJSON(t, value)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/continuity/v1/delivery/"+route {
			t.Errorf("unexpected metadata route: %s %s", r.Method, r.URL.Path)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, library, clientTestBearer(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client join: %v", err)
		}
	})
	return client
}

func TestNativeClientSelectExactExpiryBinding(t *testing.T) {
	pin := newClientTestPin([]byte("selection"), 0)
	shorter := pin.selection
	shorter.ExpiresUnix--
	client := clientTestFixedResponse(t, pin.selection.LibraryID, "select", shorter)
	if _, err := client.Select(context.Background(), pin.selectRequest); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("shortened selected expiry accepted: %v", err)
	}
}

func TestNativeClientInspectionCurrentStateInvariants(t *testing.T) {
	pin := newClientTestPin([]byte("status"), 0)
	base := catalog.DeliveryInspection{OriginalSelection: pin.selection, RecordState: catalog.DeliveryRecordState{Terminal: "none"}, RequesterDeliveryEligibility: catalog.DeliveryEligibility{Eligible: true, Reason: "eligible"}}
	for _, tc := range []struct {
		name   string
		mutate func(*catalog.DeliveryInspection)
		valid  bool
	}{
		{"legacy_canceled", func(s *catalog.DeliveryInspection) {
			s.RecordState.Canceled = true
			s.RecordState.Terminal = "legacy"
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "canceled"}
		}, true},
		{"expired_only", func(s *catalog.DeliveryInspection) {
			s.RecordState.Expired = true
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "expired"}
		}, true},
		{"canceled_and_expired_committed", func(s *catalog.DeliveryInspection) {
			s.RecordState.Canceled = true
			s.RecordState.Expired = true
			s.RecordState.Terminal = "committed"
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "canceled"}
		}, true},
		{"canceled_but_eligible", func(s *catalog.DeliveryInspection) {
			s.RecordState.Canceled = true
			s.RecordState.Terminal = "committed"
		}, false},
		{"committed_but_not_canceled", func(s *catalog.DeliveryInspection) { s.RecordState.Terminal = "committed" }, false},
		{"canceled_without_terminal", func(s *catalog.DeliveryInspection) {
			s.RecordState.Canceled = true
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "canceled"}
		}, false},
		{"expired_but_eligible", func(s *catalog.DeliveryInspection) { s.RecordState.Expired = true }, false},
		{"canceled_expired_wrong_precedence", func(s *catalog.DeliveryInspection) {
			s.RecordState.Canceled = true
			s.RecordState.Expired = true
			s.RecordState.Terminal = "committed"
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "expired"}
		}, false},
		{"current_claims_canceled_without_bit", func(s *catalog.DeliveryInspection) {
			s.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "canceled"}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := base
			tc.mutate(&status)
			if status.OriginalSelection.Canceled {
				t.Fatal("fixture lost historical OriginalSelection.Canceled=false")
			}
			client := clientTestFixedResponse(t, pin.selection.LibraryID, "inspect", status)
			got, err := client.Inspect(context.Background(), catalog.DeliveryInspectRequest{Version: 1, LibraryID: pin.selection.LibraryID, SelectionID: pin.selection.SelectionID})
			if tc.valid && (err != nil || got != status) {
				t.Fatalf("valid historical inspection refused: %+v %v", got, err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("contradictory inspection accepted: %+v %v", got, err)
			}
		})
	}
}

func TestNativeClientCancelBindsReceiptAndCurrentStatus(t *testing.T) {
	pin := newClientTestPin([]byte("cancel"), 0)
	request := catalog.DeliveryCancelRequest{Version: 1, LibraryID: pin.selection.LibraryID, ID: "cancel-replay", SelectionID: pin.selection.SelectionID}
	_, digest, err := catalog.CanonicalDeliveryCancelRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	base := catalog.DeliveryCancelResult{
		OriginalReceipt: catalog.TerminalReceipt{Version: 1, LibraryID: pin.selection.LibraryID, OperationID: request.ID, Kind: "selection_cancel", PayloadSHA256: digest, ActorSubjectID: catalog.SubjectID("sub_" + strings.Repeat("a", 32)), AuthorityIncarnation: "auth_" + strings.Repeat("a", 32), RecordedUnixNano: time.Now().UnixNano(), SelectionID: request.SelectionID},
		CurrentStatus:   catalog.DeliveryInspection{OriginalSelection: pin.selection, RecordState: catalog.DeliveryRecordState{Canceled: true, Terminal: "committed"}, RequesterDeliveryEligibility: catalog.DeliveryEligibility{Reason: "canceled"}},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*catalog.DeliveryCancelResult)
		valid  bool
	}{
		{"receipt_replay_current_canceled", func(*catalog.DeliveryCancelResult) {}, true},
		{"receipt_replay_current_canceled_and_expired", func(r *catalog.DeliveryCancelResult) { r.CurrentStatus.RecordState.Expired = true }, true},
		{"foreign_current_library", func(r *catalog.DeliveryCancelResult) {
			r.CurrentStatus.OriginalSelection.LibraryID = continuity.LibraryID("lib_" + strings.Repeat("b", 32))
		}, false},
		{"uncanceled_current_status", func(r *catalog.DeliveryCancelResult) {
			r.CurrentStatus.RecordState = catalog.DeliveryRecordState{Terminal: "none"}
			r.CurrentStatus.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Eligible: true, Reason: "eligible"}
		}, false},
		{"canceled_without_committed_terminal", func(r *catalog.DeliveryCancelResult) { r.CurrentStatus.RecordState.Terminal = "legacy" }, false},
		{"canceled_with_expired_reason", func(r *catalog.DeliveryCancelResult) {
			r.CurrentStatus.RecordState.Expired = true
			r.CurrentStatus.RequesterDeliveryEligibility = catalog.DeliveryEligibility{Reason: "expired"}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := base
			tc.mutate(&result)
			client := clientTestFixedResponse(t, pin.selection.LibraryID, "cancel", result)
			got, err := client.Cancel(context.Background(), request)
			if tc.valid && (err != nil || got != result) {
				t.Fatalf("valid cancellation replay refused: %+v %v", got, err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("contradictory cancellation accepted: %+v %v", got, err)
			}
		})
	}
}

func TestNativeClientReconcileFiniteContentBound(t *testing.T) {
	pin := newClientTestPin([]byte("attempt"), 0)
	for _, tc := range []struct {
		name  string
		size  int64
		valid bool
	}{
		{"maximum", mutationauthority.MaxImageBytes, true},
		{"maximum_plus_one", mutationauthority.MaxImageBytes + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := pin.attempt
			attempt.Content.Size = tc.size
			client := clientTestFixedResponse(t, pin.selection.LibraryID, "reconcile", reconcileResult{Version: 1, Attempt: attempt})
			got, err := client.Reconcile(context.Background(), pin.selection.SelectionID, pin.selection.OperationID)
			if tc.valid && (err != nil || got != attempt) {
				t.Fatalf("maximum finite attempt refused: %+v %v", got, err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("oversize attempt accepted: %+v %v", got, err)
			}
		})
	}
}

type clientTestRoundTrip func(*http.Request) (*http.Response, error)

func (f clientTestRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type clientTestBody struct {
	reader   io.Reader
	closed   atomic.Int32
	closeErr error
}

func (b *clientTestBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *clientTestBody) Close() error               { b.closed.Add(1); return b.closeErr }

type clientTestFinalReadCancel struct {
	payload  []byte
	cancel   context.CancelFunc
	returned bool
}

func (r *clientTestFinalReadCancel) Read(p []byte) (int, error) {
	if r.returned {
		return 0, io.EOF
	}
	r.returned = true
	n := copy(p, r.payload)
	r.cancel()
	return n, io.EOF
}

type clientTestHeldClose struct {
	reader  io.Reader
	entered chan struct{}
	release chan struct{}
	closed  atomic.Int32
}

func (b *clientTestHeldClose) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *clientTestHeldClose) Close() error {
	b.closed.Add(1)
	close(b.entered)
	<-b.release
	return nil
}

func clientTestSyntheticResponse(pin clientTestPin, body io.ReadCloser, r *http.Request) *http.Response {
	headerJSON, _ := delivery.CanonicalTransferHeader(pin.header)
	endJSON, _ := delivery.CanonicalTransferEnd(pin.end)
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}, "X-Jm-Delivery": {base64.RawURLEncoding.EncodeToString(headerJSON)}}, Body: body, Trailer: http.Header{http.CanonicalHeaderKey("X-JM-Transfer-End"): {base64.RawURLEncoding.EncodeToString(endJSON)}}, ContentLength: -1, Request: r}
}

type clientTestGateWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *clientTestGateWriter) Write(p []byte) (int, error) {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	<-w.release
	return len(p), nil
}

func TestNativeClientCloseWaitsOriginalTransferBody(t *testing.T) {
	pin := newClientTestPin([]byte("blocked-original"), 0)
	body := &clientTestBody{reader: bytes.NewReader(pin.payload)}
	endpoint, err := ParseEndpoint("http://127.0.0.1:25001", true)
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(endpoint, pin.selection.LibraryID, clientTestBearer(), &http.Client{Transport: clientTestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return clientTestSyntheticResponse(pin, body, r), nil
	})}, nil)
	writer := &clientTestGateWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := client.Transfer(context.Background(), pin.request, pin.selection, writer)
		done <- err
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original transfer remained unjoined")
				<-done
			}
		}
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client remained unjoined: %v", err)
		}
	})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("original transfer did not reach destination")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Close(deadline); !errors.Is(err, ErrClientDraining) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close claimed joined original: %v", err)
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("canceled original transfer claimed success")
		}
	case <-time.After(time.Second):
		t.Fatal("original transfer did not join after destination released")
	}
	if err := client.Close(context.Background()); err != nil || body.closed.Load() != 1 {
		t.Fatalf("original body close/join: count=%d err=%v", body.closed.Load(), err)
	}
}

func TestNativeClientFinalReadBytesAndEOFCancellation(t *testing.T) {
	pin := newClientTestPin([]byte("final-read-eof"), 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &clientTestFinalReadCancel{payload: pin.payload, cancel: cancel}
	body := &clientTestBody{reader: reader}
	endpoint, _ := ParseEndpoint("http://127.0.0.1:25003", true)
	client := newClient(endpoint, pin.selection.LibraryID, clientTestBearer(), &http.Client{Transport: clientTestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return clientTestSyntheticResponse(pin, body, r), nil
	})}, nil)
	var dst bytes.Buffer
	progress, err := client.Transfer(ctx, pin.request, pin.selection, &dst)
	if err == nil || progress.End != (delivery.TransferEnd{}) || body.closed.Load() != 1 || progress.BytesWritten != int64(len(pin.payload)) {
		t.Fatalf("canceled final n+EOF certified End: progress=%+v closed=%d err=%v", progress, body.closed.Load(), err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeClientCloseWaitsHeldOriginalBodyClose(t *testing.T) {
	pin := newClientTestPin([]byte("held-close"), 0)
	body := &clientTestHeldClose{reader: bytes.NewReader(pin.payload), entered: make(chan struct{}), release: make(chan struct{})}
	endpoint, _ := ParseEndpoint("http://127.0.0.1:25004", true)
	client := newClient(endpoint, pin.selection.LibraryID, clientTestBearer(), &http.Client{Transport: clientTestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return clientTestSyntheticResponse(pin, body, r), nil
	})}, nil)
	type outcome struct {
		progress TransferProgress
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		var dst bytes.Buffer
		progress, err := client.Transfer(context.Background(), pin.request, pin.selection, &dst)
		done <- outcome{progress, err}
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(body.release) }) }
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("held original Body.Close did not join")
				<-done
			}
		}
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client still owns held Body.Close: %v", err)
		}
	})
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("original Body.Close was not entered")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Close(deadline); !errors.Is(err, ErrClientDraining) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close falsely joined held original Body.Close: %v", err)
	}
	release()
	select {
	case result := <-done:
		joined = true
		if result.err == nil || result.progress.End != (delivery.TransferEnd{}) {
			t.Fatalf("canceled held close certified End: %+v %v", result.progress, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("original transfer did not join after Body.Close release")
	}
	if err := client.Close(context.Background()); err != nil || body.closed.Load() != 1 {
		t.Fatalf("original Body.Close count=%d join=%v", body.closed.Load(), err)
	}
}

func TestNativeClientRejectsRedirectAndUntrustedTLS(t *testing.T) {
	pin := newClientTestPin([]byte("x"), 0)
	var redirected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			redirected.Add(1)
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, pin.selection.LibraryID, clientTestBearer(), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Select(context.Background(), pin.selectRequest); !errors.Is(err, ErrInvalidResponse) || redirected.Load() != 0 {
		t.Fatalf("redirect followed or accepted: count=%d err=%v", redirected.Load(), err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS request reached handler") }))
	defer tlsServer.Close()
	tlsClient, err := NewClient(tlsServer.URL, pin.selection.LibraryID, clientTestBearer(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tlsClient.Select(context.Background(), pin.selectRequest); !errors.Is(err, ErrTransport) {
		t.Fatalf("untrusted TLS accepted: %v", err)
	}
	if err := tlsClient.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeClientBodyCloseErrorCannotCertifyEnd(t *testing.T) {
	pin := newClientTestPin([]byte("close-error"), 0)
	body := &clientTestBody{reader: bytes.NewReader(pin.payload), closeErr: errors.New("body close failed")}
	endpoint, _ := ParseEndpoint("http://127.0.0.1:25002", true)
	client := newClient(endpoint, pin.selection.LibraryID, clientTestBearer(), &http.Client{Transport: clientTestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return clientTestSyntheticResponse(pin, body, r), nil
	})}, nil)
	var dst bytes.Buffer
	progress, err := client.Transfer(context.Background(), pin.request, pin.selection, &dst)
	if !errors.Is(err, ErrInvalidResponse) || progress.End != (delivery.TransferEnd{}) || body.closed.Load() != 1 || !bytes.Equal(dst.Bytes(), pin.payload) {
		t.Fatalf("body Close failure certified transfer: progress=%+v close=%d err=%v", progress, body.closed.Load(), err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

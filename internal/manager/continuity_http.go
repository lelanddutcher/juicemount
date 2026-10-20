package manager

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const continuityBodyLimit = 32 << 10

type continuityWriter struct {
	response   http.ResponseWriter
	controller *http.ResponseController
	latest     time.Time
	started    bool
}

func (w *continuityWriter) Write(body []byte) (int, error) {
	if !w.started {
		w.started = true
		deadline := time.Now().Add(2 * time.Second)
		if w.latest.Before(deadline) {
			deadline = w.latest
		}
		if err := w.controller.SetWriteDeadline(deadline); err != nil {
			return 0, err
		}
	}
	n, err := w.response.Write(body)
	if err == nil {
		// net/http buffers small writes. Flush while the service still owns
		// this call and its write deadline, including full-write-plus-error.
		err = w.controller.Flush()
	}
	return n, err
}

func continuityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Del("ETag")
}

func continuityStatus(code string) int {
	switch code {
	case "invalid":
		return http.StatusBadRequest
	case "unauthenticated":
		return http.StatusUnauthorized
	case "not_found":
		return http.StatusNotFound
	case "restart_query":
		return http.StatusConflict
	case "work_budget":
		return http.StatusUnprocessableEntity
	case "deadline":
		return http.StatusGatewayTimeout
	case "method":
		return http.StatusMethodNotAllowed
	default:
		return http.StatusServiceUnavailable
	}
}

func writeContinuityError(w http.ResponseWriter, output io.Writer, code string) {
	continuityHeaders(w)
	w.WriteHeader(continuityStatus(code))
	_, _ = io.WriteString(output, `{"version":1,"error":"`+code+`"}`)
}

func (c *Continuity) serveHTTP(w http.ResponseWriter, r *http.Request, base string) {
	start := time.Now()
	continuityHeaders(w)
	controller := http.NewResponseController(w)
	output := &continuityWriter{response: w, controller: controller, latest: start.Add(4 * time.Second)}
	// Even overload/early errors need bounded body cleanup and output. Do not
	// clear these deadlines here: net/http still owns final flush/body cleanup
	// after the handler returns, and resets deadlines for the next request.
	if controller.SetReadDeadline(start.Add(2*time.Second)) != nil ||
		controller.SetWriteDeadline(output.latest) != nil {
		// No safe fallback write exists on a transport without deadline
		// support. net/http closes this response without exposing raw errors.
		panic(http.ErrAbortHandler)
	}
	route := "unknown"
	if strings.HasPrefix(r.URL.Path, base) {
		switch candidate := strings.TrimPrefix(r.URL.Path, base); candidate {
		case "search", "asset", "revisions", "history", "relationships", "locations", "counts", "page":
			route = candidate
		}
	}
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		writeContinuityError(w, output, "unavailable")
		return
	}
	requestID := hex.EncodeToString(randomID[:])
	outcome := "unavailable"
	var result catalog.DisclosureResult
	defer func() {
		// Fields have fixed shapes. Never log a URL, body, bearer, page token,
		// raw error, query counts, or any value before the release fence.
		record, _ := json.Marshal(struct {
			RequestID string            `json:"request_id"`
			Subject   catalog.SubjectID `json:"subject"`
			Selector  string            `json:"selector"`
			Route     string            `json:"route"`
			Outcome   string            `json:"outcome"`
		}{requestID, result.SubjectID, result.Selector, route, outcome})
		log.Printf("continuity %s", record)
	}()
	if !c.admit() {
		outcome = "busy"
		writeContinuityError(w, output, outcome)
		return
	}
	defer c.leave()
	ctx, cancel := context.WithDeadline(r.Context(), start.Add(2*time.Second))
	defer cancel()
	stopCancel := context.AfterFunc(c.ctx, cancel)
	defer stopCancel()
	fail := func(code string) {
		outcome = code
		writeContinuityError(w, output, code)
	}
	if route == "unknown" {
		fail("not_found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		fail("method")
		return
	}
	if !continuityOriginAllowed(r) || r.URL.RawQuery != "" || r.URL.ForceQuery {
		fail("invalid")
		return
	}
	bearer, ok := continuityBearer(r.Header.Values("Authorization"))
	if !ok {
		fail("unauthenticated")
		return
	}
	defer clear(bearer)
	r.Header.Del("Authorization")
	if !continuityContentType(r.Header) || r.ContentLength > continuityBodyLimit {
		fail("invalid")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, continuityBodyLimit))
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || time.Now().After(start.Add(2*time.Second)) {
			fail("deadline")
		} else {
			fail("invalid")
		}
		return
	}
	request, err := decodeContinuityRequest(route, body)
	if err != nil {
		fail("invalid")
		return
	}
	if ctx.Err() != nil {
		fail("deadline")
		return
	}
	result, err = c.service.Query(ctx, bearer, request, output)
	if err != nil {
		outcome = catalog.DiscoveryErrorCode(err)
		switch outcome {
		case "invalid", "unauthenticated", "not_found", "restart_query", "work_budget", "busy", "unavailable", "deadline":
		default:
			outcome = "unavailable"
		}
		if !result.Started && !output.started {
			writeContinuityError(w, output, outcome)
		}
		return
	}
	outcome = "ok"
}

func continuityBearer(headers []string) ([]byte, bool) {
	if len(headers) != 1 || !strings.HasPrefix(headers[0], "Bearer ") {
		return nil, false
	}
	bearer := headers[0][7:]
	if len(bearer) != 102 || !strings.HasPrefix(bearer, "jmc1_") || bearer[37] != '.' {
		return nil, false
	}
	for i := 5; i < len(bearer); i++ {
		if i == 37 {
			continue
		}
		if !(bearer[i] >= '0' && bearer[i] <= '9' || bearer[i] >= 'a' && bearer[i] <= 'f') {
			return nil, false
		}
	}
	return []byte(bearer), true
}

func continuityContentType(h http.Header) bool {
	values := h.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	media, params, err := mime.ParseMediaType(values[0])
	if err != nil || media != "application/json" {
		return false
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	encodings := h.Values("Content-Encoding")
	return len(encodings) == 0 || len(encodings) == 1 && strings.EqualFold(encodings[0], "identity")
}

func continuityOriginAllowed(r *http.Request) bool {
	fetch := r.Header.Values("Sec-Fetch-Site")
	if len(fetch) > 1 {
		return false
	}
	if len(fetch) == 1 && fetch[0] != "none" && fetch[0] != "same-origin" {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) > 1 {
		return false
	}
	if len(origins) == 0 {
		return true
	}
	u, err := url.Parse(origins[0])
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return err == nil && u.Scheme == scheme && strings.EqualFold(u.Host, r.Host) &&
		u.User == nil && u.Opaque == "" && u.Path == "" && u.RawQuery == "" &&
		!u.ForceQuery && u.Fragment == ""
}

func decodeContinuityRequest(kind string, body []byte) (catalog.DiscoveryRequest, error) {
	request := catalog.DiscoveryRequest{Kind: kind}
	invalid := errors.New("invalid discovery request")
	if !utf8.Valid(body) {
		return request, invalid
	}
	fields := map[string]any{"version": &request.Version, "library_id": &request.LibraryID}
	switch kind {
	case "search":
		fields["text"], fields["context_asset_ids"], fields["page_size"] = &request.Text, &request.ContextAssetIDs, &request.PageSize
	case "asset", "counts":
		fields["asset_id"] = &request.AssetID
	case "revisions", "history", "relationships", "locations":
		fields["asset_id"], fields["page_size"] = &request.AssetID, &request.PageSize
	case "page":
		fields["token"] = &request.Token
	default:
		return request, invalid
	}
	d := json.NewDecoder(bytes.NewReader(body))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return request, invalid
	}
	seen := make(map[string]bool, len(fields))
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		target, allowed := fields[name]
		if err != nil || !ok || !allowed || seen[name] {
			return request, invalid
		}
		seen[name] = true
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) ||
			json.Unmarshal(raw, target) != nil {
			return request, invalid
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || len(seen) != len(fields) {
		return request, invalid
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF || request.Version != 1 || request.LibraryID.Validate() != nil {
		return request, invalid
	}
	if kind == "page" {
		if len(request.Token) != 70 || !strings.HasPrefix(request.Token, "page1_") {
			return request, invalid
		}
		for _, c := range request.Token[6:] {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return request, invalid
			}
		}
		return request, nil
	}
	if kind != "search" && request.AssetID.Validate() != nil {
		return request, invalid
	}
	if kind != "asset" && kind != "counts" && (request.PageSize < 1 || request.PageSize > 100) {
		return request, invalid
	}
	if kind == "search" {
		if len(request.Text) > 256 || strings.ContainsRune(request.Text, 0) || len(request.ContextAssetIDs) > 64 {
			return request, invalid
		}
		ids := make(map[continuity.AssetID]bool, len(request.ContextAssetIDs))
		for _, id := range request.ContextAssetIDs {
			if id.Validate() != nil || ids[id] {
				return request, invalid
			}
			ids[id] = true
		}
	}
	return request, nil
}

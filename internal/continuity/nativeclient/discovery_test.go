package nativeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func discoveryTestHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func TestNativeDiscoveryLargeEscapedPageBothHTTPPrefixes(t *testing.T) {
	library := continuity.LibraryID("lib_11111111111111111111111111111111")
	token := "page1_" + strings.Repeat("a", 64)
	items := make([]catalog.DiscoveryAsset, 30)
	for i := range items {
		items[i] = catalog.DiscoveryAsset{AssetID: continuity.AssetID(fmt.Sprintf("asset_%032x", i+1)), Label: strings.Repeat("&", 4096), Reasons: []string{"stable_id"}}
	}
	first := catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: items, MatchedCount: len(items) + 1, CountScope: "frozen_window", Complete: true, Next: &token, Sort: "discovery2"}
	large, err := json.Marshal(first)
	if err != nil || len(large) <= 512<<10 || len(large) > 1<<20 {
		t.Fatalf("escaped page must exercise distinct 1 MiB client bound: %d %v", len(large), err)
	}
	last := catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: []catalog.DiscoveryAsset{{AssetID: continuity.AssetID(fmt.Sprintf("asset_%032x", len(items)+1)), Label: "last", Reasons: []string{"stable_id"}}}, MatchedCount: len(items) + 1, CountScope: "frozen_window", Complete: true, Next: nil, Sort: "discovery2"}
	lastJSON, _ := json.Marshal(last)
	search := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, Text: "", ContextAssetIDs: []continuity.AssetID{}, PageSize: 30}
	page := catalog.DiscoveryRequest{Version: 2, Kind: "page", LibraryID: library, Token: token}
	for _, prefix := range []string{"", "/manager"} {
		t.Run(prefix, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ordinal := calls.Add(1)
				kind, want := "search", search
				body := large
				if ordinal == 2 {
					kind, want, body = "page", page, lastJSON
				}
				wantJSON, _ := catalog.CanonicalNativeDiscoveryRequest(want, library)
				gotJSON, readErr := io.ReadAll(io.LimitReader(r.Body, 32<<10+1))
				if r.Method != http.MethodPost || r.URL.Path != prefix+"/api/continuity/v2/"+kind || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer "+string(clientTestBearer()) || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Cookie")) != 0 || readErr != nil || !bytes.Equal(gotJSON, wantJSON) {
					t.Errorf("discovery route, pin or body mismatch: route=%s call=%d", r.URL.Path, ordinal)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				discoveryTestHeaders(w)
				_, _ = w.Write(body)
			}))
			defer server.Close()
			client, err := NewClient(server.URL+prefix, library, clientTestBearer(), true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(context.Background()); err != nil {
					t.Errorf("client original close: %v", err)
				}
			}()
			got, err := client.Search(context.Background(), search)
			if err != nil || len(got.Items) != len(items) || got.Next == nil || *got.Next != token || got.Items[0].Label != items[0].Label {
				t.Fatalf("large typed search body: items=%d err=%v", len(got.Items), err)
			}
			final, err := client.Page(context.Background(), token)
			if err != nil || final.Kind != "search" || final.MatchedCount != len(items)+1 || len(final.Items) != 1 || final.Next != nil || calls.Load() != 2 {
				t.Fatalf("original search page replay: %+v %v calls=%d", final, err, calls.Load())
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal("joined client close:", err)
			}
			if _, err := client.Search(context.Background(), search); !errors.Is(err, ErrClientClosed) {
				t.Fatalf("closed client admitted new search: %v", err)
			}
		})
	}
}

func TestNativeDiscoveryRejectsMalformedAndOversizedReplies(t *testing.T) {
	library := continuity.LibraryID("lib_11111111111111111111111111111111")
	request := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
	for _, tc := range []struct {
		name string
		code int
		body []byte
		want string
	}{
		{"duplicate", 200, []byte(`{"version":2,"version":2}`), "invalid"},
		{"oversize", 200, bytes.Repeat([]byte("x"), (1<<20)+1), "invalid"},
		{"restart", 409, []byte(`{"version":1,"error":"restart_query"}`), "restart_query"},
		{"work_budget", 422, []byte(`{"version":1,"error":"work_budget"}`), "work_budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				discoveryTestHeaders(w)
				w.WriteHeader(tc.code)
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, library, clientTestBearer(), true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(context.Background()); err != nil {
					t.Errorf("client original close: %v", err)
				}
			}()
			_, err = client.Search(context.Background(), request)
			if tc.want == "invalid" {
				if !errors.Is(err, ErrInvalidResponse) {
					t.Fatalf("invalid discovery response admitted: %v", err)
				}
			} else {
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.Code != tc.want || remote.StatusCode != tc.code {
					t.Fatalf("wrong typed discovery error: %v", err)
				}
			}
		})
	}
}

func TestNativeDiscoveryJoinsOriginalResponseBodyBeforeResult(t *testing.T) {
	library := continuity.LibraryID("lib_11111111111111111111111111111111")
	request := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
	page := catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: []catalog.DiscoveryAsset{}, MatchedCount: 0, CountScope: "frozen_window", Complete: true, Sort: "discovery2"}
	wire, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	body := &clientTestBody{reader: bytes.NewReader(wire)}
	endpoint, err := ParseEndpoint("http://127.0.0.1:25001", true)
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(endpoint, library, clientTestBearer(), &http.Client{Transport: clientTestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}}, Body: body, ContentLength: -1, Request: r}, nil
	})}, nil)
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client original close: %v", err)
		}
	}()
	got, err := client.Search(context.Background(), request)
	if err != nil || got.Items == nil || body.closed.Load() != 1 {
		t.Fatalf("discovery returned before exact original Body.Close: %+v %v closes=%d", got, err, body.closed.Load())
	}
}

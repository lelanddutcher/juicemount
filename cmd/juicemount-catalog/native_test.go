package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/nativeclient"
)

func nativeTestBearer() string {
	return "jmc1_" + strings.Repeat("a", 32) + "." + strings.Repeat("b", 64)
}
func nativeTestEnvelope(t *testing.T, request []byte) []byte {
	t.Helper()
	body, err := json.Marshal(collectionMutationEnvelope{Version: 1, Bearer: nativeTestBearer(), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
func nativeTestRun(root, endpoint string, input []byte, flags []string, command ...string) (int, []byte, []byte) {
	args := []string{"--native-root", root, "--endpoint", endpoint, "--library", string(cliLibrary), "--allow-loopback-http"}
	args = append(args, flags...)
	args = append(args, command...)
	var output, diagnostic bytes.Buffer
	code := run(context.Background(), args, bytes.NewReader(input), &output, &diagnostic)
	return code, output.Bytes(), diagnostic.Bytes()
}
func nativeTestDiagnostic(t *testing.T, b []byte, expectedCode string, uncertain bool) {
	t.Helper()
	var value struct {
		Error            string `json:"error"`
		Code             string `json:"code"`
		OutcomeUncertain bool   `json:"outcome_uncertain"`
	}
	if json.Unmarshal(b, &value) != nil || value.Code != expectedCode || value.OutcomeUncertain != uncertain || value.Error == "" || strings.Contains(string(b), nativeTestBearer()) {
		t.Fatalf("wrong bounded native diagnostic: %s", b)
	}
}

func TestNativeHelperEarlyDispatchDiscoverySelectAndExactRetry(t *testing.T) {
	const copyID = "copy_11111111111111111111111111111111"
	content := []byte("selected content")
	sum := sha256.Sum256(content)
	expires := time.Now().Add(5 * time.Minute).Unix()
	revision := continuity.RevisionID("rev1_" + strings.Repeat("a", 64))
	request := catalog.DeliverySelectRequest{Version: 1, LibraryID: cliLibrary, ID: "native-selected-A", AssetID: cliAssetA, Mode: "exact", RevisionID: revision, ExpiresUnix: expires, MaxBytes: 1 << 20}
	selection := catalog.DeliverySelection{Version: 1, LibraryID: cliLibrary, SelectionID: "dsel_" + strings.Repeat("a", 32), OperationID: request.ID, AssetID: cliAssetA, RevisionID: revision, SourceID: "dsrc_" + strings.Repeat("b", 32), SourceVersion: 1, Content: continuity.Content{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content))}, ExpiresUnix: expires}
	search := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: cliLibrary, Text: "photo", ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
	searchBody, _ := catalog.CanonicalNativeDiscoveryRequest(search, cliLibrary)
	page := catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: []catalog.DiscoveryAsset{{AssetID: cliAssetA, Label: "Photo A", Reasons: []string{"label_prefix"}}}, MatchedCount: 1, CountScope: "frozen_window", Complete: true, Next: nil, Sort: "discovery2"}
	pageBody, _ := json.Marshal(page)
	selectBody, _, err := catalog.CanonicalDeliverySelectRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	selectedBody, _ := json.Marshal(selection)
	root := filepath.Join(t.TempDir(), "native-profile")
	intent := nativeclient.Intent{Version: 1, Endpoint: "", LibraryID: cliLibrary, OperationID: request.ID, CopyID: copyID, SelectRequest: request, Basename: "approved.png", MaxBytes: request.MaxBytes, MetadataReservationBytes: nativeclient.MetadataReservationBytes}
	var searchCalls, selectCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+nativeTestBearer() {
			t.Error("helper sent wrong HTTP method or credential")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/continuity/v2/search":
			searchCalls.Add(1)
			body, err := io.ReadAll(io.LimitReader(r.Body, int64(len(searchBody)+1)))
			if err != nil || !bytes.Equal(body, searchBody) {
				t.Errorf("noncanonical discovery request: %s", body)
			}
			_, _ = w.Write(pageBody)
		case "/api/continuity/v1/delivery/select":
			ordinal := selectCalls.Add(1)
			// The immutable local Intent must already exist before any remote
			// request can succeed, including the first attempt and exact retry.
			wantIntent := intent
			wantIntent.Endpoint = "http://" + r.Host
			wantRecord, _, encodeErr := nativeclient.EncodeIntent(wantIntent, true)
			gotRecord, readErr := os.ReadFile(filepath.Join(root, "staging", copyID, "intent.json"))
			if encodeErr != nil || readErr != nil || !bytes.Equal(gotRecord, wantRecord) {
				t.Errorf("select reached HTTP before exact durable local Intent: encode=%v read=%v", encodeErr, readErr)
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, int64(len(selectBody)+1)))
			if err != nil || !bytes.Equal(body, selectBody) {
				t.Errorf("noncanonical select request: %s", body)
			}
			if ordinal == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"version":1,"error":"busy"}`))
				return
			}
			_, _ = w.Write(selectedBody)
		default:
			t.Errorf("unexpected helper route %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	code, out, diagnostic := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create")
	if code != 0 || string(out) != "{\"version\":1,\"created\":true}\n" || len(diagnostic) != 0 {
		t.Fatalf("native profile create without --catalog: code=%d out=%s err=%s", code, out, diagnostic)
	}
	// The helper accepts canonical user input before the server's search-text
	// normalization, without requiring Swift to perform private normalization.
	mixedSearch := bytes.Replace(searchBody, []byte(`"text":"photo"`), []byte(`"text":" Photo "`), 1)
	code, out, diagnostic = nativeTestRun(root, server.URL, nativeTestEnvelope(t, mixedSearch), nil, "native", "search")
	var discovered catalog.DiscoverySearchPage
	if code != 0 || json.Unmarshal(out, &discovered) != nil || len(discovered.Items) != 1 || discovered.Items[0].AssetID != cliAssetA || searchCalls.Load() != 1 || len(diagnostic) != 0 {
		t.Fatalf("native search failed: code=%d out=%s err=%s", code, out, diagnostic)
	}
	flags := []string{"--copy-id", copyID, "--basename", "approved.png"}
	for retry := 0; retry < 3; retry++ {
		code, out, diagnostic = nativeTestRun(root, server.URL, nativeTestEnvelope(t, selectBody), flags, "native", "select")
		if retry == 0 {
			if code != 3 || len(out) != 0 {
				t.Fatalf("failed first remote attempt disclosed selection: code=%d out=%s", code, out)
			}
			nativeTestDiagnostic(t, diagnostic, "busy", true)
			pending, openErr := nativeclient.OpenJournal(root, server.URL, cliLibrary, true)
			if openErr != nil {
				t.Fatal(openErr)
			}
			pendingUsage, usageErr := pending.Usage()
			closeErr := pending.Close()
			if usageErr != nil || closeErr != nil || pendingUsage.IntentCount != 1 || pendingUsage.LogicalBytes != 0 || pendingUsage.MetadataBytes != nativeclient.MetadataReservationBytes {
				t.Fatalf("failed remote select lost or recharged original Intent: %+v %v %v", pendingUsage, usageErr, closeErr)
			}
			continue
		}
		var got nativeSelectResult
		if code != 0 || json.Unmarshal(out, &got) != nil || got.CopyID != copyID || got.Selection != selection || len(diagnostic) != 0 {
			t.Fatalf("native select/retry %d failed: code=%d out=%s err=%s", retry, code, out, diagnostic)
		}
	}
	if selectCalls.Load() != 3 {
		t.Fatalf("same operation retries did not call real server three times: %d", selectCalls.Load())
	}
	journal, err := nativeclient.OpenJournal(root, server.URL, cliLibrary, true)
	if err != nil {
		t.Fatal(err)
	}
	usage, usageErr := journal.Usage()
	closeErr := journal.Close()
	if usageErr != nil || closeErr != nil || usage.IntentCount != 1 || usage.LogicalBytes != int64(len(content)) || usage.MetadataBytes != nativeclient.MetadataReservationBytes {
		t.Fatalf("exact retry changed local reservation: %+v %v %v", usage, usageErr, closeErr)
	}
	changed := []string{"--copy-id", copyID, "--basename", "other.png"}
	code, out, diagnostic = nativeTestRun(root, server.URL, nativeTestEnvelope(t, selectBody), changed, "native", "select")
	if code != 3 || len(out) != 0 || selectCalls.Load() != 3 {
		t.Fatal("changed Intent unexpectedly reached server or returned output")
	}
	nativeTestDiagnostic(t, diagnostic, "conflict", true)
	code, out, diagnostic = nativeTestRun(root, server.URL, nil, []string{"--copy-id", copyID, "--timeout", "30s"}, "native", "copy-status")
	if code != 3 || len(out) != 0 {
		t.Fatal("unverified copy reported offline success")
	}
	nativeTestDiagnostic(t, diagnostic, "conflict", false)
}

func TestNativeHelperRefusesCatalogFlagsAndForeignProfileBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	root := filepath.Join(t.TempDir(), "profile")
	code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create")
	if code != 0 {
		t.Fatal("profile create failed")
	}
	for _, tc := range []struct {
		name     string
		endpoint string
		flags    []string
		command  []string
	}{
		{"catalog_nonempty", server.URL, []string{"--catalog", filepath.Join(t.TempDir(), "catalog.sqlite")}, []string{"native", "search"}},
		{"catalog_empty", server.URL, []string{"--catalog="}, []string{"native", "search"}},
		{"owner_flag", server.URL, []string{"--secret-out=-"}, []string{"native", "search"}},
		{"bare_native", server.URL, []string{"--catalog", filepath.Join(t.TempDir(), "catalog.sqlite")}, []string{"native"}},
		{"foreign_endpoint", "http://127.0.0.1:12345", nil, []string{"native", "search"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, diagnostic := nativeTestRun(root, tc.endpoint, nil, tc.flags, tc.command...)
			if code != 3 || len(out) != 0 || calls.Load() != 0 {
				t.Fatalf("invalid native dispatch reached server or output: code=%d out=%s", code, out)
			}
			var errorValue struct {
				Code string `json:"code"`
			}
			if json.Unmarshal(diagnostic, &errorValue) != nil || errorValue.Code == "" {
				t.Fatalf("missing fixed native error: %s", diagnostic)
			}
		})
	}
}

func TestNativeHelperPreservesFixedRemoteDiscoveryCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path != "/api/continuity/v2/search" {
			t.Error("wrong discovery route")
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"version":1,"error":"restart_query"}`))
	}))
	defer server.Close()
	root := filepath.Join(t.TempDir(), "profile")
	if code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create"); code != 0 {
		t.Fatal("profile create failed")
	}
	request := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: cliLibrary, ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
	body, err := catalog.CanonicalNativeDiscoveryRequest(request, cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	code, out, diagnostic := nativeTestRun(root, server.URL, nativeTestEnvelope(t, body), nil, "native", "search")
	if code != 3 || len(out) != 0 {
		t.Fatalf("restart query returned success or output: code=%d out=%s", code, out)
	}
	var value struct {
		Code             string `json:"code"`
		HTTPStatus       int    `json:"http_status"`
		OutcomeUncertain bool   `json:"outcome_uncertain"`
	}
	if json.Unmarshal(diagnostic, &value) != nil || value.Code != "restart_query" || value.HTTPStatus != 409 || value.OutcomeUncertain || strings.Contains(string(diagnostic), nativeTestBearer()) {
		t.Fatalf("remote restart code lost or leaked: %s", diagnostic)
	}
}

func TestNativeHelperReadOnlyOwnerCloseFaultSuppressesReceipt(t *testing.T) {
	library := cliLibrary
	request := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
	wire, err := catalog.CanonicalNativeDiscoveryRequest(request, library)
	if err != nil {
		t.Fatal(err)
	}
	page, err := json.Marshal(catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: []catalog.DiscoveryAsset{}, MatchedCount: 0, CountScope: "frozen_window", Complete: true, Sort: "discovery2"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path != "/api/continuity/v2/search" {
			t.Error("wrong discovery route")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(page)
	}))
	defer server.Close()
	root := filepath.Join(t.TempDir(), "profile")
	if code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create"); code != 0 {
		t.Fatal("profile create failed")
	}
	fault := errors.New("DO-NOT-ECHO-native-journal-close")
	files := defaultFiles()
	closes := 0
	files.closeNativeJournal = func(j *nativeclient.Journal) error {
		closes++
		if err := j.Close(); err != nil {
			return err
		}
		return fault
	}
	args := []string{"--native-root", root, "--endpoint", server.URL, "--library", string(library), "--allow-loopback-http", "native", "search"}
	var output, diagnostic bytes.Buffer
	code := runConfigured(context.Background(), args, bytes.NewReader(nativeTestEnvelope(t, wire)), &output, &diagnostic, false, files)
	if code != 3 || output.Len() != 0 || closes != 1 || calls.Load() != 1 {
		t.Fatalf("read-only owner-close error exposed result or missed original: code=%d out=%s closes=%d calls=%d", code, output.Bytes(), closes, calls.Load())
	}
	nativeTestDiagnostic(t, diagnostic.Bytes(), "uncertain", true)
	if bytes.Contains(diagnostic.Bytes(), []byte(fault.Error())) {
		t.Fatal("private owner close error leaked")
	}
	fresh, err := nativeclient.OpenJournal(root, server.URL, library, true)
	if err != nil {
		t.Fatalf("original owner was not released: %v", err)
	}
	if _, err := fresh.Usage(); err != nil {
		t.Errorf("fresh owner cannot read accounting: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Errorf("fresh original close: %v", err)
	}
}

func TestNativeHelperReconcileInvalidReplyRemainsUncertain(t *testing.T) {
	request := nativeReconcileRequest{Version: 1, LibraryID: cliLibrary, SelectionID: "dsel_" + strings.Repeat("a", 32), OperationID: "helper-reconcile"}
	wire, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, readErr := io.ReadAll(io.LimitReader(r.Body, int64(len(wire)+1)))
		if r.Method != http.MethodPost || r.URL.Path != "/api/continuity/v1/delivery/reconcile" || readErr != nil || !bytes.Equal(body, wire) || r.Header.Get("Authorization") != "Bearer "+nativeTestBearer() {
			t.Errorf("reconcile did not reach exact pinned operation: path=%s err=%v", r.URL.Path, readErr)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write([]byte(`{"version":1,"attempt":{}}`))
	}))
	defer server.Close()
	root := filepath.Join(t.TempDir(), "profile")
	if code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create"); code != 0 {
		t.Fatal("profile create failed")
	}
	code, output, diagnostic := nativeTestRun(root, server.URL, nativeTestEnvelope(t, wire), nil, "native", "reconcile")
	if code != 3 || len(output) != 0 || calls.Load() != 1 {
		t.Fatalf("invalid reply after reconcile hid uncertain outcome: code=%d out=%s calls=%d", code, output, calls.Load())
	}
	nativeTestDiagnostic(t, diagnostic, "invalid_response", true)
}

type nativeObservedInput struct {
	fileResource
	entered chan struct{}
	once    sync.Once
}

func (r *nativeObservedInput) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.fileResource.Read(p)
}

func TestNativeHelperOwnedStdioCloseFaultAndCancellation(t *testing.T) {
	for _, mode := range []string{"input_close", "output_close"} {
		t.Run(mode, func(t *testing.T) {
			request := catalog.DiscoveryRequest{Version: 2, Kind: "search", LibraryID: cliLibrary, ContextAssetIDs: []continuity.AssetID{}, PageSize: 1}
			wire, err := catalog.CanonicalNativeDiscoveryRequest(request, cliLibrary)
			if err != nil {
				t.Fatal(err)
			}
			page, _ := json.Marshal(catalog.DiscoverySearchPage{Version: 2, Kind: "search", Items: []catalog.DiscoveryAsset{}, MatchedCount: 0, CountScope: "frozen_window", Complete: true, Sort: "discovery2"})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				_, _ = w.Write(page)
			}))
			defer server.Close()
			root := filepath.Join(t.TempDir(), "profile")
			if code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create"); code != 0 {
				t.Fatal("profile create failed")
			}
			originals := make([]*os.File, 3)
			for i, name := range []string{"input", "output", "diagnostic"} {
				originals[i], err = os.CreateTemp(t.TempDir(), name)
				if err != nil {
					t.Fatal(err)
				}
				file := originals[i]
				t.Cleanup(func() {
					if e := file.Close(); e != nil && !errors.Is(e, os.ErrClosed) {
						t.Error(e)
					}
				})
			}
			if _, err := originals[0].Write(nativeTestEnvelope(t, wire)); err != nil {
				t.Fatal(err)
			}
			if _, err := originals[0].Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			input := &faultOutput{File: originals[0]}
			output := &faultOutput{File: originals[1]}
			if mode == "input_close" {
				input.closeErr = errors.New("DO-NOT-ECHO-native-input-close")
			} else {
				output.closeErr = errors.New("DO-NOT-ECHO-native-output-close")
			}
			diagnostic := &faultOutput{File: originals[2]}
			files := defaultFiles()
			restores := 0
			files.stdio = func([]*os.File) ([]fileResource, func() error, error) {
				return []fileResource{input, output, diagnostic}, func() error { restores++; return nil }, nil
			}
			args := []string{"--native-root", root, "--endpoint", server.URL, "--library", string(cliLibrary), "--allow-loopback-http", "native", "search"}
			code := runConfigured(context.Background(), args, originals[0], originals[1], originals[2], true, files)
			if code != 3 || input.closes != 1 || output.closes != 1 || diagnostic.closes != 1 || restores != 1 {
				t.Fatalf("owned originals did not join exactly once: code=%d closes=%d/%d/%d restores=%d", code, input.closes, output.closes, diagnostic.closes, restores)
			}
			result, err := os.ReadFile(originals[1].Name())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "input_close" && len(result) != 0 || mode == "output_close" && len(result) == 0 {
				t.Fatalf("owned %s fault violated output gate: bytes=%d", mode, len(result))
			}
			diag, err := os.ReadFile(originals[2].Name())
			if err != nil {
				t.Fatal(err)
			}
			// stopOwners may report the same cached close fault after the early
			// stdout disposition, so every diagnostic line must remain bounded.
			for _, line := range bytes.Split(bytes.TrimSpace(diag), []byte{'\n'}) {
				nativeTestDiagnostic(t, line, "uncertain", true)
			}
			if mode == "output_close" && (!bytes.Contains(diag, []byte(`"output_close_failed":true`)) || !bytes.Contains(diag, []byte(`"partial_output":true`))) || mode == "input_close" && bytes.Contains(diag, []byte(`"partial_output":true`)) || bytes.Contains(diag, []byte("DO-NOT-ECHO")) || bytes.Contains(diag, []byte(nativeTestBearer())) {
				t.Fatal("owned output close fault lost disposition or leaked cause")
			}
		})
	}

	t.Run("cancel_blocked_input", func(t *testing.T) {
		const endpoint = "http://127.0.0.1:25001"
		root := filepath.Join(t.TempDir(), "profile")
		if code, _, _ := nativeTestRun(root, endpoint, nil, nil, "native", "profile", "create"); code != 0 {
			t.Fatal("profile create failed")
		}
		inputRead, inputWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = inputWrite.Close() }()
		outputFile, err := os.CreateTemp(t.TempDir(), "output")
		if err != nil {
			t.Fatal(err)
		}
		diagnosticFile, err := os.CreateTemp(t.TempDir(), "diagnostic")
		if err != nil {
			t.Fatal(err)
		}
		input := &faultOutput{File: inputRead}
		output := &faultOutput{File: outputFile}
		diagnostic := &faultOutput{File: diagnosticFile}
		entered := make(chan struct{})
		observed := &nativeObservedInput{fileResource: input, entered: entered}
		files := defaultFiles()
		restores := 0
		files.stdio = func([]*os.File) ([]fileResource, func() error, error) {
			return []fileResource{observed, output, diagnostic}, func() error { restores++; return nil }, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		args := []string{"--native-root", root, "--endpoint", endpoint, "--library", string(cliLibrary), "--allow-loopback-http", "native", "search"}
		done := make(chan int, 1)
		go func() { done <- runConfigured(ctx, args, inputRead, outputFile, diagnosticFile, true, files) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Error("native owned input was never entered")
		}
		cancel()
		select {
		case code := <-done:
			if code != 3 {
				t.Errorf("canceled native command claimed success: %d", code)
			}
		case <-time.After(3 * time.Second):
			t.Error("cancellation did not join owned input promptly")
			_ = inputRead.Close()
			_ = inputWrite.Close()
			code := <-done
			if code != 3 {
				t.Errorf("forced cleanup claimed success: %d", code)
			}
		}
		if input.closes != 1 || output.closes != 1 || diagnostic.closes != 1 || restores != 1 {
			t.Fatalf("canceled native stdio originals not joined: closes=%d/%d/%d restores=%d", input.closes, output.closes, diagnostic.closes, restores)
		}
		if result, err := os.ReadFile(outputFile.Name()); err != nil || len(result) != 0 {
			t.Fatalf("canceled native output: %v bytes=%d", err, len(result))
		}
		fresh, err := nativeclient.OpenJournal(root, endpoint, cliLibrary, true)
		if err != nil {
			t.Fatalf("canceled helper retained profile owner: %v", err)
		}
		if err := fresh.Close(); err != nil {
			t.Errorf("fresh profile close: %v", err)
		}
	})
}

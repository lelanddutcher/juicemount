package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func cliSavedQueryCreate(t *testing.T, n int, anchors ...continuity.AssetID) catalog.SavedQueryRequest {
	t.Helper()
	if anchors == nil {
		anchors = []continuity.AssetID{}
	}
	return catalog.SavedQueryRequest{Version: 2, LibraryID: cliLibrary, ID: catalog.CollectionOperationID(fmt.Sprintf("cop_%032x", n)), CollectionID: catalog.CollectionID(fmt.Sprintf("col_%032x", n)), Kind: "create", Value: cliCollectionRaw(t, catalog.CreateSavedQuery{Name: "Synthetic saved query", CreatorGrantID: catalog.CollectionGrantID(fmt.Sprintf("cgr_%032x", n+2000)), Definition: catalog.SavedQueryDefinition{Version: 1, Kind: "ranked_assets", Text: "A", ContextAssetIDs: anchors}})}
}
func cliSavedQueryEdit(t *testing.T, n int, id catalog.CollectionID, version catalog.CollectionVersion, kind string, value any) catalog.SavedQueryRequest {
	t.Helper()
	return catalog.SavedQueryRequest{Version: 2, LibraryID: cliLibrary, ID: catalog.CollectionOperationID(fmt.Sprintf("cop_%032x", n)), CollectionID: id, ExpectedVersion: version, Kind: kind, Value: cliCollectionRaw(t, value)}
}
func cliSavedQueryBody(t *testing.T, bearer []byte, request catalog.SavedQueryRequest) []byte {
	t.Helper()
	body, _, err := catalog.CanonicalSavedQueryRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	// Construct the public shape independently of the production envelope type.
	return cliCollectionRaw(t, struct {
		Version int             `json:"version"`
		Bearer  string          `json:"bearer"`
		Request json.RawMessage `json:"request"`
	}{2, string(bearer), body})
}
func cliSavedQueryMutate(t *testing.T, path string, bearer []byte, request catalog.SavedQueryRequest) catalog.SavedQueryReceipt {
	t.Helper()
	code, output, diagnostic := invokeCLI(path, nil, cliSavedQueryBody(t, bearer, request), "query", "mutate")
	if code != 0 || len(diagnostic) != 0 || len(output) == 0 || output[len(output)-1] != '\n' {
		t.Fatalf("query mutation failed: code=%d", code)
	}
	receipt, err := catalog.DecodeSavedQueryReceipt(output[:len(output)-1])
	if err != nil || receipt.Version != 2 || receipt.OperationID != request.ID || receipt.CollectionID != request.CollectionID {
		t.Fatal("query receipt identity")
	}
	want := cliCollectionRaw(t, struct {
		Version       int                           `json:"version"`
		OperationID   catalog.CollectionOperationID `json:"operation_id"`
		CollectionID  catalog.CollectionID          `json:"collection_id"`
		ResultVersion catalog.CollectionVersion     `json:"result_version"`
	}{2, request.ID, request.CollectionID, receipt.ResultVersion})
	if !bytes.Equal(output, append(want, '\n')) {
		t.Fatal("receipt is not minimal canonical JSON plus one LF")
	}
	return receipt
}
func cliSavedQuerySetup(t *testing.T) (string, catalog.AuthorityStatus, []byte) {
	t.Helper()
	path, authority := cliCollectionSetup(t)
	bearer := cliCollectionIssue(t, path, authority, "saved-owner", cliOwner, cliCollectionActions)
	cliCollectionMutate(t, path, bearer, cliCollectionCreate(t, 700))
	if code, _, _ := invokeCLI(path, nil, nil, "upgrade-saved-queries"); code != 0 {
		t.Fatal("saved-query upgrade")
	}
	return path, authority, bearer
}

func TestCLISavedQueriesExplicitTargets(t *testing.T) {
	t.Run("new-schema6-exclusive", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "saved.sqlite")
		if code, out, diagnostic := invokeCLI(path, nil, nil, "init-saved-queries"); code != 0 || len(out) != 0 || len(diagnostic) != 0 {
			t.Fatal("schema6 init")
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("new catalog privacy")
		}
		// This refuses schema6; it would succeed if init silently created schema5.
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 3 {
			t.Fatal("init did not create schema6")
		}
		for i := 0; i < 2; i++ {
			if code, _, _ := invokeCLI(path, nil, nil, "upgrade-saved-queries"); code != 0 {
				t.Fatal("idempotent schema6 barrier")
			}
		}
		for _, command := range []string{"init-saved-queries", "init-collections", "init-authority", "init-preferences"} {
			if code, _, _ := invokeCLI(path, nil, nil, command); code != 3 {
				t.Fatal("exclusive init adopted existing target")
			}
		}
		label := "owner"
		code, output, _ := invokeCLI(path, nil, cliRequest(t, "saved-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: cliOwner, CreateLabel: &label}), "owner", "bootstrap")
		var receipt catalog.AdminReceipt
		if code != 0 || json.Unmarshal(output, &receipt) != nil || receipt.Result.Incarnation == "" {
			t.Fatal("new schema6 bootstrap")
		}
	})
	t.Run("explicit-five-to-six", func(t *testing.T) {
		path, a := cliCollectionSetup(t)
		bearer := cliCollectionIssue(t, path, a, "saved-schema5", cliOwner, cliCollectionActions)
		request := cliSavedQueryCreate(t, 701)
		if code, out, _ := invokeCLI(path, nil, cliSavedQueryBody(t, bearer, request), "query", "mutate"); code != 3 || len(out) != 0 {
			t.Fatal("mutation migrated schema5")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 0 {
			t.Fatal("schema5 changed on refused mutation")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-saved-queries"); code != 0 {
			t.Fatal("explicit upgrade")
		}
		cliSavedQueryMutate(t, path, bearer, request)
	})
	t.Run("earlier-missing-wrong-library", func(t *testing.T) {
		path, _ := cliSetup(t)
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-saved-queries"); code != 3 {
			t.Fatal("schema3 implicitly upgraded")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 0 {
			t.Fatal("schema3 preservation")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-saved-queries"); code != 3 {
			t.Fatal("schema4 implicitly upgraded")
		}
		missing := filepath.Join(t.TempDir(), "absent.sqlite")
		if code, _, _ := invokeCLI(missing, nil, nil, "upgrade-saved-queries"); code != 3 {
			t.Fatal("missing upgrade accepted")
		}
		if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("upgrade created missing target")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 0 {
			t.Fatal("schema4-to5 setup")
		}
		var out, diagnostic bytes.Buffer
		if code := run(context.Background(), []string{"--catalog", path, "--library", "lib_11111111111111111111111111111111", "upgrade-saved-queries"}, bytes.NewReader(nil), &out, &diagnostic); code != 3 {
			t.Fatal("wrong library accepted")
		}
	})
}

func TestCLISavedQueryMutationsAndManualCoexistence(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	create := cliSavedQueryCreate(t, 710, cliAssetA)
	created := cliSavedQueryMutate(t, path, bearer, create)
	replace := cliSavedQueryEdit(t, 711, create.CollectionID, created.ResultVersion, "replace_definition", catalog.ReplaceSavedQueryDefinition{Definition: catalog.SavedQueryDefinition{Version: 1, Kind: "ranked_assets", Text: "B", ContextAssetIDs: []continuity.AssetID{cliAssetB}}})
	replaced := cliSavedQueryMutate(t, path, bearer, replace)
	current := replaced
	current = cliSavedQueryMutate(t, path, bearer, cliSavedQueryEdit(t, 712, create.CollectionID, current.ResultVersion, "rename", catalog.RenameCollection{Name: "Renamed <query>"}))
	current = cliSavedQueryMutate(t, path, bearer, cliSavedQueryEdit(t, 713, create.CollectionID, current.ResultVersion, "archive", catalog.SetCollectionArchived{Archived: true}))
	value := catalog.CollectionGrantValue{SubjectID: cliReaderA, Scope: catalog.CollectionGrantScope{Kind: "collection", CollectionID: create.CollectionID}, Actions: []string{"collections.edit", "collections.read"}}
	current = cliSavedQueryMutate(t, path, bearer, cliSavedQueryEdit(t, 714, create.CollectionID, current.ResultVersion, "grants", catalog.EditCollectionGrants{Changes: []catalog.CollectionGrantChange{{Kind: "put", ID: "cgr_00000000000000000000000000000f01", Value: &value}}}))
	if current.ResultVersion == created.ResultVersion {
		t.Fatal("mutations did not advance version")
	}
	if cliSavedQueryMutate(t, path, bearer, create) != created || cliSavedQueryMutate(t, path, bearer, replace) != replaced {
		t.Fatal("historical retry became current-state mutation")
	}
	// Real manual v1 operations and saved-query v2 remain independent families.
	manual := cliCollectionMutate(t, path, bearer, cliCollectionCreate(t, 715))
	cliCollectionMutate(t, path, bearer, catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: "cop_000000000000000000000000000002cc", CollectionID: manual.CollectionID, ExpectedVersion: manual.ResultVersion, Kind: "rename", Value: cliCollectionRaw(t, catalog.RenameCollection{Name: "Manual remains manual"})})
	wrong := cliSavedQueryEdit(t, 717, manual.CollectionID, manual.ResultVersion, "rename", catalog.RenameCollection{Name: "DO-NOT-ECHO"})
	if code, out, _ := invokeCLI(path, nil, cliSavedQueryBody(t, bearer, wrong), "query", "mutate"); code != 3 || len(out) != 0 {
		t.Fatal("query took manual ID")
	}
	old := catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: "cop_000000000000000000000000000002ce", CollectionID: create.CollectionID, ExpectedVersion: current.ResultVersion, Kind: "rename", Value: cliCollectionRaw(t, catalog.RenameCollection{Name: "DO-NOT-ECHO"})}
	if code, out, _ := invokeCLI(path, nil, cliCollectionEnvelope(t, bearer, old), "collection", "mutate"); code != 3 || len(out) != 0 {
		t.Fatal("manual took query ID")
	}
}

func TestCLISavedQueryCurrentRightsAnchorsAndService(t *testing.T) {
	path, a, owner := cliSavedQuerySetup(t)
	reader := cliCollectionIssue(t, path, a, "saved-reader", cliReaderA, cliCollectionActions)
	service := cliCollectionIssue(t, path, a, "saved-service", cliCollectionService, cliCollectionActions)
	readOnly := cliCollectionIssue(t, path, a, "saved-read-only", cliOwner, []string{"assets.discover", "collections.read"})
	create := cliSavedQueryCreate(t, 720, cliAssetB)
	created := cliSavedQueryMutate(t, path, owner, create)
	for _, tc := range []struct {
		name    string
		bearer  []byte
		request catalog.SavedQueryRequest
	}{
		{"hidden-anchor", reader, cliSavedQueryCreate(t, 721, cliAssetB)},
		{"service", service, cliSavedQueryCreate(t, 722)},
		{"missing-edit-on-retry", readOnly, create},
		{"no-current-grant", reader, cliSavedQueryEdit(t, 723, create.CollectionID, created.ResultVersion, "rename", catalog.RenameCollection{Name: "DO-NOT-ECHO"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, nil, cliSavedQueryBody(t, tc.bearer, tc.request), "query", "mutate")
			if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, tc.bearer) || strings.Contains(string(diagnostic), "DO-NOT-ECHO") {
				t.Fatal("denied query disclosed or succeeded")
			}
		})
	}
	// Distinct operations against the denied identities prove no create was committed.
	for _, n := range []int{721, 722} {
		q := cliSavedQueryCreate(t, n)
		q.ID = catalog.CollectionOperationID(fmt.Sprintf("cop_%032x", n+100))
		cliSavedQueryMutate(t, path, owner, q)
	}
	cliSavedQueryMutate(t, path, reader, cliSavedQueryCreate(t, 724, cliAssetA))
	if retry := cliSavedQueryMutate(t, path, owner, create); retry != created {
		t.Fatal("current authorized retry changed")
	}
}

func TestCLISavedQueryStrictEnvelopeAndReaderFailures(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	request := cliSavedQueryCreate(t, 730)
	good := cliSavedQueryBody(t, bearer, request)
	manual := cliCollectionRaw(t, struct {
		Version int             `json:"version"`
		Bearer  string          `json:"bearer"`
		Request json.RawMessage `json:"request"`
	}{2, string(bearer), cliCollectionRaw(t, cliCollectionCreate(t, 730))})
	cases := map[string][]byte{
		"unknown":          bytes.Replace(good, []byte(`"version":2`), []byte(`"version":2,"DO-NOT-ECHO":true`), 1),
		"duplicate":        bytes.Replace(good, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1),
		"wrong-version":    bytes.Replace(good, []byte(`"version":2`), []byte(`"version":1`), 1),
		"missing":          bytes.Replace(good, []byte(`"version":2,`), nil, 1),
		"alternate-key":    bytes.Replace(good, []byte(`"version"`), []byte(`"\u0076ersion"`), 1),
		"alternate-bearer": bytes.Replace(good, []byte("jmc1_"), []byte("jmc1X"), 1),
		"wrong-library":    bytes.Replace(good, []byte(cliLibrary), []byte("lib_11111111111111111111111111111111"), 1),
		"manual-nested":    manual,
		"nested-actor":     bytes.Replace(good, []byte(`"kind":"create"`), []byte(`"actor_subject_id":"sub_11111111111111111111111111111111","kind":"create"`), 1),
		"null":             cliCollectionRaw(t, savedQueryMutationEnvelope{Version: 2, Bearer: string(bearer), Request: json.RawMessage("null")}),
		"trailing":         append(append([]byte(nil), good...), []byte(`{}`)...),
		"non-ASCII-trim":   append([]byte("\u00a0"), good...),
		"oversize":         append(bytes.Repeat([]byte{' '}, catalog.MaxSavedQueryRequestBytes+513-len(good)), good...),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, nil, body, "query", "mutate")
			if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, bearer) || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) || bytes.Contains(diagnostic, []byte(`"operation_id"`)) {
				t.Fatal("invalid envelope accepted/echoed")
			}
		})
	}
	readFailure := errors.New("DO-NOT-ECHO-reader")
	raw := bytes.NewReader(good)
	reader := cliReadFunc(func(p []byte) (int, error) {
		n, err := raw.Read(p)
		if raw.Len() == 0 {
			return n, readFailure
		}
		return n, err
	})
	var out, diagnostic bytes.Buffer
	if code := run(context.Background(), cliArgs(path, nil, "query", "mutate"), reader, &out, &diagnostic); code != 3 || out.Len() != 0 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || bytes.Contains(diagnostic.Bytes(), bearer) {
		t.Fatal("bytes plus read error accepted/echoed")
	}
	q := request
	q.ID = "cop_0000000000000000000000000000033e"
	cliSavedQueryMutate(t, path, bearer, q)
	// Equality at the complete outer-input cap is permitted; +1 above was refused.
	bounded := cliSavedQueryCreate(t, 731)
	body := cliSavedQueryBody(t, bearer, bounded)
	body = append(bytes.Repeat([]byte{' '}, catalog.MaxSavedQueryRequestBytes+512-len(body)), body...)
	if code, output, _ := invokeCLI(path, nil, body, "query", "mutate"); code != 0 || len(output) == 0 {
		t.Fatal("exact bound rejected")
	}
}

func TestCLISavedQueryNamedInputOwnership(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	request := cliSavedQueryCreate(t, 740)
	inputPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(inputPath, append([]byte(" \t\r\n"), cliSavedQueryBody(t, bearer, request)...), 0600); err != nil {
		t.Fatal(err)
	}
	if code, output, _ := invokeCLI(path, []string{"--input", inputPath}, []byte("bad borrowed stdin"), "query", "mutate"); code != 0 || len(output) == 0 {
		t.Fatal("named input was ignored")
	}
	request = cliSavedQueryCreate(t, 741)
	if err := os.WriteFile(inputPath, cliSavedQueryBody(t, bearer, request), 0600); err != nil {
		t.Fatal(err)
	}
	files := defaultFiles()
	var input *faultOutput
	files.input = func(p string) (fileResource, error) {
		f, err := openInput(p)
		if err != nil {
			return nil, err
		}
		input = &faultOutput{File: f, closeErr: errors.New("DO-NOT-ECHO-input-close")}
		return input, nil
	}
	var out, diagnostic bytes.Buffer
	if code := runConfigured(context.Background(), cliArgs(path, []string{"--input", inputPath}, "query", "mutate"), bytes.NewReader(nil), &out, &diagnostic, false, files); code != 3 || out.Len() != 0 || input == nil || input.closes != 1 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || bytes.Contains(diagnostic.Bytes(), bearer) {
		t.Fatal("input Close not joined before mutation")
	}
	request.ID = "cop_00000000000000000000000000000349"
	cliSavedQueryMutate(t, path, bearer, request)
	if err := os.WriteFile(inputPath, cliSavedQueryBody(t, bearer, cliSavedQueryCreate(t, 742)), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(inputPath, link); err != nil {
		t.Fatal(err)
	}
	if code, output, _ := invokeCLI(path, []string{"--input", link}, nil, "query", "mutate"); code != 3 || len(output) != 0 {
		t.Fatal("symlink input accepted")
	}
}

func TestCLISavedQueryMutationUncertainty(t *testing.T) {
	for _, kind := range []string{"full-write-error", "short-write", "cancel-after-receipt", "LF-error"} {
		t.Run(kind, func(t *testing.T) {
			path, _, bearer := cliSavedQuerySetup(t)
			request := cliSavedQueryCreate(t, 750)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			capture := &cliCollectionCaptureFault{short: kind == "short-write"}
			var writer io.Writer = capture
			if kind == "cancel-after-receipt" {
				capture.cancel = cancel
			}
			lf := &cliSavedQueryLFFault{}
			if kind == "LF-error" {
				writer = lf
			}
			var diagnostic bytes.Buffer
			code := run(ctx, cliArgs(path, nil, "query", "mutate"), bytes.NewReader(cliSavedQueryBody(t, bearer, request)), writer, &diagnostic)
			if code != 3 || bytes.Contains(diagnostic.Bytes(), bearer) || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || strings.Contains(diagnostic.String(), "revoke/reissue") {
				t.Fatal("uncertain query misreported/echoed")
			}
			if kind != "cancel-after-receipt" && !strings.Contains(diagnostic.String(), "retry same exact request or refresh current state") {
				t.Fatal("missing exact retry guidance")
			}
			body := capture.Bytes()
			if kind == "LF-error" {
				body = lf.Bytes()
			}
			retained, err := catalog.DecodeSavedQueryReceipt(body)
			if err != nil {
				t.Fatal("did not exercise actual committed receipt output")
			}
			if cliSavedQueryMutate(t, path, bearer, request) != retained {
				t.Fatal("lost response reapplied mutation")
			}
		})
	}
}

type cliSavedQueryLFFault struct {
	bytes.Buffer
	calls int
}

func (w *cliSavedQueryLFFault) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return w.Buffer.Write(p)
	}
	return 0, errors.New("DO-NOT-ECHO-LF")
}

// Cancellation closes the same original owned reader, rather than abandoning a
// blocked read or launching a replacement operation after a timeout.
type cliSavedQueryBlockedInput struct {
	entered, closed chan struct{}
	once            sync.Once
	closes          int
}

func (f *cliSavedQueryBlockedInput) Read([]byte) (int, error) {
	close(f.entered)
	<-f.closed
	return 0, errors.New("closed input")
}
func (f *cliSavedQueryBlockedInput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (f *cliSavedQueryBlockedInput) Sync() error               { return nil }
func (f *cliSavedQueryBlockedInput) Name() string              { return "synthetic-input" }
func (f *cliSavedQueryBlockedInput) Close() error {
	f.once.Do(func() { f.closes++; close(f.closed) })
	return nil
}
func TestCLISavedQueryCancellationJoinsOwnedInput(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	input := &cliSavedQueryBlockedInput{entered: make(chan struct{}), closed: make(chan struct{})}
	files := defaultFiles()
	files.input = func(string) (fileResource, error) { return input, nil }
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var out, diagnostic bytes.Buffer
	done := make(chan int, 1)
	defer func() { cancel(); wg.Wait() }()
	go func() {
		defer wg.Done()
		done <- runConfigured(ctx, cliArgs(path, []string{"--input", "synthetic-input"}, "query", "mutate"), bytes.NewReader(nil), &out, &diagnostic, false, files)
	}()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	select {
	case <-input.entered:
	case code := <-done:
		t.Fatalf("input returned before gate: %d", code)
	case <-guard.C:
		t.Fatal("input did not enter")
	}
	cancel()
	wg.Wait()
	if code := <-done; code != 3 || out.Len() != 0 || input.closes != 1 {
		t.Fatal("owned cancellation failed to close/join original")
	}
	cliSavedQueryMutate(t, path, bearer, cliSavedQueryCreate(t, 760))
}

func TestCLISavedQueryOwnedOutputCloseUncertainty(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	request := cliSavedQueryCreate(t, 770)
	var originals []*os.File
	for _, name := range []string{"in", "out", "diagnostic"} {
		f, err := os.CreateTemp(t.TempDir(), name)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		originals = append(originals, f)
	}
	if _, err := originals[0].Write(cliSavedQueryBody(t, bearer, request)); err != nil {
		t.Fatal(err)
	}
	if _, err := originals[0].Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	files := defaultFiles()
	output := &faultOutput{File: originals[1], closeErr: errors.New("DO-NOT-ECHO-output-close")}
	files.stdio = func([]*os.File) ([]fileResource, func() error, error) {
		return []fileResource{originals[0], output, originals[2]}, func() error { return nil }, nil
	}
	if code := runConfigured(context.Background(), cliArgs(path, nil, "query", "mutate"), originals[0], originals[1], originals[2], true, files); code != 3 || output.closes != 1 {
		t.Fatal("owned output Close failure ignored")
	}
	body, err := os.ReadFile(originals[1].Name())
	if err != nil {
		t.Fatal(err)
	}
	diagnostic, err := os.ReadFile(originals[2].Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(diagnostic, []byte("retry same exact request or refresh current state")) || !bytes.Contains(diagnostic, []byte(`"output_close_failed":true`)) || bytes.Contains(diagnostic, bearer) || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) {
		t.Fatal("owned Close uncertainty/redaction")
	}
	receipt, err := catalog.DecodeSavedQueryReceipt(bytes.TrimSuffix(body, []byte{'\n'}))
	if err != nil {
		t.Fatal(err)
	}
	if cliSavedQueryMutate(t, path, bearer, request) != receipt {
		t.Fatal("owned Close lost exact retry")
	}
}

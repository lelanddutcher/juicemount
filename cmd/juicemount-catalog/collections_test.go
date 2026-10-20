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
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const cliCollectionService catalog.SubjectID = "sub_44444444444444444444444444444444"

var cliCollectionActions = []string{"assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read"}

func cliCollectionRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func cliCollectionPolicyBody(t *testing.T, authority catalog.AuthorityStatus, id int, category string, collection catalog.CollectionID, changes ...catalog.CollectionGrantChange) []byte {
	t.Helper()
	policy := catalog.CollectionPolicyRequest{Version: 2, LibraryID: cliLibrary, ID: catalog.CollectionPolicyOperationID(fmt.Sprintf("cpo_%032x", id)), Category: category, CollectionID: collection, Changes: changes}
	request := catalog.CollectionOwnerRequest{Version: 1, LibraryID: cliLibrary, ID: string(policy.ID), ExpectedIncarnation: authority.Incarnation, Kind: "apply_collection_policy", Value: cliCollectionRaw(t, catalog.ApplyCollectionPolicyRequest{Operation: policy})}
	b, _, err := catalog.CanonicalCollectionOwnerRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func cliCollectionSetup(t *testing.T) (string, catalog.AuthorityStatus) {
	t.Helper()
	path, authority := cliSetup(t)
	for _, command := range []string{"upgrade-preferences", "upgrade-collections"} {
		if code, _, _ := invokeCLI(path, nil, nil, command); code != 0 {
			t.Fatal("collection upgrade failed", command)
		}
	}
	changes := []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: cliReaderA, Value: catalog.SubjectValue{Kind: "person", Label: "Reader", Enabled: true}}),
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: cliCollectionService, Value: catalog.SubjectValue{Kind: "service", Label: "Service", Enabled: true}}),
	}
	for i, subject := range []catalog.SubjectID{cliOwner, cliReaderA, cliCollectionService} {
		scope := catalog.Scope{Kind: "library"}
		if subject == cliReaderA {
			scope = catalog.Scope{Kind: "asset", AssetID: cliAssetA}
		}
		changes = append(changes, catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: fmt.Sprintf("cli-collection-assets-%d", i), Value: catalog.GrantValue{SubjectID: subject, Scope: scope, Actions: []string{"assets.discover"}}}))
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: cliLibrary, ID: "cli-collection-subjects", Administration: "local_owner", OperatorSubjectID: cliOwner, Changes: changes}
	body := cliRequest(t, policy.ID, authority.Incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})
	if code, _, _ := invokeCLI(path, nil, body, "policy", "apply"); code != 0 {
		t.Fatal("collection subject setup failed")
	}
	var grants []catalog.CollectionGrantChange
	for i, subject := range []catalog.SubjectID{cliOwner, cliReaderA} {
		value := catalog.CollectionGrantValue{SubjectID: subject, Scope: catalog.CollectionGrantScope{Kind: "library_create"}, Actions: []string{"collections.create"}}
		grants = append(grants, catalog.CollectionGrantChange{Kind: "put", ID: catalog.CollectionGrantID(fmt.Sprintf("cgr_%032x", i+1)), Value: &value})
	}
	body = cliCollectionPolicyBody(t, authority, 1, "library_create", "", grants...)
	if code, _, _ := invokeCLI(path, nil, body, "collection", "policy", "apply"); code != 0 {
		t.Fatal("library-create provisioning failed")
	}
	return path, authority
}
func cliCollectionIssueBody(t *testing.T, authority catalog.AuthorityStatus, id string, subject catalog.SubjectID, actions []string) []byte {
	t.Helper()
	request := catalog.AdminRequestV2{Version: 2, LibraryID: cliLibrary, ID: id, ExpectedIncarnation: authority.Incarnation, Kind: "issue_credential", Value: cliCollectionRaw(t, catalog.IssueCredentialRequestV2{SubjectID: subject, Scope: catalog.Scope{Kind: "library"}, Actions: actions, ExpiresUnix: time.Now().Unix() + 3600})}
	b, _, err := catalog.CanonicalAdminRequestV2(request)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func cliCollectionIssue(t *testing.T, path string, authority catalog.AuthorityStatus, id string, subject catalog.SubjectID, actions []string) []byte {
	t.Helper()
	code, bearer, diagnostic := invokeCLI(path, []string{"--secret-out", "-"}, cliCollectionIssueBody(t, authority, id, subject, actions), "credential", "issue-v2")
	if code != 0 || len(bearer) != 103 || bytes.Contains(diagnostic, []byte("jmc1_")) {
		t.Fatal("v2 issuance failed or leaked secret")
	}
	return bytes.TrimSuffix(bearer, []byte{'\n'})
}
func cliCollectionCreate(t *testing.T, id int) catalog.CollectionRequest {
	t.Helper()
	value := catalog.CreateCollection{Name: "Synthetic project", CreatorGrantID: catalog.CollectionGrantID(fmt.Sprintf("cgr_%032x", id+1000)), Entries: []catalog.NewCollectionEntry{{ID: catalog.CollectionEntryID(fmt.Sprintf("cent_%032x", id)), AssetID: cliAssetA}}}
	return catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: catalog.CollectionOperationID(fmt.Sprintf("cop_%032x", id)), CollectionID: catalog.CollectionID(fmt.Sprintf("col_%032x", id)), Kind: "create", Value: cliCollectionRaw(t, value)}
}
func cliCollectionEnvelope(t *testing.T, bearer []byte, request catalog.CollectionRequest) []byte {
	t.Helper()
	b, _, err := catalog.CanonicalCollectionRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	// Independent stdin shape rather than calling the CLI envelope decoder.
	return cliCollectionRaw(t, struct {
		Version int             `json:"version"`
		Bearer  string          `json:"bearer"`
		Request json.RawMessage `json:"request"`
	}{1, string(bearer), b})
}
func cliCollectionMutate(t *testing.T, path string, bearer []byte, request catalog.CollectionRequest) catalog.CollectionReceipt {
	t.Helper()
	code, output, diagnostic := invokeCLI(path, nil, cliCollectionEnvelope(t, bearer, request), "collection", "mutate")
	if code != 0 || len(diagnostic) != 0 || len(output) == 0 || output[len(output)-1] != '\n' {
		t.Fatalf("collection mutation failed: code=%d", code)
	}
	receipt, err := catalog.DecodeCollectionReceipt(bytes.TrimSuffix(output, []byte{'\n'}))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OperationID != request.ID || receipt.CollectionID != request.CollectionID {
		t.Fatal("receipt identity mismatch")
	}
	return receipt
}

func TestCLICollectionsExplicitTargetsAndRefusals(t *testing.T) {
	t.Run("new-private-schema5", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "collections.sqlite")
		if code, _, _ := invokeCLI(path, nil, nil, "init-collections"); code != 0 {
			t.Fatal("init failed")
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("catalog not private")
		}
		label := "owner"
		body := cliRequest(t, "bootstrap-collections", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: cliOwner, CreateLabel: &label})
		if code, _, _ := invokeCLI(path, nil, body, "owner", "bootstrap"); code != 0 {
			t.Fatal("schema5 bootstrap failed")
		}
		for _, command := range []string{"init-collections", "init-authority", "init-preferences", "upgrade-authority", "upgrade-preferences"} {
			if code, _, _ := invokeCLI(path, nil, nil, command); code != 3 {
				t.Fatal("old or exclusive target adopted schema5", command)
			}
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 0 {
			t.Fatal("repeat upgrade barrier failed")
		}
	})
	t.Run("explicit-four-to-five", func(t *testing.T) {
		path, _ := cliSetup(t)
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 3 {
			t.Fatal("schema3 implicitly upgraded")
		}
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 0 {
			t.Fatal("schema4 upgrade failed")
		}
		for i := 0; i < 2; i++ {
			if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 0 {
				t.Fatal("schema5 upgrade failed")
			}
		}
	})
	t.Run("missing-and-wrong-library", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "absent.sqlite")
		if code, _, _ := invokeCLI(path, nil, nil, "upgrade-collections"); code != 3 {
			t.Fatal("missing target accepted")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("upgrade created target")
		}
		path, _ = cliCollectionSetup(t)
		var out, diagnostic bytes.Buffer
		args := []string{"--catalog", path, "--library", "lib_11111111111111111111111111111111", "upgrade-collections"}
		if code := run(context.Background(), args, bytes.NewReader(nil), &out, &diagnostic); code != 3 {
			t.Fatal("wrong library accepted")
		}
	})
}

func TestCLICollectionsV2IssuanceRetryStatusAndRevoke(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	actions := []string{"activity.record", "assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read", "locations.read", "relationships.read", "revisions.read"}
	body := cliCollectionIssueBody(t, authority, "cli-v2-file", cliOwner, actions)
	output := filepath.Join(t.TempDir(), "secret")
	code, stdout, diagnostic := invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue-v2")
	var first catalog.IssuanceResultV2
	if code != 0 || len(stdout) != 0 || json.Unmarshal(diagnostic, &first) != nil || first.Receipt.Version != 2 || len(first.Bearer) != 0 || bytes.Contains(diagnostic, []byte("jmc1_")) {
		t.Fatal("v2 first issuance failed or disclosed bearer")
	}
	before, err := os.ReadFile(output)
	if err != nil || len(before) != 103 {
		t.Fatal("secret delivery failed")
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("secret permissions")
	}
	for _, revoked := range []bool{false, true} {
		if revoked {
			revoke := cliRequest(t, "cli-v2-revoke", authority.Incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: first.Receipt.Result.Selector, ExpectedGeneration: 1})
			if code, _, _ := invokeCLI(path, nil, revoke, "credential", "revoke"); code != 0 {
				t.Fatal("v2 credential revoke failed")
			}
		}
		code, stdout, diagnostic = invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue-v2")
		var retry catalog.IssuanceResultV2
		if code != 0 || len(stdout) != 0 || json.Unmarshal(diagnostic, &retry) != nil || retry.Receipt != first.Receipt || len(retry.Bearer) != 0 {
			t.Fatal("retry altered original receipt")
		}
		want := "active"
		if revoked {
			want = "revoked"
		}
		if retry.Status != want {
			t.Fatal("retry status mismatch")
		}
		code, status, _ := invokeCLI(path, []string{"--operation-id", "cli-v2-file"}, nil, "credential", "status-v2")
		var lookup catalog.IssuanceResultV2
		if code != 0 || json.Unmarshal(status, &lookup) != nil || lookup.Receipt != retry.Receipt || lookup.Status != retry.Status || len(lookup.Bearer) != 0 {
			t.Fatal("status-v2 mismatch")
		}
		after, err := os.ReadFile(output)
		afterInfo, statErr := os.Stat(output)
		if err != nil || statErr != nil || !bytes.Equal(before, after) || !os.SameFile(info, afterInfo) || !info.ModTime().Equal(afterInfo.ModTime()) {
			t.Fatal("retry rewrote exclusive output")
		}
	}
	for _, command := range [][]string{{"credential", "issue"}, {"credential", "status"}} {
		if code, _, _ := invokeCLI(path, []string{"--secret-out", "-", "--operation-id", "cli-v2-file"}, body, command...); code != 3 {
			t.Fatal("v1 API reinterpreted codec2")
		}
	}
	if code, _, _ := invokeCLI(path, nil, nil, "credential", "status-v2"); code != 3 {
		t.Fatal("v2 status omitted operation id")
	}
	request, err := catalog.DecodeAdminRequestV2(body)
	if err != nil {
		t.Fatal(err)
	}
	var value catalog.IssueCredentialRequestV2
	if err := json.Unmarshal(request.Value, &value); err != nil {
		t.Fatal(err)
	}
	value.ExpiresUnix++
	request.Value = cliCollectionRaw(t, value)
	changed, _, err := catalog.CanonicalAdminRequestV2(request)
	if err != nil {
		t.Fatal(err)
	}
	files := defaultFiles()
	reserved := false
	files.reserve = func(string) (fileResource, error) { reserved = true; return nil, errors.New("DO-NOT-ECHO-reservation") }
	var out, failure bytes.Buffer
	code = runConfigured(context.Background(), cliArgs(path, []string{"--secret-out", output}, "credential", "issue-v2"), bytes.NewReader(changed), &out, &failure, false, files)
	if code != 3 || reserved || out.Len() != 0 || strings.Contains(failure.String(), "DO-NOT-ECHO") {
		t.Fatal("changed retry reserved output or succeeded")
	}
}

func TestCLICollectionsV2SecretOutputFailures(t *testing.T) {
	for _, kind := range []string{"write", "sync", "close", "receipt"} {
		t.Run(kind, func(t *testing.T) {
			path, authority := cliCollectionSetup(t)
			body := cliCollectionIssueBody(t, authority, "v2-fault-"+kind, cliOwner, cliCollectionActions)
			output := filepath.Join(t.TempDir(), "secret")
			files := defaultFiles()
			var owned *faultOutput
			sentinel := errors.New("DO-NOT-ECHO-secret-error")
			files.reserve = func(path string) (fileResource, error) {
				f, err := reserveOutput(path)
				if err != nil {
					return nil, err
				}
				owned = &faultOutput{File: f}
				switch kind {
				case "write":
					owned.writeErr = sentinel
					owned.full = true
				case "sync":
					owned.syncErr = sentinel
				case "close":
					owned.closeErr = sentinel
				}
				return owned, nil
			}
			var out, diagnostic bytes.Buffer
			var stderr io.Writer = &diagnostic
			failedReceipt := &onceFailWriter{err: sentinel}
			if kind == "receipt" {
				stderr = failedReceipt
			}
			code := runConfigured(context.Background(), cliArgs(path, []string{"--secret-out", output}, "credential", "issue-v2"), bytes.NewReader(body), &out, stderr, false, files)
			if kind == "receipt" {
				diagnostic.Write(failedReceipt.Bytes())
			}
			if code != 3 || owned == nil || owned.closes != 1 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || strings.Contains(diagnostic.String(), "jmc1_") || !strings.Contains(diagnostic.String(), `"operation_id":"v2-fault-`) {
				t.Fatal("v2 output failure lost ownership or redaction")
			}
			if kind == "close" && !strings.Contains(diagnostic.String(), `"output_close_failed":true`) {
				t.Fatal("v2 close error hidden")
			}
			secret, err := os.ReadFile(output)
			if err != nil || (kind == "receipt" && len(secret) != 0) {
				t.Fatal("secret delivered before receipt")
			}
			if code, out, diagnostic := invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue-v2"); code != 0 || len(out) != 0 || bytes.Contains(diagnostic, []byte("jmc1_")) {
				t.Fatal("uncertain issuance duplicated secret")
			}
		})
	}
}

func TestCLICollectionsAuthenticatedCreateRenameAndHistoricalRetry(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	bearer := cliCollectionIssue(t, path, authority, "creator", cliOwner, cliCollectionActions)
	create := cliCollectionCreate(t, 10)
	created := cliCollectionMutate(t, path, bearer, create)
	rename := catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: "cop_0000000000000000000000000000000b", CollectionID: create.CollectionID, ExpectedVersion: created.ResultVersion, Kind: "rename", Value: cliCollectionRaw(t, catalog.RenameCollection{Name: "Corrected project"})}
	renamed := cliCollectionMutate(t, path, bearer, rename)
	if renamed.ResultVersion == created.ResultVersion {
		t.Fatal("correction reused version")
	}
	if got := cliCollectionMutate(t, path, bearer, create); got != created {
		t.Fatal("retry returned current version instead of original receipt")
	}
	if got := cliCollectionMutate(t, path, bearer, rename); got != renamed {
		t.Fatal("rename retry changed receipt across CLI reopen")
	}
	stale := rename
	stale.ID = "cop_0000000000000000000000000000000c"
	stale.Value = cliCollectionRaw(t, catalog.RenameCollection{Name: "Stale correction"})
	if code, out, _ := invokeCLI(path, nil, cliCollectionEnvelope(t, bearer, stale), "collection", "mutate"); code != 3 || len(out) != 0 {
		t.Fatal("stale CAS accepted")
	}
	changed := rename
	changed.Value = cliCollectionRaw(t, catalog.RenameCollection{Name: "Conflicting retry"})
	if code, out, diagnostic := invokeCLI(path, nil, cliCollectionEnvelope(t, bearer, changed), "collection", "mutate"); code != 3 || len(out) != 0 || bytes.Contains(diagnostic, bearer) {
		t.Fatal("changed retry accepted or leaked")
	}
}

func TestCLICollectionsCurrentRightsAndSubjectRefusals(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	owner := cliCollectionIssue(t, path, authority, "owner-full", cliOwner, cliCollectionActions)
	reader := cliCollectionIssue(t, path, authority, "reader-full", cliReaderA, cliCollectionActions)
	service := cliCollectionIssue(t, path, authority, "service-full", cliCollectionService, cliCollectionActions)
	readonly := cliCollectionIssue(t, path, authority, "owner-read", cliOwner, []string{"assets.discover", "collections.read"})
	createdRequest := cliCollectionCreate(t, 20)
	created := cliCollectionMutate(t, path, owner, createdRequest)
	rename := catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: "cop_00000000000000000000000000000015", CollectionID: createdRequest.CollectionID, ExpectedVersion: created.ResultVersion, Kind: "rename", Value: cliCollectionRaw(t, catalog.RenameCollection{Name: "Denied correction"})}
	hidden := cliCollectionCreate(t, 30)
	hidden.Value = cliCollectionRaw(t, catalog.CreateCollection{Name: "Hidden member", CreatorGrantID: "cgr_00000000000000000000000000000406", Entries: []catalog.NewCollectionEntry{{ID: "cent_0000000000000000000000000000001e", AssetID: cliAssetB}}})
	for _, tc := range []struct {
		name    string
		token   []byte
		request catalog.CollectionRequest
	}{
		{"no-collection-grant", reader, rename}, {"read-only-ceiling", readonly, rename}, {"service-create", service, cliCollectionCreate(t, 31)}, {"hidden-member", reader, hidden}, {"invalid-bearer", bytes.Repeat([]byte{'x'}, 102), rename},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, nil, cliCollectionEnvelope(t, tc.token, tc.request), "collection", "mutate")
			if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, tc.token) || bytes.Contains(diagnostic, []byte("Denied correction")) {
				t.Fatal("denied mutation disclosed or succeeded")
			}
		})
	}
	// The same person succeeds with current create and asset grants; refusals did
	// not install a duplicate collection or leave a service/store handle open.
	cliCollectionMutate(t, path, reader, cliCollectionCreate(t, 32))
}

func TestCLICollectionsMalformedMutationEnvelopeRedactsInputs(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	bearer := cliCollectionIssue(t, path, authority, "malformed-owner", cliOwner, cliCollectionActions)
	good := cliCollectionEnvelope(t, bearer, cliCollectionCreate(t, 40))
	cases := map[string][]byte{
		"unknown":            bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"DO-NOT-ECHO":true`), 1),
		"duplicate":          bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"actor":              bytes.Replace(good, []byte(`"kind":"create"`), []byte(`"actor_subject_id":"sub_11111111111111111111111111111111","kind":"create"`), 1),
		"wrong-library":      bytes.Replace(good, []byte(cliLibrary), []byte("lib_11111111111111111111111111111111"), 1),
		"missing":            bytes.Replace(good, []byte(`"version":1,`), nil, 1),
		"null-request":       cliCollectionRaw(t, collectionMutationEnvelope{Version: 1, Bearer: string(bearer), Request: json.RawMessage("null")}),
		"alternate-encoding": bytes.Replace(good, []byte(`"version"`), []byte(`"\u0076ersion"`), 1),
		"oversize":           bytes.Repeat([]byte{'x'}, catalog.MaxCollectionRequestBytes+513),
		"trailing":           append(append([]byte(nil), good...), []byte(`{}`)...),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, nil, body, "collection", "mutate")
			if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, bearer) || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) || bytes.Contains(diagnostic, []byte(`"operation_id"`)) {
				t.Fatal("invalid envelope accepted or echoed")
			}
		})
	}
	var out, diagnostic bytes.Buffer
	reader := cliReadFunc(func(p []byte) (int, error) { return copy(p, good), errors.New("DO-NOT-ECHO-reader") })
	if code := run(context.Background(), cliArgs(path, nil, "collection", "mutate"), reader, &out, &diagnostic); code != 3 || out.Len() != 0 || bytes.Contains(diagnostic.Bytes(), bearer) || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
		t.Fatal("bytes-plus-error accepted or leaked")
	}
	cliCollectionMutate(t, path, bearer, cliCollectionCreate(t, 40))
}

func TestCLICollectionsAdministrativeInputRefusals(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	issue := cliCollectionIssueBody(t, authority, "invalid-admin", cliOwner, cliCollectionActions)
	value := catalog.CollectionGrantValue{SubjectID: cliOwner, Scope: catalog.CollectionGrantScope{Kind: "library_create"}, Actions: []string{"collections.create"}}
	policy := cliCollectionPolicyBody(t, authority, 100, "library_create", "", catalog.CollectionGrantChange{Kind: "put", ID: "cgr_00000000000000000000000000000064", Value: &value})
	for _, tc := range []struct {
		name    string
		body    []byte
		command []string
		flags   []string
	}{
		{"issue", issue, []string{"credential", "issue-v2"}, []string{"--secret-out", "-"}},
		{"policy", policy, []string{"collection", "policy", "apply"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, body := range map[string][]byte{
				"unknown":       bytes.Replace(tc.body, []byte(`"kind":`), []byte(`"DO-NOT-ECHO":0,"kind":`), 1),
				"duplicate":     bytes.Replace(tc.body, []byte(`"library_id":`), []byte(`"version":1,"library_id":`), 1),
				"wrong-library": bytes.ReplaceAll(tc.body, []byte(cliLibrary), []byte("lib_11111111111111111111111111111111")),
				"oversized":     bytes.Repeat([]byte{'x'}, catalog.MaxRequestBytes+1),
			} {
				t.Run(name, func(t *testing.T) {
					if code, out, diagnostic := invokeCLI(path, tc.flags, body, tc.command...); code != 3 || len(out) != 0 || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) || bytes.Contains(diagnostic, []byte(`"operation_id"`)) {
						t.Fatal("bad admin input accepted or echoed")
					}
				})
			}
			var out, diagnostic bytes.Buffer
			reader := cliReadFunc(func(p []byte) (int, error) { return copy(p, tc.body), errors.New("DO-NOT-ECHO-reader") })
			if code := run(context.Background(), cliArgs(path, tc.flags, tc.command...), reader, &out, &diagnostic); code != 3 || out.Len() != 0 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
				t.Fatal("admin bytes-plus-error accepted")
			}
		})
	}
	if code, _, _ := invokeCLI(path, nil, issue, "credential", "issue-v2"); code != 3 {
		t.Fatal("secret-out omission accepted")
	}
}

type cliCollectionCaptureFault struct {
	bytes.Buffer
	cancel context.CancelFunc
	short  bool
}

func (w *cliCollectionCaptureFault) Write(p []byte) (int, error) {
	w.Buffer.Write(p)
	if w.cancel != nil {
		w.cancel()
		return len(p), nil
	}
	if w.short {
		return len(p) - 1, nil
	}
	return len(p), errors.New("DO-NOT-ECHO-output")
}

func TestCLICollectionsMutationUncertaintyAndCleanup(t *testing.T) {
	for _, kind := range []string{"full-write-error", "short-write", "cancel-after-receipt"} {
		t.Run(kind, func(t *testing.T) {
			path, authority := cliCollectionSetup(t)
			bearer := cliCollectionIssue(t, path, authority, "uncertain-owner", cliOwner, cliCollectionActions)
			request := cliCollectionCreate(t, 50)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			output := &cliCollectionCaptureFault{short: kind == "short-write"}
			if kind == "cancel-after-receipt" {
				output.cancel = cancel
			}
			var diagnostic bytes.Buffer
			code := run(ctx, cliArgs(path, nil, "collection", "mutate"), bytes.NewReader(cliCollectionEnvelope(t, bearer, request)), output, &diagnostic)
			if code != 3 || bytes.Contains(diagnostic.Bytes(), bearer) || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || strings.Contains(diagnostic.String(), "revoke/reissue") {
				t.Fatal("mutation failure misreported or echoed")
			}
			if kind != "cancel-after-receipt" && !strings.Contains(diagnostic.String(), "retry same exact request or refresh current state") {
				t.Fatal("missing collection uncertainty guidance")
			}
			captured, err := catalog.DecodeCollectionReceipt(output.Bytes())
			if err != nil {
				t.Fatal("failure did not exercise committed receipt output")
			}
			if retry := cliCollectionMutate(t, path, bearer, request); retry != captured {
				t.Fatal("uncertain operation duplicated or changed receipt")
			}
		})
	}
	t.Run("cancel-before-admission", func(t *testing.T) {
		path, authority := cliCollectionSetup(t)
		bearer := cliCollectionIssue(t, path, authority, "cancel-owner", cliOwner, cliCollectionActions)
		request := cliCollectionCreate(t, 51)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out, diagnostic bytes.Buffer
		if code := run(ctx, cliArgs(path, nil, "collection", "mutate"), bytes.NewReader(cliCollectionEnvelope(t, bearer, request)), &out, &diagnostic); code != 3 || out.Len() != 0 {
			t.Fatal("cancelled operation admitted")
		}
		cliCollectionMutate(t, path, bearer, request)
	})
}

func TestCLICollectionsOwnerPolicyUncertainRetry(t *testing.T) {
	path, authority := cliCollectionSetup(t)
	value := catalog.CollectionGrantValue{SubjectID: cliOwner, Scope: catalog.CollectionGrantScope{Kind: "library_create"}, Actions: []string{"collections.create"}}
	body := cliCollectionPolicyBody(t, authority, 200, "library_create", "", catalog.CollectionGrantChange{Kind: "put", ID: "cgr_000000000000000000000000000000c8", Value: &value})
	output := &cliCollectionCaptureFault{}
	var diagnostic bytes.Buffer
	code := run(context.Background(), cliArgs(path, nil, "collection", "policy", "apply"), bytes.NewReader(body), output, &diagnostic)
	if code != 3 || !strings.Contains(diagnostic.String(), "retry same exact request or refresh current state") || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
		t.Fatal("owner policy uncertainty misreported")
	}
	code, retry, diagnosticBytes := invokeCLI(path, nil, body, "collection", "policy", "apply")
	if code != 0 || !bytes.Equal(retry, output.Bytes()) || len(diagnosticBytes) != 0 {
		t.Fatal("owner policy retry changed receipt")
	}
}

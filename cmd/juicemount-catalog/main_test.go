package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const cliLibrary continuity.LibraryID = "lib_000102030405060708090a0b0c0d0e0f"
const cliAssetA continuity.AssetID = "asset_101112131415161718191a1b1c1d1e1f"
const cliAssetB continuity.AssetID = "asset_202122232425262728292a2b2c2d2e2f"
const cliOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const cliReaderA catalog.SubjectID = "sub_22222222222222222222222222222222"
const cliReaderB catalog.SubjectID = "sub_33333333333333333333333333333333"

func cliRequest(t *testing.T, id, inc, kind string, value any) []byte {
	t.Helper()
	v, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := catalog.CanonicalAdminRequest(catalog.AdminRequest{Version: 1, LibraryID: cliLibrary, ID: id, ExpectedIncarnation: inc, Kind: kind, Value: v})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func cliArgs(path string, flags []string, command ...string) []string {
	args := []string{"--catalog", path, "--library", string(cliLibrary)}
	args = append(args, flags...)
	return append(args, command...)
}
func invokeCLI(path string, flags []string, body []byte, command ...string) (int, []byte, []byte) {
	var out, diagnostic bytes.Buffer
	code := run(context.Background(), cliArgs(path, flags, command...), bytes.NewReader(body), &out, &diagnostic)
	return code, out.Bytes(), diagnostic.Bytes()
}
func cliSetup(t *testing.T) (string, catalog.AuthorityStatus) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	code, _, _ := invokeCLI(path, nil, nil, "init-authority")
	if code != 0 {
		t.Fatal("CLI initialization failed")
	}
	label := "owner"
	body := cliRequest(t, "bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: cliOwner, CreateLabel: &label})
	code, out, _ := invokeCLI(path, nil, body, "owner", "bootstrap")
	if code != 0 {
		t.Fatal("CLI bootstrap failed")
	}
	var receipt catalog.AdminReceipt
	if err := json.Unmarshal(out, &receipt); err != nil {
		t.Fatal(err)
	}
	// Metadata comes from the trusted catalog API; all authority mutations and
	// token checks below use the actual CLI request/response path.
	s, err := catalog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Apply(context.Background(), catalog.Operation{Version: 1, LibraryID: cliLibrary, ID: "seed-assets", Actor: "test-fixture", Changes: []catalog.Change{{CreateAsset: &catalog.CreateAsset{AssetID: cliAssetA, Label: "A"}}, {CreateAsset: &catalog.CreateAsset{AssetID: cliAssetB, Label: "B"}}}})
	err = errors.Join(err, s.Close())
	if err != nil {
		t.Fatal(err)
	}
	return path, catalog.AuthorityStatus{Incarnation: receipt.Result.Incarnation, OwnerSubjectID: cliOwner}
}
func cliIssueBody(t *testing.T, id string, a catalog.AuthorityStatus, subject catalog.SubjectID) []byte {
	return cliRequest(t, id, a.Incarnation, "issue_credential", catalog.IssueCredentialRequest{SubjectID: subject, Scope: catalog.Scope{Kind: "library", AssetID: ""}, Actions: []string{"assets.discover"}, ExpiresUnix: time.Now().Unix() + 3600})
}
func cliIssue(t *testing.T, path string, body []byte) ([]byte, catalog.IssuanceResult) {
	t.Helper()
	code, bearer, status := invokeCLI(path, []string{"--secret-out", "-"}, body, "credential", "issue")
	if code != 0 || len(bearer) != 103 {
		t.Fatalf("CLI issuance code=%d bearer bytes=%d", code, len(bearer))
	}
	var result catalog.IssuanceResult
	if err := json.Unmarshal(status, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Bearer) != 0 {
		t.Fatal("receipt contained bearer")
	}
	return bearer, result
}

func TestCLITwoIssuedTokensEnforceDifferentCurrentGrants(t *testing.T) {
	path, a := cliSetup(t)
	changes := []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: cliReaderA, Value: catalog.SubjectValue{Kind: "person", Label: "A", Enabled: true}}),
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: cliReaderB, Value: catalog.SubjectValue{Kind: "person", Label: "B", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "grant-a", Value: catalog.GrantValue{SubjectID: cliReaderA, Scope: catalog.Scope{Kind: "asset", AssetID: cliAssetA}, Actions: []string{"assets.discover"}}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "grant-b", Value: catalog.GrantValue{SubjectID: cliReaderB, Scope: catalog.Scope{Kind: "asset", AssetID: cliAssetB}, Actions: []string{"assets.discover"}}}),
	}
	p := catalog.PolicyOperation{Version: 1, LibraryID: cliLibrary, ID: "readers", Administration: "local_owner", OperatorSubjectID: cliOwner, Changes: changes}
	body := cliRequest(t, p.ID, a.Incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: p})
	if code, _, _ := invokeCLI(path, nil, body, "policy", "apply"); code != 0 {
		t.Fatal("CLI policy failed")
	}
	first, r := cliIssue(t, path, cliIssueBody(t, "issue-a", a, cliReaderA))
	second, _ := cliIssue(t, path, cliIssueBody(t, "issue-b", a, cliReaderB))
	for _, tc := range []struct {
		name   string
		bearer []byte
		asset  continuity.AssetID
		want   int
	}{{"a-to-a", first, cliAssetA, 0}, {"a-to-b", first, cliAssetB, 1}, {"b-to-a", second, cliAssetA, 1}, {"b-to-b", second, cliAssetB, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, []string{"--asset", string(tc.asset), "--action", "assets.discover"}, tc.bearer, "auth", "check")
			want := "denied\n"
			if tc.want == 0 {
				want = "allowed\n"
			}
			if code != tc.want || string(out) != want || len(diagnostic) != 0 {
				t.Fatalf("access code=%d output=%q diagnostics bytes=%d", code, out, len(diagnostic))
			}
		})
	}
	revoke := cliRequest(t, "revoke-a", a.Incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: r.Receipt.Result.Selector, ExpectedGeneration: 1})
	if code, _, _ := invokeCLI(path, nil, revoke, "credential", "revoke"); code != 0 {
		t.Fatal("CLI revoke failed")
	}
	code, out, _ := invokeCLI(path, []string{"--asset", string(cliAssetA), "--action", "assets.discover"}, first, "auth", "check")
	if code != 2 || string(out) != "denied\n" {
		t.Fatalf("revoked token code=%d output=%q", code, out)
	}
}

func TestCLIIssuanceExactRetryLeavesOriginalPathUntouched(t *testing.T) {
	path, a := cliSetup(t)
	body := cliIssueBody(t, "file-issue", a, cliOwner)
	output := filepath.Join(t.TempDir(), "secret")
	code, out, status := invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue")
	if code != 0 || len(out) != 0 {
		t.Fatal("file issuance failed")
	}
	var first catalog.IssuanceResult
	if err := json.Unmarshal(status, &first); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(output)
	if err != nil || len(before) != 103 {
		t.Fatal("secret not written")
	}
	if err := os.Chmod(output, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(output, 0600)
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, revoked := range []bool{false, true} {
		if revoked {
			revoke := cliRequest(t, "revoke", a.Incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: first.Receipt.Result.Selector, ExpectedGeneration: 1})
			if code, _, _ := invokeCLI(path, nil, revoke, "credential", "revoke"); code != 0 {
				t.Fatal("revoke failed")
			}
		}
		code, out, status = invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue")
		var retry catalog.IssuanceResult
		if err := json.Unmarshal(status, &retry); err != nil {
			t.Fatal(err)
		}
		want := "active"
		if revoked {
			want = "revoked"
		}
		if code != 0 || len(out) > 0 || retry.Receipt != first.Receipt || retry.Status != want || len(retry.Bearer) > 0 {
			t.Fatalf("retry code=%d status=%s secret bytes=%d", code, retry.Status, len(retry.Bearer))
		}
		after, err := os.Stat(output)
		if err != nil || !os.SameFile(info, after) || after.Mode().Perm() != 0 || !after.ModTime().Equal(info.ModTime()) {
			t.Fatal("exact retry touched original destination")
		}
	}
	var changed catalog.AdminRequest
	if err := json.Unmarshal(body, &changed); err != nil {
		t.Fatal(err)
	}
	var value catalog.IssueCredentialRequest
	if err := json.Unmarshal(changed.Value, &value); err != nil {
		t.Fatal(err)
	}
	value.ExpiresUnix++
	changed.Value, _ = json.Marshal(value)
	changedBody, _ := json.Marshal(changed)
	if code, _, _ := invokeCLI(path, []string{"--secret-out", output}, changedBody, "credential", "issue"); code != 3 {
		t.Fatal("changed retry accepted")
	}
	if err := os.Chmod(output, 0600); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retry changed secret bytes")
	}
}

func TestCLIConcurrentMatchedIssuePreservesEmptyReservation(t *testing.T) {
	path, a := cliSetup(t)
	body := cliIssueBody(t, "racing-issue", a, cliOwner)
	request, err := catalog.DecodeAdminRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "secret")
	files := defaultFiles()
	reserved := 0
	var competing catalog.IssuanceResult
	var competitorErr error
	// Interpose only at the real output-constructor seam, after LookupIssuance
	// returned absent and before this invocation's transactional recheck.
	files.reserve = func(destination string) (fileResource, error) {
		f, e := reserveOutput(destination)
		if e != nil {
			return nil, e
		}
		reserved++
		done := make(chan struct{})
		go func() {
			defer close(done)
			s, e := catalog.Open(path)
			if e != nil {
				competitorErr = e
				return
			}
			owner, e := s.LocalOwner(cliLibrary)
			if e == nil {
				competing, e = owner.IssueCredential(context.Background(), request)
			}
			competitorErr = errors.Join(e, s.Close())
		}()
		<-done
		return f, nil
	}
	var out, diagnostic bytes.Buffer
	code := runConfigured(context.Background(), cliArgs(path, []string{"--secret-out", output}, "credential", "issue"), bytes.NewReader(body), &out, &diagnostic, false, files)
	var result catalog.IssuanceResult
	if err := json.Unmarshal(diagnostic.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(output)
	if code != 0 || competitorErr != nil || reserved != 1 || len(competing.Bearer) != 102 || result.Receipt != competing.Receipt || result.Status != "active" || len(result.Bearer) != 0 || out.Len() != 0 || err != nil || info.Size() != 0 || info.Mode().Perm() != 0600 {
		t.Fatalf("concurrent retry code=%d reservations=%d competitor=%v output=%v", code, reserved, competitorErr, err)
	}
}

type faultOutput struct {
	*os.File
	writeErr, closeErr, syncErr error
	full                        bool
	closes                      int
}

func (f *faultOutput) Write(p []byte) (int, error) {
	if f.writeErr == nil {
		return f.File.Write(p)
	}
	n := len(p) / 2
	if f.full {
		n = len(p)
	}
	written, err := f.File.Write(p[:n])
	return written, errors.Join(f.writeErr, err)
}
func (f *faultOutput) Close() error { f.closes++; return errors.Join(f.File.Close(), f.closeErr) }
func (f *faultOutput) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}

type onceFailWriter struct {
	bytes.Buffer
	calls int
	err   error
}

func (w *onceFailWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func TestCLISecretOutputFailureRetainsStatusAndIdentity(t *testing.T) {
	for _, kind := range []string{"partial-write", "full-write-error", "sync", "close", "receipt"} {
		t.Run(kind, func(t *testing.T) {
			path, a := cliSetup(t)
			body := cliIssueBody(t, "uncertain-"+kind, a, cliOwner)
			output := filepath.Join(t.TempDir(), "secret")
			files := defaultFiles()
			sentinel := errors.New("DO-NOT-ECHO-error-content")
			var file *faultOutput
			files.reserve = func(path string) (fileResource, error) {
				f, err := reserveOutput(path)
				if err != nil {
					return nil, err
				}
				file = &faultOutput{File: f}
				switch kind {
				case "partial-write":
					file.writeErr = sentinel
				case "full-write-error":
					file.writeErr = sentinel
					file.full = true
				case "sync":
					file.syncErr = sentinel
				case "close":
					file.closeErr = sentinel
				case "receipt":
					file.closeErr = sentinel
				}
				return file, nil
			}
			var out, diagnostic bytes.Buffer
			var stderr io.Writer = &diagnostic
			failedReceipt := &onceFailWriter{err: sentinel}
			if kind == "receipt" {
				stderr = failedReceipt
			}
			code := runConfigured(context.Background(), cliArgs(path, []string{"--secret-out", output}, "credential", "issue"), bytes.NewReader(body), &out, stderr, false, files)
			if kind == "receipt" {
				diagnostic.Write(failedReceipt.Bytes())
			}
			if code != 3 || file == nil || file.closes != 1 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || !strings.Contains(diagnostic.String(), `"operation_id":"uncertain-`+kind+`"`) || !strings.Contains(diagnostic.String(), `"selector":"cred_`) {
				t.Fatalf("uncertainty lost identity/redaction/close: code=%d file=%v", code, file != nil)
			}
			if (kind == "close" || kind == "receipt") && !strings.Contains(diagnostic.String(), `"output_close_failed":true`) {
				t.Fatal("output close error discarded")
			}
			code, status, _ := invokeCLI(path, []string{"--operation-id", "uncertain-" + kind}, nil, "credential", "status")
			var state catalog.IssuanceResult
			if err := json.Unmarshal(status, &state); err != nil {
				t.Fatal(err)
			}
			if code != 0 || state.Status != "active" || len(state.Bearer) > 0 {
				t.Fatal("failed delivery silently revoked or lost issuance")
			}
			code, retry, status := invokeCLI(path, []string{"--secret-out", output}, body, "credential", "issue")
			if code != 0 || len(retry) > 0 || bytes.Contains(status, []byte("jmc1_")) {
				t.Fatal("uncertain delivery retry returned a secret")
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "full-write-error" && len(data) != 103 {
				t.Fatal("full-write-plus-error oracle did not capture whole secret")
			}
			if kind == "receipt" && len(data) != 0 {
				t.Fatal("secret written after receipt output failure")
			}
		})
	}
}

func TestCLIIssueFailurePropagatesReservedCloseError(t *testing.T) {
	path, a := cliSetup(t)
	body := cliRequest(t, "expired", a.Incarnation, "issue_credential", catalog.IssueCredentialRequest{SubjectID: cliOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"assets.discover"}, ExpiresUnix: 1})
	files := defaultFiles()
	var file *faultOutput
	files.reserve = func(path string) (fileResource, error) {
		f, err := reserveOutput(path)
		if err != nil {
			return nil, err
		}
		file = &faultOutput{File: f, closeErr: errors.New("DO-NOT-ECHO-close")}
		return file, nil
	}
	var out, diagnostic bytes.Buffer
	code := runConfigured(context.Background(), cliArgs(path, []string{"--secret-out", filepath.Join(t.TempDir(), "reserved")}, "credential", "issue"), bytes.NewReader(body), &out, &diagnostic, false, files)
	if code != 3 || file == nil || file.closes != 1 || !strings.Contains(diagnostic.String(), `"output_close_failed":true`) || !strings.Contains(diagnostic.String(), `"operation_id":"expired"`) || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
		t.Fatal("issuance-error cleanup did not propagate redacted close failure")
	}
	s, err := catalog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o, err := s.LocalOwner(cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.IssuanceStatus(context.Background(), "expired"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("failed issue reserved an issuance identity: %v", err)
	}
}

type cliReadFunc func([]byte) (int, error)

func (f cliReadFunc) Read(p []byte) (int, error) { return f(p) }
func TestCLIRejectsMalformedBoundedInputsWithoutEcho(t *testing.T) {
	path, a := cliSetup(t)
	good := cliIssueBody(t, "malformed", a, cliOwner)
	for name, body := range map[string][]byte{"unknown": bytes.Replace(good, []byte(`"expires_unix":`), []byte(`"DO-NOT-ECHO":`), 1), "duplicate": bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), "trailing": append(append([]byte(nil), good...), []byte(`{}`)...), "oversized": []byte(strings.Repeat("DO-NOT-ECHO", 7000))} {
		t.Run(name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, []string{"--secret-out", "-"}, body, "credential", "issue")
			if code != 3 || len(out) > 0 || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) {
				t.Fatalf("malformed input accepted/leaked: code=%d", code)
			}
		})
	}
	code, out, diagnostic := invokeCLI(path, []string{"--DO-NOT-ECHO", "secret"}, nil, "owner", "status")
	if code != 3 || len(out) > 0 || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) {
		t.Fatal("flag diagnostic echoed input")
	}
	var output, stderr bytes.Buffer
	reader := cliReadFunc(func(p []byte) (int, error) { return copy(p, good), errors.New("DO-NOT-ECHO-reader") })
	code = run(context.Background(), cliArgs(path, []string{"--secret-out", "-"}, "credential", "issue"), reader, &output, &stderr)
	if code != 3 || output.Len() > 0 || strings.Contains(stderr.String(), "DO-NOT-ECHO") {
		t.Fatal("bytes-plus-error reader accepted/leaked")
	}
	code, out, _ = invokeCLI(path, []string{"--asset", string(cliAssetA), "--action", "assets.discover"}, []byte(strings.Repeat("x", 104)), "auth", "check")
	if code != 2 || string(out) != "denied\n" {
		t.Fatal("oversized bearer accepted")
	}
}

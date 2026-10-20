// Command juicemount-catalog administers a local catalog through OS file access.
// Collection mutations use the authenticated in-process discovery service;
// this command does not start a Manager, mount or HTTP server.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), terminationSignals()...)
	code := runOwned(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func writeBytes(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeBytes(w, append(b, '\n'))
}
func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, errors.New("input limit")
	}
	return b, nil
}

var errOutputClose = errors.New("output close failed")

func closeOutput(f io.Closer) error {
	if f == nil {
		return nil
	}
	if err := f.Close(); err != nil {
		return errors.Join(errOutputClose, err)
	}
	return nil
}
func finishOutput(f fileResource) error {
	path := f.Name()
	err := errors.Join(f.Sync(), closeOutput(f))
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Flags precede the command. Decoder and flag errors are intentionally fixed:
// neither argv mistakes nor stdin data are copied into diagnostics.
func runConfigured(parent context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, ownStdio bool, files cliFiles) (code int) {
	knownOperationID, knownSelector := "", ""
	outputCloseFailed := false
	preferenceMaintenance := false
	collectionMutationAttempted := false
	fail := func(uncertain bool) int {
		message := "catalog operation failed"
		if uncertain {
			message = "catalog outcome uncertain; inspect operation status before revoke/reissue"
			if preferenceMaintenance {
				message = "catalog preference maintenance outcome uncertain"
			}
			if collectionMutationAttempted {
				message = "catalog collection outcome uncertain; retry same exact request or refresh current state"
			}
		}
		_ = writeJSON(stderr, struct {
			Error             string `json:"error"`
			OperationID       string `json:"operation_id,omitempty"`
			Selector          string `json:"selector,omitempty"`
			OutputCloseFailed bool   `json:"output_close_failed,omitempty"`
		}{message, knownOperationID, knownSelector, outputCloseFailed})
		return 3
	}
	fs := flag.NewFlagSet("juicemount-catalog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("catalog", "", "catalog path")
	libraryText := fs.String("library", "", "exact library ID")
	secretOut := fs.String("secret-out", "", "new secret file or -")
	action := fs.String("action", "", "action")
	asset := fs.String("asset", "", "exact asset ID")
	selector := fs.String("selector", "", "credential selector")
	operationID := fs.String("operation-id", "", "issuance operation ID")
	after := fs.String("after", "", "list cursor")
	limit := fs.Int("limit", 100, "list page size")
	inputPath := fs.String("input", "-", "stream input")
	outputPath := fs.String("output", "-", "new stream output")
	maxBytes := fs.Int64("max-bytes", 0, "stream byte budget")
	maxEvents := fs.Int64("max-events", 0, "combined event budget")
	maxRows := fs.Int64("max-rows", 0, "projection row budget")
	timeout := fs.Duration("timeout", 10*time.Second, "operation timeout")
	parseErr := fs.Parse(args)
	effectiveTimeout := *timeout
	if effectiveTimeout <= 0 {
		effectiveTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, effectiveTimeout)
	defer cancel()
	owners := &fileOwners{}
	var restoreStdio func() error
	if ownStdio {
		input, inOK := stdin.(*os.File)
		output, outOK := stdout.(*os.File)
		diagnostic, errOK := stderr.(*os.File)
		if !inOK || !outOK || !errOK {
			return 3
		}
		prepared, restore, err := files.stdio([]*os.File{input, output, diagnostic})
		if err != nil {
			return 3
		}
		restoreStdio = restore
		stdin, stdout, stderr = owners.track(prepared[0]), owners.track(prepared[1]), owners.track(prepared[2])
	}
	stopOwners := owners.watch(ctx)
	defer func() {
		err := stopOwners()
		if restoreStdio != nil {
			err = errors.Join(err, restoreStdio())
		}
		if err != nil || ctx.Err() != nil {
			code = 3
		}
	}()
	stdin = contextReader{ctx, stdin}
	stdout = contextWriter{ctx, stdout}
	stderr = contextWriter{ctx, stderr}
	if parseErr != nil || *timeout <= 0 {
		return fail(false)
	}
	command := strings.Join(fs.Args(), " ")
	preferenceMaintenance = command == "init-preferences" || command == "upgrade-preferences" || command == "owner preference prune"
	if command == "subject new-id" {
		id, err := catalog.NewSubjectID()
		if err != nil {
			return fail(false)
		}
		if writeBytes(stdout, []byte(string(id)+"\n")) != nil {
			return fail(false)
		}
		return 0
	}
	library := continuity.LibraryID(*libraryText)
	if library.Validate() != nil || *path == "" {
		return fail(false)
	}
	budgets := catalog.Limits{MaxBytes: *maxBytes, MaxEvents: *maxEvents, MaxProjectionRows: *maxRows}
	if command == "restore" {
		reader := stdin
		var f *ownedFile
		if *inputPath != "-" {
			var err error
			raw, openErr := files.input(*inputPath)
			err = openErr
			if err != nil {
				return fail(false)
			}
			f = owners.track(raw)
			reader = f
		}
		err := catalog.Restore(ctx, *path, library, reader, budgets)
		if f != nil {
			err = errors.Join(err, f.Close())
		}
		if err != nil {
			return fail(strings.Contains(err.Error(), "uncertain"))
		}
		return 0
	}
	if command == "upgrade-authority" {
		if err := catalog.UpgradeAuthority(ctx, *path, library); err != nil {
			return fail(strings.Contains(err.Error(), "uncertain"))
		}
		return 0
	}
	if command == "upgrade-preferences" {
		if err := catalog.UpgradePreferences(ctx, *path, library); err != nil {
			return fail(strings.Contains(err.Error(), "uncertain"))
		}
		return 0
	}
	if command == "upgrade-collections" {
		if err := catalog.UpgradeCollections(ctx, *path, library); err != nil {
			return fail(strings.Contains(err.Error(), "uncertain"))
		}
		return 0
	}
	var s *catalog.Store
	var err error
	if command == "init-authority" {
		s, err = catalog.CreateAuthority(ctx, *path, library)
	} else if command == "init-preferences" {
		s, err = catalog.CreatePreferences(ctx, *path, library)
	} else if command == "init-collections" {
		s, err = catalog.CreateCollections(ctx, *path, library)
	} else {
		s, err = catalog.OpenContext(ctx, *path)
	}
	if err != nil {
		return fail(strings.Contains(err.Error(), "uncertain"))
	}
	defer func() {
		if err := s.Close(); err != nil {
			code = fail(true)
		}
	}()
	actual, err := s.LibraryID()
	if err != nil || actual != library {
		return fail(false)
	}
	if command == "init-authority" || command == "init-preferences" || command == "init-collections" {
		return 0
	}
	if command == "collection mutate" {
		request, bearer, err := readCollectionMutation(stdin, library)
		if err != nil {
			return fail(false)
		}
		knownOperationID = string(request.ID)
		collectionMutationAttempted, err = runCollectionMutation(ctx, s, library, request, bearer, stdout)
		if err != nil {
			return fail(collectionMutationAttempted)
		}
		return 0
	}
	if command == "export" {
		writer := stdout
		var f *ownedFile
		if *outputPath != "-" {
			raw, reserveErr := files.reserve(*outputPath)
			err = reserveErr
			if err != nil {
				return fail(false)
			}
			f = owners.track(raw)
			writer = f
		}
		err = s.Export(ctx, library, writer, budgets)
		if f != nil {
			if err != nil {
				err = errors.Join(err, closeOutput(f))
			} else {
				err = finishOutput(f)
			}
		}
		if err != nil {
			outputCloseFailed = errors.Is(err, errOutputClose)
			return fail(true)
		}
		return 0
	}
	if command == "auth check" || command == "auth whoami" {
		denied := func() int {
			if writeBytes(stdout, []byte("denied\n")) != nil {
				return fail(false)
			}
			return 2
		}
		data, err := readBounded(stdin, 103)
		if err != nil {
			return denied()
		}
		if len(data) == 103 && data[102] == '\n' {
			data = data[:102]
		}
		if len(data) != 102 {
			return denied()
		}
		handle, err := s.Authenticate(ctx, library, data)
		if err != nil {
			return denied()
		}
		if command == "auth whoami" {
			identity, err := s.WhoAmI(ctx, handle)
			if err != nil {
				return 2
			}
			if writeJSON(stdout, identity) != nil {
				return fail(false)
			}
			return 0
		}
		decision, err := s.CheckAccess(ctx, handle, catalog.AccessRequest{LibraryID: library, Action: *action, AssetID: continuity.AssetID(*asset)})
		if err != nil {
			if errors.Is(err, catalog.ErrUnauthenticated) {
				return denied()
			}
			return fail(false)
		}
		text := "denied\n"
		code := 1
		if decision.Allowed {
			text = "allowed\n"
			code = 0
		}
		if writeBytes(stdout, []byte(text)) != nil {
			return fail(false)
		}
		return code
	}
	owner, err := s.LocalOwner(library)
	if err != nil {
		return fail(false)
	}
	switch command {
	case "credential status-v2":
		if *operationID == "" {
			return fail(false)
		}
		v, err := owner.IssuanceStatusV2(ctx, *operationID)
		if err != nil || writeJSON(stdout, v) != nil {
			return fail(false)
		}
		return 0
	case "credential issue-v2":
		body, err := readBounded(stdin, 64<<10)
		if err != nil {
			return fail(false)
		}
		request, err := catalog.DecodeAdminRequestV2(bytes.TrimSpace(body))
		if err != nil || request.LibraryID != library || request.Kind != "issue_credential" || *secretOut == "" {
			return fail(false)
		}
		knownOperationID = request.ID
		return runCollectionIssuance(ctx, owner, request, *secretOut, stdout, stderr, owners, files, &knownSelector, &outputCloseFailed, fail)
	case "collection policy apply":
		body, err := readBounded(stdin, catalog.MaxRequestBytes)
		if err != nil {
			return fail(false)
		}
		request, err := catalog.DecodeCollectionOwnerRequest(bytes.TrimSpace(body))
		if err != nil || request.LibraryID != library || request.Kind != "apply_collection_policy" {
			return fail(false)
		}
		knownOperationID = request.ID
		collectionMutationAttempted = true
		receipt, err := owner.ApplyCollectionPolicy(ctx, request)
		if err != nil || writeJSON(stdout, receipt) != nil {
			return fail(true)
		}
		return 0
	case "owner preference prune":
		if err := owner.PrunePreferences(ctx); err != nil {
			return fail(strings.Contains(err.Error(), "uncertain"))
		}
		if writeJSON(stdout, struct {
			Version int    `json:"version"`
			Status  string `json:"status"`
		}{1, "complete"}) != nil {
			return fail(true)
		}
		return 0
	case "owner status":
		v, err := owner.Status(ctx)
		if err != nil {
			return fail(false)
		}
		if writeJSON(stdout, v) != nil {
			return fail(false)
		}
		return 0
	case "subject list":
		v, err := owner.ListSubjects(ctx, catalog.SubjectID(*after), *limit)
		if err != nil {
			return fail(false)
		}
		if writeJSON(stdout, v) != nil {
			return fail(false)
		}
		return 0
	case "grant list":
		v, err := owner.ListGrants(ctx, *after, *limit)
		if err != nil {
			return fail(false)
		}
		if writeJSON(stdout, v) != nil {
			return fail(false)
		}
		return 0
	case "credential status":
		if *operationID != "" {
			v, err := owner.IssuanceStatus(ctx, *operationID)
			if err != nil {
				return fail(false)
			}
			if writeJSON(stdout, v) != nil {
				return fail(false)
			}
		} else {
			v, err := owner.CredentialStatus(ctx, *selector)
			if err != nil {
				return fail(false)
			}
			if writeJSON(stdout, struct {
				Status string `json:"status"`
			}{v}) != nil {
				return fail(false)
			}
		}
		return 0
	}
	wanted := map[string]string{"owner bootstrap": "bootstrap", "owner recover": "recover", "policy apply": "apply_policy", "credential issue": "issue_credential", "credential revoke": "revoke_credential"}
	kind, ok := wanted[command]
	if !ok {
		return fail(false)
	}
	data, err := readBounded(stdin, 64<<10)
	if err != nil {
		return fail(false)
	}
	request, err := catalog.DecodeAdminRequest(bytes.TrimSpace(data))
	if err != nil || request.Kind != kind || request.LibraryID != library {
		return fail(false)
	}
	knownOperationID = request.ID
	if kind == "issue_credential" {
		if *secretOut == "" {
			return fail(false)
		}
		prior, found, err := owner.LookupIssuance(ctx, request)
		if err != nil {
			return fail(false)
		}
		if found {
			knownSelector = prior.Receipt.Result.Selector
			if writeJSON(stderr, prior) != nil {
				return fail(true)
			}
			return 0
		}
		writer := stdout
		var f *ownedFile
		if *secretOut != "-" {
			raw, reserveErr := files.reserve(*secretOut)
			err = reserveErr
			if err != nil {
				return fail(false)
			}
			f = owners.track(raw)
			writer = f
		}
		result, err := owner.IssueCredential(ctx, request)
		if err != nil {
			if f != nil {
				err = errors.Join(err, closeOutput(f))
			}
			outputCloseFailed = errors.Is(err, errOutputClose)
			return fail(strings.Contains(err.Error(), "uncertain") || outputCloseFailed)
		}
		knownSelector = result.Receipt.Result.Selector
		if err = writeJSON(stderr, result); err != nil {
			if f != nil {
				err = errors.Join(err, closeOutput(f))
			}
			outputCloseFailed = errors.Is(err, errOutputClose)
			return fail(true)
		}
		if len(result.Bearer) > 0 {
			err = writeBytes(writer, append(result.Bearer, '\n'))
		}
		if f != nil {
			if err != nil {
				err = errors.Join(err, closeOutput(f))
			} else {
				err = finishOutput(f)
			}
		}
		if err != nil {
			outputCloseFailed = errors.Is(err, errOutputClose)
			return fail(true)
		}
		return 0
	}
	var receipt catalog.AdminReceipt
	switch kind {
	case "bootstrap":
		receipt, err = owner.Bootstrap(ctx, request)
	case "recover":
		receipt, err = owner.Recover(ctx, request)
	case "apply_policy":
		receipt, err = owner.ApplyPolicy(ctx, request)
	case "revoke_credential":
		receipt, err = owner.RevokeCredential(ctx, request)
	}
	if err != nil {
		return fail(strings.Contains(err.Error(), "uncertain"))
	}
	if writeJSON(stdout, receipt) != nil {
		return fail(true)
	}
	return 0
}

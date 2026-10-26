package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/nativeclient"
	"github.com/lelanddutcher/juicemount/internal/version"
)

type nativeOptions struct {
	command           string
	root              string
	endpoint          string
	library           continuity.LibraryID
	allowLoopbackHTTP bool
	copyID            string
	basename          string
	closeJournal      func(*nativeclient.Journal) error
}

var errNativeOwnerClose = errors.New("native original owner close failed")

// Only fixed, public codes cross the CLI diagnostic boundary. In particular,
// raw transport, database, path and JSON errors are never printed.
func nativeFailureCode(err error) (string, int) {
	if errors.Is(err, errNativeOwnerClose) || errors.Is(err, nativeclient.ErrNativeExposureUncertain) {
		return "uncertain", 0
	}
	var remote *nativeclient.RemoteError
	if errors.As(err, &remote) {
		return remote.Code, remote.StatusCode
	}
	switch {
	case errors.Is(err, nativeclient.ErrJournalLimit):
		return "admission_limit", 0
	case errors.Is(err, nativeclient.ErrJournalConflict), errors.Is(err, nativeclient.ErrJournalUnverifiedTail):
		return "conflict", 0
	case errors.Is(err, nativeclient.ErrJournalCorrupt):
		return "integrity", 0
	case errors.Is(err, catalog.ErrInvalid), errors.Is(err, nativeclient.ErrInvalidRecord), errors.Is(err, nativeclient.ErrInvalidClient), errors.Is(err, nativeclient.ErrInvalidBasename), errors.Is(err, nativeclient.ErrInvalidEndpoint):
		return "invalid", 0
	case errors.Is(err, nativeclient.ErrInvalidResponse):
		return "invalid_response", 0
	case errors.Is(err, nativeclient.ErrClientDraining):
		return "draining", 0
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline", 0
	case errors.Is(err, context.Canceled):
		return "canceled", 0
	case errors.Is(err, nativeclient.ErrTransport):
		return "transport", 0
	default:
		return "unavailable", 0
	}
}

// Only these finite commands can cross the pre-catalog native dispatch.
// Check Visit as well as values: an explicit --catalog= is still forbidden.
func validateNativeFlags(fs *flag.FlagSet, command string) error {
	allowed := map[string]bool{
		"native-root": true, "endpoint": true, "library": true,
		"allow-loopback-http": true, "timeout": true,
	}
	switch command {
	case "native version":
		allowed = map[string]bool{"timeout": true}
	case "native profile create", "native search", "native page", "native inspect", "native lookup", "native cancel", "native reconcile":
	case "native select":
		allowed["copy-id"], allowed["basename"] = true, true
	case "native receive", "native recover", "native copy-status":
		allowed["copy-id"] = true
	default:
		return catalog.ErrInvalid
	}
	var invalid bool
	var explicitTimeout bool
	fs.Visit(func(f *flag.Flag) {
		if !allowed[f.Name] {
			invalid = true
		}
		if f.Name == "timeout" {
			explicitTimeout = true
		}
	})
	if invalid {
		return catalog.ErrInvalid
	}
	if command == "native receive" || command == "native recover" || command == "native copy-status" {
		if !explicitTimeout {
			return catalog.ErrInvalid
		}
	}
	timeout := fs.Lookup("timeout")
	duration, err := time.ParseDuration(timeout.Value.String())
	if err != nil || duration <= 0 || duration > 900*time.Second {
		return catalog.ErrInvalid
	}
	return nil
}

func nativeEnvelope(input io.Reader, max int64) (json.RawMessage, []byte, error) {
	body, err := readBounded(input, max+512)
	if err != nil {
		return nil, nil, catalog.ErrInvalid
	}
	body = bytes.TrimSpace(body)
	var value collectionMutationEnvelope
	if json.Unmarshal(body, &value) != nil || value.Version != 1 || len(value.Bearer) != 102 || len(value.Request) == 0 {
		return nil, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(body, canonical) {
		return nil, nil, catalog.ErrInvalid
	}
	return value.Request, []byte(value.Bearer), nil
}

func nativeReceipt(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > 1<<20 {
		return nil, catalog.ErrInvalid
	}
	return append(encoded, '\n'), nil
}

type nativeSelectResult struct {
	Version   int                       `json:"version"`
	CopyID    string                    `json:"copy_id"`
	Selection catalog.DeliverySelection `json:"selection"`
}
type nativeProfileResult struct {
	Version int  `json:"version"`
	Created bool `json:"created"`
}
type nativeLocalResult struct {
	CopyID               string `json:"copy_id"`
	Basename             string `json:"basename"`
	Size                 int64  `json:"size"`
	SHA256               string `json:"sha256"`
	Path                 string `json:"path"`
	PreviouslyDownloaded bool   `json:"previously_downloaded"`
}

func localReceipt(s nativeclient.LocalCopyStatus) nativeLocalResult {
	return nativeLocalResult{s.CopyID, s.Basename, s.Size, s.SHA256, s.Path, s.PreviouslyDownloaded}
}

type nativeReconcileRequest struct {
	Version     int                  `json:"version"`
	LibraryID   continuity.LibraryID `json:"library_id"`
	SelectionID string               `json:"selection_id"`
	OperationID string               `json:"operation_id"`
}

// runNativeCLI owns one strict local profile and, for remote commands, one
// pinned HTTP Client. Results remain undisclosed until both originals close.
func runNativeCLI(ctx context.Context, opts nativeOptions, input io.Reader) (pending []byte, resultErr error) {
	if ctx == nil {
		return nil, catalog.ErrInvalid
	}
	// Keep build identity reachable so the bundled executable carries real
	// internal/version linker stamps. This command owns stdio as usual, but
	// never opens a catalog, profile, credential, or network connection.
	if opts.command == "native version" {
		return nativeReceipt(struct {
			Version        int    `json:"version"`
			ProductVersion string `json:"product_version"`
			Commit         string `json:"commit"`
		}{1, version.Version, version.Commit})
	}
	if opts.library.Validate() != nil || !filepath.IsAbs(opts.root) || opts.root == "" {
		return nil, catalog.ErrInvalid
	}
	parsed, err := nativeclient.ParseEndpoint(opts.endpoint, opts.allowLoopbackHTTP)
	if err != nil || parsed.URL != opts.endpoint {
		return nil, catalog.ErrInvalid
	}
	closeJournal := opts.closeJournal
	if closeJournal == nil {
		closeJournal = func(j *nativeclient.Journal) error { return j.Close() }
	}
	if opts.command == "native profile create" {
		if opts.copyID != "" || opts.basename != "" {
			return nil, catalog.ErrInvalid
		}
		journal, err := nativeclient.CreateJournal(opts.root, opts.endpoint, opts.library, opts.allowLoopbackHTTP)
		if err != nil {
			return nil, err
		}
		defer func() {
			if closeErr := closeJournal(journal); closeErr != nil {
				resultErr = errors.Join(resultErr, errNativeOwnerClose, closeErr)
				pending = nil
			}
		}()
		return nativeReceipt(nativeProfileResult{Version: 1, Created: true})
	}
	journal, err := nativeclient.OpenJournal(opts.root, opts.endpoint, opts.library, opts.allowLoopbackHTTP)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeJournal(journal); closeErr != nil {
			resultErr = errors.Join(resultErr, errNativeOwnerClose, closeErr)
			pending = nil
		}
	}()
	if opts.command == "native recover" || opts.command == "native copy-status" {
		if opts.copyID == "" || opts.basename != "" {
			return nil, catalog.ErrInvalid
		}
		var status nativeclient.LocalCopyStatus
		if opts.command == "native recover" {
			permit, err := journal.RecoverPrepared(ctx, opts.copyID)
			if err != nil {
				return nil, err
			}
			status, err = permit.ExposeNoReplace(ctx)
			if closeErr := permit.Close(); closeErr != nil {
				err = errors.Join(err, errNativeOwnerClose, closeErr)
			}
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			status, err = journal.CopyStatus(ctx, opts.copyID)
			if err != nil {
				return nil, err
			}
		}
		return nativeReceipt(localReceipt(status))
	}
	if opts.command == "native select" {
		if opts.copyID == "" || nativeclient.ValidateBasename(opts.basename) != nil {
			return nil, catalog.ErrInvalid
		}
	} else if opts.command == "native receive" {
		if opts.copyID == "" || opts.basename != "" {
			return nil, catalog.ErrInvalid
		}
	} else if opts.copyID != "" || opts.basename != "" {
		return nil, catalog.ErrInvalid
	}
	var raw json.RawMessage
	var bearer []byte
	if opts.command == "native receive" {
		bearer, err = readDeliveryBearer(input)
	} else {
		raw, bearer, err = nativeEnvelope(input, 32<<10)
	}
	if err != nil {
		return nil, err
	}
	defer clear(bearer)
	client, err := nativeclient.NewClient(opts.endpoint, opts.library, bearer, opts.allowLoopbackHTTP)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := client.Close(context.Background()); closeErr != nil {
			resultErr = errors.Join(resultErr, errNativeOwnerClose, closeErr)
			pending = nil
		}
	}()
	switch opts.command {
	case "native search", "native page":
		kind := "search"
		if opts.command == "native page" {
			kind = "page"
		}
		request, err := catalog.DecodeNativeDiscoveryRequest(kind, raw, opts.library)
		if err != nil {
			return nil, err
		}
		page, err := client.Discover(ctx, request)
		if err != nil {
			return nil, err
		}
		return nativeReceipt(page)
	case "native select":
		request, err := catalog.DecodeDeliverySelectRequest(raw)
		if err != nil || request.LibraryID != opts.library {
			return nil, catalog.ErrInvalid
		}
		intent := nativeclient.Intent{Version: 1, Endpoint: opts.endpoint, LibraryID: opts.library, OperationID: request.ID, CopyID: opts.copyID, SelectRequest: request, Basename: opts.basename, MaxBytes: request.MaxBytes, MetadataReservationBytes: nativeclient.MetadataReservationBytes}
		if err := journal.CreateIntent(intent); err != nil {
			return nil, err
		}
		selection, err := client.Select(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := journal.PinSelection(opts.copyID, selection); err != nil {
			return nil, err
		}
		return nativeReceipt(nativeSelectResult{Version: 1, CopyID: opts.copyID, Selection: selection})
	case "native receive":
		prior, err := journal.Recover(opts.copyID)
		if err == nil {
			if err := prior.Close(); err != nil {
				return nil, errors.Join(errNativeOwnerClose, err)
			}
		} else if errors.Is(err, nativeclient.ErrJournalConflict) {
			if _, err := journal.InitialPartial(opts.copyID); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
		permit, err := journal.Receive(ctx, client, opts.copyID)
		if err != nil {
			return nil, err
		}
		status, exposeErr := permit.ExposeNoReplace(ctx)
		closeErr := permit.Close()
		if closeErr != nil {
			closeErr = errors.Join(errNativeOwnerClose, closeErr)
		}
		if err := errors.Join(exposeErr, closeErr); err != nil {
			return nil, err
		}
		return nativeReceipt(localReceipt(status))
	case "native inspect":
		request, err := catalog.DecodeDeliveryInspectRequest(raw)
		if err != nil || request.LibraryID != opts.library {
			return nil, catalog.ErrInvalid
		}
		value, err := client.Inspect(ctx, request)
		if err != nil {
			return nil, err
		}
		return nativeReceipt(value)
	case "native lookup":
		request, err := catalog.DecodeDeliveryLookupRequest(raw)
		if err != nil || request.LibraryID != opts.library {
			return nil, catalog.ErrInvalid
		}
		value, err := client.Lookup(ctx, request)
		if err != nil {
			return nil, err
		}
		return nativeReceipt(value)
	case "native cancel":
		request, err := catalog.DecodeDeliveryCancelRequest(raw)
		if err != nil || request.LibraryID != opts.library {
			return nil, catalog.ErrInvalid
		}
		value, err := client.Cancel(ctx, request)
		if err != nil {
			return nil, err
		}
		return nativeReceipt(value)
	case "native reconcile":
		var request nativeReconcileRequest
		if json.Unmarshal(raw, &request) != nil || request.Version != 1 || request.LibraryID != opts.library {
			return nil, catalog.ErrInvalid
		}
		canonical, err := json.Marshal(request)
		if err != nil || !bytes.Equal(canonical, raw) {
			return nil, catalog.ErrInvalid
		}
		value, err := client.Reconcile(ctx, request.SelectionID, request.OperationID)
		if err != nil {
			return nil, err
		}
		return nativeReceipt(value)
	default:
		return nil, catalog.ErrInvalid
	}
}

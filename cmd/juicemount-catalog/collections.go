package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// The bearer exists only in the bounded stdin envelope and the service call.
// Marshal comparison also rejects duplicate, unknown and alternate-encoded
// envelope fields; the nested request has its own independent canonical check.
type collectionMutationEnvelope struct {
	Version int             `json:"version"`
	Bearer  string          `json:"bearer"`
	Request json.RawMessage `json:"request"`
}

func readCollectionMutation(input io.Reader, library continuity.LibraryID) (catalog.CollectionRequest, []byte, error) {
	body, err := readBounded(input, catalog.MaxCollectionRequestBytes+512)
	if err != nil {
		return catalog.CollectionRequest{}, nil, err
	}
	body = bytes.TrimSpace(body)
	var envelope collectionMutationEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.Version != 1 || len(envelope.Bearer) != 102 {
		return catalog.CollectionRequest{}, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(body, canonical) {
		return catalog.CollectionRequest{}, nil, catalog.ErrInvalid
	}
	request, err := catalog.DecodeCollectionRequest(envelope.Request)
	if err != nil || request.LibraryID != library {
		return catalog.CollectionRequest{}, nil, catalog.ErrInvalid
	}
	return request, []byte(envelope.Bearer), nil
}

func runCollectionMutation(ctx context.Context, store *catalog.Store, library continuity.LibraryID, request catalog.CollectionRequest, bearer []byte, output io.Writer) (attempted bool, resultErr error) {
	service, err := catalog.NewDiscoveryService(ctx, store, library)
	if err != nil {
		return false, err
	}
	defer func() {
		// Request cancellation must not prevent draining this owned service.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, service.Close(cleanup))
	}()
	_, err = service.MutateCollection(ctx, bearer, request, output)
	if err == nil {
		err = writeBytes(output, []byte{'\n'})
	}
	return true, err
}

// Keep the v2 lookup and issuance APIs distinct from v1. In particular, lookup
// validates the full exact request before any exclusive output reservation.
func runCollectionIssuance(ctx context.Context, owner *catalog.LocalOwnerService, request catalog.AdminRequestV2, secretOut string, stdout, stderr io.Writer, owners *fileOwners, files cliFiles, knownSelector *string, outputCloseFailed *bool, fail func(bool) int) int {
	prior, found, err := owner.LookupIssuanceV2(ctx, request)
	if err != nil {
		return fail(false)
	}
	if found {
		*knownSelector = prior.Receipt.Result.Selector
		if writeJSON(stderr, prior) != nil {
			return fail(true)
		}
		return 0
	}
	writer := stdout
	var file *ownedFile
	if secretOut != "-" {
		raw, err := files.reserve(secretOut)
		if err != nil {
			return fail(false)
		}
		file = owners.track(raw)
		writer = file
	}
	result, err := owner.IssueCredentialV2(ctx, request)
	if err != nil {
		if file != nil {
			err = errors.Join(err, closeOutput(file))
		}
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(strings.Contains(err.Error(), "uncertain") || *outputCloseFailed)
	}
	*knownSelector = result.Receipt.Result.Selector
	if err = writeJSON(stderr, result); err != nil {
		if file != nil {
			err = errors.Join(err, closeOutput(file))
		}
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(true)
	}
	if len(result.Bearer) != 0 {
		err = writeBytes(writer, append(result.Bearer, '\n'))
	}
	if file != nil {
		if err == nil {
			err = finishOutput(file)
		} else {
			err = errors.Join(err, closeOutput(file))
		}
	}
	if err != nil {
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(true)
	}
	return 0
}

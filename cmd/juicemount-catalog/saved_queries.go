package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// This is the saved-query-only v2 envelope. The outer comparison and the
// catalog decoder independently enforce closed canonical bytes.
type savedQueryMutationEnvelope struct {
	Version int             `json:"version"`
	Bearer  string          `json:"bearer"`
	Request json.RawMessage `json:"request"`
}

func readSavedQueryMutation(input io.Reader, library continuity.LibraryID) (catalog.SavedQueryRequest, []byte, error) {
	body, err := readBounded(input, catalog.MaxSavedQueryRequestBytes+512)
	if err != nil {
		return catalog.SavedQueryRequest{}, nil, err
	}
	defer clear(body)
	body = bytes.Trim(body, " \t\r\n")
	var envelope savedQueryMutationEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.Version != 2 || !collectionReadBearer(envelope.Bearer) {
		return catalog.SavedQueryRequest{}, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(envelope)
	defer clear(canonical)
	if err != nil || !bytes.Equal(body, canonical) {
		return catalog.SavedQueryRequest{}, nil, catalog.ErrInvalid
	}
	request, err := catalog.DecodeSavedQueryRequest(envelope.Request)
	if err != nil || request.LibraryID != library {
		return catalog.SavedQueryRequest{}, nil, catalog.ErrInvalid
	}
	return request, []byte(envelope.Bearer), nil
}

func runSavedQueryMutation(ctx context.Context, store *catalog.Store, library continuity.LibraryID, request catalog.SavedQueryRequest, bearer []byte, output io.Writer) (attempted bool, resultErr error) {
	service, err := catalog.NewDiscoveryService(ctx, store, library)
	if err != nil {
		return false, err
	}
	defer func() {
		// Retain and drain the original service even when its request is canceled.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, service.Close(cleanup))
	}()
	_, err = service.MutateSavedQuery(ctx, bearer, request, output)
	if err == nil {
		err = writeBytes(output, []byte{'\n'})
	}
	return true, err
}

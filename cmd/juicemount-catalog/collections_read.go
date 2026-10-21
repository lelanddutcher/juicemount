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

const collectionReadPageLimit = 1 << 20
const collectionReadStreamLimit = 320 << 20
const collectionReadPageCount = 8192

// Flags precede the command, for example:
// juicemount-catalog --catalog PATH --library LIB --timeout 30s collection read
// The canonical bearer envelope is read from stdin (or the owned --input file).
func readCollectionQuery(input io.Reader, library continuity.LibraryID) (catalog.CollectionQueryRequest, []byte, error) {
	body, err := readBounded(input, 32<<10)
	if err != nil {
		return catalog.CollectionQueryRequest{}, nil, err
	}
	body = bytes.Trim(body, " \t\r\n")
	var envelope catalog.CollectionCLIReadEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.Version != 1 || !collectionReadBearer(envelope.Bearer) {
		return catalog.CollectionQueryRequest{}, nil, catalog.ErrInvalid
	}
	encoded, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(body, encoded) {
		return catalog.CollectionQueryRequest{}, nil, catalog.ErrInvalid
	}
	request, err := catalog.DecodeCollectionCLIReadRequest(envelope.Request, library)
	if err != nil || request.Kind == "page" {
		return catalog.CollectionQueryRequest{}, nil, catalog.ErrInvalid
	}
	return request, []byte(envelope.Bearer), nil
}

func collectionReadBearer(value string) bool {
	if len(value) != 102 || !strings.HasPrefix(value, "jmc1_") || value[37] != '.' {
		return false
	}
	for i := 5; i < len(value); i++ {
		if i != 37 && !(value[i] >= '0' && value[i] <= '9' || value[i] >= 'a' && value[i] <= 'f') {
			return false
		}
	}
	return true
}

type collectionReadOutput struct {
	writer    io.Writer
	remaining int64
	attempted bool
}

func (o *collectionReadOutput) write(body []byte, reserveLF bool) (int, error) {
	reserve := int64(0)
	if reserveLF {
		reserve = 1
	}
	if int64(len(body)) > o.remaining-reserve {
		return 0, catalog.ErrInvalid
	}
	o.attempted = true
	n, err := o.writer.Write(body)
	if n < 0 || n > len(body) {
		return 0, io.ErrShortWrite
	}
	o.remaining -= int64(n)
	if err == nil && n != len(body) {
		err = io.ErrShortWrite
	}
	return n, err
}

// Forward the service's fenced Write immediately. This capture is only for
// parsing continuation after successful disclosure, never delayed publication.
type collectionReadTee struct {
	output *collectionReadOutput
	page   bytes.Buffer
	err    error
}

func (w *collectionReadTee) Write(body []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if len(body) > collectionReadPageLimit-w.page.Len() {
		w.err = catalog.ErrInvalid
		return 0, w.err
	}
	n, err := w.output.write(body, true)
	if n > 0 {
		_, _ = w.page.Write(body[:n])
	}
	w.err = err
	return n, err
}

type collectionReadIdentity struct {
	kind          string
	collection    catalog.CollectionSummary
	hasCollection bool
	state         string
	canCreate     bool
	count         int
	countScope    string
	complete      bool
	sort          string
	snapshot      string
}

func collectionReadDecode(body []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil {
		return catalog.ErrInvalid
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return catalog.ErrInvalid
	}
	return nil
}

func collectionReadPage(body []byte, initial catalog.CollectionQueryRequest) (collectionReadIdentity, int, *string, error) {
	var identity collectionReadIdentity
	var token *string
	version, items := 0, 0
	switch initial.Kind {
	case "open":
		var page catalog.CollectionOpenResponse
		if collectionReadDecode(body, &page) != nil || page.Version != 1 || page.Kind != "collection" || page.Collection.CollectionID != initial.CollectionID {
			return identity, 0, nil, catalog.ErrInvalid
		}
		return collectionReadIdentity{kind: "collection", collection: page.Collection, hasCollection: true}, 0, nil, nil
	case "directory":
		var page catalog.CollectionDirectoryResponse
		if collectionReadDecode(body, &page) != nil || page.Collection != nil || page.Items == nil {
			return identity, 0, nil, catalog.ErrInvalid
		}
		identity = collectionReadIdentity{kind: page.Kind, canCreate: page.CanCreate, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, items, token = page.Version, len(page.Items), page.NextPageToken
	case "members":
		var page catalog.CollectionMembersResponse
		if collectionReadDecode(body, &page) != nil || page.Items == nil || page.State != initial.State {
			return identity, 0, nil, catalog.ErrInvalid
		}
		identity = collectionReadIdentity{kind: page.Kind, collection: page.Collection, hasCollection: true, state: page.State, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, items, token = page.Version, len(page.Items), page.NextPageToken
	case "access", "history":
		var page catalog.CollectionListResponse[json.RawMessage]
		if collectionReadDecode(body, &page) != nil || page.Items == nil {
			return identity, 0, nil, catalog.ErrInvalid
		}
		identity = collectionReadIdentity{kind: page.Kind, collection: page.Collection, hasCollection: true, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, items, token = page.Version, len(page.Items), page.NextPageToken
	default:
		return identity, 0, nil, catalog.ErrInvalid
	}
	countScope, snapshot := "frozen_window", "frozen_snapshot"
	if initial.Kind == "members" {
		countScope, snapshot = "frozen_visible_membership", "frozen_membership_current_page"
	}
	if version != 1 || identity.kind != initial.Kind || identity.count < 0 || identity.count > 8192 ||
		items > initial.PageSize || identity.countScope != countScope || identity.snapshot != snapshot || identity.sort != "collections1" ||
		identity.hasCollection && identity.collection.CollectionID != initial.CollectionID || initial.Kind != "history" && !identity.complete {
		return identity, 0, nil, catalog.ErrInvalid
	}
	if token != nil {
		request := catalog.CollectionQueryRequest{Version: 1, Kind: "page", LibraryID: initial.LibraryID, Token: *token}
		if _, err := catalog.ValidateCollectionQueryRequest(request, initial.LibraryID); err != nil {
			return identity, 0, nil, catalog.ErrInvalid
		}
	}
	return identity, items, token, nil
}

type collectionReadQuery func(context.Context, []byte, catalog.CollectionQueryRequest, io.Writer) (catalog.DisclosureResult, error)

// This helper receives the production method directly; tests also use it to
// discriminate forwarding, traversal-budget and broken-continuation guards.
func streamCollectionRead(ctx context.Context, query collectionReadQuery, initial catalog.CollectionQueryRequest, bearer []byte, output *collectionReadOutput) error {
	request := initial
	seen := make(map[string]struct{})
	var first collectionReadIdentity
	totalItems := 0
	for pageNumber := 0; pageNumber < collectionReadPageCount; pageNumber++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		tee := &collectionReadTee{output: output}
		_, queryErr := query(ctx, bearer, request, tee)
		if err := errors.Join(queryErr, tee.err); err != nil {
			return err
		}
		identity, items, token, err := collectionReadPage(tee.page.Bytes(), initial)
		if err != nil {
			return err
		}
		if pageNumber == 0 {
			first = identity
		} else if identity != first {
			return catalog.ErrInvalid
		}
		totalItems += items
		if totalItems > identity.count || token != nil && (items == 0 || totalItems >= identity.count) || token == nil && totalItems != identity.count {
			return catalog.ErrInvalid
		}
		if _, err := output.write([]byte{'\n'}, false); err != nil {
			return err
		}
		if token == nil {
			return nil
		}
		if _, repeated := seen[*token]; repeated {
			return catalog.ErrInvalid
		}
		seen[*token] = struct{}{}
		request = catalog.CollectionQueryRequest{Version: 1, Kind: "page", LibraryID: initial.LibraryID, Token: *token}
	}
	return catalog.ErrInvalid
}

func runCollectionRead(ctx context.Context, store *catalog.Store, library continuity.LibraryID, request catalog.CollectionQueryRequest, bearer []byte, writer io.Writer) (attempted bool, resultErr error) {
	service, err := catalog.NewDiscoveryService(ctx, store, library)
	if err != nil {
		return false, err
	}
	output := &collectionReadOutput{writer: writer, remaining: collectionReadStreamLimit}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, service.Close(cleanup))
		attempted = output.attempted
	}()
	resultErr = streamCollectionRead(ctx, service.QueryCollections, request, bearer, output)
	return output.attempted, resultErr
}

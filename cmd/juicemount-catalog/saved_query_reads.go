package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const savedReadStreamLimit = 32 << 20

// Read envelopes and mutations deliberately have separate size ceilings.
func readSavedReadEnvelope(input io.Reader) (json.RawMessage, []byte, error) {
	body, err := readBounded(input, 32768)
	if err != nil {
		return nil, nil, err
	}
	defer clear(body)
	body = bytes.Trim(body, " \t\r\n")
	var envelope savedQueryMutationEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.Version != 2 || !collectionReadBearer(envelope.Bearer) {
		return nil, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(envelope)
	defer clear(canonical)
	if err != nil || !bytes.Equal(body, canonical) {
		return nil, nil, catalog.ErrInvalid
	}
	return envelope.Request, []byte(envelope.Bearer), nil
}

func readSavedQueryRead(input io.Reader, library continuity.LibraryID) (catalog.SavedQueryReadRequest, []byte, error) {
	body, bearer, err := readSavedReadEnvelope(input)
	if err != nil {
		return catalog.SavedQueryReadRequest{}, nil, err
	}
	request, err := catalog.DecodeSavedQueryCLIReadRequest(body, library)
	if err != nil || request.Kind == "page" {
		clear(bearer)
		return catalog.SavedQueryReadRequest{}, nil, catalog.ErrInvalid
	}
	return request, bearer, nil
}

func readRecentsRead(input io.Reader, library continuity.LibraryID) (catalog.RecentsReadRequest, []byte, error) {
	body, bearer, err := readSavedReadEnvelope(input)
	if err != nil {
		return catalog.RecentsReadRequest{}, nil, err
	}
	request, err := catalog.DecodeRecentsCLIReadRequest(body, library)
	if err != nil || request.Kind != "list" {
		clear(bearer)
		return catalog.RecentsReadRequest{}, nil, catalog.ErrInvalid
	}
	return request, bearer, nil
}

// Closed canonical responses reject omissions, duplicates, alternate spellings
// and invalid UTF-8 as well as unknown fields. Publication already happened in
// the service's fenced Write; this copy is only for continuation validation.
func savedReadDecode(body []byte, target any) error {
	if !utf8.Valid(body) || collectionReadDecode(body, target) != nil {
		return catalog.ErrInvalid
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(body, canonical) {
		return catalog.ErrInvalid
	}
	return nil
}

func savedReadText(value string, limit int, nonempty bool) bool {
	return utf8.ValidString(value) && len(value) <= limit && (!nonempty || value != "") && !strings.ContainsRune(value, 0)
}

func savedReadSummary(summary catalog.SavedQuerySummary) bool {
	return summary.CollectionID.Validate() == nil && summary.Kind == "saved_query" &&
		savedReadText(summary.Name, 256, true) && summary.DefinitionVersion.Validate() == nil &&
		(summary.Creator == "you" || summary.Creator == "another_authorized_person") && summary.Permissions.Read
}

func savedReadAsset(id continuity.AssetID, label string, head *continuity.RevisionID, reasons []string) bool {
	if id.Validate() != nil || !savedReadText(label, 4096, false) || head != nil && head.Validate() != nil || reasons == nil {
		return false
	}
	// The ordered discovery2 vocabulary has one optional label reason.
	last := -1
	labelReason := false
	for _, reason := range reasons {
		order := -1
		switch reason {
		case "context":
			order = 0
		case "declared_selection":
			order = 1
		case "label_exact", "label_prefix", "label_contains", "stable_id":
			if labelReason {
				return false
			}
			labelReason, order = true, 2
		default:
			return false
		}
		if order <= last {
			return false
		}
		last = order
	}
	return true
}

type savedReadIdentity struct {
	kind         string
	collection   catalog.SavedQuerySummary
	queryVersion catalog.CollectionVersion
	canCreate    bool
	count        int
	countScope   string
	complete     bool
	sort         string
	snapshot     string
}

type savedReadPageInfo struct {
	identity savedReadIdentity
	items    int
	token    *string
	rowIDs   []string
}

func savedReadHistory(item catalog.CollectionHistoryItem) bool {
	n, err := strconv.ParseInt(item.RecordedUnixNano, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != item.RecordedUnixNano || item.Actor != "you" && item.Actor != "another_authorized_person" {
		return false
	}
	var definition *catalog.SavedQueryDefinition
	switch item.Kind {
	case "create":
		var value catalog.SavedQueryHistoryCreate
		if savedReadDecode(item.Value, &value) != nil || !savedReadText(value.Name, 256, true) {
			return false
		}
		definition = &value.Definition
	case "replace_definition":
		var value catalog.ReplaceSavedQueryDefinition
		if savedReadDecode(item.Value, &value) != nil {
			return false
		}
		definition = &value.Definition
	case "rename":
		var value catalog.RenameCollection
		return savedReadDecode(item.Value, &value) == nil && savedReadText(value.Name, 256, true)
	case "archive":
		var value catalog.SetCollectionArchived
		return savedReadDecode(item.Value, &value) == nil
	default:
		return false
	}
	body, err := json.Marshal(definition)
	if err != nil {
		return false
	}
	_, err = catalog.DecodeSavedQueryDefinition(body)
	return err == nil
}

func savedQueryReadPage(body []byte, initial catalog.SavedQueryReadRequest) (savedReadPageInfo, error) {
	var info savedReadPageInfo
	bad := func() (savedReadPageInfo, error) { return savedReadPageInfo{}, catalog.ErrInvalid }
	version := 0
	switch initial.Kind {
	case "open":
		var page catalog.SavedQueryOpenResponse
		if savedReadDecode(body, &page) != nil || page.Version != 2 || page.Kind != "collection" || page.Collection.CollectionID != initial.CollectionID || !savedReadSummary(page.Collection) {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, collection: page.Collection}
		return info, nil
	case "definition":
		var page catalog.SavedQueryDefinitionResponse
		if savedReadDecode(body, &page) != nil || page.Version != 2 || page.Kind != "definition" || page.Collection.CollectionID != initial.CollectionID || !savedReadSummary(page.Collection) {
			return bad()
		}
		definition, err := json.Marshal(page.Definition)
		if err != nil {
			return bad()
		}
		if _, err := catalog.DecodeSavedQueryDefinition(definition); err != nil {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, collection: page.Collection}
		return info, nil
	case "directory":
		var page catalog.SavedQueryDirectoryResponse
		if savedReadDecode(body, &page) != nil || page.Collection != nil || page.Items == nil {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, canCreate: page.CanCreate, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, info.items, info.token = page.Version, len(page.Items), page.NextPageToken
		for _, row := range page.Items {
			if !savedReadSummary(row) || initial.State == "active" && row.Archived || initial.State == "archived" && !row.Archived {
				return bad()
			}
			info.rowIDs = append(info.rowIDs, string(row.CollectionID))
		}
	case "execute":
		var page catalog.SavedQueryExecuteResponse
		if savedReadDecode(body, &page) != nil || page.Items == nil || page.QueryVersion != page.Collection.DefinitionVersion || page.Collection.Archived {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, collection: page.Collection, queryVersion: page.QueryVersion, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, info.items, info.token = page.Version, len(page.Items), page.NextPageToken
		for _, row := range page.Items {
			if row.ResultID.CollectionID != initial.CollectionID || row.ResultID.AssetID != row.AssetID || !savedReadAsset(row.AssetID, row.Label, row.HeadRevisionID, row.Reasons) {
				return bad()
			}
			info.rowIDs = append(info.rowIDs, string(row.ResultID.CollectionID)+":"+string(row.ResultID.AssetID))
		}
	case "history":
		var page catalog.SavedQueryListResponse[catalog.CollectionHistoryItem]
		if savedReadDecode(body, &page) != nil || page.Items == nil {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, collection: page.Collection, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, info.items, info.token = page.Version, len(page.Items), page.NextPageToken
		for _, row := range page.Items {
			if !savedReadHistory(row) {
				return bad()
			}
		}
		// History has no public row identity. Identical events remain distinct.
	case "access":
		var page catalog.SavedQueryListResponse[catalog.CollectionAccessItem]
		if savedReadDecode(body, &page) != nil || page.Items == nil {
			return bad()
		}
		info.identity = savedReadIdentity{kind: page.Kind, collection: page.Collection, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}
		version, info.items, info.token = page.Version, len(page.Items), page.NextPageToken
		for _, row := range page.Items {
			if row.GrantID.Validate() != nil || row.Expected.Version.Validate() != nil || row.Expected.Value.SubjectID.Validate() != nil || row.Expected.Value.Scope.Kind != "collection" || row.Expected.Value.Scope.CollectionID != initial.CollectionID {
				return bad()
			}
			actions := row.Expected.Value.Actions
			if len(actions) == 0 || len(actions) > 3 {
				return bad()
			}
			for i, action := range actions {
				if action != "collections.admin" && action != "collections.edit" && action != "collections.read" || i > 0 && actions[i-1] >= action {
					return bad()
				}
			}
			info.rowIDs = append(info.rowIDs, string(row.GrantID))
		}
	default:
		return bad()
	}
	maximum, sort := 1000, "saved_queries2"
	if initial.Kind == "directory" {
		maximum = 1024
	}
	if initial.Kind == "access" {
		maximum = 128
	}
	if initial.Kind == "execute" {
		sort = "discovery2"
	}
	id := info.identity
	if version != 2 || id.kind != initial.Kind || id.count < 0 || id.count > maximum || info.items > initial.PageSize || id.countScope != "frozen_window" || id.snapshot != "frozen_snapshot" || id.sort != sort ||
		initial.Kind != "directory" && (!savedReadSummary(id.collection) || id.collection.CollectionID != initial.CollectionID) ||
		(initial.Kind == "directory" || initial.Kind == "access") && !id.complete || initial.Kind == "history" && id.count == 0 && id.complete {
		return bad()
	}
	if info.token != nil {
		_, err := catalog.ValidateSavedQueryReadRequest(catalog.SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: initial.LibraryID, QueryKind: initial.Kind, CollectionID: initial.CollectionID, Token: *info.token}, initial.LibraryID)
		if err != nil {
			return bad()
		}
	}
	return info, nil
}

func recentsReadPage(body []byte, initial catalog.RecentsReadRequest) (savedReadPageInfo, error) {
	var page catalog.RecentsResponse
	if savedReadDecode(body, &page) != nil || page.Version != 2 || page.Kind != "recents" || page.Items == nil || page.MatchedCount < 0 || page.MatchedCount > 256 || len(page.Items) > initial.PageSize || page.CountScope != "frozen_authorized_recent" || !page.WindowComplete || page.SortVersion != "recents1" || page.SnapshotScope != "frozen_snapshot" {
		return savedReadPageInfo{}, catalog.ErrInvalid
	}
	info := savedReadPageInfo{identity: savedReadIdentity{kind: page.Kind, count: page.MatchedCount, countScope: page.CountScope, complete: page.WindowComplete, sort: page.SortVersion, snapshot: page.SnapshotScope}, items: len(page.Items), token: page.NextPageToken}
	for _, row := range page.Items {
		selected, err := strconv.ParseInt(row.SelectedUnix, 10, 64)
		expires, expiryErr := strconv.ParseInt(row.ExpiresUnix, 10, 64)
		if !savedReadAsset(row.AssetID, row.Label, row.HeadRevisionID, row.Reasons) || len(row.Reasons) != 1 || row.Reasons[0] != "declared_selection" || err != nil || expiryErr != nil || selected < 0 || selected > (1<<63-1)-2592000 || expires != selected+2592000 || strconv.FormatInt(selected, 10) != row.SelectedUnix || strconv.FormatInt(expires, 10) != row.ExpiresUnix {
			return savedReadPageInfo{}, catalog.ErrInvalid
		}
		info.rowIDs = append(info.rowIDs, string(row.AssetID))
	}
	if info.token != nil {
		if _, err := catalog.ValidateRecentsReadRequest(catalog.RecentsReadRequest{Version: 2, Kind: "page", LibraryID: initial.LibraryID, Token: *info.token}, initial.LibraryID); err != nil {
			return savedReadPageInfo{}, catalog.ErrInvalid
		}
	}
	return info, nil
}

type savedReadCall func(context.Context, string, io.Writer) (catalog.DisclosureResult, error)

func streamSavedRead(ctx context.Context, call savedReadCall, decode func([]byte) (savedReadPageInfo, error), output *collectionReadOutput) error {
	seenTokens, seenRows := make(map[string]bool), make(map[string]bool)
	var first savedReadIdentity
	token, total := "", 0
	for pageNumber := 0; pageNumber < 8192; pageNumber++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		tee := &collectionReadTee{output: output}
		_, err := call(ctx, token, tee)
		if err = errors.Join(err, tee.err); err != nil {
			return err
		}
		page, err := decode(tee.page.Bytes())
		if err != nil {
			return err
		}
		if pageNumber == 0 {
			first = page.identity
		} else if page.identity != first {
			return catalog.ErrInvalid
		}
		for _, id := range page.rowIDs {
			if seenRows[id] {
				return catalog.ErrInvalid
			}
			seenRows[id] = true
		}
		total += page.items
		if total > first.count || page.token != nil && (page.items == 0 || total >= first.count || seenTokens[*page.token]) || page.token == nil && total != first.count {
			return catalog.ErrInvalid
		}
		if _, err := output.write([]byte{'\n'}, false); err != nil {
			return err
		}
		if page.token == nil {
			return nil
		}
		token = *page.token
		seenTokens[token] = true
	}
	return catalog.ErrInvalid
}

func runSavedRead(ctx context.Context, store *catalog.Store, library continuity.LibraryID, writer io.Writer, run func(*catalog.DiscoveryService, *collectionReadOutput) error) (attempted bool, resultErr error) {
	service, err := catalog.NewDiscoveryService(ctx, store, library)
	if err != nil {
		return false, err
	}
	output := &collectionReadOutput{writer: writer, remaining: savedReadStreamLimit}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, service.Close(cleanup))
		attempted = output.attempted
	}()
	resultErr = run(service, output)
	return output.attempted, resultErr
}

func runSavedQueryRead(ctx context.Context, store *catalog.Store, library continuity.LibraryID, initial catalog.SavedQueryReadRequest, bearer []byte, writer io.Writer) (bool, error) {
	return runSavedRead(ctx, store, library, writer, func(service *catalog.DiscoveryService, output *collectionReadOutput) error {
		return streamSavedRead(ctx, func(ctx context.Context, token string, out io.Writer) (catalog.DisclosureResult, error) {
			request := initial
			if token != "" {
				request = catalog.SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: library, QueryKind: initial.Kind, CollectionID: initial.CollectionID, Token: token}
			}
			return service.QuerySavedQueries(ctx, bearer, request, out)
		}, func(body []byte) (savedReadPageInfo, error) { return savedQueryReadPage(body, initial) }, output)
	})
}

func runRecentsRead(ctx context.Context, store *catalog.Store, library continuity.LibraryID, initial catalog.RecentsReadRequest, bearer []byte, writer io.Writer) (bool, error) {
	return runSavedRead(ctx, store, library, writer, func(service *catalog.DiscoveryService, output *collectionReadOutput) error {
		return streamSavedRead(ctx, func(ctx context.Context, token string, out io.Writer) (catalog.DisclosureResult, error) {
			request := initial
			if token != "" {
				request = catalog.RecentsReadRequest{Version: 2, Kind: "page", LibraryID: library, Token: token}
			}
			return service.QueryRecents(ctx, bearer, request, out)
		}, func(body []byte) (savedReadPageInfo, error) { return recentsReadPage(body, initial) }, output)
	})
}

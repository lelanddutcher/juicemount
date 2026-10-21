package manager

import (
	"context"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func decodeContinuitySavedQueryRequest(kind string, body []byte) (catalog.SavedQueryReadRequest, error) {
	r := catalog.SavedQueryReadRequest{Kind: kind}
	fields := map[string]any{"version": &r.Version, "library_id": &r.LibraryID}
	switch kind {
	case "directory":
		fields["state"], fields["page_size"] = &r.State, &r.PageSize
	case "open", "definition":
		fields["collection_id"] = &r.CollectionID
	case "execute", "history", "access":
		fields["collection_id"], fields["page_size"] = &r.CollectionID, &r.PageSize
	case "page":
		fields["query_kind"], fields["collection_id"], fields["token"] = &r.QueryKind, &r.CollectionID, &r.Token
	default:
		return catalog.SavedQueryReadRequest{}, catalog.ErrInvalid
	}
	if decodeContinuityFields(body, fields, "") != nil {
		return catalog.SavedQueryReadRequest{}, catalog.ErrInvalid
	}
	return catalog.ValidateSavedQueryReadRequest(r, r.LibraryID)
}

func decodeContinuityRecentsRequest(kind string, body []byte) (catalog.RecentsReadRequest, error) {
	r := catalog.RecentsReadRequest{Kind: kind}
	fields := map[string]any{"version": &r.Version, "library_id": &r.LibraryID}
	switch kind {
	case "list":
		fields["page_size"] = &r.PageSize
	case "page":
		fields["token"] = &r.Token
	default:
		return catalog.RecentsReadRequest{}, catalog.ErrInvalid
	}
	if decodeContinuityFields(body, fields, "") != nil {
		return catalog.RecentsReadRequest{}, catalog.ErrInvalid
	}
	return catalog.ValidateRecentsReadRequest(r, r.LibraryID)
}

func (c *Continuity) dispatchSavedQueryHTTP(ctx context.Context, bearer []byte, route string, body []byte, output io.Writer) (catalog.DisclosureResult, error) {
	if route == "mutate" {
		request, err := catalog.DecodeSavedQueryRequest(body)
		if err != nil {
			return catalog.DisclosureResult{}, catalog.ErrInvalid
		}
		return c.service.MutateSavedQuery(ctx, bearer, request, output)
	}
	request, err := decodeContinuitySavedQueryRequest(route, body)
	if err != nil {
		return catalog.DisclosureResult{}, catalog.ErrInvalid
	}
	return c.service.QuerySavedQueries(ctx, bearer, request, output)
}

func (c *Continuity) dispatchRecentsHTTP(ctx context.Context, bearer []byte, route string, body []byte, output io.Writer) (catalog.DisclosureResult, error) {
	request, err := decodeContinuityRecentsRequest(route, body)
	if err != nil {
		return catalog.DisclosureResult{}, catalog.ErrInvalid
	}
	return c.service.QueryRecents(ctx, bearer, request, output)
}

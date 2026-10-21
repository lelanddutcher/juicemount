package manager

import (
	"context"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func decodeContinuityCollectionRequest(kind string, body []byte) (catalog.CollectionQueryRequest, error) {
	request := catalog.CollectionQueryRequest{Kind: kind}
	fields := map[string]any{"version": &request.Version, "library_id": &request.LibraryID}
	switch kind {
	case "directory":
		fields["state"], fields["page_size"] = &request.State, &request.PageSize
	case "open":
		fields["collection_id"] = &request.CollectionID
	case "members":
		fields["collection_id"], fields["state"], fields["page_size"] = &request.CollectionID, &request.State, &request.PageSize
	case "history", "access":
		fields["collection_id"], fields["page_size"] = &request.CollectionID, &request.PageSize
	case "page":
		fields["token"] = &request.Token
	default:
		return catalog.CollectionQueryRequest{}, catalog.ErrInvalid
	}
	if decodeContinuityFields(body, fields, "") != nil {
		return catalog.CollectionQueryRequest{}, catalog.ErrInvalid
	}
	return catalog.ValidateCollectionQueryRequest(request, request.LibraryID)
}

func (c *Continuity) dispatchCollectionHTTP(ctx context.Context, bearer []byte, route string, body []byte, output io.Writer) (catalog.DisclosureResult, error) {
	if route == "mutate" {
		request, err := catalog.DecodeCollectionRequest(body)
		if err != nil {
			return catalog.DisclosureResult{}, catalog.ErrInvalid
		}
		return c.service.MutateCollection(ctx, bearer, request, output)
	}
	request, err := decodeContinuityCollectionRequest(route, body)
	if err != nil {
		return catalog.DisclosureResult{}, catalog.ErrInvalid
	}
	return c.service.QueryCollections(ctx, bearer, request, output)
}

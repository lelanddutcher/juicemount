package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
)

func readDeliveryBearer(input io.Reader) ([]byte, error) {
	b, err := readBounded(input, 103)
	if err != nil {
		return nil, err
	}
	if len(b) == 103 && b[102] == '\n' {
		b = b[:102]
	}
	if len(b) != 102 || !strings.HasPrefix(string(b), "jmc1_") {
		return nil, catalog.ErrInvalid
	}
	return b, nil
}
func readDeliverySelection(input io.Reader, library continuity.LibraryID) (catalog.DeliverySelectRequest, []byte, error) {
	b, err := readBounded(input, 32768+512)
	if err != nil {
		return catalog.DeliverySelectRequest{}, nil, err
	}
	b = bytes.TrimSpace(b)
	var envelope collectionMutationEnvelope
	if json.Unmarshal(b, &envelope) != nil || envelope.Version != 1 || len(envelope.Bearer) != 102 {
		return catalog.DeliverySelectRequest{}, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(b, canonical) {
		return catalog.DeliverySelectRequest{}, nil, catalog.ErrInvalid
	}
	request, err := catalog.DecodeDeliverySelectRequest(envelope.Request)
	if err != nil || request.LibraryID != library {
		return catalog.DeliverySelectRequest{}, nil, catalog.ErrInvalid
	}
	return request, []byte(envelope.Bearer), nil
}
func readSourceBind(input io.Reader, library continuity.LibraryID) (catalog.SourceBindRequest, error) {
	b, err := readBounded(input, 32768)
	if err != nil {
		return catalog.SourceBindRequest{}, err
	}
	b = bytes.TrimSpace(b)
	var request catalog.SourceBindRequest
	if json.Unmarshal(b, &request) != nil {
		return catalog.SourceBindRequest{}, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(b, canonical) || request.LibraryID != library || request.RouteID != "" || request.RouteDigest != "" || request.ReaderLeafSHA256 != "" {
		return catalog.SourceBindRequest{}, catalog.ErrInvalid
	}
	return request, nil
}

// Lifecycle commands use only the catalog. Their request bodies retain the
// catalog codec's strict canonical validation and one KiB bound.
func readDeliveryLifecycleRequest(input io.Reader) ([]byte, error) {
	b, err := readBounded(input, 1024)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(b), nil
}

func readDeliveryLifecycleEnvelope(input io.Reader) (json.RawMessage, []byte, error) {
	b, err := readBounded(input, 1024+512)
	if err != nil {
		return nil, nil, err
	}
	b = bytes.TrimSpace(b)
	var envelope collectionMutationEnvelope
	if json.Unmarshal(b, &envelope) != nil || envelope.Version != 1 || len(envelope.Bearer) != 102 || !strings.HasPrefix(envelope.Bearer, "jmc1_") {
		return nil, nil, catalog.ErrInvalid
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(b, canonical) {
		return nil, nil, catalog.ErrInvalid
	}
	return envelope.Request, []byte(envelope.Bearer), nil
}

type deliveryCommandService interface {
	Close() error
	BindSource(context.Context, catalog.SourceBindRequest, string) (catalog.SourceBindResult, error)
	Select(context.Context, []byte, catalog.DeliverySelectRequest) (catalog.DeliverySelection, error)
	Status(context.Context, []byte, string) (catalog.DeliverySelection, error)
	MaterializeForHandoff(context.Context, []byte, string, string) (string, catalog.MaterializationAttempt, *catalog.MaterializationCleanup, error)
	ReconcileForHandoff(context.Context, []byte, string, string) (string, catalog.MaterializationAttempt, *catalog.MaterializationCleanup, error)
}

// One bounded receipt stays private until the CLI owner has closed its Store.
// The borrowed Store must remain open here for a service-close correction.
func deliveryReceipt(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(b) > 32768 {
		return nil, errors.New("delivery receipt exceeds limit; outcome uncertain")
	}
	return append(b, '\n'), nil
}

func runDeliveryCLI(ctx context.Context, s *catalog.Store, library continuity.LibraryID, command, configPath, routeID, selectionID, operationID string, input io.Reader, knownOperationID *string, files cliFiles) (pending []byte, resultErr error) {
	if command == "delivery policy apply" {
		b, err := readBounded(input, 32768)
		if err != nil {
			return nil, err
		}
		request, err := catalog.DecodeBytePolicyRequest(bytes.TrimSpace(b))
		if err != nil || request.LibraryID != library {
			return nil, catalog.ErrInvalid
		}
		*knownOperationID = request.ID
		owner, err := s.LocalOwner(library)
		if err != nil {
			return nil, err
		}
		receipt, err := owner.ApplyBytePolicy(ctx, request)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(receipt)
	}
	if command == "delivery policy status" {
		owner, err := s.LocalOwner(library)
		if err != nil {
			return nil, err
		}
		*knownOperationID = operationID
		receipt, err := owner.BytePolicyStatus(ctx, operationID)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(receipt)
	}
	switch command {
	case "source status", "source disable":
		b, err := readDeliveryLifecycleRequest(input)
		if err != nil {
			return nil, err
		}
		owner, err := s.LocalOwner(library)
		if err != nil {
			return nil, err
		}
		if command == "source status" {
			request, err := catalog.DecodeSourceStatusRequest(b)
			if err != nil || request.LibraryID != library {
				return nil, catalog.ErrInvalid
			}
			result, err := owner.DeliverySourceStatus(ctx, request)
			if err != nil {
				return nil, err
			}
			return deliveryReceipt(result)
		}
		request, err := catalog.DecodeSourceDisableRequest(b)
		if err != nil || request.LibraryID != library {
			return nil, catalog.ErrInvalid
		}
		*knownOperationID = request.ID
		result, err := owner.DisableDeliverySource(ctx, request)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(result)
	case "delivery inspect", "delivery lookup", "delivery cancel":
		b, bearer, err := readDeliveryLifecycleEnvelope(input)
		if err != nil {
			return nil, err
		}
		defer clear(bearer)
		switch command {
		case "delivery inspect":
			request, err := catalog.DecodeDeliveryInspectRequest(b)
			if err != nil || request.LibraryID != library {
				return nil, catalog.ErrInvalid
			}
			result, err := s.InspectDelivery(ctx, bearer, request)
			if err != nil {
				return nil, err
			}
			return deliveryReceipt(result)
		case "delivery lookup":
			request, err := catalog.DecodeDeliveryLookupRequest(b)
			if err != nil || request.LibraryID != library {
				return nil, catalog.ErrInvalid
			}
			result, err := s.LookupDelivery(ctx, bearer, request)
			if err != nil {
				return nil, err
			}
			return deliveryReceipt(result)
		case "delivery cancel":
			request, err := catalog.DecodeDeliveryCancelRequest(b)
			if err != nil || request.LibraryID != library {
				return nil, catalog.ErrInvalid
			}
			*knownOperationID = request.ID
			result, err := s.CancelDelivery(ctx, bearer, request)
			if err != nil {
				return nil, err
			}
			return deliveryReceipt(result)
		}
	}
	if configPath == "" {
		return nil, errors.New("delivery configuration required")
	}
	create := files.deliveryService
	if create == nil {
		create = func(ctx context.Context, s *catalog.Store, l continuity.LibraryID, p string) (deliveryCommandService, error) {
			return delivery.NewService(ctx, s, l, p)
		}
	}
	service, err := create(ctx, s, library, configPath)
	if err != nil {
		return nil, err
	}
	var cleanup *catalog.MaterializationCleanup
	defer func() {
		if closeErr := service.Close(); closeErr != nil {
			pending = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("delivery handoff uncertain: service close: %w", closeErr))
			if cleanup != nil {
				resultErr = errors.Join(resultErr, cleanup.MarkUncertain())
			}
		}
	}()
	switch command {
	case "source bind":
		request, err := readSourceBind(input, library)
		if err != nil {
			return nil, err
		}
		*knownOperationID = request.ID
		receipt, err := service.BindSource(ctx, request, routeID)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(receipt)
	case "delivery select":
		request, bearer, err := readDeliverySelection(input, library)
		if err != nil {
			return nil, err
		}
		defer clear(bearer)
		*knownOperationID = request.ID
		result, err := service.Select(ctx, bearer, request)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(result)
	case "delivery status":
		bearer, err := readDeliveryBearer(input)
		if err != nil {
			return nil, err
		}
		defer clear(bearer)
		result, err := service.Status(ctx, bearer, selectionID)
		if err != nil {
			return nil, err
		}
		return deliveryReceipt(result)
	case "delivery materialize", "delivery reconcile":
		bearer, err := readDeliveryBearer(input)
		if err != nil {
			return nil, err
		}
		defer clear(bearer)
		*knownOperationID = operationID
		var path string
		var attempt catalog.MaterializationAttempt
		if command == "delivery reconcile" {
			path, attempt, cleanup, err = service.ReconcileForHandoff(ctx, bearer, selectionID, operationID)
		} else {
			path, attempt, cleanup, err = service.MaterializeForHandoff(ctx, bearer, selectionID, operationID)
		}
		if err != nil {
			return nil, err
		}
		if attempt.State != "COMPLETED" || path == "" || cleanup == nil {
			return nil, catalog.ErrReconciliationRequired
		}
		return deliveryReceipt(struct {
			Path    string                         `json:"path"`
			Attempt catalog.MaterializationAttempt `json:"attempt"`
		}{path, attempt})
	}
	return nil, catalog.ErrInvalid
}

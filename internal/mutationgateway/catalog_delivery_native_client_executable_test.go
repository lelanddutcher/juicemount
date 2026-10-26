//go:build gateway_acceptance

package mutationgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/continuity/nativeclient"
)

// This gate adds the actual configured native HTTP client to the separately
// built Manager, Catalog CLI, and Gateway originals. It ends at verified bytes
// in memory: no durable Mac copy, local journal, Finder, NAS, or package claim.
func TestCatalogDeliveryNativeClientExecutable(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	f.ok(nil, "upgrade-delivery-lifecycle")
	f.byteGrant()
	partsA := [][]byte{
		bytes.Repeat([]byte{0, 1, 2, 255}, 160*1024),
		bytes.Repeat([]byte("native-client-A-middle"), 32*1024),
		bytes.Repeat([]byte("native-client-A-tail"), 16*1024),
	}
	sealA, dataA := f.seal(91, partsA)
	revisionA := f.revision(catalogExecutableAsset, dataA, "", "native-client-actual-revision-A")
	f.bind("native-client-actual-bind-A", catalogExecutableAsset, revisionA, sealA, nil)
	manager, endpoint := catalogManagerStart(t, f)
	client, err := nativeclient.NewClient(endpoint, catalogExecutableLibrary, f.bearer, true)
	if err != nil {
		t.Fatal("native client rejected actual Manager endpoint or fixture bearer", err)
	}
	t.Cleanup(func() {
		if closeErr := client.Close(context.Background()); closeErr != nil {
			t.Errorf("native client original owner did not join: %v", closeErr)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	requestA := f.request("native-client-actual-select-A", catalogExecutableAsset, "", int64(len(dataA)))
	selectionA, err := client.Select(ctx, requestA)
	if err != nil || selectionA.RevisionID != revisionA || selectionA.Content.SHA256 != sealA.Content.SHA256 {
		t.Fatal("native client did not select immutable A through Manager", err)
	}
	inspection, err := client.Inspect(ctx, catalog.DeliveryInspectRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selectionA.SelectionID})
	if err != nil || inspection.OriginalSelection != selectionA || !inspection.RequesterDeliveryEligibility.Eligible {
		t.Fatal("native client inspect lost A pin or current eligibility", err)
	}
	lookup, err := client.Lookup(ctx, catalog.DeliveryLookupRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionOperationID: selectionA.OperationID})
	if err != nil || lookup != inspection {
		t.Fatal("native client own-operation lookup changed A inspection", err)
	}
	catalogExecutableCheckpoint(t, "native-client-original-selected-inspected-and-looked-up-A")

	partsB := [][]byte{
		bytes.Repeat([]byte("native-client-B-first"), 32*1024),
		bytes.Repeat([]byte{9, 8, 7, 254}, 160*1024),
		bytes.Repeat([]byte("native-client-B-tail"), 16*1024),
	}
	sealB, dataB := f.seal(92, partsB)
	if bytes.Equal(dataA, dataB) {
		t.Fatal("A/B fixture must have distinct bytes")
	}
	revisionB := f.revision(catalogExecutableAsset, dataB, revisionA, "native-client-actual-revision-B")
	f.bind("native-client-actual-bind-B", catalogExecutableAsset, revisionB, sealB, nil)
	selectionB, err := client.Select(ctx, f.request("native-client-actual-select-B", catalogExecutableAsset, "", int64(len(dataB))))
	if err != nil || selectionB.RevisionID != revisionB || selectionB.SelectionID == selectionA.SelectionID {
		t.Fatal("native client latest select did not advance to B", err)
	}
	retryA, err := client.Select(ctx, requestA)
	if err != nil || retryA != selectionA {
		t.Fatal("native client exact retry did not retain A after B", err)
	}
	// The client owns canonical framing, End, EOF, and Body.Close checks. This
	// outer gate independently checks destination bytes and whole-file hashes.
	transfer := func(selection catalog.DeliverySelection, source []byte, offset int64) (nativeclient.TransferProgress, []byte) {
		t.Helper()
		prefix := sha256.Sum256(source[:offset])
		request := delivery.TransferRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selection.SelectionID, OperationID: selection.OperationID, Offset: offset, PrefixSHA256: hex.EncodeToString(prefix[:])}
		var target bytes.Buffer
		progress, transferErr := client.Transfer(ctx, request, selection, &target)
		if transferErr != nil || progress.BytesWritten != int64(target.Len()) || progress.BytesWritten != int64(len(source))-offset || !bytes.Equal(target.Bytes(), source[offset:]) || progress.End.Result != "server_stream_complete" || progress.Header.Selection != selection || progress.Header.Attempt.State != "COMPLETED" || progress.Header.Attempt.OperationID != selection.OperationID || progress.End.AttemptID != progress.Header.Attempt.ID {
			t.Fatal("native client failed exact Manager byte transfer", transferErr)
		}
		joined := append(append([]byte(nil), source[:offset]...), target.Bytes()...)
		sum := sha256.Sum256(joined)
		if int64(len(joined)) != selection.Content.Size || hex.EncodeToString(sum[:]) != selection.Content.SHA256 || !bytes.Equal(joined, source) {
			t.Fatal("native client transfer failed independent whole hash")
		}
		imageExecutableRecord(t, map[string]any{"kind": "native_client_http", "event": "complete", "selection_id": selection.SelectionID, "operation_id": selection.OperationID, "offset": offset, "body_bytes": target.Len(), "attempt_id": progress.Header.Attempt.ID, "valid_end": true, "whole_sha256": selection.Content.SHA256})
		return progress, append([]byte(nil), target.Bytes()...)
	}
	firstA, receivedA := transfer(selectionA, dataA, 0)
	reconciled, err := client.Reconcile(ctx, selectionA.SelectionID, selectionA.OperationID)
	if err != nil || reconciled != firstA.Header.Attempt {
		t.Fatal("native client reconcile changed the completed A attempt", err)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_client_http", "event": "reconcile", "selection_id": selectionA.SelectionID, "operation_id": selectionA.OperationID, "attempt_id": reconciled.ID, "state": reconciled.State})
	firstB, _ := transfer(selectionB, dataB, 0)
	if firstA.Header.Attempt.ID == firstB.Header.Attempt.ID {
		t.Fatal("native client A/B transfers reused an attempt")
	}
	resumeAt := int64(len(receivedA) / 2)
	resumePrefix := sha256.Sum256(receivedA[:resumeAt])
	resumeRequest := delivery.TransferRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selectionA.SelectionID, OperationID: selectionA.OperationID, Offset: resumeAt, PrefixSHA256: hex.EncodeToString(resumePrefix[:])}
	var tail bytes.Buffer
	resumed, err := client.Transfer(ctx, resumeRequest, selectionA, &tail)
	joined := append(append([]byte(nil), receivedA[:resumeAt]...), tail.Bytes()...)
	joinedSHA := sha256.Sum256(joined)
	if err != nil || resumed.Header.Attempt.ID != firstA.Header.Attempt.ID || resumed.End.Result != "server_stream_complete" || resumed.BytesWritten != int64(tail.Len()) || resumed.BytesWritten != int64(len(dataA))-resumeAt || !bytes.Equal(joined, dataA) || hex.EncodeToString(joinedSHA[:]) != selectionA.Content.SHA256 {
		t.Fatal("native client tail resume failed original attempt or whole bytes", err)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_client_http", "event": "complete", "selection_id": selectionA.SelectionID, "operation_id": selectionA.OperationID, "offset": resumeAt, "body_bytes": tail.Len(), "attempt_id": resumed.Header.Attempt.ID, "valid_end": true, "whole_sha256": selectionA.Content.SHA256})
	endA, zeroBytes := transfer(selectionA, receivedA, int64(len(receivedA)))
	if len(zeroBytes) != 0 || endA.Header.Attempt.ID != firstA.Header.Attempt.ID || endA.BytesWritten != 0 {
		t.Fatal("native client offset=size recovery retransmitted media or changed attempt")
	}
	for _, source := range []struct {
		seal  ImageSeal
		bytes []byte
	}{{sealA, dataA}, {sealB, dataB}} {
		view, statusErr := f.reader.ImageStatus(ctx, imageRef(source.seal))
		if statusErr != nil || view.Content != source.seal.Content {
			t.Fatal("Gateway source status changed after native client transfer", statusErr)
		}
		imageExecutableFullRead(t, ctx, f.reader, view, source.bytes)
	}
	catalogExecutableCheckpoint(t, "native-client-original-A-B-whole-tail-and-zero-byte-recovery")

	emptySeal, empty := f.seal(93, [][]byte{})
	emptyRevision := f.revision(catalogExecutableEmptyAsset, empty, "", "native-client-actual-empty-revision")
	f.bind("native-client-actual-bind-empty", catalogExecutableEmptyAsset, emptyRevision, emptySeal, nil)
	emptySelection, err := client.Select(ctx, f.request("native-client-actual-select-empty", catalogExecutableEmptyAsset, "", 0))
	if err != nil {
		t.Fatal("native client did not select empty source", err)
	}
	transfer(emptySelection, empty, 0)
	cancelSelection, err := client.Select(ctx, f.request("native-client-actual-select-cancel", catalogExecutableAsset, revisionA, int64(len(dataA))))
	if err != nil {
		t.Fatal("native client did not select cancel target", err)
	}
	canceled, err := client.Cancel(ctx, catalog.DeliveryCancelRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "native-client-actual-cancel", SelectionID: cancelSelection.SelectionID})
	if err != nil || !canceled.CurrentStatus.RecordState.Canceled || canceled.CurrentStatus.OriginalSelection != cancelSelection {
		t.Fatal("native client cancel did not cut exact pin", err)
	}
	emptySHA := sha256.Sum256(nil)
	denied := delivery.TransferRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: cancelSelection.SelectionID, OperationID: cancelSelection.OperationID, Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
	var refused bytes.Buffer
	progress, err := client.Transfer(ctx, denied, cancelSelection, &refused)
	var remote *nativeclient.RemoteError
	if !errors.As(err, &remote) || remote.StatusCode != 404 || remote.Code != "unavailable" || refused.Len() != 0 || progress.End != (delivery.TransferEnd{}) {
		t.Fatal("native client failed typed canceled-selection refusal", err)
	}
	catalogExecutableCheckpoint(t, "native-client-original-empty-and-canceled-refusal")
	if err := client.Close(ctx); err != nil {
		t.Fatal("native client original requests did not join", err)
	}
	manager.stopNormally()
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
	imageExecutableRecord(t, map[string]any{"kind": "native_client_scope", "client_joined": true, "manager_original_joined": true, "gateway_original_joined": true, "local_copy_or_finder_qualified": false})
}

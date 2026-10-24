//go:build gateway_acceptance

package mutationgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// This uses fresh product CLI processes and an authenticated live Gateway.
// Terminal cuts are sequential: concurrent fetch/exposure ordering and native
// application workflows require separate qualification.
func TestCatalogDeliveryExecutableLifecycle(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	seal, payload := f.seal(71, [][]byte{bytes.Repeat([]byte("lifecycle-first\x00"), 1024), bytes.Repeat([]byte("lifecycle-last\xff"), 2048)})
	revision := f.revision(catalogExecutableAsset, payload, "", "lifecycle-executable-revision")
	bindRequest := catalog.SourceBindRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "lifecycle-executable-bind", ExpectedIncarnation: f.incarnation, AssetID: catalogExecutableAsset, RevisionID: revision, Image: imageRef(seal)}
	var bound catalog.SourceBindResult
	catalogExecutableDecode(t, f.ok(catalogExecutableJSON(t, bindRequest), "--delivery-config", f.registry, "--route-id", "gateway", "source", "bind"), &bound)
	if !bound.OriginalReceipt.Enabled || !bound.CurrentStatus.Enabled || bound.CurrentStatus.Terminal != "none" {
		t.Fatal("schema-seven bind did not separate original receipt and current status")
	}
	f.byteGrant()
	selection := f.selectDelivery(f.request("lifecycle-executable-select", catalogExecutableAsset, revision, int64(len(payload))))
	completed := f.materialize(selection, "lifecycle-executable-completed", payload)
	f.ok(nil, "upgrade-delivery-lifecycle")
	f.ok(nil, "upgrade-delivery-lifecycle")
	catalogExecutableCheckpoint(t, "lifecycle8-explicit-upgrade-and-retry-after-completed-delivery")

	// The deliberately absent registry must never be opened by local controls.
	missingConfig := filepath.Join(f.dir, "no-lifecycle-delivery-config.json")
	local := func(body []byte, command ...string) []byte {
		t.Helper()
		return f.ok(body, append([]string{"--delivery-config", missingConfig}, command...)...)
	}
	envelope := func(request any, bearer []byte) []byte {
		return catalogExecutableJSON(t, struct {
			Version int             `json:"version"`
			Bearer  string          `json:"bearer"`
			Request json.RawMessage `json:"request"`
		}{1, string(bearer), catalogExecutableJSON(t, request)})
	}
	checkTerminal := func(got, want catalog.TerminalReceipt) {
		t.Helper()
		if got.RecordedUnixNano <= 0 {
			t.Fatal("terminal receipt omitted recorded time")
		}
		want.RecordedUnixNano = got.RecordedUnixNano
		if got != want {
			t.Fatal("terminal receipt lost request, actor, incarnation or target binding")
		}
	}
	sourceRequest := catalog.SourceStatusRequest{Version: 1, LibraryID: catalogExecutableLibrary, SourceID: bound.OriginalReceipt.SourceID, SourceVersion: bound.OriginalReceipt.SourceVersion}
	var sourceStatus catalog.SourceStatus
	catalogExecutableDecode(t, local(catalogExecutableJSON(t, sourceRequest), "source", "status"), &sourceStatus)
	if !sourceStatus.Enabled || sourceStatus.SourceID != sourceRequest.SourceID || sourceStatus.Terminal != "none" {
		t.Fatal("upgraded source status lost live binding")
	}
	inspect := catalog.DeliveryInspectRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selection.SelectionID}
	lookup := catalog.DeliveryLookupRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionOperationID: selection.OperationID}
	var inspection catalog.DeliveryInspection
	inspected := local(envelope(inspect, f.bearer), "delivery", "inspect")
	catalogExecutableDecode(t, inspected, &inspection)
	if inspection.OriginalSelection != selection || inspection.RecordState.Canceled || !inspection.RequesterDeliveryEligibility.Eligible {
		t.Fatal("inspection lost original pin or requester eligibility")
	}
	if !bytes.Equal(inspected, local(envelope(lookup, f.bearer), "delivery", "lookup")) {
		t.Fatal("own-operation lookup changed the selection")
	}
	catalogExecutableCheckpoint(t, "lifecycle8-source-status-inspect-and-operation-lookup-without-configuration")

	// Issue another codec3 credential using a distinct operation, rather than
	// retrying the one-time secret issuance or inventing a fourth codec.
	freshRequest := catalog.AdminRequestV3{Version: 3, LibraryID: catalogExecutableLibrary, ID: "lifecycle-executable-fresh-credential", ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: catalogExecutableJSON(t, catalog.IssueCredentialRequest{SubjectID: catalogExecutableReader, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"assets.discover", "bytes.read", "revisions.read"}, ExpiresUnix: time.Now().Add(30 * time.Minute).Unix()})}
	out, diagnostic, code := f.cli(f.database, catalogExecutableJSON(t, freshRequest), "--secret-out", "-", "credential", "issue-v3")
	if code != 0 || len(out) != 103 || out[102] != '\n' || !bytes.HasPrefix(out, []byte("jmc1_")) {
		t.Fatal("fresh codec3 secret issuance failed")
	}
	var issued struct {
		Receipt catalog.AdminReceipt `json:"receipt"`
		Status  string               `json:"status"`
	}
	catalogExecutableDecode(t, diagnostic, &issued)
	if issued.Receipt.Version != 3 || issued.Receipt.Result.Selector == "" || issued.Receipt.Result.Selector == f.selector {
		t.Fatal("fresh credential did not have a distinct codec3 selector")
	}
	fresh := append([]byte(nil), out[:102]...)
	clear(out)
	t.Cleanup(func() { clear(fresh) })
	catalogExecutableDecode(t, local(envelope(inspect, fresh), "delivery", "inspect"), &inspection)
	if inspection.OriginalSelection != selection || inspection.RequesterDeliveryEligibility.Eligible || inspection.RequesterDeliveryEligibility.Reason != "selector_mismatch" || inspection.RecordState.Canceled {
		t.Fatal("fresh selector changed the original pin or claimed byte eligibility")
	}
	f.denied(fresh, "--delivery-config", f.registry, "--selection-id", selection.SelectionID, "--operation-id", "lifecycle-executable-wrong-selector", "delivery", "materialize")
	stillAvailable := f.materialize(selection, "lifecycle-executable-original-selector", payload)
	catalogExecutableCheckpoint(t, "lifecycle8-fresh-subject-credential-inspects-without-inheriting-byte-pin")

	cancel := catalog.DeliveryCancelRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "lifecycle-executable-cancel", SelectionID: selection.SelectionID}
	_, cancelDigest, err := catalog.CanonicalDeliveryCancelRequest(cancel)
	if err != nil {
		t.Fatal(err)
	}
	var canceled catalog.DeliveryCancelResult
	cancelBytes := local(envelope(cancel, fresh), "delivery", "cancel")
	catalogExecutableDecode(t, cancelBytes, &canceled)
	checkTerminal(canceled.OriginalReceipt, catalog.TerminalReceipt{Version: 1, LibraryID: catalogExecutableLibrary, OperationID: cancel.ID, Kind: "selection_cancel", PayloadSHA256: cancelDigest, ActorSubjectID: catalogExecutableReader, AuthorityIncarnation: f.incarnation, SelectionID: selection.SelectionID})
	if canceled.CurrentStatus.OriginalSelection != selection || !canceled.CurrentStatus.RecordState.Canceled || canceled.CurrentStatus.RecordState.Terminal != "committed" || canceled.CurrentStatus.RequesterDeliveryEligibility.Eligible {
		t.Fatal("cancel did not retain immutable selection and durable terminal provenance")
	}
	if !bytes.Equal(cancelBytes, local(envelope(cancel, fresh), "delivery", "cancel")) {
		t.Fatal("fresh process cancel retry changed terminal result")
	}
	catalogExecutableDecode(t, local(envelope(inspect, f.bearer), "delivery", "inspect"), &inspection)
	if !inspection.RecordState.Canceled || inspection.RequesterDeliveryEligibility.Reason != "canceled" || inspection.RequesterDeliveryEligibility.Eligible {
		t.Fatal("original credential did not observe durable cancellation")
	}
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", selection.SelectionID, "--operation-id", "lifecycle-executable-after-cancel", "delivery", "materialize")
	cancelAgain := cancel
	cancelAgain.ID = "lifecycle-executable-second-cancel"
	f.denied(envelope(cancelAgain, fresh), "--delivery-config", missingConfig, "delivery", "cancel")
	// Positive delivery through the same live image rules out a stopped or
	// inaccessible Gateway as the explanation for the cancellation refusal.
	second := f.selectDelivery(f.request("lifecycle-executable-select-after-cancel", catalogExecutableAsset, revision, int64(len(payload))))
	control := f.materialize(second, "lifecycle-executable-cancel-control", payload)
	catalogExecutableCheckpoint(t, "lifecycle8-cancel-cut-retry-and-conflict-with-live-byte-positive")
	readyBeforeTerminal := f.readyNames()

	disable := catalog.SourceDisableRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "lifecycle-executable-disable", ExpectedIncarnation: f.incarnation, SourceID: sourceRequest.SourceID, ExpectedSourceVersion: sourceRequest.SourceVersion}
	_, disableDigest, err := catalog.CanonicalSourceDisableRequest(disable)
	if err != nil {
		t.Fatal(err)
	}
	disableBytes := local(catalogExecutableJSON(t, disable), "source", "disable")
	var disabled catalog.SourceDisableResult
	catalogExecutableDecode(t, disableBytes, &disabled)
	checkTerminal(disabled.OriginalReceipt, catalog.TerminalReceipt{Version: 1, LibraryID: catalogExecutableLibrary, OperationID: disable.ID, Kind: "source_disable", PayloadSHA256: disableDigest, ActorSubjectID: catalogExecutableOwner, AuthorityIncarnation: f.incarnation, SourceID: sourceRequest.SourceID, SourceVersion: sourceRequest.SourceVersion})
	if disabled.CurrentStatus.Enabled || disabled.CurrentStatus.Eligibility != "disabled" || disabled.CurrentStatus.Terminal != "committed" {
		t.Fatal("source disable retained eligibility or lost committed provenance")
	}
	if !bytes.Equal(disableBytes, local(catalogExecutableJSON(t, disable), "source", "disable")) {
		t.Fatal("fresh process source disable retry changed terminal result")
	}
	secondInspect := catalog.DeliveryInspectRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: second.SelectionID}
	catalogExecutableDecode(t, local(envelope(secondInspect, f.bearer), "delivery", "inspect"), &inspection)
	if inspection.RecordState.Canceled || inspection.RequesterDeliveryEligibility.Eligible || inspection.RequesterDeliveryEligibility.Reason != "source_or_pin_changed" {
		t.Fatal("source cut failed to invalidate an uncanceled selection")
	}
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", second.SelectionID, "--operation-id", "lifecycle-executable-after-disable", "delivery", "materialize")
	ctx, cancelProbe := context.WithTimeout(context.Background(), 20*time.Second)
	view, probeErr := f.reader.ImageStatus(ctx, imageRef(seal))
	cancelProbe()
	if probeErr != nil || view.Content != seal.Content {
		t.Fatal("source-disable refusal lacks a live authenticated Gateway control")
	}
	catalogExecutableCheckpoint(t, "lifecycle8-source-cut-preserves-image-and-invalidates-uncanceled-pin")

	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
	// Subsequent actual CLI processes cannot rely on the remote endpoint, and
	// local commands still receive a deliberately absent configuration path.
	if !bytes.Equal(cancelBytes, local(envelope(cancel, fresh), "delivery", "cancel")) || !bytes.Equal(disableBytes, local(catalogExecutableJSON(t, disable), "source", "disable")) {
		t.Fatal("offline exact retry changed immutable terminal results")
	}
	offlineCancel := catalog.DeliveryCancelRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "lifecycle-executable-offline-cancel", SelectionID: second.SelectionID}
	var offlineCanceled catalog.DeliveryCancelResult
	catalogExecutableDecode(t, local(envelope(offlineCancel, fresh), "delivery", "cancel"), &offlineCanceled)
	if !offlineCanceled.CurrentStatus.RecordState.Canceled || offlineCanceled.CurrentStatus.OriginalSelection != second || offlineCanceled.OriginalReceipt.ActorSubjectID != catalogExecutableReader || offlineCanceled.OriginalReceipt.OperationID != offlineCancel.ID {
		t.Fatal("offline terminal commit failed for authorized source-ineligible selection")
	}
	var retried catalog.SourceBindResult
	catalogExecutableDecode(t, f.ok(catalogExecutableJSON(t, bindRequest), "--delivery-config", f.registry, "--route-id", "gateway", "source", "bind"), &retried)
	if retried.OriginalReceipt != bound.OriginalReceipt || !retried.OriginalReceipt.Enabled || retried.CurrentStatus.Enabled || retried.CurrentStatus.Terminal != "committed" {
		t.Fatal("offline historical bind retry confused original receipt with current source eligibility")
	}
	catalogExecutableDecode(t, local(catalogExecutableJSON(t, sourceRequest), "source", "status"), &sourceStatus)
	if sourceStatus != disabled.CurrentStatus {
		t.Fatal("offline status changed after historical bind retry")
	}
	catalogExecutableDecode(t, local(envelope(lookup, f.bearer), "delivery", "lookup"), &inspection)
	if inspection.OriginalSelection != selection || !inspection.RecordState.Canceled || inspection.RecordState.Terminal != "committed" {
		t.Fatal("offline operation lookup lost terminal selection history")
	}
	if !bytes.Equal(readyBeforeTerminal, f.readyNames()) {
		t.Fatal("terminal operations changed previously exposed ready entries")
	}
	for _, path := range []string{completed.Path, stillAvailable.Path, control.Path} {
		f.assertBytes(path, payload)
	}
	catalogExecutableCheckpoint(t, "lifecycle8-offline-commit-retries-lookup-retain-all-completed-bytes")
}

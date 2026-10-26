//go:build gateway_acceptance

package mutationgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/nativeclient"
)

// This gate stops at a durable named Mac-local PNG and offline verification.
// The executable fixture serves the standalone, unprefixed Manager route.
func TestCatalogDeliveryNativeReceiverExecutable(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	f.ok(nil, "upgrade-delivery-lifecycle")
	f.byteGrant()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Encode actual, distinct PNG media in memory. Three nonempty chunks let
	// imageExecutableFullRead independently read every Gateway source chunk.
	makePNG := func(seed uint8) []byte {
		t.Helper()
		picture := image.NewRGBA(image.Rect(0, 0, 256, 256))
		for y := 0; y < 256; y++ {
			for x := 0; x < 256; x++ {
				picture.SetRGBA(x, y, color.RGBA{R: uint8(x) + seed, G: uint8(y) + seed*3, B: uint8(x*y) + seed, A: 255})
			}
		}
		var encoded bytes.Buffer
		if err := png.Encode(&encoded, picture); err != nil {
			t.Fatal("encode fixture PNG:", err)
		}
		config, err := png.DecodeConfig(bytes.NewReader(encoded.Bytes()))
		if err != nil || config.Width != 256 || config.Height != 256 {
			t.Fatal("invalid fixture PNG:", err)
		}
		return append([]byte(nil), encoded.Bytes()...)
	}
	chunks := func(media []byte) [][]byte {
		t.Helper()
		if len(media) < 3 {
			t.Fatal("PNG cannot form three nonempty source chunks")
		}
		one := len(media) / 3
		return [][]byte{media[:one], media[one : 2*one], media[2*one:]}
	}
	pngA, pngB := makePNG(11), makePNG(59)
	if bytes.Equal(pngA, pngB) {
		t.Fatal("fixture PNG A/B bytes coincide")
	}
	sealA, dataA := f.seal(101, chunks(pngA))
	if !bytes.Equal(dataA, pngA) {
		t.Fatal("Gateway A seal changed encoded PNG bytes")
	}
	revisionA := f.revision(catalogExecutableAsset, dataA, "", "native-receiver-actual-revision-A")
	f.bind("native-receiver-actual-bind-A", catalogExecutableAsset, revisionA, sealA, nil)
	manager, endpoint := catalogManagerStart(t, f)
	client, err := nativeclient.NewClient(endpoint, catalogExecutableLibrary, f.bearer, true)
	if err != nil {
		t.Fatal("native client rejected actual Manager endpoint:", err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("native client original requests did not join: %v", err)
		}
	})

	root := filepath.Join(f.dir, "native-receiver")
	journal, err := nativeclient.CreateJournal(root, endpoint, catalogExecutableLibrary, true)
	if err != nil {
		t.Fatal("create private native journal:", err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Errorf("native journal original close: %v", err)
		}
	})
	const copyID = "copy_11111111111111111111111111111111"
	const basename = "original-a.png"
	requestA := f.request("native-receiver-actual-select-A", catalogExecutableAsset, "", int64(len(dataA)))
	intent := nativeclient.Intent{
		Version: 1, Endpoint: endpoint, LibraryID: catalogExecutableLibrary,
		OperationID: requestA.ID, CopyID: copyID, SelectRequest: requestA,
		Basename: basename, MaxBytes: requestA.MaxBytes,
		MetadataReservationBytes: nativeclient.MetadataReservationBytes,
	}
	if err := journal.CreateIntent(intent); err != nil {
		t.Fatal("persist original native Intent before select:", err)
	}
	selectionA, err := client.Select(ctx, requestA)
	if err != nil || selectionA.OperationID != intent.OperationID || selectionA.RevisionID != revisionA || selectionA.Content.SHA256 != sealA.Content.SHA256 || selectionA.Content.Size != int64(len(dataA)) {
		t.Fatal("actual Manager did not select immutable PNG A:", err)
	}
	if err := journal.PinSelection(copyID, selectionA); err != nil {
		t.Fatal("persist original A selection:", err)
	}
	checkpoint, err := journal.InitialPartial(copyID)
	if err != nil || checkpoint.PrefixLength != 0 || checkpoint.File.Size != 0 {
		t.Fatal("initial private partial was not empty:", err)
	}
	catalogExecutableCheckpoint(t, "native-receiver-original-A-intent-selection-initial-partial")

	sealB, dataB := f.seal(102, chunks(pngB))
	if !bytes.Equal(dataB, pngB) || bytes.Equal(dataA, dataB) {
		t.Fatal("Gateway B seal changed PNG bytes or matched A")
	}
	revisionB := f.revision(catalogExecutableAsset, dataB, revisionA, "native-receiver-actual-revision-B")
	f.bind("native-receiver-actual-bind-B", catalogExecutableAsset, revisionB, sealB, nil)
	selectionB, err := client.Select(ctx, f.request("native-receiver-actual-select-B", catalogExecutableAsset, "", int64(len(dataB))))
	if err != nil || selectionB.RevisionID != revisionB || selectionB.SelectionID == selectionA.SelectionID {
		t.Fatal("actual Manager latest selection did not advance to B:", err)
	}
	retryA, err := client.Select(ctx, requestA)
	if err != nil || retryA != selectionA {
		t.Fatal("actual Manager changed the original A pin after B:", err)
	}

	permit, err := journal.Receive(ctx, client, copyID)
	if err != nil || permit == nil {
		t.Fatal("actual Manager transfer did not prepare verified PNG A:", err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("prepared original copy owner did not join: %v", err)
		}
	})
	finalPath := filepath.Join(root, "copies", copyID, basename)
	if _, err := os.Lstat(finalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Prepared unexpectedly exposed named PNG:", err)
	}
	if _, err := journal.CopyStatus(ctx, copyID); !errors.Is(err, nativeclient.ErrJournalConflict) {
		t.Fatal("Prepared was reported as Verified:", err)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_receiver_actual", "event": "prepared", "copy_id": copyID, "basename": basename, "selection_id": selectionA.SelectionID, "revision_id": revisionA, "sha256": sealA.Content.SHA256, "size": len(dataA), "initial_offset": checkpoint.PrefixLength})
	if err := permit.Close(); err != nil {
		t.Fatal("prepared original copy owner did not join:", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal("initial native journal did not close:", err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal("native client original requests did not join:", err)
	}
	manager.stopNormally()
	if !manager.waited || !manager.joined || !manager.signaled || manager.stopErr != nil || manager.waitErr != nil || manager.scan.err != nil || manager.scan.listening != 1 {
		t.Fatal("Manager original wait, scanner, and normal stop did not complete")
	}
	for _, source := range []struct {
		seal  ImageSeal
		bytes []byte
	}{{sealA, dataA}, {sealB, dataB}} {
		view, err := f.reader.ImageStatus(ctx, imageRef(source.seal))
		if err != nil || view.Content != source.seal.Content {
			t.Fatal("Gateway source status changed after native receive:", err)
		}
		imageExecutableFullRead(t, ctx, f.reader, view, source.bytes)
	}
	catalogExecutableCheckpoint(t, "native-receiver-original-A-B-source-bytes-unchanged")
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()

	// Everything below is local: Manager, Gateway and the native HTTP Client
	// have already joined. Recovery mints a fresh explicit exposure permit.
	reopened, err := nativeclient.OpenJournal(root, endpoint, catalogExecutableLibrary, true)
	if err != nil {
		t.Fatal("offline Prepared journal reopen:", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("offline reopened journal close: %v", err)
		}
	})
	recovered, err := reopened.RecoverPrepared(ctx, copyID)
	if err != nil || recovered == nil {
		t.Fatal("offline exact Prepared recovery failed:", err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("offline recovered copy owner did not join: %v", err)
		}
	})
	status, err := recovered.ExposeNoReplace(ctx)
	if err != nil || status.CopyID != copyID || status.Basename != basename || status.Size != int64(len(dataA)) || status.SHA256 != sealA.Content.SHA256 || status.Path != finalPath || !status.PreviouslyDownloaded {
		t.Fatal("explicit offline exposure returned wrong named-copy status:", err)
	}
	info, err := os.Lstat(finalPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("named output is not a private regular file:", err)
	}
	actual, err := os.ReadFile(finalPath)
	sum := sha256.Sum256(actual)
	if err != nil || !bytes.Equal(actual, dataA) || int64(len(actual)) != status.Size || hex.EncodeToString(sum[:]) != status.SHA256 {
		t.Fatal("named PNG bytes or independent whole-file hash differ:", err)
	}
	decoded, err := png.Decode(bytes.NewReader(actual))
	if err != nil || decoded.Bounds() != image.Rect(0, 0, 256, 256) {
		t.Fatal("named local file is not the complete PNG media:", err)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_receiver_actual", "event": "named_exposed", "copy_id": copyID, "basename": basename, "size": len(actual), "sha256": hex.EncodeToString(sum[:]), "manager_joined": true, "gateway_joined": true})
	if err := reopened.Close(); err != nil {
		t.Fatal("exposure journal did not close:", err)
	}
	statusJournal, err := nativeclient.OpenJournal(root, endpoint, catalogExecutableLibrary, true)
	if err != nil {
		t.Fatal("offline Verified journal reopen:", err)
	}
	t.Cleanup(func() {
		if err := statusJournal.Close(); err != nil {
			t.Errorf("offline status journal close: %v", err)
		}
	})
	offline, err := statusJournal.CopyStatus(ctx, copyID)
	if err != nil || offline != status {
		t.Fatal("offline CopyStatus failed exact rehash of named PNG:", err)
	}
	if err := statusJournal.Close(); err != nil {
		t.Fatal("offline status journal did not close:", err)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_receiver_actual", "event": "offline_copy_status", "copy_id": copyID, "basename": basename, "size": offline.Size, "sha256": offline.SHA256, "previously_downloaded": offline.PreviouslyDownloaded, "manager_original_joined": true, "gateway_original_joined": true, "partial_retransmit_qualified": false, "tls_qualified": false, "finder_qualified": false})
	catalogExecutableCheckpoint(t, "native-receiver-actual-named-PNG-offline-verified")
}

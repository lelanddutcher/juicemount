package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func transferTestRequest(f *serviceRecoveryFixture, offset int64) TransferRequest {
	sum := sha256.Sum256(f.payload[:int(offset)])
	return TransferRequest{1, recoveryLibrary, f.selection.SelectionID, recoveryOperation, offset, hex.EncodeToString(sum[:])}
}
func transferTestOpen(t *testing.T, f *serviceRecoveryFixture, parent context.Context, offset int64) *Transfer {
	t.Helper()
	stream, err := f.service.OpenTransfer(parent, f.bearer, transferTestRequest(f, offset))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	})
	return stream
}
func transferTestReadAll(t *testing.T, stream *Transfer, maximum int) []byte {
	t.Helper()
	var all []byte
	for {
		b, err := stream.ReadChunk(context.Background(), maximum)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 || len(b) > maximum {
			t.Fatalf("unbounded/no-progress chunk: %d", len(b))
		}
		all = append(all, b...)
	}
	return all
}
func transferTestSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("original fixture operation did not reach expected boundary")
	}
}
func transferTestGate(f *serviceRecoveryFixture, after int32) *serviceRecoveryStatusGate {
	g := &serviceRecoveryStatusGate{ordinal: f.calls.status.Load() + after, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	f.calls.statusGate.Store(g)
	return g
}
func transferTestRelease(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// Only quota/state observation uses SQL. All source/credential/pin/materialize
// and lifecycle transitions below go through production APIs.
func transferTestObserver(t *testing.T, f *serviceRecoveryFixture) *sql.DB {
	t.Helper()
	u := url.URL{Scheme: "file", Path: f.catalogPath}
	q := url.Values{}
	q.Set("mode", "ro")
	q.Set("_pragma", "query_only(1)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func transferTestQuota(t *testing.T, db *sql.DB) [6]int64 {
	t.Helper()
	var result [6]int64
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM delivery_materializations),source_versions,selections,attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&result[0], &result[1], &result[2], &result[3], &result[4], &result[5]); err != nil {
		t.Fatal(err)
	}
	return result
}
func transferTestCompleted(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`SELECT state FROM delivery_materializations WHERE attempt_id=?`, id).Scan(&state); err != nil || state != "COMPLETED" {
		t.Fatalf("stream failure changed prepared server history: %q %v", state, err)
	}
}
func transferTestUpgrade(t *testing.T, f *serviceRecoveryFixture) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpgradeDeliveryLifecycle(context.Background(), f.catalogPath, recoveryLibrary); err != nil {
		t.Fatal(err)
	}
	s, err := catalog.Open(f.catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	f.store = s
	f.service.store = s
	f.owner, err = s.LocalOwner(recoveryLibrary)
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceTransferCachedResumeAndAdmission(t *testing.T) {
	f := newServiceRecoveryFixture(t)
	ctx := context.Background()
	initialReads := f.calls.read.Load()
	stream := transferTestOpen(t, f, ctx, 0)
	header := stream.Header()
	if header.Selection != f.selection || header.AuthorityIncarnation != f.incarnation || header.Attempt.State != "COMPLETED" {
		t.Fatalf("header not authenticated original snapshot: %+v", header)
	}
	if f.calls.read.Load() != initialReads+2 {
		t.Fatal("first transfer did not materialize exact original image")
	}
	changed := header
	changed.Selection.Content.Size++
	changed.Attempt.OperationID = "caller-mutated"
	if changed == header {
		t.Fatal("test mutation did not change detached value")
	}
	if stream.Header() != header {
		t.Fatal("caller mutated detached Header snapshot")
	}
	status := f.calls.status.Load()
	first, err := stream.ReadChunk(ctx, 7)
	if err != nil || !bytes.Equal(first, f.payload[:7]) || f.calls.status.Load() != status+1 {
		t.Fatalf("first admitted buffer: %q %v", first, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if e, err := stream.Complete(ctx); err == nil || e != (TransferEnd{}) {
		t.Fatalf("abort issued End: %+v %v", e, err)
	}
	db := transferTestObserver(t, f)
	before := transferTestQuota(t, db)
	reads := f.calls.read.Load()
	resumed := transferTestOpen(t, f, ctx, int64(len(first)))
	if resumed.Header().Attempt != header.Attempt {
		t.Fatal("resume replaced immutable operation/attempt")
	}
	if got := transferTestReadAll(t, resumed, 9); !bytes.Equal(append(first, got...), f.payload) {
		t.Fatal("resumed complete bytes differ")
	}
	status = f.calls.status.Load()
	end, err := resumed.Complete(ctx)
	if err != nil || f.calls.status.Load() != status+1 {
		t.Fatalf("fresh final admission: %+v %v", end, err)
	}
	if err := ValidateTransferResponse(transferTestRequest(f, int64(len(first))), f.selection, resumed.Header(), &end); err != nil {
		t.Fatal(err)
	}
	if f.calls.read.Load() != reads || transferTestQuota(t, db) != before {
		t.Fatal("resume re-downloaded/recharged or replaced original records")
	}
	f.assertBytes(t, f.readyPath(header.Attempt.ID), f.payload)
	transferTestCompleted(t, db, header.Attempt.ID)
	// Lost reply recovery can request just the terminal marker without bytes.
	atEnd := transferTestOpen(t, f, ctx, int64(len(f.payload)))
	if got := transferTestReadAll(t, atEnd, 1); len(got) != 0 {
		t.Fatal("offset=size retransmitted media")
	}
	if end, err := atEnd.Complete(ctx); err != nil || end.Length != 0 {
		t.Fatalf("offset=size end: %+v %v", end, err)
	}
	bad := transferTestRequest(f, 7)
	bad.PrefixSHA256 = strings.Repeat("a", 64)
	if got, err := f.service.OpenTransfer(ctx, f.bearer, bad); err == nil || got != nil || !strings.Contains(err.Error(), "prefix mismatch") {
		t.Fatalf("wrong prefix admitted: %v %v", got, err)
	}
	bad = transferTestRequest(f, 0)
	bad.OperationID = "different-materialization"
	if got, err := f.service.OpenTransfer(ctx, f.bearer, bad); !errors.Is(err, catalog.ErrConflict) || got != nil {
		t.Fatalf("changed operation admitted: %v %v", got, err)
	}
	if f.calls.read.Load() != reads || transferTestQuota(t, db) != before {
		t.Fatal("failed retry changed records or downloaded bytes")
	}
}

func TestServiceTransferEmptyAndIncompleteStates(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newServiceRecoveryFixtureWithPayload(t, nil)
		stream := transferTestOpen(t, f, context.Background(), 0)
		if stream.Header().Length != 0 {
			t.Fatal("empty stream has bytes")
		}
		if b, err := stream.ReadChunk(context.Background(), 1); err != io.EOF || len(b) != 0 {
			t.Fatalf("empty EOF: %q %v", b, err)
		}
		if end, err := stream.Complete(context.Background()); err != nil || end.Length != 0 || end.Result != "server_stream_complete" {
			t.Fatalf("empty authenticated End: %+v %v", end, err)
		}
		if f.calls.read.Load() != 0 {
			t.Fatal("empty image fetched a chunk")
		}
	})
	for _, state := range []string{"PREPARED", "UNCERTAIN"} {
		t.Run(state, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			a := f.prepareExisting(t, state, true)
			status, reads := f.calls.status.Load(), f.calls.read.Load()
			stream, err := f.service.OpenTransfer(context.Background(), f.bearer, transferTestRequest(f, 0))
			if !errors.Is(err, catalog.ErrReconciliationRequired) || stream != nil {
				t.Fatalf("ambiguous original attempt implicitly reconciled: %v %v", stream, err)
			}
			if f.calls.status.Load() != status || f.calls.read.Load() != reads {
				t.Fatal("ambiguous retry fetched image")
			}
			f.assertBytes(t, f.readyPath(a.ID), f.payload)
		})
	}
	t.Run("early_complete", func(t *testing.T) {
		f := newServiceRecoveryFixture(t)
		stream := transferTestOpen(t, f, context.Background(), 0)
		if end, err := stream.Complete(context.Background()); !errors.Is(err, ErrTransferIncomplete) || end != (TransferEnd{}) {
			t.Fatalf("unread stream End: %+v %v", end, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if end, err := stream.Complete(context.Background()); err == nil || end != (TransferEnd{}) {
			t.Fatalf("failed completion revived: %+v %v", end, err)
		}
	})
}

func TestServiceTransferPostAdmissionFileRevalidation(t *testing.T) {
	for _, phase := range []string{"header", "end"} {
		for _, offsetKind := range []string{"zero", "size"} {
			for _, damage := range []string{"unchanged", "replace", "remove"} {
				t.Run(phase+"/"+offsetKind+"/"+damage, func(t *testing.T) {
					f := newServiceRecoveryFixture(t)
					ready, attempt := f.materialize(t)
					offset := int64(0)
					if offsetKind == "size" {
						offset = int64(len(f.payload))
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var stream *Transfer
					if phase == "end" {
						stream = transferTestOpen(t, f, ctx, offset)
						_ = transferTestReadAll(t, stream, 7)
					}
					after := int32(1)
					if phase == "header" {
						after = 2
					} // recovery status, then final header admission
					gate := transferTestGate(f, after)
					release := transferTestRelease(gate.release)
					type outcome struct {
						stream *Transfer
						end    TransferEnd
						err    error
					}
					result := make(chan outcome, 1)
					joined := false
					t.Cleanup(func() {
						cancel()
						release()
						if !joined {
							got := <-result
							if got.stream != nil {
								_ = got.stream.Close()
							}
						}
						if stream != nil {
							_ = stream.Close()
						}
					})
					go func() {
						if phase == "header" {
							got, err := f.service.OpenTransfer(ctx, f.bearer, transferTestRequest(f, offset))
							result <- outcome{stream: got, err: err}
						} else {
							end, err := stream.Complete(context.Background())
							result <- outcome{end: end, err: err}
						}
					}()
					select {
					case <-gate.entered:
					case got := <-result:
						joined = true
						if got.stream != nil {
							_ = got.stream.Close()
						}
						t.Fatalf("operation missed selected post-verification status: %+v", got)
					case <-time.After(10 * time.Second):
						t.Fatal("status admission boundary not reached")
					}
					if damage != "unchanged" {
						if err := os.Rename(ready, ready+".original"); err != nil {
							t.Fatal(err)
						}
						if damage == "replace" {
							if err := os.WriteFile(ready, f.payload, 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					release()
					got := <-result
					joined = true
					transferTestSignal(t, gate.returned)
					if damage == "unchanged" {
						if got.err != nil {
							t.Fatalf("unchanged admitted identity refused: %v", got.err)
						}
						if phase == "header" {
							stream = got.stream
							if stream == nil {
								t.Fatal("missing Header owner")
							}
							_ = transferTestReadAll(t, stream, 7)
							if _, err := stream.Complete(context.Background()); err != nil {
								t.Fatal(err)
							}
						} else if got.end.Result != "server_stream_complete" {
							t.Fatalf("positive End missing: %+v", got.end)
						}
					} else {
						if got.err == nil || got.stream != nil || got.end != (TransferEnd{}) {
							if got.stream != nil {
								_ = got.stream.Close()
							}
							t.Fatalf("changed file escaped after admitted status: %+v", got)
						}
						if damage == "replace" && !strings.Contains(got.err.Error(), "ready path changed") {
							t.Fatalf("wrong refusal stage: %v", got.err)
						}
						if damage == "remove" && !errors.Is(got.err, os.ErrNotExist) {
							t.Fatalf("wrong missing-file refusal: %v", got.err)
						}
						f.assertBytes(t, ready+".original", f.payload)
					}
					if stream != nil {
						if err := stream.Close(); err != nil {
							t.Fatal(err)
						}
					}
					transferTestCompleted(t, transferTestObserver(t, f), attempt.ID)
				})
			}
		}
	}
}

func TestServiceTransferCutsAndNoEnd(t *testing.T) {
	for _, phase := range []string{"open", "read", "end"} {
		for _, cut := range []string{"gateway", "credential", "cancel", "disable"} {
			t.Run(phase+"/"+cut, func(t *testing.T) {
				f := newServiceRecoveryFixture(t)
				if cut == "cancel" || cut == "disable" {
					transferTestUpgrade(t, f)
				}
				ready, a := f.materialize(t)
				var stream *Transfer
				if phase != "open" {
					offset := int64(0)
					if phase == "end" {
						offset = int64(len(f.payload))
					}
					stream = transferTestOpen(t, f, context.Background(), offset)
				}
				db := transferTestObserver(t, f)
				quota := transferTestQuota(t, db)
				reads := f.calls.read.Load()
				switch cut {
				case "gateway":
					f.calls.failStatus.Store(true)
				case "credential":
					if _, err := f.owner.RevokeCredential(context.Background(), serviceRecoveryAdmin(t, "transfer-revoke", f.incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: f.selector, ExpectedGeneration: 1})); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					if _, err := f.store.CancelDelivery(context.Background(), f.bearer, catalog.DeliveryCancelRequest{Version: 1, LibraryID: recoveryLibrary, ID: "transfer-cancel", SelectionID: f.selection.SelectionID}); err != nil {
						t.Fatal(err)
					}
				case "disable":
					if _, err := f.owner.DisableDeliverySource(context.Background(), catalog.SourceDisableRequest{Version: 1, LibraryID: recoveryLibrary, ID: "transfer-disable", ExpectedIncarnation: f.incarnation, SourceID: f.selection.SourceID, ExpectedSourceVersion: f.selection.SourceVersion}); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				var end TransferEnd
				if phase == "open" {
					var got *Transfer
					got, err = f.service.OpenTransfer(context.Background(), f.bearer, transferTestRequest(f, 0))
					if got != nil {
						_ = got.Close()
						t.Fatal("cut returned Header owner")
					}
				} else if phase == "read" {
					var b []byte
					b, err = stream.ReadChunk(context.Background(), 1)
					if len(b) != 0 {
						t.Fatal("cut emitted next buffer")
					}
				} else {
					end, err = stream.Complete(context.Background())
				}
				if cut == "gateway" {
					if err == nil || !strings.Contains(err.Error(), "injected service status failure") {
						t.Fatalf("wrong Gateway refusal: %v", err)
					}
				} else {
					want := catalog.ErrNotFound
					if cut == "credential" {
						want = catalog.ErrUnauthenticated
					}
					if !errors.Is(err, want) {
						t.Fatalf("wrong catalog refusal: %v want %v", err, want)
					}
				}
				if end != (TransferEnd{}) {
					t.Fatalf("cut returned End: %+v", end)
				}
				if stream != nil {
					if e, err := stream.Complete(context.Background()); err == nil || e != (TransferEnd{}) {
						t.Fatalf("failed stream revived: %+v %v", e, err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if f.calls.read.Load() != reads || transferTestQuota(t, db) != quota {
					t.Fatal("cut stream downloaded/recharged/replaced records")
				}
				transferTestCompleted(t, db, a.ID)
				f.assertBytes(t, ready, f.payload)
			})
		}
	}
}

func TestServiceTransferCorruptionRefusesReadinessAndEnd(t *testing.T) {
	for _, phase := range []string{"before_open", "unsent_tail"} {
		t.Run(phase, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			ready, a := f.materialize(t)
			if phase == "before_open" {
				changed := serviceRecoveryTamper(t, ready, f.payload)
				if got, err := f.service.OpenTransfer(context.Background(), f.bearer, transferTestRequest(f, 0)); err == nil || got != nil {
					if got != nil {
						_ = got.Close()
					}
					t.Fatalf("same-size content edit returned Header: %v", err)
				}
				f.assertBytes(t, ready, changed)
			} else {
				stream := transferTestOpen(t, f, context.Background(), 0)
				b, err := stream.ReadChunk(context.Background(), 3)
				if err != nil || !bytes.Equal(b, f.payload[:3]) {
					t.Fatalf("first buffer: %q %v", b, err)
				}
				file, err := os.OpenFile(ready, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := file.WriteAt([]byte{'X'}, int64(len(f.payload)-1))
				if err := errors.Join(writeErr, file.Close()); err != nil {
					t.Fatal(err)
				}
				_ = transferTestReadAll(t, stream, 7)
				if end, err := stream.Complete(context.Background()); err == nil || end != (TransferEnd{}) || !strings.Contains(err.Error(), "changed content") {
					t.Fatalf("changed aggregate digest issued End: %+v %v", end, err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			}
			transferTestCompleted(t, transferTestObserver(t, f), a.ID)
		})
	}
}

func TestServiceTransferRetainsOriginalContext(t *testing.T) {
	for _, kind := range []string{"original_cancel", "original_deadline", "caller_cancel", "abort", "invalid_chunk"} {
		t.Run(kind, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			f.materialize(t)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			var deadlineCancel context.CancelFunc
			if kind == "original_deadline" {
				parent, deadlineCancel = context.WithTimeout(parent, 3*time.Second)
				defer deadlineCancel()
			}
			stream := transferTestOpen(t, f, parent, 0)
			// The retained deadline is no later than either original deadline or pin expiry.
			deadline, ok := stream.ctx.Deadline()
			if !ok || deadline.After(time.Unix(f.selection.ExpiresUnix, 0)) {
				t.Fatal("transfer extended pin expiry")
			}
			if original, ok := parent.Deadline(); ok && deadline.After(original) {
				t.Fatal("transfer extended original deadline")
			}
			switch kind {
			case "original_cancel":
				cancel()
				<-parent.Done()
			case "original_deadline":
				<-parent.Done()
				if !errors.Is(parent.Err(), context.DeadlineExceeded) {
					t.Fatal(parent.Err())
				}
			case "caller_cancel":
				caller, cancelCall := context.WithCancel(context.Background())
				cancelCall()
				if b, err := stream.ReadChunk(caller, 1); !errors.Is(err, context.Canceled) || len(b) != 0 {
					t.Fatalf("caller cancellation lost: %q %v", b, err)
				}
			case "abort":
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			case "invalid_chunk":
				if b, err := stream.ReadChunk(context.Background(), MaxTransferChunkBytes+1); !errors.Is(err, ErrInvalidTransfer) || len(b) != 0 {
					t.Fatalf("unbounded chunk: %q %v", b, err)
				}
			}
			if b, err := stream.ReadChunk(context.Background(), 1); err == nil || len(b) != 0 {
				t.Fatalf("Background revived terminal operation: %q %v", b, err)
			}
			if end, err := stream.Complete(context.Background()); err == nil || end != (TransferEnd{}) {
				t.Fatalf("terminal operation issued End: %+v %v", end, err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServiceTransferCloseJoinsOriginalStatus(t *testing.T) {
	for _, closer := range []string{"transfer", "service"} {
		t.Run(closer, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			_, a := f.materialize(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := transferTestOpen(t, f, ctx, 0)
			root := &lifecycleRoot{ManagedRoot: f.root}
			f.service.root = root
			gate := transferTestGate(f, 1)
			releaseHandler := transferTestRelease(gate.release)
			route := f.service.routes["service-recovery-route"]
			_, httpClient := route.client.Transport()
			original := httpClient.Transport
			transport := &lifecycleTransport{original: original, canceled: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}), established: gate.entered}
			httpClient.Transport = transport
			releaseTransport := transferTestRelease(transport.release)
			type readResult struct {
				count int
				err   error
			}
			readDone := make(chan readResult, 1)
			closeDone := make(chan error, 1)
			readJoined, closeJoined := false, false
			t.Cleanup(func() {
				cancel()
				releaseHandler()
				releaseTransport()
				if !readJoined {
					<-readDone
				}
				if !closeJoined {
					<-closeDone
				}
				_ = stream.Close()
				httpClient.Transport = original
			})
			go func() {
				b, err := stream.ReadChunk(context.Background(), 1)
				readDone <- readResult{count: len(b), err: err}
			}()
			select {
			case <-gate.entered:
			case read := <-readDone:
				readJoined = true
				go func() { closeDone <- stream.Close() }()
				t.Fatalf("read missed actual TLS status: count=%d err=%v", read.count, read.err)
			case <-time.After(10 * time.Second):
				go func() { closeDone <- stream.Close() }()
				t.Fatal("original status not established")
			}
			go func() {
				if closer == "service" {
					closeDone <- f.service.Close()
				} else {
					closeDone <- stream.Close()
				}
			}()
			transferTestSignal(t, transport.canceled)
			transferTestSignal(t, gate.returned)
			select {
			case <-gate.canceled:
			default:
				t.Fatal("real TLS handler did not observe request cancellation")
			}
			if root.calls.Load() != 0 {
				t.Fatal("root closed before original status return")
			}
			// The exact attempt lock remains owned while the original caller is held.
			bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if other, err := f.root.OpenOwnedReady(a.ID, bound.Root, bound.Directory, bound.Lock, bound.File); err == nil {
				_ = other.Close()
				t.Fatal("original status owner released attempt lock early")
			}
			select {
			case err := <-closeDone:
				closeJoined = true
				t.Fatalf("Close returned before held original: %v", err)
			default:
			}
			releaseTransport()
			transferTestSignal(t, transport.returned)
			read := <-readDone
			readJoined = true
			if read.count != 0 {
				t.Fatalf("canceled original emitted %d bytes", read.count)
			}
			if !errors.Is(read.err, context.Canceled) {
				t.Fatalf("original read cancellation: %v", read.err)
			}
			if err := <-closeDone; err != nil {
				closeJoined = true
				t.Fatal(err)
			}
			closeJoined = true
			if end, err := stream.Complete(context.Background()); err == nil || end != (TransferEnd{}) {
				t.Fatalf("closed original issued End: %+v %v", end, err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.service.Close(); err != nil {
				t.Fatal(err)
			}
			if root.calls.Load() != 1 {
				t.Fatalf("root closure count: %d", root.calls.Load())
			}
			if err := f.service.Close(); err != nil || root.calls.Load() != 1 {
				t.Fatalf("repeat service close lost shared outcome: %v", err)
			}
		})
	}
}

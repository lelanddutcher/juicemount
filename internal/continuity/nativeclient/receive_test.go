package nativeclient

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// A deliberately broken admission guard may return an owner on a negative
// path. Join that exact original before reporting the intended assertion.
func nativeReceiveTestCloseUnexpectedPermit(t *testing.T, permit *VerifiedLocalCopy) {
	t.Helper()
	if permit != nil {
		if err := permit.Close(); err != nil {
			t.Errorf("unexpected permit original close: %v", err)
		}
	}
}

func nativeReceiveTestFixture(t *testing.T, pin clientTestPin, handler func(http.ResponseWriter, *http.Request)) (*Journal, *Client, string, Intent) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	path := filepath.Join(t.TempDir(), "native")
	j, err := CreateJournal(path, server.URL, pin.selection.LibraryID, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Errorf("journal original close: %v", err)
		}
	})
	client, err := NewClient(server.URL, pin.selection.LibraryID, clientTestBearer(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("client original close: %v", err)
		}
	})
	intent := maxVectors().intent
	intent.Endpoint = server.URL
	intent.LibraryID = pin.selection.LibraryID
	intent.OperationID = pin.selection.OperationID
	intent.SelectRequest = pin.selectRequest
	intent.MaxBytes = int64(len(pin.payload))
	intent.Basename = "verified-media.bin"
	if err := j.CreateIntent(intent); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(intent.CopyID, pin.selection); err != nil {
		t.Fatal(err)
	}
	if _, err := j.InitialPartial(intent.CopyID); err != nil {
		t.Fatal(err)
	}
	return j, client, path, intent
}

func TestNativeReceiveDurablePreparedExplicitExposureAndOfflineStatus(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("local durable media bytes")} {
		body := body
		t.Run(string(body), func(t *testing.T) {
			pin := newClientTestPin(body, 0)
			var requests atomic.Int32
			j, client, root, intent := nativeReceiveTestFixture(t, pin, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				clientTestTransferHandler(t, pin, "", "")(w, r)
			})
			permit, err := j.Receive(context.Background(), client, intent.CopyID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := permit.Close(); err != nil {
					t.Errorf("prepared owner close: %v", err)
				}
			})
			path := filepath.Join(root, "copies", intent.CopyID, intent.Basename)
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Prepared auto-exposed: %v", err)
			}
			if _, err := j.CopyStatus(context.Background(), intent.CopyID); !errors.Is(err, ErrJournalConflict) {
				t.Fatalf("Prepared claimed Verified: %v", err)
			}
			if err := permit.Close(); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenJournal(root, intent.Endpoint, intent.LibraryID, true)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened journal close: %v", err)
				}
			})
			recovered, err := reopened.RecoverPrepared(context.Background(), intent.CopyID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := recovered.Close(); err != nil {
					t.Errorf("recovered owner close: %v", err)
				}
			})
			status, err := recovered.ExposeNoReplace(context.Background())
			if err != nil || status.Path != path || !status.PreviouslyDownloaded || status.Size != int64(len(body)) || status.SHA256 != clientTestDigest(body) {
				t.Fatalf("Verified status: %+v %v", status, err)
			}
			if requests.Load() != 1 {
				t.Fatalf("offline recovery made network request: %d", requests.Load())
			}
			got, err := os.ReadFile(status.Path)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("named local bytes: %q %v", got, err)
			}
			status, err = reopened.CopyStatus(context.Background(), intent.CopyID)
			if err != nil || status.Path != path {
				t.Fatalf("offline status: %+v %v", status, err)
			}
		})
	}
}

// Finder can write .DS_Store while showing the named copy. It is never a
// retained receipt or a permitted media basename, but cannot strand the
// original verified bytes on the next credential-free profile open.
func TestNativeReceiveFinderMetadataProfileOpenAndOfflineStatus(t *testing.T) {
	if ValidateBasename(".DS_Store") == nil {
		t.Fatal("Finder metadata could be a valid retained media basename")
	}
	body := []byte("verified named bytes survive Finder metadata and offline reopen")
	pin := newClientTestPin(body, 0)
	var requests atomic.Int32
	j, client, root, intent := nativeReceiveTestFixture(t, pin, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		clientTestTransferHandler(t, pin, "", "")(w, r)
	})
	permit, err := j.Receive(context.Background(), client, intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("original permit close: %v", err)
		}
	})
	status, err := permit.ExposeNoReplace(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Close(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{filepath.Join(root, "copies"), filepath.Join(root, "copies", intent.CopyID)} {
		leaf := filepath.Join(dir, ".DS_Store")
		if err := os.WriteFile(leaf, bytes.Repeat([]byte{0xa5}, 6148), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(leaf, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := OpenJournal(root, intent.Endpoint, intent.LibraryID, true)
	if err != nil {
		t.Fatalf("Finder metadata blocked strict existing profile open: %v", err)
	}
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Errorf("fresh journal close: %v", err)
		}
	})
	if err := fresh.RecoverProfile(); err != nil {
		t.Fatalf("Finder metadata blocked credential-free profile recovery: %v", err)
	}
	reopened, err := fresh.CopyStatus(context.Background(), intent.CopyID)
	if err != nil || reopened != status || requests.Load() != 1 {
		t.Fatalf("offline status after Finder metadata: got=%+v want=%+v requests=%d err=%v", reopened, status, requests.Load(), err)
	}
	got, err := os.ReadFile(reopened.Path)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("named bytes changed: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	// TargetDirectory returns raw valid basenames. The journal alone must
	// reject a second named leaf beside the retained Intent basename, even
	// when both Finder metadata leaves have passed their separate checks.
	targetUnknown := filepath.Join(root, "copies", intent.CopyID, "unlisted")
	if err := os.WriteFile(targetUnknown, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	suspect, openErr := OpenJournal(root, intent.Endpoint, intent.LibraryID, true)
	if suspect != nil {
		recoveryErr := suspect.RecoverProfile()
		closeErr := suspect.Close()
		if closeErr != nil {
			t.Fatalf("unknown target journal original close: %v", closeErr)
		}
		if openErr != nil {
			t.Fatalf("unknown target open returned an owner and error: %v", openErr)
		}
		if !errors.Is(recoveryErr, ErrJournalCorrupt) {
			t.Fatalf("completed copy admitted unrelated target leaf: %v", recoveryErr)
		}
	} else if openErr == nil {
		t.Fatal("unknown target returned neither journal owner nor error")
	}
	if err := os.Remove(targetUnknown); err != nil {
		t.Fatal(err)
	}
	// An unrelated object in the exposed parent is still a corruption, not
	// a second copy or ignorable Finder metadata.
	if err := os.WriteFile(filepath.Join(root, "copies", "unlisted"), []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := OpenJournal(root, intent.Endpoint, intent.LibraryID, true); err == nil || unexpected != nil {
		if unexpected != nil {
			if closeErr := unexpected.Close(); closeErr != nil {
				t.Errorf("unexpected journal close: %v", closeErr)
			}
		}
		t.Fatalf("unknown copy-root object admitted: %v", err)
	}
}

func TestNativeReceiveTruncatedTailSamePinRetryAndCancelBeforeRename(t *testing.T) {
	body := []byte("original selected content for partial retry")
	first := newClientTestPin(body, 0)
	resume := newClientTestPin(body, int64(len(body)-1))
	resume.selection = first.selection
	resume.header.Selection = first.selection
	var calls atomic.Int32
	j, client, root, intent := nativeReceiveTestFixture(t, first, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			clientTestTransferHandler(t, first, "truncated", "")(w, r)
			return
		}
		clientTestTransferHandler(t, resume, "", "")(w, r)
	})
	if permit, err := j.Receive(context.Background(), client, intent.CopyID); err == nil || permit != nil {
		nativeReceiveTestCloseUnexpectedPermit(t, permit)
		t.Fatalf("truncated stream minted permit: %v %v", permit, err)
	}
	if _, err := j.CopyStatus(context.Background(), intent.CopyID); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("truncated stream claimed Verified: %v", err)
	}
	permit, err := j.Receive(context.Background(), client, intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("permit original close: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := permit.ExposeNoReplace(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel-before-rename: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "copies", intent.CopyID, intent.Basename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled exposure renamed: %v", err)
	}
	recovered, err := j.RecoverPrepared(context.Background(), intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("recovered original close: %v", err)
		}
	})
	status, err := recovered.ExposeNoReplace(context.Background())
	if err != nil || status.Size != int64(len(body)) || calls.Load() != 2 {
		t.Fatalf("same-pin verified retry: %+v calls=%d err=%v", status, calls.Load(), err)
	}
	got, err := os.ReadFile(status.Path)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("resumed bytes: %q %v", got, err)
	}
}

func TestNativeReceiveRejectsEndpointMismatchBeforeTransfer(t *testing.T) {
	body := []byte("endpoint binding")
	pin := newClientTestPin(body, 0)
	j, _, _, intent := nativeReceiveTestFixture(t, pin, clientTestTransferHandler(t, pin, "", ""))
	other, err := NewClient("http://127.0.0.1:1", pin.selection.LibraryID, clientTestBearer(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(context.Background()); err != nil {
			t.Errorf("other client close: %v", err)
		}
	})
	if permit, err := j.Receive(context.Background(), other, intent.CopyID); permit != nil || !errors.Is(err, ErrInvalidClient) {
		nativeReceiveTestCloseUnexpectedPermit(t, permit)
		t.Fatalf("foreign endpoint accepted: %v %v", permit, err)
	}
}

func TestNativeReceiveExplicitRecoveryAfterRenameBeforeVerified(t *testing.T) {
	body := []byte("already completed retained receipt")
	pin := newClientTestPin(body, 0)
	var requests atomic.Int32
	j, client, _, intent := nativeReceiveTestFixture(t, pin, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		clientTestTransferHandler(t, pin, "", "")(w, r)
	})
	permit, err := j.Receive(context.Background(), client, intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("prepared owner close: %v", err)
		}
	})
	exposed, err := permit.owner.ExposeNoReplace(context.Background(), permit.prepared.TargetDirectory, intent.Basename, permit.prepared.File)
	if !exposed || err != nil {
		t.Fatalf("simulated post-rename interruption: %v %v", exposed, err)
	}
	if err := permit.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := j.CopyStatus(context.Background(), intent.CopyID); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("rename without Verified claimed status: %v", err)
	}
	recovered, err := j.RecoverPrepared(context.Background(), intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("recovered owner close: %v", err)
		}
	})
	status, err := recovered.ExposeNoReplace(context.Background())
	if err != nil || !status.PreviouslyDownloaded || requests.Load() != 1 {
		t.Fatalf("offline post-rename recovery: %+v requests=%d err=%v", status, requests.Load(), err)
	}
	got, err := os.ReadFile(status.Path)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("recovered named bytes: %q %v", got, err)
	}
}

func TestNativeReceiveWholeHashRefusesValidEndForDifferentBytes(t *testing.T) {
	body := []byte("expected selected bytes")
	pin := newClientTestPin(body, 0)
	wrong := pin
	wrong.payload = []byte("changed! selected bytes")
	j, client, root, intent := nativeReceiveTestFixture(t, pin, clientTestTransferHandler(t, wrong, "", ""))
	if permit, err := j.Receive(context.Background(), client, intent.CopyID); permit != nil || !errors.Is(err, ErrJournalCorrupt) {
		nativeReceiveTestCloseUnexpectedPermit(t, permit)
		t.Fatalf("wrong whole hash minted Prepared: %v %v", permit, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "staging", intent.CopyID, "prepared.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrong hash wrote Prepared: %v", err)
	}
	if _, err := j.CopyStatus(context.Background(), intent.CopyID); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("wrong bytes claimed Verified: %v", err)
	}
}

func TestNativeReceivePreparedBarrierFailureRequiresExplicitExactRetry(t *testing.T) {
	for _, boundary := range []string{"file", "directory"} {
		t.Run(boundary, func(t *testing.T) {
			body := []byte("prepared receipt is retained across an uncertain barrier")
			pin := newClientTestPin(body, 0)
			var requests atomic.Int32
			j, client, root, intent := nativeReceiveTestFixture(t, pin, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				clientTestTransferHandler(t, pin, "", "")(w, r)
			})
			fault := errors.New("injected " + boundary + " receipt barrier fault")
			j.preparedBarrierErr = fault
			if permit, err := j.Receive(context.Background(), client, intent.CopyID); permit != nil || !errors.Is(err, fault) {
				nativeReceiveTestCloseUnexpectedPermit(t, permit)
				t.Fatalf("uncertain Prepared minted permit: %v %v", permit, err)
			}
			if permit, err := j.RecoverPrepared(context.Background(), intent.CopyID); permit != nil || !errors.Is(err, fault) {
				nativeReceiveTestCloseUnexpectedPermit(t, permit)
				t.Fatalf("continuing Prepared barrier minted permit: %v %v", permit, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "copies", intent.CopyID, intent.Basename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed barrier exposed named copy: %v", err)
			}
			j.preparedBarrierErr = nil
			permit, err := j.RecoverPrepared(context.Background(), intent.CopyID)
			if err != nil {
				t.Fatal(err)
			}
			firstRecoveredOwner := permit
			t.Cleanup(func() {
				if err := firstRecoveredOwner.Close(); err != nil {
					t.Errorf("recovered Prepared owner close: %v", err)
				}
			})
			j.preparedBarrierErr = fault
			if status, err := permit.ExposeNoReplace(context.Background()); status.Path != "" || !errors.Is(err, fault) {
				t.Fatalf("renewed Prepared failure reached rename: %+v %v", status, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "copies", intent.CopyID, intent.Basename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("renewed failure exposed named copy: %v", err)
			}
			j.preparedBarrierErr = nil
			permit, err = j.RecoverPrepared(context.Background(), intent.CopyID)
			if err != nil {
				t.Fatal(err)
			}
			finalOwner := permit
			t.Cleanup(func() {
				if err := finalOwner.Close(); err != nil {
					t.Errorf("final Prepared owner close: %v", err)
				}
			})
			status, err := permit.ExposeNoReplace(context.Background())
			if err != nil || !status.PreviouslyDownloaded || requests.Load() != 1 {
				t.Fatalf("exact durable Prepared retry: %+v requests=%d err=%v", status, requests.Load(), err)
			}
			got, err := os.ReadFile(status.Path)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("same retained bytes: %q %v", got, err)
			}
		})
	}
}

func TestNativeReceiveVerifiedBarrierFailureRequiresExactFinalRecovery(t *testing.T) {
	body := []byte("original verified local result")
	pin := newClientTestPin(body, 0)
	var requests atomic.Int32
	j, client, root, intent := nativeReceiveTestFixture(t, pin, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		clientTestTransferHandler(t, pin, "", "")(w, r)
	})
	permit, err := j.Receive(context.Background(), client, intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("original Prepared owner close: %v", err)
		}
	})
	fault := errors.New("injected Verified receipt barrier fault")
	j.verifiedBarrierErr = fault
	if status, err := permit.ExposeNoReplace(context.Background()); status.Path != "" || !errors.Is(err, fault) || !errors.Is(err, ErrNativeExposureUncertain) {
		t.Fatalf("uncertain Verified acknowledged: %+v %v", status, err)
	}
	path := filepath.Join(root, "copies", intent.CopyID, intent.Basename)
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("original named file missing after uncertain barrier: %v", err)
	}
	if status, err := j.CopyStatus(context.Background(), intent.CopyID); status.Path != "" || !errors.Is(err, fault) {
		t.Fatalf("status acknowledged continuing barrier failure: %+v %v", status, err)
	}
	if recovered, err := j.RecoverPrepared(context.Background(), intent.CopyID); recovered != nil || !errors.Is(err, fault) {
		nativeReceiveTestCloseUnexpectedPermit(t, recovered)
		t.Fatalf("recovery minted permit before Verified barrier: %v %v", recovered, err)
	}
	j.verifiedBarrierErr = nil
	recovered, err := j.RecoverPrepared(context.Background(), intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("original recovered owner close: %v", err)
		}
	})
	if !recovered.alreadyFinal {
		t.Fatal("exact final recovery attempted second rename")
	}
	status, err := recovered.ExposeNoReplace(context.Background())
	if err != nil || status.Path != path || requests.Load() != 1 {
		t.Fatalf("exact offline Verified retry: %+v requests=%d err=%v", status, requests.Load(), err)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("final inode replaced during retry: %v", err)
	}
	j.preparedBarrierErr = fault
	if status, err := j.CopyStatus(context.Background(), intent.CopyID); status.Path != "" || !errors.Is(err, fault) {
		t.Fatalf("status skipped Prepared barrier: %+v %v", status, err)
	}
	j.preparedBarrierErr = nil
	j.targetBarrierErr = fault
	if status, err := j.CopyStatus(context.Background(), intent.CopyID); status.Path != "" || !errors.Is(err, fault) {
		t.Fatalf("status skipped target barrier: %+v %v", status, err)
	}
	j.targetBarrierErr = nil
	if status, err := j.CopyStatus(context.Background(), intent.CopyID); err != nil || status.Path != path {
		t.Fatalf("durable offline status: %+v %v", status, err)
	}
}

func TestNativeReceiveConflictingAndPartialVerifiedRefuseRecovery(t *testing.T) {
	body := []byte("immutable verified record")
	pin := newClientTestPin(body, 0)
	j, client, root, intent := nativeReceiveTestFixture(t, pin, clientTestTransferHandler(t, pin, "", ""))
	permit, err := j.Receive(context.Background(), client, intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := permit.Close(); err != nil {
			t.Errorf("permit close: %v", err)
		}
	})
	if _, err := permit.ExposeNoReplace(context.Background()); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(root, "staging", intent.CopyID, "verified.json")
	valid, err := os.ReadFile(leaf)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := DecodeVerified(valid)
	if err != nil {
		t.Fatal(err)
	}
	record.FinalFile.Inode++
	conflict, _, err := EncodeVerified(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		line []byte
	}{{"conflicting", conflict}, {"partial", valid[:len(valid)/2]}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(leaf, tc.line, 0600); err != nil {
				t.Fatal(err)
			}
			if recovered, err := j.RecoverPrepared(context.Background(), intent.CopyID); recovered != nil || !errors.Is(err, ErrJournalCorrupt) {
				nativeReceiveTestCloseUnexpectedPermit(t, recovered)
				t.Fatalf("corrupt Verified recovered: %v %v", recovered, err)
			}
			if status, err := j.CopyStatus(context.Background(), intent.CopyID); status.Path != "" || !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("corrupt Verified status: %+v %v", status, err)
			}
		})
	}
}

package privatefile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func managedTransferSHA(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func managedTransferFixture(t *testing.T, payload []byte) (string, *ManagedRoot, *PreparedFile, *OwnedReady, continuity.Content) {
	t.Helper()
	path, root := managedTestRoot(t)
	p := managedTestPrepared(t, root, managedTestID(91), payload)
	if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
		t.Fatalf("original fixed exposure: %v %v", exposed, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	owned, err := root.OpenOwnedReady(p.ID(), p.RootIdentity(), p.AttemptIdentity(), p.LockIdentity(), p.FileIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !owned.closed {
			if err := owned.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return path, root, p, owned, continuity.Content{SHA256: managedTransferSHA(payload), Size: int64(len(payload))}
}
func managedTransferReadAll(t *testing.T, r *ReadyTransfer, maximum int) []byte {
	t.Helper()
	var got []byte
	for {
		b, err := r.ReadChunk(context.Background(), maximum)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 || len(b) > maximum {
			t.Fatalf("unbounded/no-progress buffer: %d", len(b))
		}
		got = append(got, b...)
	}
	return got
}

func TestManagedTransferWholePrefixAndOwnership(t *testing.T) {
	payload := bytes.Repeat([]byte("exact prefix and whole bytes"), 2049)
	for _, tc := range []struct {
		name    string
		payload []byte
		offset  int64
		maximum int
	}{
		{"whole", payload, 0, 4093}, {"prefix", payload, 32771, MaxReadyTransferChunk}, {"offset_size", payload, int64(len(payload)), 1}, {"empty", nil, 0, 1}, {"maximum_buffer", bytes.Repeat([]byte("x"), MaxReadyTransferChunk+7), 0, MaxReadyTransferChunk},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, root, p, owned, content := managedTransferFixture(t, tc.payload)
			r, err := OpenReadyTransfer(context.Background(), owned, content, tc.offset, managedTransferSHA(tc.payload[:int(tc.offset)]))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := managedOpenReady(t, root, p); err == nil {
				t.Fatal("live transfer lost permanent lock")
			}
			if err := r.Revalidate(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := managedTransferReadAll(t, r, tc.maximum)
			if !bytes.Equal(got, tc.payload[int(tc.offset):]) {
				t.Fatal("transferred bytes differ from pinned suffix")
			}
			if err := r.VerifyComplete(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatalf("repeat Close changed outcome: %v", err)
			}
			next, err := managedOpenReady(t, root, p)
			if err != nil {
				t.Fatalf("terminal close retained lock: %v", err)
			}
			if err := next.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedTransferRefusalClosesConsumedOwner(t *testing.T) {
	for _, kind := range []string{"wrong_prefix", "negative_offset", "past_end", "invalid_content", "canceled", "whole_corruption"} {
		t.Run(kind, func(t *testing.T) {
			payload := []byte("independent whole content")
			path, root, p, owned, content := managedTransferFixture(t, payload)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			offset := int64(4)
			prefix := managedTransferSHA(payload[:4])
			switch kind {
			case "wrong_prefix":
				prefix = managedTransferSHA([]byte("nope"))
			case "negative_offset":
				offset = -1
			case "past_end":
				offset = content.Size + 1
			case "invalid_content":
				content.Size = -1
			case "canceled":
				cancel()
			case "whole_corruption":
				if err := os.WriteFile(filepath.Join(path, "ready", p.ID()+".payload"), bytes.Repeat([]byte("z"), len(payload)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if r, err := OpenReadyTransfer(ctx, owned, content, offset, prefix); err == nil || r != nil {
				t.Fatalf("invalid %s returned reader: %v %v", kind, r, err)
			}
			next, err := managedOpenReady(t, root, p)
			if err != nil {
				t.Fatalf("constructor failure retained FD/lock owner: %v", err)
			}
			if err := next.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedTransferTamperCancellationAndClose(t *testing.T) {
	for _, kind := range []string{"unsent_edit", "truncate", "replace_path", "replace_lock", "retained_cancel", "invalid_chunk", "early_complete", "descriptor_close"} {
		t.Run(kind, func(t *testing.T) {
			payload := []byte("original transfer bytes still unread")
			path, root, p, owned, content := managedTransferFixture(t, payload)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := OpenReadyTransfer(ctx, owned, content, 0, managedTransferSHA(nil))
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = r.Close()
				}
			})
			ready := filepath.Join(path, "ready", p.ID()+".payload")
			if kind == "unsent_edit" {
				first, err := r.ReadChunk(context.Background(), 3)
				if err != nil || !bytes.Equal(first, payload[:3]) {
					t.Fatalf("first admitted bytes: %q %v", first, err)
				}
				f, err := os.OpenFile(ready, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := f.WriteAt([]byte{'X'}, content.Size-1)
				closeErr := f.Close()
				if err := errors.Join(writeErr, closeErr); err != nil {
					t.Fatal(err)
				}
				_ = managedTransferReadAll(t, r, 7)
				if err := r.VerifyComplete(context.Background()); err == nil {
					t.Fatal("same-inode unsent content edit completed")
				}
			} else if kind == "early_complete" {
				if err := r.VerifyComplete(context.Background()); err == nil {
					t.Fatal("unread content completed")
				}
			} else {
				switch kind {
				case "truncate":
					if err := os.Truncate(ready, content.Size-1); err != nil {
						t.Fatal(err)
					}
				case "replace_path":
					if err := os.Rename(ready, ready+".original"); err != nil {
						t.Fatal(err)
					}
					managedTestFile(t, ready, payload)
				case "replace_lock":
					lock := filepath.Join(path, "attempts", p.ID(), "owner.lock")
					if err := os.Rename(lock, lock+".original"); err != nil {
						t.Fatal(err)
					}
					managedTestFile(t, lock, nil)
				case "retained_cancel":
					cancel()
				case "descriptor_close":
					if err := owned.read.Close(); err != nil {
						t.Fatal(err)
					}
				}
				maximum := 7
				if kind == "invalid_chunk" {
					maximum = MaxReadyTransferChunk + 1
				}
				b, err := r.ReadChunk(context.Background(), maximum)
				if err == nil || len(b) != 0 {
					t.Fatalf("failed read released bytes: %q %v", b, err)
				}
				if kind == "retained_cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("Background revived retained cancellation: %v", err)
				}
				if err := r.VerifyComplete(context.Background()); err == nil {
					t.Fatal("failed reader later completed")
				}
			}
			err = r.Close()
			closed = true
			if kind == "descriptor_close" {
				if err == nil {
					t.Fatal("original FD close uncertainty hidden")
				}
				if repeated := r.Close(); repeated != err {
					t.Fatal("repeat Close changed original error")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// For intact paths, terminal closure releases the actual permanent lock.
			if kind != "truncate" && kind != "replace_path" && kind != "replace_lock" {
				next, err := managedOpenReady(t, root, p)
				if err != nil {
					t.Fatalf("terminal close retained lock: %v", err)
				}
				if err := next.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

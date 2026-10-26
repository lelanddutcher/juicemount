package privatefile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func nativeReceiveDigest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

func TestNativeReceiveOwnedAppendResumeAndWholeHash(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	initial, err := copy.CreateZeroPartial()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	actual, prefix, whole, err := copy.HashPartial(context.Background(), 0)
	if err != nil || actual != initial || prefix != nativeReceiveDigest(nil) || whole != prefix {
		t.Fatalf("zero hash: %+v %q %q %v", actual, prefix, whole, err)
	}
	appendOwner, err := copy.OpenAppend(initial, 7)
	if err != nil {
		t.Fatal(err)
	}
	firstOwner := appendOwner
	t.Cleanup(func() {
		if err := firstOwner.Close(); err != nil {
			t.Errorf("first append original close: %v", err)
		}
	})
	if n, err := appendOwner.Write([]byte("abc")); err != nil || n != 3 {
		t.Fatalf("first append: %d %v", n, err)
	}
	if err := appendOwner.Close(); err != nil {
		t.Fatalf("first original close: %v", err)
	}
	if err := appendOwner.Close(); err != nil {
		t.Fatalf("cached original close: %v", err)
	}
	actual, prefix, whole, err = copy.HashPartial(context.Background(), 3)
	if err != nil || actual.Size != 3 || prefix != nativeReceiveDigest([]byte("abc")) || whole != prefix {
		t.Fatalf("first durable prefix: %+v %q %q %v", actual, prefix, whole, err)
	}
	if unexpected, err := copy.OpenAppend(initial, 7); !errors.Is(err, ErrNativeCopyChanged) || unexpected != nil {
		if unexpected != nil {
			err = errors.Join(err, unexpected.Close())
		}
		t.Fatalf("stale zero identity opened for append: %v", err)
	}
	appendOwner, err = copy.OpenAppend(actual, 7)
	if err != nil {
		t.Fatal(err)
	}
	secondOwner := appendOwner
	t.Cleanup(func() {
		if err := secondOwner.Close(); err != nil {
			t.Errorf("second append original close: %v", err)
		}
	})
	if n, err := appendOwner.Write([]byte("defg")); err != nil || n != 4 {
		t.Fatalf("resume append: %d %v", n, err)
	}
	if n, err := appendOwner.Write([]byte("x")); n != 0 || !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("beyond selected maximum appended: %d %v", n, err)
	}
	if err := appendOwner.Close(); err != nil {
		t.Fatalf("failed writer original close: %v", err)
	}
	actual, prefix, whole, err = copy.HashPartial(context.Background(), 3)
	if err != nil || actual.Size != 7 || prefix != nativeReceiveDigest([]byte("abc")) || whole != nativeReceiveDigest([]byte("abcdefg")) {
		t.Fatalf("full durable hash: %+v %q %q %v", actual, prefix, whole, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := copy.HashPartial(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hash continued: %v", err)
	}
}

func TestNativeReceiveAppendRejectsPartialInodeSubstitution(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	original, err := copy.CreateZeroPartial()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(path, "staging", nativeCopyTestID(), "payload.partial")
	moved, replaced := false, false
	t.Cleanup(func() {
		if replaced {
			if err := os.Remove(leaf); err != nil {
				t.Errorf("remove replacement partial: %v", err)
			}
		}
		if moved {
			if err := os.Rename(leaf+".old", leaf); err != nil {
				t.Errorf("restore original partial: %v", err)
			}
		}
	})
	if err := os.Rename(leaf, leaf+".old"); err != nil {
		t.Fatal(err)
	}
	moved = true
	if err := os.WriteFile(leaf, nil, 0600); err != nil {
		t.Fatal(err)
	}
	replaced = true
	if unexpected, err := copy.OpenAppend(original, 10); !errors.Is(err, ErrNativeCopyChanged) || unexpected != nil {
		if unexpected != nil {
			err = errors.Join(err, unexpected.Close())
		}
		t.Fatalf("replacement inode adopted: %v", err)
	}
	if data, err := os.ReadFile(leaf); err != nil || len(data) != 0 {
		t.Fatalf("replacement mutated: %q %v", data, err)
	}
}

func TestNativeReceiveNamedNoReplaceAndCancellationBoundary(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	zero, err := copy.CreateZeroPartial()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	appendOwner, err := copy.OpenAppend(zero, 4)
	if err != nil {
		t.Fatal(err)
	}
	ownedAppend := appendOwner
	t.Cleanup(func() {
		if err := ownedAppend.Close(); err != nil {
			t.Errorf("named append original close: %v", err)
		}
	})
	if n, err := appendOwner.Write([]byte("data")); err != nil || n != 4 {
		t.Fatalf("append: %d %v", n, err)
	}
	if err := appendOwner.Close(); err != nil {
		t.Fatal(err)
	}
	actual, _, digest, err := copy.HashPartial(context.Background(), 0)
	if err != nil || digest != nativeReceiveDigest([]byte("data")) {
		t.Fatalf("hash: %+v %s %v", actual, digest, err)
	}
	control, err = root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	target, err := control.EnsureTargetDirectory(nativeCopyTestID())
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if exposed, err := copy.ExposeNoReplace(ctx, target, "movie.mp4", actual); exposed || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rename reached syscall: %v %v", exposed, err)
	}
	collision := filepath.Join(path, "copies", nativeCopyTestID(), "movie.mp4")
	collisionActive := false
	t.Cleanup(func() {
		if collisionActive {
			if err := os.Remove(collision); err != nil {
				t.Errorf("remove collision: %v", err)
			}
		}
	})
	if err := os.WriteFile(collision, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	collisionActive = true
	if exposed, err := copy.ExposeNoReplace(context.Background(), target, "movie.mp4", actual); exposed || err == nil {
		t.Fatalf("foreign target collision replaced: %v %v", exposed, err)
	}
	if got, err := os.ReadFile(collision); err != nil || string(got) != "foreign" {
		t.Fatalf("foreign target mutated: %q %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(path, "staging", nativeCopyTestID(), "payload.partial")); err != nil || string(got) != "data" {
		t.Fatalf("source partial mutated by collision: %q %v", got, err)
	}
	if err := os.Remove(collision); err != nil {
		t.Fatal(err)
	}
	collisionActive = false
	if exposed, err := copy.ExposeNoReplace(context.Background(), target, "movie.mp4", actual); !exposed || err != nil {
		t.Fatalf("named rename: %v %v", exposed, err)
	}
	if _, err := os.Lstat(filepath.Join(path, "staging", nativeCopyTestID(), "payload.partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial survived rename: %v", err)
	}
	final, got, err := copy.HashTarget(context.Background(), "movie.mp4", target, 4)
	if err != nil || final != actual || got != digest {
		t.Fatalf("named final identity/hash: %+v %s %v", final, got, err)
	}
	if exposed, err := copy.ExposeNoReplace(context.Background(), target, "second.mp4", actual); exposed || err == nil {
		t.Fatalf("second rename replaced result: %v %v", exposed, err)
	}
}

type nativeTargetSwapContext struct {
	context.Context
	calls   int
	swap    func() error
	swapErr error
}

func (c *nativeTargetSwapContext) Err() error {
	c.calls++
	if c.calls == 2 {
		c.swapErr = c.swap()
	}
	if c.swapErr != nil {
		return c.swapErr
	}
	return c.Context.Err()
}

func TestNativeReceiveHashTargetRejectsReplacedLiveDirectory(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	zero, err := copy.CreateZeroPartial()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 65536)
	for i := range body {
		body[i] = byte(i)
	}
	appendOwner, err := copy.OpenAppend(zero, int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	ownedAppend := appendOwner
	t.Cleanup(func() {
		if err := ownedAppend.Close(); err != nil {
			t.Errorf("hash append original close: %v", err)
		}
	})
	if n, err := appendOwner.Write(body); err != nil || n != len(body) {
		t.Fatalf("write: %d %v", n, err)
	}
	if err := appendOwner.Close(); err != nil {
		t.Fatal(err)
	}
	file, _, digest, err := copy.HashPartial(context.Background(), 0)
	if err != nil || digest != nativeReceiveDigest(body) {
		t.Fatalf("partial: %+v %s %v", file, digest, err)
	}
	control, err = root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	target, err := control.EnsureTargetDirectory(nativeCopyTestID())
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if exposed, err := copy.ExposeNoReplace(context.Background(), target, "movie.mp4", file); !exposed || err != nil {
		t.Fatalf("expose: %v %v", exposed, err)
	}
	live := filepath.Join(path, "copies", nativeCopyTestID())
	escaped := filepath.Join(t.TempDir(), "held-original")
	replaced := false
	t.Cleanup(func() {
		if !replaced {
			return
		}
		if err := os.Remove(filepath.Join(live, "movie.mp4")); err != nil {
			t.Errorf("remove substitute: %v", err)
		}
		if err := os.Remove(live); err != nil {
			t.Errorf("remove substitute directory: %v", err)
		}
		if err := os.Rename(escaped, live); err != nil {
			t.Errorf("restore original target: %v", err)
		}
	})
	ctx := &nativeTargetSwapContext{Context: context.Background(), swap: func() error {
		if err := os.Rename(live, escaped); err != nil {
			return err
		}
		replaced = true
		if err := os.Mkdir(live, 0700); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(live, "movie.mp4"), []byte("substitute"), 0600)
	}}
	if _, _, err := copy.HashTarget(ctx, "movie.mp4", target, int64(len(body))); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("detached original hash claimed live target: %v", err)
	}
	if ctx.calls < 2 || ctx.swapErr != nil {
		t.Fatalf("swap boundary not exercised: calls=%d err=%v", ctx.calls, ctx.swapErr)
	}
}

func TestNativeReceiveReceiptWriteFailureRetainsExactLeafAndBarrierRetry(t *testing.T) {
	for _, record := range []string{"prepared", "verified"} {
		for _, barrier := range []string{"file", "directory"} {
			t.Run(record+"_"+barrier, func(t *testing.T) {
				_, root := nativeCopyTestRoot(t)
				control := nativeCopyTestControl(t, root)
				copy := nativeCopyTestCreate(t, control)
				if record == "prepared" {
					if _, err := control.EnsureTargetDirectory(nativeCopyTestID()); err != nil {
						t.Fatal(err)
					}
				}
				if err := control.Close(); err != nil {
					t.Fatal(err)
				}
				fault := errors.New("injected original receipt sync failure")
				copy.writeOps = &nativeWriteOps{}
				if barrier == "file" {
					copy.writeOps.syncFile = func(*os.File) error { return fault }
				} else {
					copy.writeOps.syncDir = func(*os.File) error { return fault }
				}
				line := []byte("exact immutable receipt\n")
				var write func([]byte) error
				var syncExact func() error
				var read func() ([]byte, error)
				if record == "prepared" {
					fresh, err := root.LockControl()
					if err != nil {
						t.Fatal(err)
					}
					target, _, err := fresh.TargetDirectory(nativeCopyTestID())
					if e := fresh.Close(); e != nil {
						err = errors.Join(err, e)
					}
					if err != nil {
						t.Fatal(err)
					}
					write = func(b []byte) error { return copy.WritePreparedOwned(b, target) }
					syncExact = copy.SyncPreparedOwned
					read = copy.ReadPreparedOwned
				} else {
					write = copy.WriteVerifiedOwned
					syncExact = copy.SyncVerifiedOwned
					read = copy.ReadVerifiedOwned
				}
				if err := write(line); !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
					t.Fatalf("complete receipt write fault: %v", err)
				}
				if got, err := read(); err != nil || string(got) != string(line) {
					t.Fatalf("retained exact receipt: %q %v", got, err)
				}
				if err := write(line); !errors.Is(err, os.ErrExist) {
					t.Fatalf("uncertain receipt overwritten: %v", err)
				}
				copy.writeOps = nil
				if err := syncExact(); err != nil {
					t.Fatalf("exact owned barrier retry: %v", err)
				}
			})
		}
	}
}

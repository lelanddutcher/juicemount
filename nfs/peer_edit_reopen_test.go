package nfs

import (
	"os"
	"path/filepath"
	"testing"
)

// Bug B1 (laptop run 2026-11-13): a peer's edit of a file this process holds
// open stayed invisible, because the pooled read descriptor kept juicefs's
// open-file slice list and nothing reopened it. Both content hooks (the
// juicemount:metadata update path and the SCAN / folder / attr-push upsert
// path) must now reopen the READ slot, and must never touch the WRITE slot.
func TestContentHooksReopenReadSlotOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		fire func(h *JuiceMountHandler, p string)
	}{
		{"update event", func(h *JuiceMountHandler, p string) { h.onContentInvalidated(p, false) }},
		{"scan upsert", func(h *JuiceMountHandler, p string) { h.onScanContentInvalidated(p, 42) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			full := filepath.Join(dir, "clip.mov")
			if err := os.WriteFile(full, []byte("old"), 0o644); err != nil {
				t.Fatal(err)
			}
			p := NewFDPool()
			defer p.Stop()
			h := &JuiceMountHandler{fdPool: p, fusePath: dir}
			rfd, err := p.Get(full)
			if err != nil {
				t.Fatal(err)
			}
			wfd, err := p.GetWrite(full, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			p.Release(full)
			p.ReleaseWrite(full)

			tc.fire(h, "clip.mov")

			wfd2, err := p.GetWrite(full, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if wfd2 != wfd {
				t.Fatal("a content hook dropped the WRITE slot (the xattr-loss bug)")
			}
			p.ReleaseWrite(full)
			rfd2, err := p.Get(full)
			if err != nil {
				t.Fatal(err)
			}
			if rfd2 == rfd {
				t.Fatal("the READ slot survived a content change: a peer's edit stays invisible (B1)")
			}
			p.Release(full)
		})
	}
}

// A held read descriptor cannot be closed under its reader; the deferred-close
// variant must still mark it so the next Get opens afresh.
func TestInvalidateReadsDeferCloseMarksHeldEntry(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "clip.mov")
	if err := os.WriteFile(full, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := NewFDPool()
	defer p.Stop()
	held, err := p.Get(full)
	if err != nil {
		t.Fatal(err)
	}
	if closed, marked := p.InvalidateReadsDeferClose(full); closed != 0 || marked != 1 {
		t.Fatalf("held entry: closed %d, marked %d; want 0, 1", closed, marked)
	}
	if _, err := held.ReadAt(make([]byte, 3), 0); err != nil {
		t.Fatalf("the holder's fd was closed under it: %v", err)
	}
	fresh, err := p.Get(full)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == held {
		t.Fatal("a stale held entry was served again")
	}
	p.Release(full)
	p.Release(full)
}

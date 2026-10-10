package nfs

import (
	"errors"
	"os"
	"testing"
)

func TestFDPoolRejectsReadAndWriteAfterStop(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/closed.dat"
	if err := os.WriteFile(path, []byte("closed"), 0o600); err != nil {
		t.Fatal(err)
	}

	p := NewFDPool()
	p.Stop()
	p.Stop() // shutdown is intentionally idempotent

	if fd, err := p.Get(path); fd != nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Get after Stop = (%v, %v), want (nil, ErrClosed)", fd, err)
	}
	if fd, err := p.GetWrite(path, os.O_RDWR, 0o600); fd != nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("GetWrite after Stop = (%v, %v), want (nil, ErrClosed)", fd, err)
	}
	if open, active := p.Stats(); open != 0 || active != 0 {
		t.Fatalf("Stats after Stop = (%d, %d), want (0, 0)", open, active)
	}
	// Late cleanup and invalidation from an RPC tail must stay harmless.
	p.Release(path)
	p.ReleaseWrite(path)
	p.Invalidate(path)
}

func TestFDPoolClosesReadOpenThatFinishesAfterStop(t *testing.T) {
	dir := t.TempDir()
	fifo := mkfifoT(t, dir, "read-after-stop")
	p := NewFDPool()

	result := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(fifo)
		result <- getResult{fd: fd, err: err}
	}()
	waitOpensInFlight(t, p, 1)
	p.Stop()

	writer, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	got := <-result
	if got.fd != nil || !errors.Is(got.err, os.ErrClosed) {
		t.Fatalf("in-flight Get after Stop = (%v, %v), want (nil, ErrClosed)", got.fd, got.err)
	}
}

func TestFDPoolClosesWriteOpenThatFinishesAfterStop(t *testing.T) {
	dir := t.TempDir()
	fifo := mkfifoT(t, dir, "write-after-stop")
	p := NewFDPool()

	result := make(chan getResult, 1)
	go func() {
		fd, err := p.GetWrite(fifo, os.O_WRONLY, 0o600)
		result <- getResult{fd: fd, err: err}
	}()
	waitOpensInFlight(t, p, 1)
	p.Stop()

	reader, err := os.OpenFile(fifo, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()

	got := <-result
	if got.fd != nil || !errors.Is(got.err, os.ErrClosed) {
		t.Fatalf("in-flight GetWrite after Stop = (%v, %v), want (nil, ErrClosed)", got.fd, got.err)
	}
}

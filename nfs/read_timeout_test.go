package nfs

import (
	"io"
	"testing"
	"time"
)

func TestReadAtCallBoundedRetainsCompletionUntilSyscallReturns(t *testing.T) {
	unblock := make(chan struct{})
	after := make(chan struct{})
	buf := make([]byte, 4)
	start := time.Now()
	n, err, done := readAtCallBounded(func(p []byte, _ int64) (int, error) {
		<-unblock
		copy(p, "late")
		return 4, io.EOF
	}, buf, 0, 25*time.Millisecond, func() { close(after) })
	if done || n != 0 || err != nil {
		t.Fatalf("timed read = (%d, %v, done=%v), want (0, nil, false)", n, err, done)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("timeout returned after %v, want prompt bounded return", elapsed)
	}
	select {
	case <-after:
		t.Fatal("completion callback ran before blocked syscall returned")
	default:
	}
	close(unblock)
	select {
	case <-after:
	case <-time.After(time.Second):
		t.Fatal("completion callback did not run after syscall returned")
	}
	if string(buf) != "late" {
		t.Fatalf("late syscall result = %q, want late", buf)
	}
}

func TestReadAtCallBoundedHealthy(t *testing.T) {
	buf := make([]byte, 4)
	after := false
	n, err, done := readAtCallBounded(func(p []byte, _ int64) (int, error) {
		return copy(p, "done"), nil
	}, buf, 0, time.Second, func() { after = true })
	if !done || n != 4 || err != nil || string(buf) != "done" || !after {
		t.Fatalf("healthy read = (%d, %v, done=%v, buf=%q, after=%v)", n, err, done, buf, after)
	}
}

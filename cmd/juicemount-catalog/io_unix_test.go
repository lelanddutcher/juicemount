//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func testFile(t *testing.T, name string) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}
func testPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	return r, w
}

// Keep an independent descriptor to the SAME description after runOwned closes
// its original and I/O duplicates. Use Control rather than File.Fd(), which can
// itself switch a Go file to blocking mode and spoil the flag oracle.
func flagMonitor(t *testing.T, f *os.File, blocking bool) (int, int) {
	t.Helper()
	connection, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	monitor, flags := -1, 0
	var opErr error
	err = connection.Control(func(fd uintptr) {
		flags, opErr = descriptorFlags(int(fd))
		if opErr != nil {
			return
		}
		if blocking {
			flags &^= syscall.O_NONBLOCK
			opErr = setDescriptorFlags(int(fd), flags)
			if opErr != nil {
				return
			}
		}
		monitor, opErr = duplicateDescriptor(fd)
	})
	if err != nil || opErr != nil {
		t.Fatalf("flag monitor: %v %v", err, opErr)
	}
	t.Cleanup(func() { _ = syscall.Close(monitor) })
	return monitor, flags
}
func fillPipe(t *testing.T, w *os.File) {
	t.Helper()
	connection, err := w.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var opErr error
	written := 0
	err = connection.Control(func(fd uintptr) {
		flags, e := descriptorFlags(int(fd))
		if e != nil {
			opErr = e
			return
		}
		if e = setDescriptorFlags(int(fd), flags|syscall.O_NONBLOCK); e != nil {
			opErr = e
			return
		}
		defer func() { opErr = errors.Join(opErr, setDescriptorFlags(int(fd), flags)) }()
		block := make([]byte, 4096)
		for i := 0; i < 4096; i++ {
			n, e := syscall.Write(int(fd), block)
			written += n
			if errors.Is(e, syscall.EAGAIN) {
				return
			}
			if e != nil {
				opErr = e
				return
			}
		}
		opErr = errors.New("pipe did not fill within bound")
	})
	if err != nil || opErr != nil || written <= 0 {
		t.Fatalf("pipe fill: bytes=%d error=%v %v", written, err, opErr)
	}
}

type observedResource struct {
	fileResource
	entered chan struct{}
	read    bool
	once    sync.Once
	closes  atomic.Int32
}

func (f *observedResource) Read(p []byte) (int, error) {
	if f.read {
		f.once.Do(func() { close(f.entered) })
	}
	return f.fileResource.Read(p)
}
func (f *observedResource) Write(p []byte) (int, error) {
	if !f.read {
		f.once.Do(func() { close(f.entered) })
	}
	return f.fileResource.Write(p)
}
func (f *observedResource) Close() error { f.closes.Add(1); return f.fileResource.Close() }

func TestCLIOwnedPipeCancellationJoinsAndRestoresFlags(t *testing.T) {
	for _, kind := range []string{"stdin", "stdout", "stderr"} {
		t.Run(kind, func(t *testing.T) {
			input, output, diagnostic := testFile(t, "input"), testFile(t, "output"), testFile(t, "diagnostic")
			index := 0
			read := true
			args := cliArgs(filepath.Join(t.TempDir(), "restore.sqlite"), []string{"--max-bytes", "8192", "--max-events", "4", "--max-rows", "16"}, "restore")
			var monitored *os.File
			var peer *os.File
			if kind == "stdin" {
				r, w := testPipe(t)
				input = r
				monitored = r
				peer = w
			} else {
				r, w := testPipe(t)
				peer = r
				fillPipe(t, w)
				monitored = w
				read = false
				if kind == "stdout" {
					output = w
					index = 1
					args = []string{"subject", "new-id"}
				} else {
					diagnostic = w
					index = 2
					args = []string{"--invalid-option"}
				}
			}
			monitor, wantFlags := flagMonitor(t, monitored, true)
			files := defaultFiles()
			prepare := files.stdio
			entered := make(chan struct{})
			var observed *observedResource
			files.stdio = func(originals []*os.File) ([]fileResource, func() error, error) {
				prepared, cleanup, err := prepare(originals)
				if err != nil {
					return nil, nil, err
				}
				observed = &observedResource{fileResource: prepared[index], entered: entered, read: read}
				prepared[index] = observed
				return prepared, cleanup, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan int, 1)
			go func() { result <- runConfigured(ctx, args, input, output, diagnostic, true, files) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				cancel()
				_ = peer.Close()
				<-result
				t.Fatal("owned pipe operation was not attempted")
			}
			select {
			case code := <-result:
				t.Fatalf("full/open pipe operation returned before cancellation: %d", code)
			default:
			}
			cancel()
			select {
			case code := <-result:
				if code != 3 {
					t.Fatalf("cancellation exit=%d", code)
				}
			case <-time.After(10 * time.Second):
				_ = peer.Close()
				<-result
				t.Fatal("canceled owned pipe did not drain")
			}
			got, err := descriptorFlags(monitor)
			if err != nil || got != wantFlags {
				t.Fatalf("inherited flags changed: got=%d want=%d err=%v", got, wantFlags, err)
			}
			if observed == nil || observed.closes.Load() != 1 {
				t.Fatal("owned stream not closed exactly once")
			}
		})
	}
}

func TestCLIOwnedDeadlineAndClosedOutputReturnAfterCleanup(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		r, w := testPipe(t)
		inputMonitor, flags := flagMonitor(t, r, true)
		output, diagnostic := testFile(t, "out"), testFile(t, "err")
		args := cliArgs(filepath.Join(t.TempDir(), "catalog.sqlite"), []string{"--timeout", "50ms", "--max-bytes", "8192", "--max-events", "4", "--max-rows", "16"}, "restore")
		done := make(chan int, 1)
		go func() { done <- runOwned(context.Background(), args, r, output, diagnostic) }()
		select {
		case code := <-done:
			if code != 3 {
				t.Fatalf("deadline exit=%d", code)
			}
		case <-time.After(10 * time.Second):
			_ = w.Close()
			<-done
			t.Fatal("main-owned deadline did not cancel input")
		}
		got, err := descriptorFlags(inputMonitor)
		if err != nil || got != flags {
			t.Fatal("deadline did not restore inherited flags")
		}
	})
	t.Run("closed-output", func(t *testing.T) {
		r, w := testPipe(t)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		monitor, flags := flagMonitor(t, w, true)
		code := runOwned(context.Background(), []string{"subject", "new-id"}, testFile(t, "in"), w, testFile(t, "err"))
		if code != 3 {
			t.Fatalf("closed output exit=%d", code)
		}
		got, err := descriptorFlags(monitor)
		if err != nil || got != flags {
			t.Fatal("EPIPE cleanup did not restore flags")
		}
	})
}

func TestCLIStdioPreparationRestoresOnSuccessAndSetupFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "setup-failure"
		}
		t.Run(name, func(t *testing.T) {
			r, w := testPipe(t)
			monitor, flags := flagMonitor(t, w, true)
			input, diagnostic := testFile(t, "input"), testFile(t, "diagnostic")
			if fail {
				if err := diagnostic.Close(); err != nil {
					t.Fatal(err)
				}
			}
			files, cleanup, err := prepareOwnedStdio([]*os.File{input, w, diagnostic})
			if fail {
				if err == nil || cleanup != nil {
					t.Fatal("closed stdio setup accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range files {
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if err := cleanup(); err != nil {
					t.Fatal(err)
				}
			}
			got, err := descriptorFlags(monitor)
			if err != nil || got != flags {
				t.Fatalf("stdio flags not restored: %d/%d %v", got, flags, err)
			}
			_ = r
		})
	}
}

func TestCLINamedInputFIFORefusesBeforeReservation(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "input.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "target.sqlite")
	done := make(chan int, 1)
	go func() {
		code, _, _ := invokeCLI(path, []string{"--input", fifo, "--max-bytes", "8192", "--max-events", "4", "--max-rows", "16"}, nil, "restore")
		done <- code
	}()
	select {
	case code := <-done:
		if code != 3 {
			t.Fatalf("FIFO accepted: %d", code)
		}
	case <-time.After(10 * time.Second):
		// Release the exact regression under test: a blocking read-only FIFO
		// open. Closing its writer also supplies EOF if the reader progressed.
		writer, openErr := syscall.Open(fifo, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
		var closeErr error
		if openErr == nil {
			closeErr = syscall.Close(writer)
		}
		select {
		case code := <-done:
			t.Fatalf("named FIFO open blocked; invocation joined after cleanup: code=%d writer open=%v close=%v", code, openErr, closeErr)
		case <-time.After(2 * time.Second):
			t.Fatalf("named FIFO open blocked; cleanup could not join invocation: writer open=%v close=%v", openErr, closeErr)
		}
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("FIFO restore reserved target: %v", err)
	}
}

type borrowedBuffer struct {
	bytes.Buffer
	closed bool
}

func (b *borrowedBuffer) Close() error { b.closed = true; return nil }
func TestCLIBorrowedIOKeepsCallerOwnershipAndReportsWriteFailure(t *testing.T) {
	input, output, diagnostic := &borrowedBuffer{}, &borrowedBuffer{}, &borrowedBuffer{}
	if code := run(context.Background(), []string{"subject", "new-id"}, input, output, diagnostic); code != 0 {
		t.Fatalf("borrowed run=%d", code)
	}
	if input.closed || output.closed || diagnostic.closed {
		t.Fatal("borrowed I/O was closed")
	}
	writer := &onceFailWriter{err: io.ErrClosedPipe}
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"subject", "new-id"}, bytes.NewReader(nil), writer, &stderr); code != 3 {
		t.Fatal("borrowed writer error discarded")
	}
}

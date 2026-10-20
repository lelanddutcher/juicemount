package main

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

// run borrows generic I/O: context checks cannot interrupt an arbitrary blocked
// Read/Write. main uses runOwned with OS files whose ownership is transferred.
// There is no goroutine per read or write, and the sole cancellation watcher is
// joined before restoring inherited descriptor flags or returning.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return runConfigured(ctx, args, stdin, stdout, stderr, false, defaultFiles())
}
func runOwned(ctx context.Context, args []string, stdin, stdout, stderr *os.File) int {
	return runConfigured(ctx, args, stdin, stdout, stderr, true, defaultFiles())
}

type fileResource interface {
	io.Reader
	io.Writer
	io.Closer
	Sync() error
	Name() string
}
type cliFiles struct {
	reserve func(string) (fileResource, error)
	input   func(string) (fileResource, error)
	stdio   func([]*os.File) ([]fileResource, func() error, error)
}

func defaultFiles() cliFiles {
	return cliFiles{
		reserve: func(path string) (fileResource, error) { return reserveOutput(path) },
		input:   func(path string) (fileResource, error) { return openInput(path) },
		stdio: func(originals []*os.File) ([]fileResource, func() error, error) {
			prepared, cleanup, err := prepareOwnedStdio(originals)
			if err != nil {
				return nil, nil, err
			}
			files := make([]fileResource, len(prepared))
			for i, f := range prepared {
				files[i] = f
			}
			return files, cleanup, nil
		},
	}
}

type ownedFile struct {
	fileResource
	once     sync.Once
	closeErr error
}

func (f *ownedFile) Close() error {
	f.once.Do(func() { f.closeErr = f.fileResource.Close() })
	return f.closeErr
}

type fileOwners struct {
	mu      sync.Mutex
	files   []*ownedFile
	stopped bool
}

func (o *fileOwners) track(f fileResource) *ownedFile {
	owned := &ownedFile{fileResource: f}
	o.mu.Lock()
	stopped := o.stopped
	o.files = append(o.files, owned)
	o.mu.Unlock()
	if stopped {
		_ = owned.Close()
	}
	return owned
}
func (o *fileOwners) closeAll() error {
	o.mu.Lock()
	o.stopped = true
	files := append([]*ownedFile(nil), o.files...)
	o.mu.Unlock()
	var err error
	for _, f := range files {
		err = errors.Join(err, f.Close())
	}
	return err
}
func (o *fileOwners) watch(ctx context.Context) func() error {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = o.closeAll()
		case <-stop:
		}
	}()
	return func() error { close(stop); <-done; return o.closeAll() }
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

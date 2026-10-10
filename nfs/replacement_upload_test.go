package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
)

type replacementMemoryIO struct {
	data           []byte
	record         replaceguard.File
	fault          string
	creates, opens int
	mode           os.FileMode
}

func (b *replacementMemoryIO) Check() error { return nil }
func (b *replacementMemoryIO) Open(name string, create bool) (replacementPublishFile, error) {
	if create {
		b.creates++
		if b.record.Inode != 0 {
			return nil, os.ErrExist
		}
		b.record = replaceguard.File{Name: name, Parent: 3, Inode: 7, CtimeNS: 123}
	} else {
		b.opens++
	}
	return &replacementMemoryFile{b: b, created: create}, nil
}
func (b *replacementMemoryIO) Create(name string, mode os.FileMode) (replacementPublishFile, error) {
	f, err := b.Open(name, true)
	if err == nil {
		b.mode = mode
	}
	return f, err
}
func (b *replacementMemoryIO) OpenOwned(name string, expected replaceguard.File) (replacementPublishFile, error) {
	if b.record != expected {
		return nil, metadata.ErrMirrorConflict
	}
	return b.Open(name, false)
}
func (b *replacementMemoryIO) Stat(string) (replaceguard.File, error) { return b.record, nil }
func (b *replacementMemoryIO) Rename(string, string) error            { panic("upload must not rename") }

type replacementMemoryFile struct {
	b       *replacementMemoryIO
	created bool
}

func (f *replacementMemoryFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.b.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (f *replacementMemoryFile) WriteAt(p []byte, off int64) (int, error) {
	size := int(off) + len(p)
	if size > len(f.b.data) {
		f.b.data = append(f.b.data, make([]byte, size-len(f.b.data))...)
	}
	copy(f.b.data[off:], p)
	f.b.record.Length = uint64(len(f.b.data))
	return len(p), nil
}
func (f *replacementMemoryFile) Stat() (replaceguard.File, error) { return f.b.record, nil }
func (f *replacementMemoryFile) SetMtime(ns int64) error          { f.b.record.MtimeNS = ns; return nil }
func (f *replacementMemoryFile) SetMode(mode os.FileMode) error   { f.b.mode = mode; return nil }
func (f *replacementMemoryFile) Sync() error {
	if f.b.fault == "sync" {
		return errors.New("injected backend fsync refusal")
	}
	return nil
}
func (f *replacementMemoryFile) Close() error {
	if f.created {
		f.created = false
		if f.b.fault == "substitute" {
			f.b.record.Inode++
		}
		if f.b.fault == "corrupt" && len(f.b.data) > 0 {
			f.b.data[0] ^= 1
		}
	}
	return nil
}
func TestReplacementUploadRetainsExactCreatedStage(t *testing.T) {
	for _, fault := range []string{"success", "empty", "substitute", "corrupt", "sync", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			spool := newTestSpoolStore(t, 0)
			body := []byte("accepted bytes retained after upload ambiguity")
			if fault == "empty" {
				body = nil
			}
			name := filepath.Join(spool.root, SpoolFilesSubdir, "source.bin")
			if err := os.WriteFile(name, body, 0600); err != nil {
				t.Fatal(err)
			}
			image, err := spool.openReplacementImage(name, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			witness, err := image.Seal()
			if err != nil {
				t.Fatal(err)
			}
			backend := &replacementMemoryIO{fault: fault}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if fault == "canceled" {
				cancel()
			}
			record, err := uploadReplacementStage(ctx, backend, ".juicemount-streaming-drain-test", image, witness, 0644, func() (replacementPublishFile, error) {
				return backend.Create(".juicemount-streaming-drain-test", 0644)
			})
			success := fault == "success" || fault == "empty"
			if success {
				if err != nil || record.Inode != 7 || !bytes.Equal(backend.data, body) || backend.mode != 0644 || backend.creates != 1 || backend.opens != 1 {
					t.Fatalf("upload result=%+v err=%v backend=%+v", record, err, backend)
				}
			} else if err == nil {
				t.Fatal("uncertain stage accepted")
			}
			if fault == "substitute" && !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatalf("substitute error=%v", err)
			}
			if fault == "canceled" && (backend.creates != 0 || !errors.Is(err, context.Canceled)) {
				t.Fatalf("canceled upload mutated backend: %+v %v", backend, err)
			}
			got, err := os.ReadFile(name)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("private source lost: %q %v", got, err)
			}
		})
	}
}

func TestReplacementFDSetMtimeRetainsDescriptor(t *testing.T) {
	name := filepath.Join(t.TempDir(), "stage")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Rename(name, name+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte("foreign name"), 0600); err != nil {
		t.Fatal(err)
	}
	foreign, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	const ns = int64(1700000000123456789)
	if err := replacementFDSetMtime(int(f.Fd()), ns); err != nil {
		t.Fatal(err)
	}
	kept, err := f.Stat()
	if err != nil || kept.ModTime().UnixNano() != ns {
		t.Fatalf("retained timestamp=%v err=%v", kept, err)
	}
	after, err := os.Stat(name)
	if err != nil || after.ModTime() != foreign.ModTime() {
		t.Fatal("timestamp mutation followed substituted name")
	}
}

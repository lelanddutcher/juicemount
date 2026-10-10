/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

type heldInodeAllocator struct {
	meta.Meta
	calls   atomic.Int32
	entered chan meta.Ino
	release chan struct{}
	status  syscall.Errno
}

func (m *heldInodeAllocator) NewSliceForInode(_ meta.Context, inode meta.Ino, id *uint64) syscall.Errno {
	m.calls.Add(1)
	m.entered <- inode
	<-m.release
	if m.status == 0 {
		*id = 123
	}
	return m.status
}

func TestInodeSliceAllocationSerializesInitialAndFlush(t *testing.T) {
	for _, status := range []syscall.Errno{0, syscall.EPERM} {
		t.Run(status.Error(), func(t *testing.T) {
			m := &heldInodeAllocator{entered: make(chan meta.Ino, 2), release: make(chan struct{}), status: status}
			f := &fileWriter{w: &dataWriter{m: m}, inode: 17}
			s := &sliceWriter{chunk: &chunkWriter{file: f}}
			var joined sync.WaitGroup
			joined.Add(2)
			go func() { defer joined.Done(); s.prepareID(meta.Background(), false) }()
			select {
			case inode := <-m.entered:
				if inode != 17 {
					t.Fatal("allocator lost inode identity")
				}
			case <-time.After(time.Second):
				t.Fatal("initial allocator did not enter")
			}
			started := make(chan struct{})
			go func() { defer joined.Done(); close(started); s.prepareID(meta.Background(), true) }()
			<-started
			select {
			case <-m.entered:
				close(m.release)
				joined.Wait()
				t.Fatal("flush allocated a second ID while initial registration was pending")
			case <-time.After(50 * time.Millisecond):
			}
			close(m.release)
			joined.Wait()
			if m.calls.Load() != 1 || s.err != status {
				t.Fatalf("calls=%d status=%v", m.calls.Load(), s.err)
			}
			if (status == 0 && s.id != 123) || (status != 0 && s.id != 0) {
				t.Fatalf("unbound or missing slice ID: %d", s.id)
			}
		})
	}
}

type smodeCase struct {
	mode uint16
	str  string
}

var cases = []smodeCase{
	{syscall.S_IFDIR | 00755, "drwxr-xr-x"},
	{syscall.S_IFREG | 01644, "-rw-r--r-T"},
	{syscall.S_IFLNK | 03755, "lrwxr-sr-t"},
	{syscall.S_IFSOCK | 06700, "srws--S---"},
}

func TestSmode(t *testing.T) {
	for _, s := range cases {
		res := smode(s.mode).String()
		if res != s.str {
			t.Fatalf("str of %o: %s != %s", s.mode, res, s.str)
		}
	}
}

func TestEntryString(t *testing.T) {
	var e *Entry
	if e.String() != "" {
		t.Fatalf("empty entry should be ''")
	}
	e = &Entry{Inode: 2, Name: []byte("test")}
	if e.String() != " (2)" {
		t.Fatalf("empty entry should be ` (2)`")
	}

	e.Attr = &meta.Attr{
		Typ:    meta.TypeFile,
		Mode:   01755,
		Nlink:  1,
		Uid:    2,
		Gid:    3,
		Atime:  4,
		Mtime:  5,
		Ctime:  6,
		Length: 7,
	}
	if e.String() != " (2,[-rwxr-xr-t:0101755,1,2,3,4,5,6,7])" {
		t.Fatalf("string of entry is not expected: %s", e.String())
	}
}

func TestError(t *testing.T) {
	if strerr(0) != "OK" {
		t.Fatalf("expect 'OK' but got %q", strerr(0))
	}
	if strerr(syscall.EACCES) != "permission denied" {
		t.Fatalf("expect 'Access denied', but got %q", strerr(syscall.EACCES))
	}
}

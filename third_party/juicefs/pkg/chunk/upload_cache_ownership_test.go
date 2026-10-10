package chunk

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

type retainedUploadCache struct {
	CacheManager
	page *Page
}

func (c *retainedUploadCache) cache(_ string, p *Page, _, _ bool) {
	p.Acquire()
	c.page = p
}

type shrinkingUploadCompressor struct{ fail bool }

func (c shrinkingUploadCompressor) Name() string            { return "test-shrinking" }
func (c shrinkingUploadCompressor) CompressBound(n int) int { return n }
func (c shrinkingUploadCompressor) Compress(dst, src []byte) (int, error) {
	n := len(src) / 2
	for i := 0; i < n; i++ {
		dst[i] = 0xa5
	}
	if c.fail {
		return 0, errors.New("test compression failure")
	}
	return n, nil
}
func (c shrinkingUploadCompressor) Decompress(_, _ []byte) (int, error) {
	return 0, errors.New("not used")
}

func TestUploadKeepsPublishedCachePageImmutable(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "compression-error"
		}
		t.Run(name, func(t *testing.T) {
			objects, err := object.CreateStorage("mem", "", "", "", "")
			if err != nil {
				t.Fatal(err)
			}
			conf := Config{BlockSize: 64 << 10, CacheDir: "memory", CacheSize: 1 << 20, BufferSize: 1 << 20, CacheLargeWrite: true, MaxUpload: 1, PutTimeout: time.Second, GetTimeout: time.Second, Compress: "none"}
			// Exercise upload directly without constructor background workers or
			// mutating dependencies after those workers have started.
			retained := &retainedUploadCache{}
			store := &cachedStore{storage: objects, conf: conf, bcache: retained,
				compressor: shrinkingUploadCompressor{fail: fail}}
			store.initMetrics()
			body := bytes.Repeat([]byte{0x44}, 64<<10)
			page := NewPage(append([]byte(nil), body...))
			t.Cleanup(func() {
				if retained.page != nil {
					retained.page.Release()
				}
			})
			const key = "chunks/0/0/1_0_65536"
			err = store.upload(key, page, &wSlice{})
			if (err != nil) != fail {
				t.Fatalf("upload err=%v fail=%v", err, fail)
			}
			if retained.page != page || !bytes.Equal(retained.page.Data, body) {
				t.Fatal("uploader mutated the cache consumer's retained Page header or bytes")
			}
			if refs := atomic.LoadInt32(&page.refs); refs != 1 {
				t.Fatalf("cache must be sole remaining owner: refs=%d", refs)
			}
			reader, err := objects.Get(key, 0, -1)
			if fail {
				if err == nil {
					_ = reader.Close()
					t.Fatal("failed compression uploaded an object")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			closeErr := reader.Close()
			if err != nil || closeErr != nil || !bytes.Equal(got, bytes.Repeat([]byte{0xa5}, len(body)/2)) {
				t.Fatalf("object must contain independent compressed output: read=%v close=%v size=%d", err, closeErr, len(got))
			}
		})
	}
}

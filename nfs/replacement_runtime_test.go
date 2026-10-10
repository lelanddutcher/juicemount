//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsfs "github.com/juicedata/juicefs/pkg/fs"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// replacementRuntime owns one proxy incarnation and one fresh SDK generation.
// The object root belongs to the test and survives stop/restart. CachedStore
// has no Close, so this models fresh object/client generations, not a process
// kill or complete goroutine retirement.
type replacementRuntime struct {
	fs         *jfsfs.FileSystem
	m          jfsmeta.Meta
	proxy      *respproxy.Proxy
	engine     *respproxy.LeaseEngine
	objects    object.ObjectStorage
	store      chunk.ChunkStore
	authHash   string
	url        string
	objectRoot string
	served     chan error
	once       sync.Once
}

func (r *replacementRuntime) stop(t *testing.T) {
	t.Helper()
	r.once.Do(func() {
		if r.fs != nil {
			if err := r.fs.Close(); err != nil {
				t.Errorf("replacement SDK close: %v", err)
			}
		}
		if r.m != nil {
			if err := r.m.Shutdown(); err != nil {
				t.Errorf("replacement metadata shutdown: %v", err)
			}
		}
		if r.engine != nil {
			r.engine.Close()
		}
		if r.proxy != nil {
			if err := r.proxy.Close(); err != nil {
				t.Errorf("replacement proxy close: %v", err)
			}
		}
		if r.served != nil {
			select {
			case err := <-r.served:
				if err != nil {
					t.Errorf("replacement proxy Serve: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("replacement proxy Serve did not join")
			}
		}
	})
}

func newReplacementRuntime(t *testing.T, x *boundCacheFixture, objectRoot string, disk bool) *replacementRuntime {
	return newReplacementRuntimeWithSweep(t, x, objectRoot, disk, time.Hour)
}

func newReplacementRuntimeWithSweep(t *testing.T, x *boundCacheFixture, objectRoot string, disk bool, sweepEvery time.Duration) *replacementRuntime {
	return newReplacementRuntimeWithBirth(t, x, objectRoot, disk, sweepEvery, true)
}

func newReplacementRuntimeWithBirth(t *testing.T, x *boundCacheFixture, objectRoot string, disk bool, sweepEvery time.Duration, birth bool) *replacementRuntime {
	t.Helper()
	if x == nil || x.volumeUUID == "" || !filepath.IsAbs(objectRoot) || sweepEvery <= 0 {
		t.Fatal("replacement runtime needs a qualified fixture and absolute object root")
	}
	if err := os.MkdirAll(objectRoot, 0700); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(x.raw)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	hash, err := respproxy.HashPassword("mac-secret", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	principals, err := respproxy.ParsePrincipals([]byte(fmt.Sprintf(
		`{"principals":[{"name":"mac-test","kind":"mac","host":"test-host","argon2id":%q},{"name":"mac-peer","kind":"mac","host":"peer-host","argon2id":%q}]}`,
		hash, hash)))
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := respproxy.New(respproxy.Config{
		Upstream: u.Host, UpstreamUser: u.User.Username(), UpstreamPassword: password,
		Principals: principals, Mode: respproxy.ModeEnforce, VolumeDB: 3, AllowedDBs: []int{3},
		Singleton: true, StageBirth: birth, ModePoll: time.Hour, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &replacementRuntime{proxy: proxy, objectRoot: objectRoot, authHash: hash}
	t.Cleanup(func() { r.stop(t) })
	complete := false
	defer func() {
		if !complete {
			r.stop(t)
		}
	}()
	r.engine = respproxy.NewLeaseEngine(respproxy.LeaseConfig{
		Side: proxy.Side(), VolumeDB: 3, NoScan: true,
		SweepEvery: sweepEvery, FlushEvery: time.Hour, Logf: t.Logf,
	})
	if err := r.engine.Start(); err != nil {
		t.Fatal(err)
	}
	proxy.SetPolicy(r.engine)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r.served = make(chan error, 1)
	go func() { r.served <- proxy.Serve(ln) }()
	r.url = (&url.URL{Scheme: "redis", Host: ln.Addr().String(),
		User: url.UserPassword("mac-test", "mac-secret"), Path: "/3"}).String()
	conf := jfsmeta.DefaultConf()
	conf.MaxDeletes, conf.NoBGJob = 0, true
	r.m = jfsmeta.NewClient(r.url, conf)
	format, err := r.m.Load(true)
	if err != nil || format == nil || format.UUID != x.volumeUUID {
		t.Fatalf("replacement runtime format=%+v err=%v", format, err)
	}
	chunkConfig := chunk.Config{BlockSize: format.BlockSize << 10, Compress: format.Compression,
		MaxUpload: 1, BufferSize: 30 << 20, CacheDir: "memory", CacheSize: 10 << 20,
		MaxRetries: 1, GetTimeout: 3 * time.Second, PutTimeout: 3 * time.Second, Writeback: false}
	if disk {
		configureReplacementResidentCacheVolume(t, &chunkConfig, x.volumeUUID)
	}
	r.objects, err = object.CreateStorage("file", objectRoot+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	r.store = chunk.NewCachedStore(r.objects, chunkConfig, nil)
	config := vfs.Config{Meta: conf, Format: *format, Version: "replacement-fresh-runtime",
		Chunk: &chunkConfig, FuseOpts: &vfs.FuseOptions{}}
	r.fs, err = jfsfs.NewFileSystem(&config, r.m, r.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	complete = true
	return r
}

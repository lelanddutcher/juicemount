//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsfs "github.com/juicedata/juicefs/pkg/fs"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
)

// The SDK adapter uses the real JuiceFS fs/VFS and chunk/object stack. Each
// operation rechecks the saved parent inode; stage names never pick a parent.
type replacementSDKIO struct {
	fs              *jfsfs.FileSystem
	m               jfsmeta.Meta
	w               metadata.SpoolReplacementWitness
	ctx             jfsmeta.Context
	parent          jfsmeta.Ino
	lostRenameReply bool
	renames         int
}

func (b *replacementSDKIO) Check() error {
	parent := jfsmeta.RootInode
	for _, node := range b.w.Destination.Chain[:len(b.w.Destination.Chain)-1] {
		var ino jfsmeta.Ino
		var attr jfsmeta.Attr
		if st := b.m.Lookup(b.ctx, parent, path.Base(node.Path), &ino, &attr, true); st != 0 {
			return st
		}
		if uint64(ino) != node.Inode || attr.Typ != jfsmeta.TypeDirectory {
			return metadata.ErrMirrorConflict
		}
		parent = ino
	}
	if parent != b.parent {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (b *replacementSDKIO) full(name string) string {
	return "/" + path.Join(path.Dir(b.w.Destination.Path), name)
}

func (b *replacementSDKIO) record(name string, inode jfsmeta.Ino, attr jfsmeta.Attr) (replaceguard.File, error) {
	if attr.Typ != jfsmeta.TypeFile || attr.Nlink != 1 || attr.Parent != b.parent {
		return replaceguard.File{}, metadata.ErrMirrorConflict
	}
	return replaceguard.File{Name: name, Parent: uint64(b.parent), Inode: uint64(inode),
		Length: attr.Length, MtimeNS: attr.Mtime*1_000_000_000 + int64(attr.Mtimensec),
		CtimeNS: attr.Ctime*1_000_000_000 + int64(attr.Ctimensec)}, nil
}

func (b *replacementSDKIO) Stat(name string) (replaceguard.File, error) {
	if err := b.Check(); err != nil {
		return replaceguard.File{}, err
	}
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := b.m.Lookup(b.ctx, b.parent, name, &ino, &attr, true); st != 0 {
		return replaceguard.File{}, st
	}
	return b.record(name, ino, attr)
}

func (b *replacementSDKIO) Open(name string, create bool) (replacementPublishFile, error) {
	if err := b.Check(); err != nil {
		return nil, err
	}
	var f *jfsfs.File
	var st error
	if create {
		var errno syscall.Errno
		f, errno = b.fs.Create(b.ctx, b.full(name), 0600, 0)
		st = errnoError(errno)
	} else {
		var errno syscall.Errno
		f, errno = b.fs.Open(b.ctx, b.full(name), jfsmeta.MODE_MASK_R)
		st = errnoError(errno)
	}
	if st != nil {
		return nil, st
	}
	return &replacementSDKFile{owner: b, f: f, name: name}, nil
}

func (b *replacementSDKIO) Create(name string, mode os.FileMode) (replacementPublishFile, error) {
	if !replacementSimpleName(name) {
		return nil, metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return nil, err
	}
	f, st := b.fs.Create(b.ctx, b.full(name), uint16(mode.Perm()), 0)
	if st != 0 {
		return nil, st
	}
	file := &replacementSDKFile{owner: b, f: f, name: name}
	var attr jfsmeta.Attr
	attrStatus := b.m.GetAttr(b.ctx, f.Inode(), &attr)
	descriptor, descriptorErr := file.Stat()
	named, namedErr := b.Stat(name)
	checkErr := b.Check()
	if attrStatus != 0 || descriptorErr != nil || namedErr != nil || checkErr != nil ||
		descriptor != named || attr.Mode != uint16(mode.Perm()) {
		_ = file.Close()
		return nil, metadata.ErrMirrorConflict
	}
	return file, nil
}

func (b *replacementSDKIO) OpenOwned(name string, expected replaceguard.File) (replacementPublishFile, error) {
	if !replacementSimpleName(name) || expected.Name != name || expected.Parent != uint64(b.parent) || expected.Inode <= 1 {
		return nil, metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return nil, err
	}
	f, st := b.fs.Lopen(b.ctx, b.full(name), jfsmeta.MODE_MASK_R|jfsmeta.MODE_MASK_W)
	if st != 0 {
		return nil, st
	}
	file := &replacementSDKFile{owner: b, f: f, name: name}
	descriptor, descriptorErr := file.Stat()
	named, namedErr := b.Stat(name)
	checkErr := b.Check()
	if descriptorErr != nil || namedErr != nil || checkErr != nil || descriptor != expected || named != expected {
		_ = file.Close()
		return nil, metadata.ErrMirrorConflict
	}
	return file, nil
}

func (b *replacementSDKIO) Rename(source, destination string) error {
	b.renames++
	if err := b.Check(); err != nil {
		return err
	}
	st := b.fs.Rename(b.ctx, b.full(source), b.full(destination), 0)
	if st != 0 {
		return st
	}
	if b.lostRenameReply {
		return errors.New("injected lost rename acknowledgement after real backend commit")
	}
	return nil
}

type replacementSDKFile struct {
	owner *replacementSDKIO
	f     *jfsfs.File
	name  string
}

func (f *replacementSDKFile) Stat() (replaceguard.File, error) {
	var attr jfsmeta.Attr
	if st := f.owner.m.GetAttr(f.owner.ctx, f.f.Inode(), &attr); st != 0 {
		return replaceguard.File{}, st
	}
	return f.owner.record(f.name, f.f.Inode(), attr)
}
func (f *replacementSDKFile) ReadAt(b []byte, off int64) (int, error) {
	return f.f.Pread(f.owner.ctx, b, off)
}
func (f *replacementSDKFile) WriteAt(b []byte, off int64) (int, error) {
	n, st := f.f.Pwrite(f.owner.ctx, b, off)
	return n, errnoError(st)
}
func (f *replacementSDKFile) SetMode(mode os.FileMode) error {
	return errnoError(f.f.Chmod(f.owner.ctx, uint16(mode.Perm())))
}
func (f *replacementSDKFile) SetMtime(ns int64) error {
	return errnoError(f.f.Utime2(f.owner.ctx, -1, -1, ns/1_000_000_000, ns%1_000_000_000))
}
func (f *replacementSDKFile) Sync() error  { return errnoError(f.f.Fsync(f.owner.ctx)) }
func (f *replacementSDKFile) Close() error { return errnoError(f.f.Close(f.owner.ctx)) }

func errnoError(st syscall.Errno) error {
	if st == 0 {
		return nil
	}
	return st
}

func replacementNestedFixture(t *testing.T) (*boundCacheFixture, *SpoolStore, *Drainer, *metadata.SpoolRow, *metadata.SpoolReplacementWitness, []byte) {
	return replacementNestedFixtureBody(t, []byte("actual new bytes uploaded through JuiceFS object storage"))
}

func replacementNestedFixtureBody(t *testing.T, body []byte) (*boundCacheFixture, *SpoolStore, *Drainer, *metadata.SpoolRow, *metadata.SpoolReplacementWitness, []byte) {
	return replacementNestedFixtureBodyVolume(t, body, "12345678-1234-abcd-5678-123456789abc")
}

func replacementNestedFixtureBodyVolume(t *testing.T, body []byte, uuid string) (*boundCacheFixture, *SpoolStore, *Drainer, *metadata.SpoolRow, *metadata.SpoolReplacementWitness, []byte) {
	t.Helper()
	x := newBoundCacheFixtureWithRedisVolume(t, redistestfixture.Start(t), uuid)
	return seedReplacementNestedFixtureBody(t, x, body, t.TempDir())
}

// seedReplacementNestedFixtureBody uses a caller-owned spool root when needed.
func seedReplacementNestedFixtureBody(t *testing.T, x *boundCacheFixture, body []byte, spoolRoot string) (*boundCacheFixture, *SpoolStore, *Drainer, *metadata.SpoolRow, *metadata.SpoolReplacementWitness, []byte) {
	t.Helper()
	format := &jfsmeta.Format{Name: "selected-volume", UUID: x.volumeUUID, Storage: "mem", BlockSize: 4096, Compression: "none"}
	bootstrap := jfsmeta.NewClient(x.raw, jfsmeta.DefaultConf())
	defer bootstrap.Shutdown()
	if err := bootstrap.Init(format, true); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	attr := func(typ uint8, parent, size uint64) []byte {
		return (&jfsmeta.Attr{Typ: typ, Mode: 0777, Nlink: 2, Parent: jfsmeta.Ino(parent), Length: size,
			Mtime: 1700000000, Mtimensec: 123, Ctime: 1700000000, Ctimensec: 123}).Marshal()
	}
	old := (&jfsmeta.Attr{Typ: jfsmeta.TypeFile, Mode: 0644, Nlink: 1, Parent: 4,
		Length: uint64(len(x.want)), Mtime: 1700000000, Mtimensec: 123,
		Ctime: 1700000000, Ctimensec: 123}).Marshal()
	edge := func(typ byte, inode uint64) []byte {
		v := make([]byte, 9)
		v[0] = typ
		binary.BigEndian.PutUint64(v[1:], inode)
		return v
	}
	pipe := x.rdb.Pipeline()
	pipe.Set(ctx, "i1", attr(jfsmeta.TypeDirectory, 1, 4096), 0)
	pipe.Set(ctx, "i2", attr(jfsmeta.TypeDirectory, 1, 4096), 0)
	pipe.Set(ctx, "i4", attr(jfsmeta.TypeDirectory, 2, 4096), 0)
	pipe.Set(ctx, "i3", old, 0)
	// The base fixture seeds inode/chunk values directly. Real JuiceFS create
	// needs allocators beyond those values and nonnegative usage counters.
	pipe.Set(ctx, "nextinode", "4", 0)
	pipe.Set(ctx, "nextchunk", "100", 0)
	pipe.Set(ctx, "usedSpace", "1048576", 0)
	pipe.Set(ctx, "totalInodes", "4", 0)
	pipe.HDel(ctx, "d2", "clip.mov")
	pipe.HSet(ctx, "d2", "project", edge(jfsmeta.TypeDirectory, 4))
	pipe.HSet(ctx, "d4", "clip.mov", edge(jfsmeta.TypeFile, 3))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := x.store.Delete("media/clip.mov"); err != nil {
		t.Fatal(err)
	}
	for _, e := range []*metadata.Entry{
		metadata.MakeEntry("media", true, 4096, time.Unix(1700000000, 123), 2),
		metadata.MakeEntry("media/project", true, 4096, time.Unix(1700000000, 123), 4),
		metadata.MakeEntry("media/project/clip.mov", false, int64(len(x.want)), time.Unix(1700000000, 123), 3),
	} {
		e.MtimeNsecKnown = true
		if err := x.store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	oldBacking := filepath.Join(x.fuseRoot, "media/clip.mov")
	newBacking := filepath.Join(x.fuseRoot, "media/project/clip.mov")
	if err := os.MkdirAll(filepath.Dir(newBacking), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldBacking, newBacking); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newBacking, time.Unix(1700000000, 123), time.Unix(1700000000, 123)); err != nil {
		t.Fatal(err)
	}
	reader := x.session.Reader()
	if reader == nil {
		t.Fatal("qualified cache reader absent")
	}
	volume, ok := reader.VolumeBinding()
	if !ok {
		t.Fatal("backend binding absent")
	}
	binding := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	observation, err := x.paths.CaptureCachePath("media/project/clip.mov")
	if err != nil {
		t.Fatalf("capture original path: %v", err)
	}
	proofCtx, cancelProof := context.WithTimeout(ctx, 5*time.Second)
	proof, err := x.rc.ReadParentPathProof(proofCtx, volume.UUID, observation.Path, observation.ExpectedInodes())
	cancelProof()
	if err != nil {
		t.Fatalf("prove original Redis path: %v", err)
	}
	if err := x.paths.ConfirmCachePath(binding, observation, proof); err != nil {
		t.Fatalf("confirm original path: %v; local=%+v remote=%+v", err, observation.Chain, proof.Nodes)
	}
	opened, err := x.jfs.OpenFile("media/project/clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	cached, ok := opened.(*cachedFile)
	if !ok {
		_ = opened.Close()
		t.Fatalf("original read used %T", opened)
	}
	cached.memBuf, cached.readahead = nil, nil
	warmed := make([]byte, len(x.want))
	n, readErr := cached.ReadAt(warmed, 0)
	if err := cached.Close(); err != nil {
		t.Fatal(err)
	}
	if n != len(warmed) || readErr != nil || !bytes.Equal(warmed, x.want) || cached.fuseFD != nil {
		t.Fatalf("original graph read n=%d err=%v FUSE=%v", n, readErr, cached.fuseFD != nil)
	}
	if err := metadata.InitSpoolSchema(x.store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(spoolRoot, 0, metadata.NewSpoolStore(x.store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: x.fuseRoot})
	if err != nil {
		t.Fatal(err)
	}
	x.jfs.handler.SetSpool(spool, d)
	temp := "media/project/.replacement-save.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.jfs.Rename(temp, "media/project/clip.mov"); err != nil {
		t.Fatalf("accept offline replacement: %v", err)
	}
	row, err := spool.Meta().LookupByPath("media/project/clip.mov")
	if err != nil || row.DrainState != metadata.DrainReady {
		t.Fatalf("accepted row=%+v err=%v", row, err)
	}
	w, err := spool.Meta().ReplacementWitness(row)
	if err != nil || w == nil || w.Destination.Path != row.NFSPath || w.GraphDigest == "" {
		t.Fatalf("accepted witness=%+v err=%v", w, err)
	}
	x.session.SetOnline(true)
	pin.SetOffline(false)
	return x, spool, d, row, w, body
}

func replacementProxyAndFS(t *testing.T, x *boundCacheFixture, spool *SpoolStore, d *Drainer) (*jfsfs.FileSystem, string) {
	return replacementProxyAndFSConfig(t, x, spool, d, nil)
}

func replacementProxyAndFSConfig(t *testing.T, x *boundCacheFixture, spool *SpoolStore, d *Drainer, configure func(*chunk.Config)) (*jfsfs.FileSystem, string) {
	return replacementProxyAndFSConfigBirth(t, x, spool, d, configure, true)
}

func replacementProxyAndFSConfigBirth(t *testing.T, x *boundCacheFixture, spool *SpoolStore, d *Drainer, configure func(*chunk.Config), birth bool) (*jfsfs.FileSystem, string) {
	t.Helper()
	u, err := url.Parse(x.raw)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	hash, err := respproxy.HashPassword("mac-secret", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	principals, err := respproxy.ParsePrincipals([]byte(fmt.Sprintf(`{"principals":[{"name":"mac-test","kind":"mac","host":"test-host","argon2id":%q},{"name":"mac-peer","kind":"mac","host":"peer-host","argon2id":%q}]}`, hash, hash)))
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := respproxy.New(respproxy.Config{Upstream: u.Host, UpstreamUser: u.User.Username(), UpstreamPassword: password,
		Principals: principals, Mode: respproxy.ModeEnforce, VolumeDB: 3, AllowedDBs: []int{3}, Singleton: true, StageBirth: birth,
		ModePoll: time.Hour, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	engine := respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: proxy.Side(), VolumeDB: 3, NoScan: true,
		SweepEvery: time.Hour, FlushEvery: time.Hour, Logf: t.Logf})
	var served chan error
	t.Cleanup(func() {
		engine.Close()
		_ = proxy.Close()
		if served == nil {
			return
		}
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("proxy Serve: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("owned proxy Serve did not join")
		}
	})
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	proxy.SetPolicy(engine)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served = make(chan error, 1)
	go func() { served <- proxy.Serve(ln) }()

	proxyURL := (&url.URL{Scheme: "redis", Host: ln.Addr().String(), User: url.UserPassword("mac-test", "mac-secret"), Path: "/3"}).String()
	client, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: proxyURL, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Stop)
	d.SetProjectLeases(client)
	conf := jfsmeta.DefaultConf()
	conf.MaxDeletes = 0
	conf.NoBGJob = true
	m := jfsmeta.NewClient(proxyURL, conf)
	t.Cleanup(func() { _ = m.Shutdown() })
	format, err := m.Load(true)
	if err != nil || format.UUID != x.volumeUUID {
		t.Fatalf("proxy-backed format=%+v err=%v", format, err)
	}
	chunkConfig := chunk.Config{BlockSize: format.BlockSize << 10, Compress: format.Compression,
		MaxUpload: 1, BufferSize: 30 << 20, CacheDir: "memory", CacheSize: 10 << 20,
		MaxRetries: 1, GetTimeout: 3 * time.Second, PutTimeout: 3 * time.Second}
	if configure != nil {
		configure(&chunkConfig)
	}
	config := vfs.Config{Meta: conf, Format: *format, Version: "replacement-no-mount",
		Chunk: &chunkConfig, FuseOpts: &vfs.FuseOptions{}}
	objects, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := chunk.NewCachedStore(objects, chunkConfig, nil)
	fs, err := jfsfs.NewFileSystem(&config, m, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return fs, proxyURL
}

func TestReplacementPublishActualJuiceFSNoMount(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "normal"
		if lost {
			name = "lost-rename-reply"
		}
		t.Run(name, func(t *testing.T) {
			x, spool, d, row, witness, body := replacementNestedFixture(t)
			fs, _ := replacementProxyAndFS(t, x, spool, d)
			usedBefore, _ := spool.Capacity()
			var backend *replacementSDKIO
			factoryCalls := 0
			factory := func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				factoryCalls++
				if w.OperationID != witness.OperationID || w.GraphDigest != witness.GraphDigest {
					return nil, nil, metadata.ErrMirrorConflict
				}
				backend = &replacementSDKIO{fs: fs, m: fs.Meta(), w: w, ctx: jfsmeta.Background(), parent: 4, lostRenameReply: lost}
				return backend, func() {}, nil
			}
			binding := func(got metadata.CachePathBinding) error {
				if got != witness.Binding {
					return metadata.ErrMirrorConflict
				}
				return nil
			}
			if err := d.publishReplacement(context.Background(), row.ID, factory, binding); err != nil {
				t.Fatalf("real no-mount publication: %v", err)
			}
			if backend == nil || backend.renames != 1 || factoryCalls != 1 {
				t.Fatalf("backend operations factory=%d backend=%+v", factoryCalls, backend)
			}
			current, err := spool.Meta().Get(row.ID)
			if err != nil || current.DrainState != metadata.DrainReady || current.RetiredBy != 0 || current.SpoolFile != row.SpoolFile {
				t.Fatalf("accepted READY source changed=%+v err=%v", current, err)
			}
			journal, err := spool.Meta().ReplacementPublication(current)
			if err != nil || journal == nil || journal.Phase != metadata.ReplacementPublicationCommitted {
				t.Fatalf("committed journal=%+v err=%v", journal, err)
			}
			receipt, err := replaceguard.Decode([]byte(journal.Record))
			if err != nil || receipt.Phase != "committed" || receipt.GraphDigest != witness.GraphDigest || receipt.Destination.Inode != 3 {
				t.Fatalf("canonical committed receipt=%+v err=%v", receipt, err)
			}
			if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("private image lost: %v", err)
			}
			if usedAfter, _ := spool.Capacity(); usedAfter != usedBefore {
				t.Fatalf("capacity released before settlement: %d -> %d", usedBefore, usedAfter)
			}
			opened, st := fs.Open(jfsmeta.Background(), "/media/project/clip.mov", jfsmeta.MODE_MASK_R)
			if st != 0 {
				t.Fatalf("open published file: %v", st)
			}
			readback := make([]byte, len(body))
			n, readErr := opened.Pread(jfsmeta.Background(), readback, 0)
			_ = opened.Close(jfsmeta.Background())
			if n != len(body) || readErr != nil && !errors.Is(readErr, io.EOF) || !bytes.Equal(readback, body) {
				t.Fatalf("object-backed readback n=%d err=%v", n, readErr)
			}
			pin.SetOffline(true)
			if err := d.publishReplacement(context.Background(), row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				t.Fatal("committed replay opened backend")
				return nil, nil, nil
			}, binding); err != nil {
				t.Fatalf("committed replay: %v", err)
			}
			store, recovered := reopenReplacementPublicationFixture(t, x, spool.root)
			if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
				t.Fatalf("reopen recovery: %v", err)
			}
			reopenedRow, err := recovered.Meta().Get(row.ID)
			if err != nil || reopenedRow.DrainState != metadata.DrainReady {
				t.Fatalf("reopened READY row=%+v err=%v", reopenedRow, err)
			}
			reopenedJournal, err := recovered.Meta().ReplacementPublication(reopenedRow)
			if err != nil || reopenedJournal == nil || *reopenedJournal != *journal {
				t.Fatalf("reopened receipt=%+v err=%v", reopenedJournal, err)
			}
			mirror := store.LookupByPath("media/project/clip.mov")
			if mirror == nil || !mirror.LocalOnly || mirror.Size != int64(len(body)) || mirror.Version != journal.Expected {
				t.Fatalf("reopened local mirror=%+v", mirror)
			}
			pin.SetOffline(true)
			reader := &juiceFS{handler: NewHandler(store, x.fuseRoot)}
			t.Cleanup(func() { reader.handler.StopHandler() })
			reader.handler.SetSpool(recovered, nil)
			f, err := reader.OpenFile("media/project/clip.mov", os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			local := make([]byte, len(body))
			n, err = f.ReadAt(local, 0)
			_ = f.Close()
			if n != len(body) || err != nil && !errors.Is(err, io.EOF) || !bytes.Equal(local, body) {
				t.Fatalf("reopened local read n=%d err=%v", n, err)
			}
		})
	}
}

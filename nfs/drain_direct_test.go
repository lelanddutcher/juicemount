package nfs

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"

	"github.com/lelanddutcher/juicemount/metadata"
)

// fakeCreateFiles plays juicefs's JMCreateFiles on the test's FUSE root (a
// plain folder): each file is created O_EXCL in the folder with the parent's
// inode, with its mode, bytes and mtime.
type fakeCreateFiles struct {
	t        *testing.T
	root     string
	mu       sync.Mutex
	messages [][]string // names per message
	preInos  [][]uint64 // reserved numbers per message
	// before runs before each message; after runs on each result.
	before func([]jfsNewFile) error
	after  func(i int, f jfsNewFile, c *jfsCreated)
}

func (f *fakeCreateFiles) dirOf(ino uint64) (string, bool) {
	var found string
	_ = filepath.WalkDir(f.root, func(p string, de fs.DirEntry, err error) error {
		if err != nil || !de.IsDir() || found != "" {
			return nil
		}
		var st syscall.Stat_t
		if syscall.Stat(p, &st) == nil && st.Ino == ino {
			found = p
			return filepath.SkipAll
		}
		return nil
	})
	return found, found != ""
}

func (f *fakeCreateFiles) send(files []jfsNewFile) ([]jfsCreated, error) {
	f.mu.Lock()
	names := make([]string, len(files))
	inos := make([]uint64, len(files))
	for i, nf := range files {
		names[i], inos[i] = nf.name, nf.preIno
	}
	f.messages = append(f.messages, names)
	f.preInos = append(f.preInos, inos)
	f.mu.Unlock()
	if f.before != nil {
		if err := f.before(files); err != nil {
			return nil, err
		}
	}
	out := make([]jfsCreated, len(files))
	for i, nf := range files {
		dir, ok := f.dirOf(nf.parent)
		if !ok {
			out[i].err = syscall.ENOENT
			continue
		}
		p := filepath.Join(dir, nf.name)
		h, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, nf.mode)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				out[i].err = syscall.EEXIST
			} else {
				out[i].err = err
			}
		} else {
			_, _ = h.Write(nf.data)
			_ = h.Chmod(nf.mode)
			_ = h.Close()
			_ = os.Chtimes(p, nf.mtime, nf.mtime)
			var st syscall.Stat_t
			_ = syscall.Stat(p, &st)
			out[i].ino = st.Ino
		}
		if f.after != nil {
			f.after(i, nf, &out[i])
		}
	}
	return out, nil
}

func withFakeCreateFiles(t *testing.T, d *Drainer) *fakeCreateFiles {
	t.Helper()
	if d.direct == nil {
		t.Fatal("direct publish is not wired")
	}
	f := &fakeCreateFiles{t: t, root: d.fuseRoot}
	d.direct.send = f.send
	return f
}

func (f *fakeCreateFiles) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

// A small new file is published whole: no stage, the client's mode and
// mtime, one message, and the row completes as on the stage path.
func TestDrainDirectPublishesSmallNewFile(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	payload := patternedPayload(20480, 11)
	e := writeSpoolEntry(t, spool, "/proj/notes.xmp", payload)
	mtime := time.Unix(1794490456, 123456789)
	if err := os.Chtimes(e.SpoolFilePath(), mtime, mtime); err != nil {
		t.Fatal(err)
	}

	drainOnce(t, d)

	dest := filepath.Join(d.fuseRoot, "/proj/notes.xmp")
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("dest: %d bytes, err %v", len(got), err)
	}
	fi, _ := os.Stat(dest)
	if fi.Mode().Perm() != 0o644 {
		t.Errorf("mode %v, want 0644", fi.Mode().Perm())
	}
	if !fi.ModTime().Equal(mtime) {
		t.Errorf("mtime %v, want %v", fi.ModTime(), mtime)
	}
	if fake.count() != 1 {
		t.Errorf("%d messages, want 1", fake.count())
	}
	if n := d.Metrics().DrainsDirect.Load(); n != 1 {
		t.Errorf("direct drains %d, want 1", n)
	}
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done", row)
	}
	if _, err := os.Stat(e.SpoolFilePath()); !os.IsNotExist(err) {
		t.Errorf("spool file kept after a direct drain: %v", err)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

// A name created by another writer after the base decision is never
// replaced: the bytes go beside it as a conflict copy and the row keeps its
// spool source, as on the stage path.
func TestDrainDirectNameTakenPublishesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	withFakeCreateFiles(t, d)
	const name = "/proj/reel.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	ours := patternedPayload(4096, 37)
	e := writeSpoolEntry(t, spool, name, ours)
	theirs := []byte("another writer created this name first")
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if err := os.WriteFile(dest, theirs, 0o644); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("direct publish replaced another writer's file (%d bytes)", len(got))
	}
	conflict := filepath.Join(filepath.Dir(dest), "reel (conflict mac2 2026-10-06 140309).xmp")
	if got, err := os.ReadFile(conflict); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("conflict copy: %d bytes err=%v", len(got), err)
	}
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState != metadata.DrainFailed || !isConflictCopyFailure(row.LastError) {
		t.Fatalf("row %+v, want failed with the conflict-copy marker", row)
	}
	if _, err := os.Stat(e.SpoolFilePath()); err != nil {
		t.Fatalf("spool source must be retained: %v", err)
	}
}

// Our own unfinished older row's file at the name: retry (in place), never a
// conflict copy.
func TestDrainDirectNameTakenByOwnOlderRowRetries(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	withFakeCreateFiles(t, d)
	const name = "/proj/save.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	olderID := seedLegacyUnfinishedPredecessor(t, spool, name)
	e := writeSpoolEntry(t, spool, name, []byte("newer save"))
	restoreLegacyUnfinishedPredecessor(t, spool, olderID)
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if err := os.WriteFile(dest, []byte("older save"), 0o644); err != nil {
			t.Error(err)
		}
		fi, _ := os.Lstat(dest)
		st := fi.Sys().(*syscall.Stat_t)
		d.recordInstalledIdentity(olderID, uint64(st.Dev), uint64(st.Ino))
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	conflict := filepath.Join(filepath.Dir(dest), "save (conflict mac2 2026-10-06 140309).xmp")
	if _, err := os.Lstat(conflict); err == nil {
		t.Fatal("own predecessor's file turned the newer save into a conflict copy")
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState == metadata.DrainFailed {
		t.Fatalf("newer save must stay retryable: %+v", row)
	}
}

// A juicefs without the message: the file drains through a stage, and the
// direct path stays off.
func TestDrainDirectUnsupportedFallsBackToStage(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	fake.before = func([]jfsNewFile) error { return &errCreateFilesRefused{errControlUnsupported} }
	a := writeSpoolEntry(t, spool, "/proj/a.xmp", []byte("first"))
	drainOnce(t, d)
	b := writeSpoolEntry(t, spool, "/proj/b.xmp", []byte("second"))
	drainOnce(t, d)
	for _, e := range []*SpoolEntry{a, b} {
		row, _ := spool.Meta().Get(e.ID())
		if row == nil || row.DrainState != metadata.DrainDone {
			t.Fatalf("row %+v, want done through the stage", row)
		}
	}
	if got, _ := os.ReadFile(filepath.Join(d.fuseRoot, "proj/b.xmp")); string(got) != "second" {
		t.Fatalf("b: %q", got)
	}
	if fake.count() != 1 {
		t.Errorf("%d messages, want 1 (then the path stays off)", fake.count())
	}
	if d.Metrics().DrainsDirect.Load() != 0 {
		t.Error("counted a direct drain")
	}
	assertNoDrainStages(t, filepath.Join(d.fuseRoot, "proj"))
}

// No clear answer, and the name holds exactly our bytes (the transaction
// committed): adopt it, no conflict copy, no second file.
func TestDrainDirectAmbiguousCommittedIsAdopted(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	fake.after = func(_ int, _ jfsNewFile, c *jfsCreated) { c.err, c.ino = syscall.EIO, 0 }
	payload := patternedPayload(3000, 5)
	e := writeSpoolEntry(t, spool, "/proj/x.xmp", payload)
	drainOnce(t, d)
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done (adopted)", row)
	}
	entries, _ := os.ReadDir(filepath.Join(d.fuseRoot, "proj"))
	if len(entries) != 1 {
		t.Fatalf("%d entries in the folder, want 1", len(entries))
	}
}

// No clear answer and the name holds other bytes (or nothing): the row stays
// retryable and nothing is adopted.
func TestDrainDirectAmbiguousNotCommittedRetries(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	fake.before = func([]jfsNewFile) error { return errors.New("juicefs control: no reply after 10m0s") }
	e := writeSpoolEntry(t, spool, "/proj/y.xmp", []byte("data"))
	drainOnce(t, d)
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState == metadata.DrainDone || row.DrainState == metadata.DrainFailed {
		t.Fatalf("row %+v, want retryable", row)
	}
	if _, err := os.Lstat(filepath.Join(d.fuseRoot, "proj/y.xmp")); err == nil {
		t.Fatal("a file appeared")
	}
	fake.before = nil
	time.Sleep(50 * time.Millisecond) // past the test backoff
	drainOnce(t, d)
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("retry: row %+v, want done", row)
	}
}

// A file over 1 MiB, or one this process could not open back, keeps the
// stage path.
func TestDrainDirectOnlySmallFiles(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	big := patternedPayload(directMaxBytes+1, 3)
	e := writeSpoolEntry(t, spool, "/proj/big.mov", big)
	drainOnce(t, d)
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done", row)
	}
	if fake.count() != 0 {
		t.Fatalf("a file over 1 MiB was sent whole")
	}
}

// A drain canceled before the install publishes nothing.
func TestDrainDirectCanceledBeforeInstallPublishesNothing(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	e := writeSpoolEntry(t, spool, "/proj/gone.xmp", []byte("canceled"))
	drainPreInstallHook = func(row *metadata.SpoolRow) {
		if _, err := spool.Meta().Owner().DB().Exec(`DELETE FROM spool_entries WHERE id=?`, row.ID); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()
	drainOnce(t, d)
	if fake.count() != 0 {
		t.Fatal("a canceled drain sent its file")
	}
	if _, err := os.Lstat(filepath.Join(d.fuseRoot, "proj/gone.xmp")); err == nil {
		t.Fatal("a canceled drain created its name")
	}
	_ = e
}

// Files queued while a message is in flight go together in the next one,
// and each caller gets its own answer.
func TestDirectPublisherBatchesWhileInFlight(t *testing.T) {
	p := &directPublisher{}
	release := make(chan struct{})
	var mu sync.Mutex
	var sizes []int
	p.send = func(files []jfsNewFile) ([]jfsCreated, error) {
		mu.Lock()
		first := len(sizes) == 0
		sizes = append(sizes, len(files))
		mu.Unlock()
		if first {
			<-release
		}
		out := make([]jfsCreated, len(files))
		for i, f := range files {
			if strings.HasPrefix(f.name, "taken") {
				out[i].err = syscall.EEXIST
			} else {
				out[i].ino = uint64(100 + len(f.name))
			}
		}
		return out, nil
	}
	var wg sync.WaitGroup
	results := make(map[string]directResult)
	var rmu sync.Mutex
	do := func(name string) {
		defer wg.Done()
		r := p.publish(jfsNewFile{name: name, data: []byte(name)})
		rmu.Lock()
		results[name] = r
		rmu.Unlock()
	}
	wg.Add(1)
	go do("a")
	for {
		mu.Lock()
		n := len(sizes)
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	names := []string{"bb", "ccc", "taken1", "dddd"}
	for _, n := range names {
		wg.Add(1)
		go do(n)
	}
	for {
		p.mu.Lock()
		q := len(p.queue)
		p.mu.Unlock()
		if q == len(names) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	if len(sizes) != 2 || sizes[0] != 1 || sizes[1] != len(names) {
		t.Fatalf("message sizes %v, want [1 %d]", sizes, len(names))
	}
	for _, n := range append([]string{"a"}, names...) {
		r := results[n]
		if strings.HasPrefix(n, "taken") {
			if !errors.Is(r.err, syscall.EEXIST) || !r.refused {
				t.Errorf("%s: %+v, want a refused EEXIST", n, r)
			}
			continue
		}
		if r.err != nil || r.ino != uint64(100+len(n)) {
			t.Errorf("%s: %+v", n, r)
		}
	}
	if p.sending || len(p.queue) != 0 {
		t.Fatalf("publisher left sending=%v queue=%d", p.sending, len(p.queue))
	}
}

// A batch never exceeds the file or byte bound; the head always goes.
func TestDirectPublisherBatchBounds(t *testing.T) {
	p := &directPublisher{}
	for i := 0; i < directBatchFiles+5; i++ {
		p.queue = append(p.queue, &directReq{file: jfsNewFile{data: make([]byte, 100)}})
	}
	if b := p.takeLocked(); len(b) != directBatchFiles {
		t.Fatalf("batch of %d files, want %d", len(b), directBatchFiles)
	}
	p.queue = nil
	for i := 0; i < 12; i++ {
		p.queue = append(p.queue, &directReq{file: jfsNewFile{data: make([]byte, 1<<20)}})
	}
	if b := p.takeLocked(); len(b) != directBatchBytes>>20 {
		t.Fatalf("batch of %d MiB files, want %d", len(b), directBatchBytes>>20)
	}
}

// The message layout matches the juicefs parser (pkg/vfs/jm_create_files.go):
// header, count, then per file parent, reserved inode, mode, cumask, mtime,
// name, data.
func TestJFSCreateFilesEncoding(t *testing.T) {
	c := newJFSControl(t.TempDir())
	var sent []byte
	c.testExchange = func(msg []byte, replyLen int) ([]byte, error) {
		sent = append([]byte(nil), msg...)
		if replyLen != 4+9*2 {
			t.Fatalf("reply length %d", replyLen)
		}
		r := []byte{jfsCreateFilesReplyMarker, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 7, byte(syscall.EEXIST), 0, 0, 0, 0, 0, 0, 0, 0}
		return r, nil
	}
	mt := time.Unix(1794490456, 42)
	res, err := c.createFiles([]jfsNewFile{
		{parent: 9, preIno: 0x1234, name: "a.xmp", mode: 0o640, mtime: mt, data: []byte("xyz")},
		{parent: 10, name: "b", mode: 0o644, mtime: mt},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].ino != 7 || res[0].err != nil || !errors.Is(res[1].err, syscall.EEXIST) {
		t.Fatalf("results %+v", res)
	}
	want := []byte{0, 0, 0x04, 0x50} // 1104
	body := []byte{0, 2,
		0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x01, 0xa0, 0, 0, 0, 0, 0, 0, 0x6a, 0xf5, 0xc0, 0x58, 0, 0, 0, 42, 5, 'a', '.', 'x', 'm', 'p', 0, 0, 0, 3, 'x', 'y', 'z',
		0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0xa4, 0, 0, 0, 0, 0, 0, 0x6a, 0xf5, 0xc0, 0x58, 0, 0, 0, 42, 1, 'b', 0, 0, 0, 0}
	want = append(want, 0, 0, 0, byte(len(body)))
	want = append(want, body...)
	if !bytes.Equal(sent, want) {
		t.Fatalf("message\n got %x\nwant %x", sent, want)
	}
	c.testExchange = func([]byte, int) ([]byte, error) {
		return append([]byte{jfsCreateFilesReplyMarker, byte(syscall.ENOTSUP), 0, 1}, make([]byte, 9)...), nil
	}
	_, err = c.createFiles([]jfsNewFile{{parent: 1, name: "c"}})
	var refused *errCreateFilesRefused
	if !errors.As(err, &refused) || !errors.Is(err, errControlUnsupported) {
		t.Fatalf("whole refusal: %v", err)
	}
	// An older juicefs answers an unknown message with one EINVAL byte.
	c.testExchange = func([]byte, int) ([]byte, error) { return []byte{byte(syscall.EINVAL)}, nil }
	_, err = c.createFiles([]jfsNewFile{{parent: 1, name: "c"}})
	if !errors.As(err, &refused) || !errors.Is(err, errControlUnsupported) {
		t.Fatalf("older juicefs: %v", err)
	}
}

// The nfs message numbers are the juicefs ones (patch 0022, 0025).
func TestJFSControlMessageNumbersMatchJuiceFS(t *testing.T) {
	for _, c := range []struct {
		name     string
		nfs, jfs int
	}{
		{"JMReserveInodes", jfsMsgReserveInodes, jfsmeta.JMReserveInodes},
		{"JMMkdirInode", jfsMsgMkdirInode, jfsmeta.JMMkdirInode},
		{"JMReleaseInodes", jfsMsgReleaseInodes, jfsmeta.JMReleaseInodes},
		{"JMCreateFiles", jfsMsgCreateFiles, jfsmeta.JMCreateFiles},
	} {
		if c.nfs != c.jfs {
			t.Errorf("%s: nfs sends %d, juicefs reads %d", c.name, c.nfs, c.jfs)
		}
	}
}

// reservedInodeFile writes payload under a hidden name in dir and puts its
// inode number in the drainer's pool: the number juicefs would give the file
// sent with it. Renaming the hidden file to the destination then plays
// juicefs committing that send.
func reservedInodeFile(t *testing.T, d *Drainer, dir string, payload []byte) (string, uint64) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, ".committed-later")
	if err := os.WriteFile(p, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(p, &st); err != nil {
		t.Fatal(err)
	}
	ino := uint64(st.Ino)
	d.dirInodes = &dirInodePool{ctl: d.direct.ctl, ranges: []inodeRange{{next: ino, end: ino + 1}}}
	return p, ino
}

// HIGH-2: the reply is lost (timeout), juicefs is still running the send on
// retry (EAGAIN), then commits after the drainer stopped waiting. The number
// recorded before the first send makes the next drainer, after a restart,
// recognise the file as its own: no conflict copy, no second file, no second
// send.
func TestDrainDirectLateCommitIsOwnFileAfterRestart(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	const name = "/proj/late.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	payload := patternedPayload(5000, 17)
	e := writeSpoolEntry(t, spool, name, payload)
	hidden, ino := reservedInodeFile(t, d, filepath.Dir(dest), payload)

	fake.before = func([]jfsNewFile) error { return errors.New("juicefs control: no reply after 2m0s") }
	drainOnce(t, d)
	if base, _ := d.rowDrainBase(mustRow(t, spool, e.ID())); base.Kind != metadata.SpoolBaseClaimed || base.Ino != ino {
		t.Fatalf("base %+v: the reserved number was not recorded before sending", base)
	}
	// Retry while juicefs still runs the first send.
	fake.before = nil
	fake.after = func(_ int, f jfsNewFile, c *jfsCreated) {
		if f.preIno == ino {
			c.err, c.ino = syscall.EAGAIN, 0
		}
	}
	fake.root = t.TempDir() // the send must not create anything itself
	time.Sleep(50 * time.Millisecond)
	drainOnce(t, d)
	if row := mustRow(t, spool, e.ID()); row.DrainState == metadata.DrainDone || row.DrainState == metadata.DrainFailed {
		t.Fatalf("row %+v while the first send runs, want retryable", row)
	}
	// The first send commits late, then the app restarts.
	if err := os.Rename(hidden, dest); err != nil {
		t.Fatal(err)
	}
	d2 := restartTestDrainer(t, spool, d)
	fake2 := withFakeCreateFiles(t, d2)
	time.Sleep(50 * time.Millisecond)
	drainOnce(t, d2)

	row := mustRow(t, spool, e.ID())
	if row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done (own file recognised)", row)
	}
	if fake2.count() != 0 {
		t.Errorf("the restarted drainer sent the file again")
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatalf("%d entries in the folder (a conflict copy?), want 1", len(entries))
	}
	if got := fake.preInos; len(got) != 2 || got[0][0] != ino || got[1][0] != ino {
		t.Fatalf("reserved numbers sent %v, want [%d] twice", got, ino)
	}
}

// HIGH-2, no reserved number: an ambiguous reply then a late commit of
// exactly these bytes at a name that was free is adopted on retry.
func TestDrainDirectLateCommitIdenticalBytesAdopted(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	d.dirInodes = nil
	const name = "/proj/late2.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	payload := patternedPayload(7000, 3)
	e := writeSpoolEntry(t, spool, name, payload)
	fake.before = func([]jfsNewFile) error { return errors.New("juicefs control: no reply after 2m0s") }
	drainOnce(t, d)
	if err := os.WriteFile(dest, payload, 0o644); err != nil { // the late commit
		t.Fatal(err)
	}
	fake.before = nil
	time.Sleep(50 * time.Millisecond)
	drainOnce(t, d)
	if row := mustRow(t, spool, e.ID()); row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done", row)
	}
	if fake.count() != 1 {
		t.Errorf("%d messages, want 1 (the retry adopts)", fake.count())
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatalf("%d entries, want 1", len(entries))
	}
}

// The name appears after the base decision and holds the number this row
// sent: juicefs's EEXIST is about our own earlier send, not a conflict.
func TestDrainDirectEEXISTOwnReservedInode(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	withFakeCreateFiles(t, d)
	const name = "/proj/own.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	payload := patternedPayload(4000, 9)
	e := writeSpoolEntry(t, spool, name, payload)
	hidden, _ := reservedInodeFile(t, d, filepath.Dir(dest), payload)
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if err := os.Rename(hidden, dest); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()
	drainOnce(t, d)
	if row := mustRow(t, spool, e.ID()); row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done", row)
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatalf("%d entries (a conflict copy?), want 1", len(entries))
	}
}

// juicefs no longer holds the number (it restarted): one resend with a new
// number (none left here: juicefs picks).
func TestDrainDirectVoidReservedInodeResends(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	d.dirInodes = &dirInodePool{ctl: d.direct.ctl, ranges: []inodeRange{{next: 1 << 40, end: 1<<40 + 1}}}
	fake.after = func(_ int, f jfsNewFile, c *jfsCreated) {
		if f.preIno != 0 { // refused: nothing is created
			_ = os.Remove(filepath.Join(d.fuseRoot, "proj", f.name))
			c.err, c.ino = syscall.ESRCH, 0
		}
	}
	e := writeSpoolEntry(t, spool, "/proj/void.xmp", []byte("void"))
	drainOnce(t, d)
	if row := mustRow(t, spool, e.ID()); row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done", row)
	}
	if got := fake.preInos; len(got) != 2 || got[0][0] != 1<<40 || got[1][0] != 0 {
		t.Fatalf("reserved numbers sent %v", got)
	}
}

// HIGH-1: macOS juicefs cannot invalidate the kernel's negative entry, so the
// open after a successful publish may answer ENOENT for a few seconds. The
// drain waits it out instead of failing (whose retry made a conflict copy of
// its own file).
func TestDrainDirectStaleNegativeEntryWaitedOut(t *testing.T) {
	t.Setenv("JM_META_NEGATIVE_CACHE", "2s")
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	fake := withFakeCreateFiles(t, d)
	const name = "/proj/neg.xmp"
	dest := filepath.Join(d.fuseRoot, name)
	fake.after = func(_ int, _ jfsNewFile, c *jfsCreated) {
		if c.err != nil {
			return
		}
		// The name answers ENOENT for 400 ms (the stale negative entry).
		hidden := dest + ".hidden"
		if err := os.Rename(dest, hidden); err != nil {
			t.Error(err)
			return
		}
		go func() {
			time.Sleep(400 * time.Millisecond)
			_ = os.Rename(hidden, dest)
		}()
	}
	e := writeSpoolEntry(t, spool, name, patternedPayload(2048, 1))
	drainOnce(t, d)
	if row := mustRow(t, spool, e.ID()); row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v, want done in one attempt", row)
	}
	if n := d.Metrics().DirectOpenWaits.Load(); n != 1 {
		t.Errorf("open waits %d, want 1", n)
	}
	if fake.count() != 1 {
		t.Errorf("%d messages, want 1", fake.count())
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatalf("%d entries, want 1", len(entries))
	}
}

func mustRow(t *testing.T, spool *SpoolStore, id int64) *metadata.SpoolRow {
	t.Helper()
	row, err := spool.Meta().Get(id)
	if err != nil || row == nil {
		t.Fatalf("row %d: %v", id, err)
	}
	return row
}

// restartTestDrainer builds a new drainer over the same spool and FUSE root,
// as the app after a restart.
func restartTestDrainer(t *testing.T, spool *SpoolStore, old *Drainer) *Drainer {
	t.Helper()
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: old.fuseRoot, BackoffBase: 5 * time.Millisecond, PollFallback: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	store := spool.Meta().Owner()
	d.SetOnDrainCommit(func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
		if store.LookupByPath(item.NFSPath) == nil {
			if err := store.Insert(metadata.MakeEntry(item.NFSPath, false, 0, time.Now(), uint64(item.SpoolID))); err != nil {
				return metadata.DrainCommitResult{}, err
			}
		}
		return store.DrainComplete(item)
	})
	return d
}

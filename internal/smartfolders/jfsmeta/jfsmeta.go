//go:build linux || darwin || windows

// Package jfsmeta is the smartfolders.FS for a Manager without the volume
// mounted (standalone mode): it writes smart-folder views through the
// JuiceFS metadata client, metadata only. It looks names up, creates
// directories and symlinks, reads links, renames (no-replace and exchange
// only) and unlinks; it never reads or writes file contents, so it needs no
// object-store access.
//
// It deliberately opens NO JuiceFS session. A session starts the client's
// background refresh, which calls os.Exit when the volume's format is
// removed, changes UUID or gains a newer metadata version (pkg/meta
// baseMeta.refresh); the Manager must never exit because of smart folders.
// None of the operations used needs a session: sessions hold open files
// and locks, and this client opens neither. Its writes go through whatever
// endpoint the URL names (in the default deployment the RESP proxy, as the
// Manager's principal), governed by the same leases as every other client.
// Usage counters are flushed after each batch (Flush) instead of by the
// session's background flusher.
package jfsmeta

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/redis/go-redis/v9"
)

// FS implements smartfolders.FS on a JuiceFS metadata client.
type FS struct {
	m        meta.Meta
	ctx      meta.Context
	readOnly bool
}

var _ smartfolders.FS = (*FS)(nil)

// OpenTimeout bounds Open: the JuiceFS client cannot be cancelled, so an
// unreachable endpoint must not hold the caller for its own retry budget.
const OpenTimeout = 15 * time.Second

// Open connects to the volume at metaURL (redis:// or rediss://) and loads
// its format, within OpenTimeout. The URL is checked first: the JuiceFS
// client exits the process on a URL it cannot parse.
func Open(metaURL string) (*FS, error) {
	return OpenWithin(metaURL, OpenTimeout)
}

// OpenWithin is Open with an explicit bound. A connection that completes
// after the bound is shut down in the background.
func OpenWithin(metaURL string, limit time.Duration) (*FS, error) {
	return open(metaURL, limit, false)
}

// OpenReadOnly opens a client that cannot write: the JuiceFS client refuses
// every mutation (conf.ReadOnly) and Flush/Close write no usage counters.
// It is for readers (the continuity binder), never for smart-folder views.
func OpenReadOnly(metaURL string) (*FS, error) {
	return open(metaURL, OpenTimeout, true)
}

func open(metaURL string, limit time.Duration, readOnly bool) (*FS, error) {
	u, err := url.Parse(metaURL)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
		return nil, errors.New("jfsmeta: the metadata URL must be redis:// or rediss://host")
	}
	if _, err := redis.ParseURL(metaURL); err != nil {
		return nil, errors.New("jfsmeta: the metadata URL is not a valid Redis URL")
	}
	type opened struct {
		fs  *FS
		err error
	}
	done := make(chan opened, 1)
	abandoned := make(chan struct{})
	go func() {
		conf := meta.DefaultConf()
		conf.NoBGJob = true // cleanup jobs belong to the volume's own clients
		conf.MaxDeletes = 0 // never deletes file data (it only unlinks symlinks)
		conf.Retries = 1    // one retry per command, not the default ten
		conf.ReadOnly = readOnly
		m := meta.NewClient(metaURL, conf)
		if _, err := m.Load(true); err != nil {
			_ = m.Shutdown()
			done <- opened{err: fmt.Errorf("jfsmeta: loading the volume format: %w", err)}
			return
		}
		f := &FS{m: m, ctx: meta.Background(), readOnly: readOnly}
		select {
		case done <- opened{fs: f}:
		case <-abandoned:
			_ = m.Shutdown()
		}
	}()
	select {
	case o := <-done:
		return o.fs, o.err
	case <-time.After(limit):
		close(abandoned)
		return nil, fmt.Errorf("jfsmeta: the metadata endpoint did not answer within %s", limit)
	}
}

// Flush writes the usage counters (used space, inodes, directory stats)
// the client accumulated; without a session nothing flushes them otherwise.
func (f *FS) Flush() {
	if !f.readOnly {
		f.m.FlushSession()
	}
}

// Close flushes the counters and disconnects. There is no session to close.
func (f *FS) Close() error {
	f.Flush()
	return f.m.Shutdown()
}

// errno turns a JuiceFS status into an error. syscall.Errno values satisfy
// errors.Is with fs.ErrNotExist / fs.ErrExist, as the FS contract needs.
func errno(st syscall.Errno) error {
	if st == 0 {
		return nil
	}
	return st
}

func split(p string) ([]string, error) {
	if p == "" || strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return nil, fmt.Errorf("invalid volume path %q: %w", p, syscall.EINVAL)
	}
	parts := strings.Split(p, "/")
	for _, c := range parts {
		if c == "" || c == "." || c == ".." {
			return nil, fmt.Errorf("invalid volume path %q: %w", p, syscall.EINVAL)
		}
	}
	return parts, nil
}

// parent resolves the directory holding p's last component, by inode from
// the volume root, refusing any symlink on the way.
func (f *FS) parent(p string) (meta.Ino, string, error) {
	parts, err := split(p)
	if err != nil {
		return 0, "", err
	}
	dir := meta.RootInode
	for _, c := range parts[:len(parts)-1] {
		var ino meta.Ino
		var attr meta.Attr
		if st := f.m.Lookup(f.ctx, dir, c, &ino, &attr, false); st != 0 {
			return 0, "", st
		}
		switch attr.Typ {
		case meta.TypeDirectory:
		case meta.TypeSymlink:
			return 0, "", smartfolders.ErrSymlinkInPath
		default:
			return 0, "", syscall.ENOTDIR
		}
		dir = ino
	}
	return dir, parts[len(parts)-1], nil
}

func (f *FS) lookup(p string) (meta.Ino, meta.Attr, error) {
	var ino meta.Ino
	var attr meta.Attr
	dir, leaf, err := f.parent(p)
	if err != nil {
		return 0, attr, err
	}
	if st := f.m.Lookup(f.ctx, dir, leaf, &ino, &attr, false); st != 0 {
		return 0, attr, st
	}
	return ino, attr, nil
}

func info(ino meta.Ino, attr *meta.Attr) smartfolders.Info {
	in := smartfolders.Info{Type: smartfolders.TypeOther, Ino: uint64(ino), Perm: fs.FileMode(attr.Mode & 0o777)}
	switch attr.Typ {
	case meta.TypeFile:
		in.Type = smartfolders.TypeRegular
	case meta.TypeDirectory:
		in.Type = smartfolders.TypeDir
	case meta.TypeSymlink:
		in.Type = smartfolders.TypeSymlink
	}
	return in
}

func (f *FS) Lstat(p string) (smartfolders.Info, error) {
	ino, attr, err := f.lookup(p)
	if err != nil {
		return smartfolders.Info{}, err
	}
	return info(ino, &attr), nil
}

func (f *FS) Readlink(p string) (string, error) {
	ino, attr, err := f.lookup(p)
	if err != nil {
		return "", err
	}
	if attr.Typ != meta.TypeSymlink {
		return "", syscall.EINVAL
	}
	var buf []byte
	if st := f.m.ReadLink(f.ctx, ino, &buf); st != 0 {
		return "", st
	}
	return string(buf), nil
}

func (f *FS) Mkdir(p string, perm fs.FileMode) (smartfolders.Info, error) {
	dir, leaf, err := f.parent(p)
	if err != nil {
		return smartfolders.Info{}, err
	}
	var ino meta.Ino
	var attr meta.Attr
	if st := f.m.Mkdir(f.ctx, dir, leaf, uint16(perm.Perm()), 0, 0, &ino, &attr); st != 0 {
		return smartfolders.Info{}, st
	}
	return info(ino, &attr), nil
}

func (f *FS) Symlink(text, p string) (smartfolders.Info, error) {
	dir, leaf, err := f.parent(p)
	if err != nil {
		return smartfolders.Info{}, err
	}
	var ino meta.Ino
	var attr meta.Attr
	if st := f.m.Symlink(f.ctx, dir, leaf, text, &ino, &attr); st != 0 {
		return smartfolders.Info{}, st
	}
	return info(ino, &attr), nil
}

// Chmod sets perm on the directory p if it is still inode ino. (Mkdir here
// passes no umask, so the Manager's directories never need it.)
func (f *FS) Chmod(p string, ino uint64, perm fs.FileMode) error {
	got, attr, err := f.lookup(p)
	if err != nil {
		return err
	}
	if uint64(got) != ino || attr.Typ != meta.TypeDirectory {
		return fmt.Errorf("%s is no longer the directory the Manager created", p)
	}
	attr.Mode = uint16(perm.Perm())
	return errno(f.m.SetAttr(f.ctx, got, meta.SetAttrMode, 0, &attr))
}

// Place renames tmp to p, never replacing an existing p.
func (f *FS) Place(tmp, p string) error { return f.rename(tmp, p, meta.RenameNoReplace) }

// Swap atomically exchanges tmp and p. Never a plain rename, so nothing at
// p is ever destroyed or moved into the trash.
func (f *FS) Swap(tmp, p string) error { return f.rename(tmp, p, meta.RenameExchange) }

func (f *FS) rename(tmp, p string, flags uint32) error {
	if path.Dir(tmp) != path.Dir(p) {
		return fmt.Errorf("rename across directories: %w", syscall.EINVAL)
	}
	dir, leaf, err := f.parent(p)
	if err != nil {
		return err
	}
	var ino meta.Ino
	var attr meta.Attr
	return errno(f.m.Rename(f.ctx, dir, path.Base(tmp), dir, leaf, flags, &ino, &attr))
}

// Remove unlinks a non-directory without moving it to the trash. It is a
// lookup and then an unlink by name (JuiceFS has no unlink-if-inode); the
// reconciler only removes under random hidden temp names it recorded.
func (f *FS) Remove(p string) error {
	dir, leaf, err := f.parent(p)
	if err != nil {
		return err
	}
	var ino meta.Ino
	var attr meta.Attr
	if st := f.m.Lookup(f.ctx, dir, leaf, &ino, &attr, false); st != 0 {
		return st
	}
	if attr.Typ == meta.TypeDirectory {
		return syscall.EISDIR
	}
	return errno(f.m.Unlink(f.ctx, dir, leaf, true))
}

// List returns up to max entry names of the directory p.
func (f *FS) List(p string, max int) ([]string, error) {
	ino, attr, err := f.lookup(p)
	if err != nil {
		return nil, err
	}
	if attr.Typ != meta.TypeDirectory {
		return nil, syscall.ENOTDIR
	}
	var entries []*meta.Entry
	if st := f.m.Readdir(f.ctx, ino, 0, &entries); st != 0 {
		return nil, st
	}
	var out []string
	for _, e := range entries {
		if name := string(e.Name); name != "." && name != ".." && len(out) < max {
			out = append(out, name)
		}
	}
	return out, nil
}

// Rmdir removes the empty directory p without moving it to the trash. Like
// Remove it works by name; the reconciler only removes a view directory
// after moving it to a random hidden name it recorded and checking that
// the directory there is its own.
func (f *FS) Rmdir(p string) error {
	dir, leaf, err := f.parent(p)
	if err != nil {
		return err
	}
	return errno(f.m.Rmdir(f.ctx, dir, leaf, true))
}

// RemoveIno unlinks the non-directory p, bypassing the trash, only if the
// lookup right before it finds inode ino.
func (f *FS) RemoveIno(p string, ino uint64) error {
	dir, leaf, err := f.parent(p)
	if err != nil {
		return err
	}
	var got meta.Ino
	var attr meta.Attr
	if st := f.m.Lookup(f.ctx, dir, leaf, &got, &attr, false); st != 0 {
		return st
	}
	if uint64(got) != ino {
		return fmt.Errorf("%s is no longer the entry that was checked", p)
	}
	if attr.Typ == meta.TypeDirectory {
		return syscall.EISDIR
	}
	return errno(f.m.Unlink(f.ctx, dir, leaf, true))
}

// The read-only lookups below serve the continuity binder. Each is plain
// metadata reads (GET i<ino>, HGET/HGETALL d<dir>, HGETALL p<ino>, or the
// allow-listed scriptLookup): reads pass the RESP proxy unchecked
// (respproxy needsGate), whatever the principal.

// VolumeUUID is the UUID in the volume's format, loaded by Open.
func (f *FS) VolumeUUID() string { return f.m.GetFormat().UUID }

func node(ino meta.Ino, attr *meta.Attr) Node {
	return Node{
		Ino:     uint64(ino),
		Type:    info(ino, attr).Type,
		Nlink:   attr.Nlink,
		Parent:  uint64(attr.Parent),
		InTrash: attr.Parent.IsTrash(),
		Ctime:   time.Unix(attr.Ctime, int64(attr.Ctimensec)),
	}
}

// LookupIno resolves a volume-relative path to its inode without following
// a symlink in any component (smartfolders.ErrSymlinkInPath).
func (f *FS) LookupIno(p string) (Node, error) {
	ino, attr, err := f.lookup(p)
	if err != nil {
		return Node{}, err
	}
	return node(ino, &attr), nil
}

// StatIno reads one inode's attributes; a removed inode is ENOENT.
func (f *FS) StatIno(ino uint64) (Node, error) {
	if !meta.Ino(ino).IsNormal() || ino == uint64(meta.RootInode) {
		return Node{}, syscall.EINVAL
	}
	var attr meta.Attr
	if st := f.m.GetAttr(f.ctx, meta.Ino(ino), &attr); st != 0 {
		return Node{}, st
	}
	return node(meta.Ino(ino), &attr), nil
}

// GetPaths returns the volume-relative paths (no leading slash, sorted)
// at which the inode is linked outside the trash: none for a file only in
// the trash, several for a hard-linked one. JuiceFS's own GetPaths skips a
// directory it fails to read (logging only), so a list with fewer names
// (trash ones included) than the inode's link count is EIO, never a short
// answer. It reads every directory on the way up (whole listings), so it
// is for rare paths, not every event.
func (f *FS) GetPaths(ino uint64) ([]string, error) {
	n, err := f.StatIno(ino)
	if err != nil {
		return nil, err
	}
	all := f.m.GetPaths(f.ctx, meta.Ino(ino))
	if uint64(len(all)) < uint64(n.Nlink) {
		return nil, fmt.Errorf("inode %d: found %d of its %d names: %w", ino, len(all), n.Nlink, syscall.EIO)
	}
	var out []string
	for _, p := range all {
		// Anything not absolute is JuiceFS's "outside of the mounted
		// root" note, not a path.
		rel, ok := strings.CutPrefix(p, "/")
		if !ok || rel == "" || rel == meta.TrashName || strings.HasPrefix(rel, meta.TrashName+"/") {
			continue
		}
		out = append(out, rel)
	}
	sort.Strings(out)
	return out, nil
}

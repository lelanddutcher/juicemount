package nfs

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// A namespace mutation that outlives the RPC mutation bound is answered
// NFS3ERR_JUKEBOX while its worker keeps the names and finishes the change
// (namespace_operations.go). The client then resends the same request, and by
// then the change it asked for has already happened. Running it a second time
// acts on whatever the names hold now:
//
//   - RENAME: with the source gone, the ordinary path cancelled the
//     destination's spool entry before discovering that, discarding the bytes
//     of the file just renamed there. With the source recreated by someone
//     else, it replaced the file the first attempt moved.
//   - REMOVE/RMDIR: a file or folder recreated at the name is deleted.
//   - CREATE: unchecked truncates a file someone has already written to;
//     GUARDED and EXCLUSIVE fail NFS3ERR_EXIST on the file they created.
//
// completedMutations remembers each such late success briefly, so the resend
// of exactly that request is answered success without running it again. There
// is no XID duplicate-request cache to lean on: the macOS client gives a
// JUKEBOX retry a new XID (NFS kext nfs_request_finish: "req->r_xid = 0; //
// get a new XID"). The key is therefore the request itself: the procedure
// (REMOVE and RMDIR apart), the names and, for CREATE, the mode and verifier.
// An entry is replayed at most once.
//
// A replay does not depend on what the names hold now. The request is known
// to have completed after the client last saw these names, so anything there
// now was put there by someone else afterwards, and running the request again
// could only act on that. The price is ambiguity the server cannot resolve: a
// genuinely new, identical request inside the TTL, before any resend consumed
// the entry, is answered without running. The client cannot tell the two
// apart either. Masking it costs "it did not happen, try again"; running a
// resend costs silent loss of someone's unuploaded data. One-shot consumption
// keeps the window to a request the client sent instead of its own resend,
// which needs the first request to have been abandoned (an interrupted call
// on an intr mount) and the same request repeated within the TTL. An
// EXCLUSIVE CREATE has no such window: every new one carries a new verifier.
//
// The key also carries the RPC credential, but in production that separates
// nothing: Mount offers only AUTH_NULL and the mount options set no sec=, so
// macOS sends AUTH_NONE and every request shares one principal. Even under
// AUTH_SYS, macOS sends an empty hostname, so the credential names a uid, not
// a client. It is kept as defense in depth for an AUTH_SYS client.
//
// The TTL follows the same client code. It waits NFS_TRYLATERDEL (4 s) before
// the first resend and doubles the wait up to a 30 s cap. A resend that lands
// while the worker still owns the names is answered JUKEBOX again, so the
// resend that finds the finished state arrives within one cap interval of the
// worker finishing (the entry is recorded at that moment). 40 s is that cap
// plus margin for queueing and the client's one-second sleep granularity.
// It is kept short on purpose: on an intr mount a resend can be abandoned,
// leaving the entry live, and an app that saves atomically under a fixed
// temporary name and is killed mid-save could otherwise have its next save's
// identical rename answered without running.
const (
	completedMutationTTL = 40 * time.Second
	completedMutationCap = 256
)

type completedMutationKey struct {
	op        string // the procedure
	principal string
	from, to  string // RENAME's names; REMOVE, RMDIR and CREATE use from
	how       string // CREATE's mode and verifier
}

type completedMutations struct {
	mu      sync.Mutex
	entries map[completedMutationKey]time.Time // completion time
	now     func() time.Time                   // tests only
}

func (c *completedMutations) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *completedMutations) record(key completedMutationKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if c.entries == nil {
		c.entries = make(map[completedMutationKey]time.Time)
	}
	if _, ok := c.entries[key]; !ok && len(c.entries) >= completedMutationCap {
		var oldest completedMutationKey
		var oldestAt time.Time
		for k, at := range c.entries {
			if now.Sub(at) >= completedMutationTTL {
				delete(c.entries, k)
				continue
			}
			if oldestAt.IsZero() || at.Before(oldestAt) {
				oldest, oldestAt = k, at
			}
		}
		if len(c.entries) >= completedMutationCap {
			delete(c.entries, oldest)
		}
	}
	c.entries[key] = now
}

// take removes the entry for key and reports whether it was live.
func (c *completedMutations) take(key completedMutationKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.entries[key]
	if !ok {
		return false
	}
	delete(c.entries, key)
	return c.clock().Sub(at) < completedMutationTTL
}

// SetMutationPrincipal scopes remembered late mutations to the RPC
// credential that sent them (see completedMutations; under the production
// AUTH_NULL export every request has the same one). It writes the receiver, so
// it relies on FromHandle returning a fresh filesystem value for every RPC. A
// wrapper or cache that shared one value across requests would let concurrent
// RPCs overwrite each other's principal.
func (jfs *juiceFS) SetMutationPrincipal(principal string) {
	jfs.retryPrincipal = principal
}

func (jfs *juiceFS) mutationKey(op, from, to string) completedMutationKey {
	return completedMutationKey{op: op, principal: jfs.retryPrincipal, from: from, to: to}
}

// createKey also carries the mode and verifier: a resend repeats both, and a
// different mode or a new EXCLUSIVE verifier is a different request.
func (jfs *juiceFS) createKey(filename string, how nfslib.CreateHow) completedMutationKey {
	key := jfs.mutationKey("create", filename, "")
	key.how = fmt.Sprintf("%d:%x", how.Mode, how.Verifier)
	return key
}

// replayOrRun runs inside the namespace worker, owning the request's names.
// A remembered late success of the same request is answered without running
// it; otherwise run executes it. Either success is remembered if this reply,
// too, is lost to the bound (op.completed runs only then). replay may decline
// (return false) when there is nothing a second run could damage.
func (jfs *juiceFS) replayOrRun(op *namespaceOperation, key completedMutationKey, replay func() bool, run func() error) error {
	remember := func() { jfs.handler.completedMutations.record(key) }
	if jfs.handler.completedMutations.take(key) && (replay == nil || replay()) {
		op.completed = remember
		op.checkpoint("replay", key.from)
		return nil
	}
	if err := run(); err != nil {
		return err
	}
	op.completed = remember
	return nil
}

// renameOnce is rename with replay of a late success.
func (jfs *juiceFS) renameOnce(oldpath, newpath string, op *namespaceOperation) error {
	return jfs.replayOrRun(op, jfs.mutationKey("rename", oldpath, newpath), nil, func() error {
		return jfs.rename(oldpath, newpath, op)
	})
}

// removeOnce is remove with replay of a late success. proc is "remove" or
// "rmdir": the two share remove, but a resend repeats its own procedure, so
// a late REMOVE must not answer an RMDIR of the same name (or the reverse).
func (jfs *juiceFS) removeOnce(proc, filename string, op *namespaceOperation) error {
	return jfs.replayOrRun(op, jfs.mutationKey(proc, filename, ""), nil, func() error {
		return jfs.remove(filename, op)
	})
}

// createOnce is create with replay of a late success. The replay answers with
// the existing file untouched instead of truncating it. If the name no longer
// holds a file there is nothing to protect, and the create runs, so the
// client is not handed a handle to a missing name.
func (jfs *juiceFS) createOnce(filename string, how nfslib.CreateHow, op *namespaceOperation) (billy.File, error) {
	var file billy.File
	exists := func() bool {
		e := jfs.handler.store.LookupByPath(filename)
		return e != nil && !e.IsDir
	}
	err := jfs.replayOrRun(op, jfs.createKey(filename, how), exists, func() error {
		var err error
		file, err = jfs.create(filename, how, op)
		return err
	})
	if err != nil {
		return nil, err
	}
	if file == nil {
		file = replayedCreate{name: filename}
	}
	return file, nil
}

// replayedCreate answers a replayed CREATE. The CREATE RPC only names and
// closes the file it gets back; any other use is refused.
type replayedCreate struct{ name string }

func (r replayedCreate) Name() string                      { return r.name }
func (r replayedCreate) Close() error                      { return nil }
func (r replayedCreate) Write([]byte) (int, error)         { return 0, os.ErrInvalid }
func (r replayedCreate) Read([]byte) (int, error)          { return 0, os.ErrInvalid }
func (r replayedCreate) ReadAt([]byte, int64) (int, error) { return 0, os.ErrInvalid }
func (r replayedCreate) Seek(int64, int) (int64, error)    { return 0, os.ErrInvalid }
func (r replayedCreate) Lock() error                       { return os.ErrInvalid }
func (r replayedCreate) Unlock() error                     { return os.ErrInvalid }
func (r replayedCreate) Truncate(int64) error              { return os.ErrInvalid }

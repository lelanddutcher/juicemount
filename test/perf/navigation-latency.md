# Local navigation latency acceptance

Run from the repository root, choosing a new directory for each receipt:

```sh
scripts/test-navigation-latency.sh "$PWD/build/navigation-latency-001" all
```

The runner builds serially, records the commit and source hashes (including
uncommitted Go inputs), and retains logs, test binaries, and the result table.
It returns nonzero if any selected gate fails or skips, or if a test selector
is empty. Select `ram`, `sqlite`, `sql-contention`, `catalog`, or `catalog-ingest`
to diagnose one part. RAM and SQLite serving run in separate
processes because the production substrate selection is fixed at startup.

The NFS gate starts the production protocol server on an ephemeral loopback
port with the real JuiceMount handler and a temporary SQLite mirror. Backend
file reads are delayed at a test-only ReadAt boundary. Cached GETATTR, LOOKUP, and
READDIRPLUS requests share the same TCP connection as the delayed reads.
Replies are identified by XID and checked for correct content as well as
latency. Each scenario validates twelve navigation responses over the 100-file
fixture; at this sample count p99 is the maximum, not a long-run percentile.
The local gate requires p99 at most 100 ms and no sample above 250 ms.
A held-backend case tests that navigation completes before backend
work is released. Timed cases measure the effect of increasing backend delay.
Saturated reads must receive a valid retry response without exhausting the
navigation admission pool.

The `catalog` gate seeds 131,000 videos in one directory (131,102 mirror rows
including the small control folder entries), then enumerates every child through
terminal EOF over the production NFS server. Every name, inode, size, cookie,
and handle is checked, including the stable verifier across pages. GETATTR and
LOOKUP run alongside each page on the same TCP connection. An unrelated READ
stays blocked inside the real foreground FD pool's `os.Open` on a private FIFO
until all navigation replies finish. The NFS request may reach its bounded
open deadline and return JUKEBOX while the underlying syscall remains owned;
otherwise the released FIFO read returns an I/O error because it cannot seek.
Cleanup releases that open and joins the server, handler and open worker before
closing the Store.

The `catalog-ingest` gate adds a concurrent worker calling the production
`Store.ScanUpsertBatch` method. It inserts and then updates a bounded set of
4,000 unrelated paths in 500-row batches; catalog names stay unchanged so the
traversal oracle remains exact. At least two real commits must finish during
the navigation interval. Retained commit timestamps establish overlap, and the
worker must stop and join before the Store closes. This adds one directory row
to the seeded mirror; ingestion and rescanning happen during measured browsing.

Both catalog gates run RAM and SQLite modes in separate processes. They require
p99 at most 250 ms and no sample above 500 ms for each navigation operation.
A five-second reply observation deadline per round retains evidence after a
timing-policy miss, and the complete traversal has a 60-second cap. The first
page includes complete local listing construction; later pages exercise verifier
reuse. Seeding is reported separately and excluded from navigation timing.
The log records every checked page, complete traversal counts, latency samples
and, for ingestion, commit overlap and worker retirement. Both gates are included
in `all`; choose `catalog` or `catalog-ingest` to run either one alone.

These catalog gates cover a locally seeded mirror with stable catalog contents.
They do not qualify concurrent changes or pruning within the enumerated folder,
video content, native thumbnails, kernel mounts, Finder, or a remote network.

The SQL gate holds the mirror's database connection while an unrelated
bulk insert, scan local-only clear, scan prune, mirror rename, conditional file
publication, or directory confirmation starts. The fixtures disable deferred
FTS and stop and join prune-ladder cleanup before reserving the connection and
recording the wait-counter baseline. Only the target operation runs before the
barrier: the global counter cannot distinguish competing SQL waiters. Under
these conditions, the database wait counter establishes that each operation
has reached SQL; cached path, inode, and directory reads must then complete
before the connection is released. This exposes coupling through the RAM
serving lock separately from backend network latency. Retain and independently
classify failures before changing source or rerunning; keep the time budget.

The `sql-wire` companion gate applies the same held-pool condition to the real
NFS handler and checks GETATTR, LOOKUP, and READDIRPLUS over one TCP connection.
The wire cases include those six operations plus prepared CREATE reservation,
abort and completion, and online write intent creation and completion. All
three navigation calls must complete with correct data before SQL is released
for each of these eleven unrelated metadata operations. The wire gate also requires zero recorded
foreground FUSE calls during navigation; background prefetch is separate. These SQL
contention gates use RAM serving: intentionally exhausting the only connection
in the SQLite reader pool would test reader-pool availability instead.

The `sql-absent-publication` gate isolates directory warming's final publication
phase after its durable insert batches have committed. It calls the same private
helper that `BulkInsertAbsent` uses and holds its actual SELECT on the SQL pool.
Cached path, inode, and directory reads must finish before that wait is released;
the published row must retain the committed database fields and version. This
is a Store boundary test, separate from the wire scenarios above.

The `sql-rename` gate holds SQL during the public rename capture and publication
methods. It requires the cache mutex to remain available as well as correct
cached reads before SQL release. A shared lock retained by capture can otherwise
block later readers behind a queued cache writer. These checks preserve the
captured tokens and verify the moved row after publication completes. Copying
cached namespace tokens and publishing a large renamed subtree still require
memory work proportional to the catalog or subtree; this gate does not bound
that work or prove mounted Finder rename performance.

The `sql-mirror` gate covers the public capture and publication methods used
by file opens and directory creation: `CaptureMirror`, `PublishExpected`,
`CaptureDirectoryForFUSE`, and `ConfirmAbsentDirectoryFromFUSE`. While each
waits on SQL, the cache mutex must remain available and unrelated cached
navigation must finish. After SQL resumes, the gate checks captured tokens and
epochs, committed fields, and preservation of an existing synthetic directory
inode. Actual CREATE ownership, replacement, and restart safety have separate
focused tests.

The `sql-intent` gate holds the pinned SQL connection used by
`PrepareCreate`, `AbortPreparedCreate`, `CompletePreparedCreate`,
`BeginOnlineMutation`, and `CompleteOnlineMutation`. It checks that the cache
mutex stays available and cached navigation finishes while durable intent
work waits. After release, direct SQL observations verify custody and row
publication, and RAM checks verify the corresponding reservation and generation
changes. FULL/WAL durability setup and commit-before-publication ordering remain
part of the production path. Concurrent cache-only successors, failed
transactions and restart refusal have separate focused safety tests.

The `sql-prune-commit` gate pauses the real SQLite driver transaction's
`Commit` after scoped-prune admission and SQL deletion. A test-only connector
forwards the driver's context, session and query interfaces; Store workers are
joined before replacing the pool, and the same file-backed WAL database and
Store ownership lock are retained. Cached path, inode and directory reads
must finish while Commit is held. After release, the gate checks exact old-inode
invalidation and deletion from RAM and direct SQL. This isolates a commit wait
that holding an unused SQL connection cannot reproduce. It is a Store boundary
check; concurrent successors, rejection, rollback and post-commit hooks are
covered by focused tests. It does not prove late-child tombstone enforcement.

These tests use local fixtures and require no mount, Redis service, privileged
network configuration, or production data. They model delay at the backend
filesystem boundary; they do not emulate packet loss, kernel NFS caching, the
Finder UI, or real JuiceFS. Mounted Finder, impaired private-route, offline
write/recovery, and packaged Mac acceptance remain separate gates. Uncached
file data still depends on backend availability.

# Continuity catalog

This internal package stores one library's asset identities, immutable revision
declarations, selected heads, relationships, location observations and ordered metadata operations in a dedicated
SQLite database. Opt-in schema 3 also stores policy subjects/grants and separate
local operational authority. It has no routes, media writes, filesystem rename
operations, or verified/published/reachable states. A head selects a declaration;
it does not certify the current contents of a filesystem path.

`Create(path, libraryID)` exclusively reserves a new 0600 file in a caller-owned
private local directory. It refuses existing destinations and orphan sidecars.
`Open(path)` requires an existing complete catalog. Both canonicalize the existing
parent and build an escaped file URI internally; caller-supplied SQLite URIs and
memory databases are refused. Final symlinks, nonregular files, multiple hardlinks,
and unsafe WAL/SHM/rollback sidecars are refused. File identity is qualified on
Darwin and Linux; other platforms refuse admission. The directory's owner and
local filesystem/hardware are deployment preconditions. This does not defend
against privileged replacement of path ancestors.

Format 2 uses application ID `0x4a4d4341`, user version 2, an exact schema-object
inventory, and a migration ledger containing the ordered format 1 and 2 SHA-256
fingerprints. New format 2 catalogs record the same supported lineage; these
entries do not claim two runtime upgrades occurred. A ledger
alone cannot validate changed DDL. Unknown, foreign, newer, incomplete, corrupt,
or constraint-invalid databases are refused without repair or replacement.
Open performs version/DDL/ledger/identity/integrity preflight in one deferred
transaction on a separate read-only connection before obtaining a write pool
and enabling WAL. A recognized complete, integrity-valid format 1 catalog returns
`ErrUpgradeRequired`; Open never migrates it. SQLite integrity and foreign-key checks run at admission;
individual revision/event reads also verify their canonical identities. Open does
not rehash the complete operation history or any media.

Initialization records `ready=0`, establishes WAL, syncs the database and parent
directory, and atomically records `ready=1`, followed by another directory sync.
The file is never removed or replaced on failure. Incomplete files refuse Open;
a failure after final commit begins may have an uncertain outcome. Recover with
Open and check the exact requested library identity. Successful unit tests do not
establish physical power-loss safety.

Each Store has one write-pool connection, with `foreign_keys=ON`, a 2000 ms busy
timeout, `synchronous=FULL`, and `_txlock=immediate` applied through its internally
constructed DSN. Effective settings are checked at admission and mutation.
`database/sql` owns transaction commit/rollback and connection release. Contexts
and the busy timeout bound lock waits. Point reads do not start transactions;
ListAssets and the relationship/location lists return one statement's snapshot
of all returned mutable fields and joined membership checks.
`Close` refuses new calls, drains admitted calls, and closes its database and
held file identity. There are no background workers. Exports use a separate
read-only pool with deferred snapshot transactions and the
same lifecycle draining; the immediate write pool is not an export handle.

`Apply` accepts an explicit library ID, version 1, an operation ID, nonempty Actor,
and 1–256 ordered changes. Exactly one pointer variant is required per change:

- `CreateAsset`: new explicit identity and bounded display label.
- `RenameAsset`: compare the exact old label and write a new display label.
- `RecordRevision`: verify C01 canonical identity and already-present same-asset
  parents; existing immutable declarations may be repeated.
- `AdvanceHead`: SQL compare-and-swap from the explicit prior head. A first head
  must have no parents; later heads must list the expected head as a direct parent.
- `PutRelationship` / `RemoveRelationship`: compare full expected state and
  correct, reactivate or tombstone a directed relationship.
- `PutLocation` / `RemoveLocation`: compare full expected state and correct,
  reactivate or tombstone an opaque location observation.

Changes and one event commit in a single immediate transaction. A failed change
rolls back the entire batch without reserving an operation ID or event sequence.
Already-recorded branches survive a separate head conflict. Callers must retain
their media independently. SQL foreign keys/constraints preserve ownership and
chronology; immutable-row triggers refuse revision, parent-edge, and event
updates/deletes. There is no asset/revision deletion API.

Relationship values contain, in wire order, `source_asset_id`, `source_revision`,
`target_asset_id`, `target_revision`, `predicate`, `provenance`, `source`.
Both endpoints must exist. Optional revision anchors are explicit empty strings
or must belong to the corresponding asset/library. Relationships can form cycles
or self edges; they are separate from the acyclic revision-parent graph.
Predicate is 1–128 UTF-8 bytes; source is 1–256 bytes. Provenance is exactly
`observed`, `claimed` or `inferred`, with caller-supplied attribution only.

Location values contain `asset_id`, `revision`, `source`, `locator`. The optional
revision must belong to the owning asset/library. Source is 1–256 UTF-8 bytes and
locator is 1–8192 bytes. The locator is preserved exactly and never resolved,
opened, parsed for credentials or sent over a network. Callers must supply
nonsecret locators; storage cannot reliably detect every secret. An observation
proves no current presence, read permission, cache state or verified bytes.

Relationship/location IDs use the operation-ID spelling/bound in separate
namespaces. Records carry immutable library, ID and CreatedSequence plus mutable
Value, UpdatedSequence and Removed. Location ownership is immutable for the
ID's lifetime, including after removal; relationship endpoints can be corrected.
Get methods return explicit tombstones; lists exclude them. Removal retains the
last value and creation sequence and sets UpdatedSequence to the removal event.
SQL guards refuse physical deletion, identity/creation changes, backwards
chronology and location owner changes. No operation opens or deletes media.

Put wire fields are `id,expected,value`; Remove fields are `id,expected`.
Expected is explicit null for Put only when the ID has never existed. Otherwise
it is `value,updated_sequence,removed`, with positive sequence and exact value,
sequence and removal-state comparison. Remove requires a non-null expected state
and conflicts if already removed. A tombstone can reactivate only through Put
with its exact expected state; expecting absence cannot reuse the ID.
Sequence comparison rejects ABA edits that restored the same value. SQL updates
also condition the current sequence/removal state. Every correction or removal
updates sequence, including a correction to the same value. Ordered changes in
one operation may share sequence because they are not separate observable commits.

Canonical requests use compact Go `encoding/json` Marshal with fixed struct field
order, HTML escaping enabled, valid UTF-8/no NUL, and no Unicode normalization.
Envelope order is `version`, `library_id`, `operation_id`, `actor`, `changes`.
Each change contains exactly its snake_case variant name. Create fields are
`asset_id,label`; rename fields are `asset_id,expected_label,label`; head fields
are `asset_id,expected_revision,revision`. Revision fields are
`version,library_id,asset_id,content,parents`; content fields are `sha256,size`.
Parents are sorted copies and no parents encodes `[]`. Change order is preserved.
Canonical requests are limited to 1,048,576 bytes. Operation IDs are 1–128 ASCII
letters/digits/dash/underscore; Actor is 1–256 UTF-8 bytes and labels 0–4096 bytes.

Payload SHA-256 hashes ASCII `juicemount.catalog.operation`, NUL, byte 1, then the
complete canonical JSON exactly once. Receipt fields/order are `version`,
`library_id`, `operation_id`, `sequence`, `payload_sha256`, `recorded_unix_nano`.
Sequence is positive and orders events. The server wall timestamp is informational.
Stored requests and receipts must decode strictly and reproduce their canonical
bytes; unknown fields/kinds, duplicate/trailing fields, and noncanonical encodings
refuse readback. Attribution and every effective field participate in retry
identity. An identical retry returns the original receipt before checking mutable
preconditions, without another event. Changed reuse returns `ErrChangedRequest`.
A commit error is uncertain: read back or retry the identical ID and payload.

`LibraryID`, `GetAsset`, `GetRevision`, `GetEvent`, `ListAssets`, `ListRevisions`,
`Events`, `GetRelationship`, `GetLocation`, `ListRelationships` and `ListLocations`
return detached records for trusted internal services. Every record
request explicitly binds the library. Pages are 1–1000 records with exclusive
asset-ID, revision-ID, or event-sequence cursors; empty identity/zero sequence
starts at the beginning. Valid identity keys need not name an existing row.
Pages are independently current, not a multi-page snapshot. Typed error categories
distinguish invalid input, missing records, conflicts, changed retries, closed
stores, and invalid/corrupt format; underlying errors remain available for inspection.

Relationship lists select active inbound/outbound edges for an existing asset,
with self edges once. Location lists select active observations for their owner.
Both use exclusive lexicographic ID cursors and the same 1–1000 page bound.
Their one-statement joins verify endpoints and optional revision membership.
Before disclosure, a permission service must constrain both endpoints, traversal,
counts and every returned field.

`Upgrade(ctx,path,expectedLibraryID)` explicitly changes a recognized format 1
catalog to format 2. It applies the same safe-file admission and read-only snapshot
preflight, then repeats schema/library/integrity checks in an immediate transaction.
The migration replaces only the constrained v1 ledger and its guards, adds the
new projections/indexes/guards, and atomically records user version 2. Original
library/asset/revision/parent/event DDL and data stay intact, including exact
canonical requests, receipts, timestamps and event sequences. Format 1's exact
DDL/fingerprint definition is retained for admission and checked-in legacy tests.
Already-valid format 2 of the expected library succeeds without schema/event
mutation. Both migration and idempotent retry close their owners and complete a
parent-directory sync before success, so retry after a failed final barrier
finishes that barrier. Sync/close/commit errors surface; final outcomes may be
uncertain and must never be recovered by replacing the catalog.

The host should drain catalog users before upgrade. SQLite serializes concurrent
writers/upgraders; transaction revalidation accepts a second already-upgraded
store instead of rerunning the migration. This does not claim exclusive process
ownership. Old writers refuse format 2. Old binaries require a separately retained
compatible backup for rollback; there is no schema downgrade that discards history.

Stored Actor strings are caller-supplied attribution, not authenticated identity
or evidence of human activity. Catalog membership/history checks grant no access
and prove no immutable source snapshot or media durability. Permissions/disclosure
belong to the trusted service boundary. Metadata portability below preserves these
declarations and their history; it grants no import authority or media guarantee.

`Store.Export(ctx,library,writer,Limits)` captures a complete schema2 metadata
snapshot. `Restore(ctx,path,expectedLibrary,reader,Limits)` replays it into a new
exclusive 0600 target only. Restore never merges, resumes, downgrades, overwrites
or removes a failed target. Existing Create/Open/Upgrade behavior is preserved.

All three Limits fields are explicit positive budgets: MaxBytes, MaxEvents and
MaxProjectionRows. Hard ceilings are 64 GiB, 1,000,000 events and 10,000,000 logical
projection rows. Zero is invalid, rather than unlimited. Empty catalogs have no
events and one library projection row. Every magic/frame/footer byte counts.
API guardrails do not make maximum-size runs cheap; callers should use smaller
operational budgets and deadlines. ErrLimit distinguishes a reached budget.

The caller owns reader/writer lifecycle, interruptible I/O/deadlines and durable
output storage. These functions do not close caller I/O, rename export files or
fsync exported bytes. Context is checked between I/O and SQL operations; ordinary
io.Reader/io.Writer cannot forcibly interrupt a blocked call. Impossible counts,
100 consecutive empty reads, read errors accompanying bytes and short nil-error
writes refuse. A full footer write returning an error can still deliver a complete
artifact; an export error is not proof of truncation.

Portable format1 begins with ASCII `juicemount.catalog.export`, NUL, byte1.
Frames contain one kind byte, uint32 big-endian payload length, then that payload.
There is exactly one header, its declared number of events, one footer, then EOF.
Any trailing byte, including whitespace, refuses. Header/footer canonical JSON
uses the existing Go encoding/json escaping and strict decode/re-encode equality.
Unknown/missing/duplicate fields, unsupported versions, malformed UTF-8, extra
frames, sequence gaps, duplicate operation IDs and noncanonical encodings refuse.

| Kind | Payload |
| --- | --- |
| 0x01 | JSON fields `version,schema_version,library_id,event_count`, requiring 1 and 2 |
| 0x02 | uint32 request length, exact request bytes, uint32 receipt length, exact receipt bytes |
| 0x03 | JSON fields `event_count,projection_rows,projection_sha256,stream_sha256` |

Header/footer payloads are at most 1 KiB; event payloads at most 1,049,608 bytes,
with the unchanged 1 MiB request and 1 KiB receipt bounds. Inner and outer lengths
must agree exactly. Lengths and remaining budgets are checked before allocation.
Memory is bounded by a frame/event plus decoder buffers or a bounded revision-ID
page, never the entire history. Stored blob lengths are checked before decoding.

P is SHA256 of ASCII `juicemount.catalog.projection`, NUL, byte1, then every
canonical projection frame. H is SHA256 of ASCII
`juicemount.catalog.export.stream`, NUL, byte1, then magic and all exact header/event
frames, then uint64 big-endian event count, uint64 big-endian projection-row count,
then the 32 decoded P bytes. The footer contains lowercase P/H; its own frame is
excluded from H. Hashes detect mismatch, not sender authenticity or authorization.

Projection frames are internal digest input, with the same framing and at most
64 KiB canonical JSON payload. Scan fixed kinds in this order using SQLite BINARY
identifier ordering, including all tombstones:

| Kind | Ordered JSON fields |
| --- | --- |
| 0x11 library | `library_id` |
| 0x12 assets | `library_id,asset_id,label,head,created_sequence,updated_sequence` |
| 0x13 revisions | `library_id,revision_id,asset_id,canonical_hex,manifest,created_sequence` |
| 0x14 parent edges | `library_id,revision_id,parent_id,asset_id` |
| 0x15 relationships | `library_id,id,value,created_sequence,updated_sequence,removed` |
| 0x16 locations | `library_id,id,value,created_sequence,updated_sequence,removed` |

Canonical manifest hex is lowercase; manifest/value objects retain their existing
field order and explicit empty optional revisions. Parent edges sort by child
then parent. Revision canonical identity, parent membership, endpoint/anchor
membership and chronology are validated. Readiness, ledger, indexes and SQLite
layout are excluded. A future logical field/table/schema needs a reviewed format.

Export holds Store lifecycle admission and a separately owned read-only pool
with one deferred transaction. Recognition, integrity, event history/count and
all projections share that snapshot. It never uses the immediate write pool.
Concurrent writers can progress under WAL; the result represents the captured
snapshot. Cursors close before nested queries. Only after source validation,
successful transaction completion and pool close can the footer be emitted.
Path/context checks precede completion. Close drains an admitted export and its
owned pool; no detached worker or caller-I/O close is used.

Restore validates magic/header/expected library and budgets before reserving a
destination. It initializes schema2 with ready=0 and initial file/directory
barriers. Each event reuses the mutation invariants in an immediate transaction,
inserts its original canonical request/digest/timestamp/receipt, and preserves
sequence exactly. It does not call public Apply or sample time. Incremental
projection-row accounting observes actual existing identities inside the same
transaction. New assets/revisions/parent edges/relationships/locations add rows;
repeated revisions, corrections, removals and reactivations do not. An event
exceeding the row budget rolls back before consuming further input or committing;
the running count advances only after commit, without growing-table rescans.

After a valid footer and EOF, a final immediate transaction independently scans
target history/projections, compares counts/P, and validates schema/integrity/path
identity before atomically setting ready=1. Complete source-history/projection
equivalence is proved by this replay comparison. A structurally valid source
projection edited outside history may export successfully but cannot restore
as the same claimed projection. Export success alone does not prove replayability.

Invalid/truncated input, late reader errors, budget failures and cancellation
before completion retain an incomplete target that Open refuses. Prior complete
replay batches may remain inside it. Final commit/close/directory-sync errors can
leave a completed target with uncertain acknowledgment. Restore succeeds only
after closing owned resources and the final directory barrier; recovery inspects
with Open and the exact expected library/history. A second Restore must refuse
that destination and cannot manufacture certainty by overwriting it.

Schema 3 adds local policy and bearer credentials through explicit
`CreateAuthority(ctx,path,library)` or `UpgradeAuthority(ctx,path,expectedLibrary)`.
Default Create and the 1→2 Upgrade retain schema 2. OpenContext recognizes exact
schema 2 or 3; policy/authentication methods require 3. Versioned DDL fingerprints
retain the historical v1/v2 ledger. Schema-3 files and existing SQLite sidecars
must be regular, single-link files without group/other permission bits; unsafe
files are refused rather than chmodded. Keep their parent directory private.

LocalOwnerService is privileged through OS catalog access. It is separate from
bearer authentication and does not identify a shell user as an authenticated
person. Bootstrap explicitly creates or binds a stable enabled person. Recovery
keeps that person and rotates the persisted incarnation, invalidating all older
credentials/handles. Exact recovery retry returns the same result only while that
result is still current. No authority is automatically activated by opening,
upgrading or restoring a catalog.

Portable subjects/grants use independent policy events, full expected-value and
sequence comparisons, subject authorization generations, retained grant
tombstones, and immutable lifetime identities. Administrative audit/idempotency,
authority binding, credential verifiers and issuance mappings are operational
tables excluded from portable history. Metadata Actor remains attribution.

IssueCredential atomically stores a random selector, verifier and nonsecret
receipt before returning a one-time 256-bit secret. Retry returns no bearer and
reports the original receipt with fresh status. Absolute expiry is explicit:
60 seconds through 24 hours after the server clock on first issuance; there is no
implicit retry-dependent default. Expiry is rechecked on authentication and each
access decision, with the trusted wall clock's rollback limitations.

Authenticate creates an opaque store/incarnation/credential-bound handle.
CheckAccess checks a current deferred snapshot and returns only allowed/denied
for one exact asset and action. Permission requires credential scope AND action
ceiling AND an active matching current grant; library grants cannot widen an
asset credential. An owner bearer has no implicit grant. The separate opt-in
discovery service applies these permissions to projected catalog metadata.
Existing browser and mount entry points do not inherit those permissions.

Schema 3 exports format 2: metadata event frames precede a separately sequenced
policy stream. The combined event count uses the existing event budget; subjects
and grants consume projection rows including tombstones. Separate format-2 hash
domains and subject/grant projection frames protect the complete portable state.
Restore replays both histories and compares the source projection, leaving all
operational tables empty. Format 1/schema 2 bytes remain unchanged.

`cmd/juicemount-catalog` is a standalone local program with global flags before
the command. Commands include init-authority, upgrade-authority, owner
status/bootstrap/recover, subject new-id/list, grant list, policy apply,
credential issue/status/revoke, auth whoami/check, export and restore. Mutations
read canonical AdminRequest JSON from stdin. Issue requires explicit absolute
expires_unix and `--secret-out PATH` (exclusive new 0600 file) or `--secret-out -`.
Exact issuance lookup precedes output reservation, so a matched historical retry
does not touch its original output path. Nonsecret issuance status goes to
stderr; only a first returned bearer goes to the selected secret output.
Credential status accepts `--operation-id` for uncertain issuance recovery or
`--selector` for an existing credential. Auth check reads one bearer line from
stdin and uses `--asset` and `--action`; it prints only allowed or denied.
Export/restore require explicit --max-bytes, --max-events and --max-rows.

The generic run entry point borrows Reader/Writer objects and checks context
cooperatively; it cannot interrupt an arbitrary blocking caller implementation.
Main uses a separate owned-I/O path. On Darwin/Linux, it captures all inherited
stdio flags before changing any, prepares pollable pipe/terminal copies on
descriptors above 2, and keeps control descriptors for restoration. A single
watcher closes owned I/O on timeout, SIGINT or SIGTERM. It is joined before flags
are restored and owners closed; there are no detached read/write workers.
O_NONBLOCK temporarily affects other holders of the same inherited open-file
description during the invocation. Flags are restored on normal completion,
cancellation and setup failure. SIGKILL/process crashes cannot run cleanup.
Regular-file kernel I/O still depends on the filesystem; there is no promise of
preempting arbitrary kernel disk stalls. Named --input files must be regular and
are opened nonblocking; use stdin for a pipe. Owned-I/O support refuses other
unqualified platforms.

Reserved-output close failures are retained alongside write/sync failures.
Uncertain diagnostics include a validated operation ID and known selector,
without raw decoder/error input or secrets. Diagnostic delivery can itself fail;
the caller already owns its explicit operation ID and can inspect credential
status with it. Partial/failed secret delivery can leave an active issuance:
inspect status before deliberate revoke/new-ID reissue. Source and declared
tests alone do not establish CLI, existing integration, or product acceptance.

## Authenticated discovery

NewDiscoveryService borrows an already-open, ready schema-3 catalog with an
active local authority binding and an exact expected library. It never creates,
upgrades or activates authority. Query takes a scoped bearer and a typed request;
caller-supplied subject identities are not authentication. The opt-in Manager
adapter owns HTTP decoding, transport deadlines and deployment configuration.
The domain service owns metadata authorization, buffering, frozen pages and the
final release fence. Disabled host integrations do not open this service.

A single deferred snapshot authenticates the credential, reads current grants
and projects the selected metadata. Permission is the intersection of credential
scope, action ceiling and active grants. Asset heads and revision content/parents
require revisions.read. Edges require discovery and relationship rights on both
endpoints, including every old endpoint in corrected-edge history. Optional edge
and location revision anchors become null without revision rights; null does not
assert absence. Counts are exact for the authorized scope, or null when the
count's action is absent. Locations remain literal observations and are never
opened or probed.

History emits whitelisted per-change before/after values, including tombstone
flags and repeated revision declarations. It preserves newest-event ordering and
original change order within an event while withholding operation IDs, ordinals,
sequences, receipts, digests and hidden changes. Actor/source strings remain
claims. Permission cannot certify that free text contains no private fact.

Search selects authorized context matches first, then binary asset IDs, into a
maximum 1000-item frozen window. Within that window it ranks context, ASCII-folded
label exact/prefix/substring match and stable ID. This is not global relevance or
activity ranking. Active grants are bounded to 4096; candidate/edge/revision/
location scans to 4096, endpoint membership probes to 8192, and history to 8192
events/32 MiB of raw requests plus receipts. Joined membership probes are charged
conservatively, so a request can exhaust that budget before its row budget.
History has no asset-event index: sparse old history can return work_budget.
Budget/decode/deadline failure never becomes empty or complete success. Stopping
at 1000 authorized items returns an incomplete frozen window.

Each collection response states matched_count for its frozen window,
count_scope=frozen_window, window_complete and sort_version=discovery1. Pages
contain at most 100 items and 1 MiB; single items are bounded to 64 KiB. Opaque random
page tokens bind the subject, credential, authorization generation and immutable
query. Replays return identical encoded bytes and next token without advancing
state or extending the five-minute monotonic lifetime. Unknown, foreign, expired
and evicted valid-shaped tokens all require restart_query. Foreign token lookup
does not expire or evict another owner's state.

The service admits 16 calls and four concurrent SQL owners without queuing. Its
64 MiB logical quota includes fixed admission scratch, working/construction/copy
reservations and retained buffer capacities plus query/item/token accounting;
it is not a Go RSS guarantee. Each subject has at most 16 retained or building
queries. Expiration precedes oldest-query eviction. Leased windows cannot be
evicted and remain charged until every output owner drains. No expiration or
response worker runs in the background.

After encoding, Query opens a fresh deferred snapshot and rechecks authority,
credential status and subject authorization generation. Its first authority read
is the release linearization point. After that snapshot closes, it rechecks
credential wall-clock bounds and cancellation immediately before one output
Write. Already-fenced plaintext cannot be recalled. DisclosureResult identity
fields are for private adapter auditing after Query returns, never response data.
Started marks an attempted write, including partial/full-write-plus-error; the
adapter must not append another body. Borrowed writers must provide their own
bounded I/O; context alone cannot interrupt arbitrary implementations. A private,
normally nil beforeFence callback only schedules package tests after encoding
and before the fresh snapshot; no public API configures it.

Close stops admission, cancels calls and drains their output owners before
releasing token state. ErrDiscoveryDraining means callers must retain the borrowed
Store and retry the incomplete close. Terminal resource errors are distinct.
Schema, historical encodings and portable exports are unchanged. Source tests
do not establish actual HTTP host wiring or product acceptance.

## Private selections and contextual ranking

CreatePreferences explicitly creates schema4; UpgradePreferences admits exact
schema3→4 or an idempotent schema4 retry. The existing creation/upgrade APIs retain
their meanings. Schema4 supports the existing authority and v1 discovery APIs,
plus opt-in v2 search/page and person-only select/clear/own-asset status. Manager
supplies the real scoped HTTP adapter; no bearer identity comes from an Actor,
subject argument or deployment key. A person declaration is not proof of a
physical human click, read or edit. Discovery, previews, indexing and prefetch
never record a selection.

Preferences are bounded private operational state in the same catalog, separate
from immutable file custody. Select requires current assets.discover AND
activity.record through credential scope, action ceiling and grants. Clear uses
the current opaque token; exact own-asset status recovers that token after client
restart. Service identities are independently refused. Tokens and generation
checks prevent a stale clear from clearing a replacement selection. Clear
physically deletes its active row; it does not delete a file, metadata or grant.

Each request has an explicit absolute expiry, initially60–900 seconds ahead.
Within validity an identical operation returns its original receipt without
changing recency; changed retry conflicts. At expiry the original request refuses
even after receipt cleanup. A receipt says operation_result and does not assert
that its token is still current. Commit/output failures can be uncertain; keep
the operation ID, retry within its finite validity or inspect current own state.
No permanent selection-operation history is promised.

The caps are1024 permanent person-generation rows,256 preferences/person and
65536 globally,128 retry receipts/person and32768 globally. Permanent rows never
reset or disappear; reads never allocate them. At capacity refuse busy rather
than evicting live state. New-person admission can permanently refuse after1024
people have used this feature. Entries expire after30days. Unexpired receipts
are never evicted; expired entries/receipts are pruned in bounded indexed batches
during an authenticated maintenance phase, committed before final mutation
admission. Thus a later refused mutation does not undo cleanup progress. The
privileged local owner can request a bounded prune batch without a new worker.
The stated <=97MiB operational charge bounds logical row values/accounting,
not SQLite/WAL file size, whole-catalog storage or RSS.

Every committed preference mutation/cleanup advances a durable wall-clock floor.
Clock rollback below it refuses across restart. Reads do not persist time; v2
responses also bind their captured lower time and the earliest contributing
selection expiry across the whole frozen window. Generation, floor and expiry
are checked in the fresh post-encoding fence and again after read cleanup. Own
select/clear or expiry requires restarting old ranked pages. Wrong-version page
presentation refuses before changing the original token's state. This does not
protect against expiry never durably observed or an off-device backup restoring
older state; credential wall-clock limitations remain explicit.

V2 includes authorized recent selections after explicit context candidates and
before the stable-ID candidate fill, so a recent high-ID asset can enter the
bounded window. Within that window the tuple is context, label tier, own recency,
stable ID, with categorical declared_selection reasons. Another person's activity
and service activity do not supply personal features. This is bounded contextual
ordering, not global relevance or proof of representation suitability. V1 behavior
is unchanged.

Schema4 metadata export deliberately emits the unchanged format2/schema3 logical
projection. All preferences, generation/counter/clock state and retry receipts
are omitted. Format2 restore creates schema3 with inactive authority and no
preferences or credentials; explicit upgrade/binding is required. This is not a
preference backup. A raw DB/WAL/backup may contain preferences and credentials,
and a local OS/catalog owner can inspect them. Cleared asset associations may
remain in finite retry requests for up to15minutes, or in WAL/snapshots/backups;
there is no secure-erasure or encryption-from-owner claim. Actual CLI/HTTP host
qualification and independent frozen-source review remain required.

The Manager Library pane (`#/library`) is an opt-in client for these existing
scoped APIs at either Manager prefix. It accepts the library ID and access
credential issued by the local owner CLI; this is separate from Manager admin
authentication. The credential stays only in the current tab's memory and is
forgotten on disconnect, reload or pagehide. No automatic catalog ingestion or
integrated credential provisioning is implied.

Library v2 searches use explicit text/context submission and opaque frozen
pages. Returning to a visible pane first revalidates the complete context prefix
(up to64IDs, at most5pages) before publishing fresh labels and search results.
Inspector sections read permitted v1 declarations with at most4 concurrent reads;
null counts/heads remain undisclosed, locators are inert recorded claims, and
unsafe JavaScript numeric values are labelled outside its exact range. Each
rendered list is bounded to1000items. Client transport streams at most8MiB under
an owned10second fetch/body deadline, with redirects refused and no cookies.

Remember/clear are deliberate personal preference actions, separate from reading
or using temporary context. A dispatched action without a matching validated
receipt retains its exact operation/expiry/body for explicit retry, including
valid503/504/restart responses which can follow COMMIT. Fresh status does not
prove a historical unknown result. Expiry discards retry bytes into one bounded
warning, requiring fresh status and explicit acknowledgement before another
new action. The UI supplies neither durable collections nor media delivery.

Client state/transport/rendering checks use the actual dependency-free module:
`node --test test/continuity-ui/library.test.cjs`. Those checks do not qualify
browser integration, storage or release readiness; real Manager browser evidence
at both prefixes remains a separate acceptance requirement.

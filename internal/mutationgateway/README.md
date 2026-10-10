# Authenticated upload custody

`juicemount-gateway` accepts immutable chunks over TLS 1.3 with verified client
certificates. A fixed volume has one owner epoch. Claim creates a random session
bound to the full client certificate SHA-256; renewal requires that exact live
session and epoch. Expiry rejects new uploads. An expired owner can still inspect
and read its exact committed chunks, including after a successor takes ownership.

The only routes are `POST /v1/claim`, `POST /v1/renew`,
`PUT /v1/custody/upload`, `POST /v1/custody/status`, and
`POST /v1/custody/readback`. `Client` implements these typed APIs, validates upload
bytes, checks receipts, and verifies readback against the complete artifact hash.
It performs no automatic mutation retry. Chunks are at most 8 MiB; the listener
bounds request bodies, concurrent requests, headers, and network timeouts.

Provision an empty mode-0700 directory on durable local storage, owned by the
gateway account. Ancestors must be trusted and remain stable. Run the executable
with `--state`, `--volume`, `--tls-cert`, `--tls-key`, and `--client-ca`; the
default listen address is loopback port 9443. The CA is a dedicated gateway client
trust root. Client certificates are credentials; protecting/revoking them and
qualifying the storage device are deployment requirements. There is no plaintext
listener or proxy-header authentication. No credentials are passed to children.

The state format is **custody-only**. Unknown databases, volume changes, and
non-custody unfinished operations are refused. Each upload first journals the
immutable operation, then exclusively creates a private artifact containing the
canonical request and payload. File/directory sync precedes the journal receipt.
No stage is overwritten or deleted. Readback and receipt retries verify the
entire artifact, not merely its name or size. Startup enumerates every PREPARED
operation, verifies and syncs exact complete artifacts, and commits reconciled
custody. Missing, partial, or corrupt artifacts stay uncertain and block successor
admission. This version has no stage garbage collection; provision capacity
accordingly. Request cancellation cannot authorize cleanup.

`Gateway.Close` permanently stops new request admission, including when it
returns `ErrInFlight`. Already admitted requests retain their resources and may
finish committing custody after the client disconnects. Drain the HTTP server,
then retry `Close`; never reuse the closing gateway. A process-pinned backend
authority returns `ErrProcessLifetime` on every drained close and keeps its
journal lock until actual process exit. A library close is not proof that all
backend workers have stopped.

**CustodyReceipt is not a media publication receipt.** This executable has no
JuiceFS mount, media namespace mutation, public path cleanup,
spool retirement, or local hole-punch operation. Existing writers have not been
migrated. Integration must preserve local bytes until an actual custody/readback
adapter is qualified. Media publication must prove object and metadata durability
and consume a distinct receipt in an exact-generation spool transaction. It must
use this same whole-volume authority after an explicit typed schema migration.
Creating a second media journal would permit conflicting owners.

Optional `--sdk-config` attaches the pinned in-process JuiceFS SDK to an exact
preexisting volume under this same journal. See the [backend configuration
contract](../mutationbackend/README.md#existing-volume-sdk-attachment). Startup
and each request verify the immutable backend binding; attachment failure never
falls back to custody-only serving. This enables session lifecycle only and does
not change any custody route or receipt into media publication.

Tests exercise real mTLS, identity/epoch refusal, immutable readback, process
crashes, and receipt recovery on local storage. They do not establish NAS power
loss durability, legacy credential isolation, or complete offline write safety.

## Redis connection-fence qualification

The opt-in `redisfenceacceptance` gate starts its own Redis 7.4.11 processes on
private Unix sockets. It accepts a binary path and its SHA-256, never an existing
Redis endpoint. Build the [official 7.4.11 archive](https://download.redis.io/releases/redis-7.4.11.tar.gz)
after verifying its SHA-256 is
`3c266ece0abd54ed3b1c912c6eb86b7508cf382cb690ee6649d3843f018f6357`
against the [upstream release hashes](https://github.com/redis/redis-hashes).
The native qualification build uses
`make -j4 MALLOC=libc BUILD_TLS=no REDIS_CFLAGS= REDIS_LDFLAGS= redis-server redis-cli`.
Then run from the repository root, supplying absolute local paths:

```sh
TMPDIR=/absolute/private/scratch/ \
JM_REDIS_FENCE_SERVER=/absolute/redis-7.4.11/src/redis-server \
JM_REDIS_FENCE_SERVER_SHA256=sha256_of_the_built_binary \
go test -tags=redisfenceacceptance ./internal/mutationgateway -run '^TestRedisFence' -count=1 -timeout=60s
```

Run the same command with `-race` for the race gate. All four top-level tests
and both restart subtests must execute; missing inputs fail rather than skip.
Each owned Redis child is joined, and every fixture directory is removed.

The tests distinguish three behaviors:

- `ACL SETUSER off` prevents fresh authentication but leaves existing writers
  usable. It is not a connection fence.
- `ACL DELUSER` deauthorizes and closes retained clients. The gate checks seven
  distinct old connection IDs, queued transactions, a blocked command with
  buffered input, pool reconnect refusal, and a successor's unchanged bytes.
  A running write script returns BUSY; it must settle before revocation succeeds.
- Saving a worker in an ACL file before revocation permits its credentials to
  return after restart. Runtime-only workers with a static default-denied ACL
  baseline fail authentication after the same data directory restarts.

This qualifies only the tested standalone, unreplicated Redis primitive. The
default user must remain explicitly disabled with all commands denied, workers
must lack alternate credentials and administrative permissions, and a fence must
reject a changed Redis process identity. Production must enforce the startup ACL
baseline and exclusive provisioning authority, including any retained privileged
control connections. Neither this test-local revoker nor a successful Redis reply
is a production gateway takeover or a durability proof. JuiceFS pool integration,
legacy writer isolation, restart/failover policy, and media publication remain
separate requirements. The optional SDK attachment uses the production controller; this primitive gate
by itself does not qualify the complete attachment.

The journal uses typed schema version 3. Before reconciliation or SDK startup, the custody-only gateway validates all stored requests, including terminal ones, and atomically classifies recognized legacy custody rows under closed admission. Custody-only startup refuses media or unknown operations even if their other fields resemble custody. With SDK configuration, supported stage-v1 media requests are separately preflighted, fenced and retained before custody reconciliation and admission; all unsupported media remains refused. Existing custody artifact headers, request digests, and wire receipt IDs remain stable: receipt IDs use the original fixed nine-field journal projection. This migration adds no media publication endpoint or permission to retire local bytes.

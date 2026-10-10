# In-process Redis authority controller

This package supplies the custody gateway's optional in-process JuiceFS 1.3.1
attachment and Redis lifecycle controller. Attachment does not publish media or
issue a media durability receipt. Existing application writers have not been
migrated.

`Open` consumes a single initialization attempt on the existing whole-volume
`mutationauthority.Journal`. It pins the journal, database, and process lock
until actual OS process exit, including after initialization failure. Calling
`Close`, closing HTTP handlers, or dropping Go references cannot release that
pin. There is intentionally no unpin API: the intended filesystem SDK has
background writers that cannot be completely joined by its library close.

The supported backend is a dedicated Redis 7.4.11 standalone, unreplicated
primary on a private same-host Unix socket, without modules or failover. Socket,
secret, and ACL paths must have owned private parents and stable canonical
ancestors that other users cannot replace. The deployment must confine every
control credential holder to the cooperating gateway process. A writable child,
external controller, or leaked administrative secret violates that requirement;
`CLIENT KILL` does not revoke the static control password.

Deployment supplies a private 64-character lowercase hexadecimal control secret
and the exact mode-0600 ACL file returned by `StaticACL`. The default user is
denied; only the control user is persisted. Startup refuses a changed baseline,
unknown users, and a Redis run ID different from any retained process record.
It never repairs unexpected state or persists runtime worker credentials.

On one non-reconnecting control connection, startup removes old control
connections, waits for their absence, and checks the static and runtime baseline
again. It revokes every previous journaled worker and waits until only its own
control connection remains. A unique new worker identity is committed to the
same journal before its restricted runtime ACL is provisioned. Worker permissions
are explicit command names; administrative and future commands are not granted.

`Check` must succeed before admitting backend work. Any control loss, baseline
change, restart, or check error permanently refuses new work in that process.
`Freeze` closes control admission and its connection, but neither joins SDK
writers nor releases the journal. Previously admitted effects still need explicit
reconciliation; revocation is not evidence that an earlier write did not happen.
The process records grant no media publication authority or durability proof.

The `backendacceptance` gate uses only its own Redis processes. It requires the
pinned binary and a persistent private scratch directory, and fails instead of
skipping when either is missing:

```sh
TMPDIR=/absolute/private/scratch/ \
JM_REDIS_FENCE_SERVER=/absolute/redis-7.4.11/src/redis-server \
JM_REDIS_FENCE_SERVER_SHA256=sha256_of_the_built_binary \
go test -tags=backendacceptance ./internal/mutationauthority ./internal/mutationbackend ./internal/mutationgateway -count=1 -timeout=90s
```

Repeat with `-race` for concurrency checks. The gate covers process-lifetime
retention, initialization refusal, immutable credential provenance, control
connection loss, unknown users, unsafe socket parents, and queued control/worker
transactions across a real process takeover. It does not qualify the actual SDK
pipeline, arbitrary queued privileged ACL changes, Redis restart reconciliation,
object/AOF power-loss durability, or external writer exclusion.

## Existing-volume SDK attachment

The dedicated gateway executable accepts `--sdk-config` naming a private 0600
JSON file with `redis` (`socket`, `acl_file`, `control_secret_file`) and
`format_sha256`. The digest is SHA-256 of the **exact existing bytes** in Redis
key `setting`, obtained during deployment provisioning. The gateway never
formats, repairs, creates a bucket, or accepts a newly discovered format. Its
`--volume` must equal the format UUID. Redis remains dedicated, same-host and
subject to all controller requirements above.

S3/MinIO additionally require explicit `region` and boolean `path_style`, plus
nonempty access/secret credentials in the existing format. Their supported
endpoint form is `http[s]://host/bucket`. File storage requires a canonical
absolute directory with a trailing slash and no provider credentials. Storage
construction preserves volume-name prefix, sharding, hash prefix, compression,
storage class when supported, and object encryption. Shards are bounded at 256;
block sizes must be powers of two from 64 KiB through 16 MiB. Ranger, unknown
fields/options, ambiguous endpoint paths, and custom TLS URL switches are
refused; default system TLS verification remains enabled. Provider deployment,
TLS certificates and power-loss durability still require separate qualification.

An encrypted private key may use `encryption_passphrase_file` and
`encryption_passphrase_sha256`: both are required together and bind the exact
private file bytes (no newline trimming). No environment passphrase is read.
Inherited AWS/MinIO/JuiceFS/provider-password/proxy variables are refused. For
S3/MinIO, the dedicated process explicitly disables AWS shared-profile and
instance-metadata discovery and installs only the configured region/style.
These process-global settings are why a process gets one SDK attachment attempt.

After pinning the same authority journal, startup validates the format and
persists a versioned digest of the full SDK configuration in that journal before
constructing provider/session capabilities. A later process cannot change the
format, routing, or passphrase binding, or omit SDK configuration and start a
custody-only fallback. Changes require an explicit migration; there is no silent
adoption or reset. Failed attachment never publishes a listener and retains its
journal until the dedicated process exits. Library diagnostic output is
suppressed to avoid disclosing provider secrets; failures identify a static
startup stage.

Every request checks the controller and the exact raw format independently of
SDK reload callbacks. Any mismatch permanently denies admission. Writeback and
deletion/background cleanup are disabled for this constructor/session-only
integration. Close performs best-effort library cleanup once, but the authority
pin remains until OS process exit. No raw SDK filesystem is exposed to clients.

The `sdkacceptance` build tag adds owned-process existing-volume, failure,
reopen, prefix/encryption, and actual gateway CLI tests. Supply the same pinned
Redis binary/digest and private TMPDIR as above, plus `JM_GATEWAY_BINARY` naming
the separately built candidate gateway. Run normal and race modes with matching
candidate binaries. These gates never accept a live Redis endpoint and do not
qualify a media publication receipt or provider power-loss durability.

The SDK's raw object PUT path requires a create-only provider operation and a full direct provider readback of the exact size and SHA-256 before returning success to the chunk writer. The gate runs below the volume prefix and encryption wrappers, through the same shard routing, so it checks the actual prefixed ciphertext without a JuiceFS cache shortcut. Input and response reads are bounded; failed, truncated, corrupt, trailing, or unclosed responses remain errors. Provider attributes, storage-class configuration, and shutdown delegation are preserved.

All SDK object deletion is refused at that same raw boundary with an explicit error, without contacting the provider. Disabling metadata background deletion alone is insufficient: the SDK's failed-upload abort path also deletes block keys synchronously. Neither upload failure nor Fsync failure authorizes removal of uncertain bytes. Failed uploads can therefore leave retained objects; reclamation requires a separately qualified exact-object ownership and retirement protocol. This refusal does not stop external storage lifecycle rules or other credential holders from deleting objects.

Admission checks a fresh reserved key in every configured shard: one verified create, a conflicting create that must return the recognized precondition error, and unchanged readback. Ignored conditions, generic conflicts, and providers that overwrite while reporting refusal prevent startup. A 15-second admission deadline retains the process pin; it does not join an outstanding provider call. Probe objects are retained under `<volume>/.juicemount-create-only/`, one small object per shard per attempt. The journal binding is now `juicefs-1.3.1-sdk-config-v2-create-only`; old bindings require explicit migration and are refused, never silently upgraded.

This adds one object GET per successful PUT attempt and therefore additional backend traffic and latency; it is currently confined to the optional gateway SDK. This raw gate establishes current readable object integrity. The operation stage API described below adds attribution and closes admission; Redis/provider durability and exclusive namespace authority remain required before any media receipt or client spool retirement.

`TestSDKTimeoutCannotOverwriteCompletedObject` (tag `sdkacceptance`) holds an encrypted PUT beyond the SDK timeout, lets its retry complete Fsync and Redis metadata publication, then releases the old request. A conditional provider refuses the old request and the acknowledged ciphertext stays identical. `TestSDKDeadProcessCannotOverwriteSuccessorObject` buffers an old SDK object's request, kills and reaps that process, admits a successor through the same journal/Redis fence, writes the same key, then releases the old request. It tests the configured object path, not filesystem namespace publication. Neither test treats cached plaintext as physical-object proof.

A conflict is always an error, even if plaintext matches. If the first attempt committed before its response was lost, a retry therefore refuses Fsync and retains uncertainty. Live S3/MinIO capability, lifecycle/delete-marker policy, external credential exclusion, and file-provider power-loss behavior still require qualification; an admission probe is not proof of these properties.

The pinned SDK library is supplied by the repository's `third_party/juicefs` module replacement. Its reviewed patches synchronize the upload-failure cancellation flag and add create-only S3/MinIO/file operations with unchanged ordinary PUT behavior. `python3 scripts/verify-juicefs-sdk.py` verifies the snapshot and recorded patch; the snapshot README documents upstream archive verification. Error propagation and the process-lifetime ownership pin are unchanged.

## Operation-bound media staging

`SDK.StageMedia` accepts immutable custody identity and a bounded byte stream, records a canonical intent before creating a reserved stage, and binds the exact inode with an operation xattr before payload writes. The shared SDK allocates each slice through an inode-aware journal hook. Every raw ciphertext PUT attempt records its slice, provider, bucket, shard, key, size and SHA-256 before provider access; exact readback marks that attempt verified. Per-slice allocation is serialized so initial upload and Fsync cannot register competing IDs for one writer. Copy and multipart mutation bypasses are refused.

`SDK.StageMediaForNewName` additionally records a versioned absolute destination and the originating operation's root-to-parent component/inode identities before private stage creation. It checks the supplied chain and absent destination without adopting replacement parents. This check is an early refusal only; publication still requires the same predicates inside its namespace transaction. Version-one stage bodies retain their exact encoding and cannot gain a destination through a changed-request retry. Both preparation APIs retain private stages with one root link; neither publishes a name, returns a media receipt, or authorizes local spool retirement. Version-two stages may subsequently use the explicit publication API below.

After Fsync and Close, exact contiguous slice coverage and the complete object set must match. The journal permanently closes admission before returning the stage checkpoint: an earlier admitted attempt blocks closure until verified, and a later callback is refused before provider access. Errors and crashes retain stages and uncertain objects. No automatic replay, adoption, deletion, namespace publication, media receipt, or client spool retirement is implemented. The custody gateway has no media endpoint. On restart, the optional SDK can retain these stages under the containment protocol below; it cannot publish them or issue media receipts.

Owned SDK acceptance kills and reaps the staging process at intent, create, xattr, inode binding, object intent, object readback, Fsync and closed-manifest boundaries, then reopens the journal. A multi-slice encrypted fixture spans both shards and checks exact raw bytes; overlapping operations must retain distinct manifests. These tests also start a fenced successor at every crash boundary and require a new distinct stage while preserving the old manifest. They do not qualify provider/Redis power-loss durability or public namespace recovery.

## Retained-stage recovery

SDK startup preflights every media row before acquiring credentials, completes the production process/Redis fence, and validates the exact canonical intent, backend binding, inode/name/xattr, slice ranges, raw object routes and closed-manifest hash. An unbound create/xattr crash gap retains its path reference without adopting or deleting the entry. Only after all candidates validate does one transaction record generation-bound retention certificates in the same journal. Every old operation remains PREPARED, including closed stages and pending PUTs. A fresh journal process must fence and validate again; persisted certificates alone never authorize Claim. New uncertain work still blocks another epoch.

The gateway preflights mixed custody/media state, classifies only recognized legacy custody rows, opens the SDK and certifies retained stages, then reconciles custody before exposing admission. Unknown, missing, malformed or terminal media records are refused. Failure after SDK attachment retains the process pin. Retention does not read back or adopt a late object, replay an old write, create a destination, issue a receipt, or authorize spool retirement. Tests include a buffered old PUT committing after successor admission and a new distinct encrypted stage.

Reserved `/.juicemount-stage-*` entries are real backend namespace files. They are separate from destination publication but are not invisible to a read plane; client-facing exclusion remains required. Stage-v1 contains no destination or parent identity. Publication needs a separate immutable operation binding those identities, complete predecessor conditions and a selected retained stage before any public effect. External writer/deletion exclusion and provider/Redis durability remain deployment qualification requirements.


## Conditional new-name publication

`SDK.PublishMediaForNewName` accepts only a closed version-two stage with its
original destination and parent identities. A schema-four journal intent binds
the exact stage snapshot and revokes previous private recovery exemptions before
any public effect. Publication excludes concurrent staging; an uncertain current
stage requires a fresh process. Repeated publication attempts are refused.

The pinned Redis primitive validates the original ancestor chain, absent target,
private inode, marker and parent map under WATCH. Its transaction creates the
public link and applies that link's directory length, space and inode deltas
together. It bypasses the ordinary SDK Link method's asynchronous accounting.
Configured directory quotas are currently refused because that boundary has not
been qualified. Existing directory totals remain derivative metadata; the
publication checkpoint is not an assertion that their historical baseline was
complete.

After a fresh process fence, recovery distinguishes the exact private tuple from
the exact published tuple. A private inode may remain in custody despite a
changed ancestor or foreign target, without replaying or adopting either. A
public tuple requires the original chain, both exact links and exact parent
counts; recovery recomputes its parent's totals under WATCH without replaying the
link. Mixed tuples are refused. Distinct private/public certificates authorize
successor admission only in the current fenced generation. An observed public
checkpoint can never downgrade to private custody.

These are namespace checkpoints: operations remain PREPARED, no media receipt is
issued, and local spool retirement is forbidden. Recovery validates the recorded
manifest and prior verified PUTs; it does not independently rehash every provider
object. Acceptance separately verifies fresh-path plaintext and recorded raw
object hashes. Exclusive writers, external deletion policy and Redis/provider
power-loss durability still require qualification.

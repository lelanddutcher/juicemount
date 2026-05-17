# QA procedure for JuiceMount

Hard-learned from the QA-14 incident (2026-05-17): the existing
self-test reported `write_ok: true` while real-world copies were
silently corrupting bytes. The probe tested a 4 KiB single-RPC
write; the bug was in concurrent multi-RPC writes sharing a pooled
fd. Green-when-broken is the worst failure mode for a filesystem.

This document codifies the practices that should have prevented
that, plus the ones we already had. The autonomous loop refers to
this document by name; any change to these practices needs an
accompanying doc update so future-me reads the new contract.

---

## Rule 1 — Byte-integrity is the primary correctness property

Before any change to the request path (nfs/, internal/nfs/, bridge/,
cache/, metadata/) merges, a write-integrity test MUST pass:

  - Write a random file >= 10 MiB through the user-visible mount
    (`/Volumes/<name>`, NOT the FUSE-internal path)
  - Read it back
  - Compare md5 / sha256 — bytes MUST match exactly

This is more fundamental than tier-1.1 (concurrent dispatch) or
tier-1.2 (no Finder freeze). A filesystem that delivers wrong bytes
is broken at the most basic level. Latency / wedges / cosmetic
issues are all secondary.

The test must use a path that crosses the same code that user
operations cross. Specifically:

  - NOT `/Users/<user>/.juicemount/fuse-internal/...` (FUSE-direct
    bypasses the NFS handler).
  - YES `/Volumes/<name>/...` (the actual NFS mount).

The harness lives at `scripts/wedge-tests/write-integrity.sh` —
see below.

## Rule 2 — Test the failure mode, not the success path

When QA reports a symptom, the FIRST test should be the simplest
one that could prove the failure mode. Examples of correct lead-
with tests:

  | Symptom | First test |
  |---|---|
  | "Files won't copy" | `echo X > file; md5 file; expect md5(X)` |
  | "Pinning doesn't download" | POST /pin; watch CachedBytes climb |
  | "Stop doesn't stop" | Click Stop; `pgrep juicefs` should return empty |
  | "Mount goes degraded" | curl /health; expect 4 components green |

Do NOT lead with theory. If the user says "I can't copy files,"
the first action is a 5-line test that writes-and-reads-back, not
a 20-minute trace of the AppleDouble code path.

Theories come AFTER the basic test has either reproduced the
failure (now we have a small repro) or failed to reproduce it
(now we know the failure mode is more specific than reported).

## Rule 3 — When closing a QA, expand the matrix BEFORE marking ✓

Pattern from this session's failures: QA-1, QA-2, QA-5 were all
"closed" because the specific scenario the user reported didn't
reproduce. Each closure documented "could not reproduce" rather
than "tested N adjacent scenarios, all pass."

QA-2 was the worst case: I tested offline-toggle with FUSE
healthy and reads worked. I closed it. The user later reproduced
it with FUSE DOWN + auto-offline engaged. My closure had narrowed
the test focus instead of broadening it.

Closure protocol:
  1. Reproduce the EXACT user scenario.
  2. If you can't, test 2-3 adjacent scenarios that vary one axis
     each (different file size; pinned vs cached; FUSE up vs FUSE
     down; user-toggle vs auto-engage; before vs after restart).
  3. Document the matrix in the closure note. "Could not reproduce
     across the following 4 scenarios: ..." vs "could not reproduce
     in the user's exact scenario."
  4. If a single axis exhibits the failure, KEEP THE QA OPEN with
     that scenario as the new reproduction.

## Rule 4 — Concurrency changes require a concurrency-aware audit

When introducing new concurrency (parallel dispatch, goroutine
pools, async handlers), the code-reviewer prompt MUST explicitly
ask:

  > "What pre-existing code becomes unsafe when its callers go
  > from sequential to parallel? List every shared-mutable-state
  > access reachable from this change."

Iter-1 (`691f550`) introduced concurrent NFS dispatch. The reviewer
checked whether the new code was internally safe. We missed that
`writeFile.Write` uses a shared fd offset — a pattern that was
safe under sequential dispatch and is unsafe under the new model.

Templates for the prompt:
  - "This change makes <call site> concurrent. Walk every method
    on the types reachable from that call site and identify any
    that mutate shared state without synchronization."
  - "Are there fd-pool, buffer-pool, or singleton resources that
    were previously serialized by callers? If yes, are they
    internally synchronized now?"

## Rule 5 — Self-test probes must exercise representative workloads

The write probe must use the SAME shape as real workloads:

  - Multi-RPC writes (file > rsize/wsize, currently 1 MiB)
  - Concurrent writes (multiple parallel goroutines, different
    offsets, same fd — simulates a multi-WRITE-RPC burst)
  - Read-back verification with md5 / sha256
  - Reject the probe as failing if the readback md5 doesn't match
    the write input, even if the write returned no error

Single-RPC, single-thread probes only catch protocol-level errors
(EPERM, ENOSPC, EIO returned by the handler). They cannot catch
data corruption bugs in the handler itself, because the handler
sees one write at a time.

## Rule 6 — Wedge harnesses cover failure paths; integrity harnesses
  cover the happy path

This session built three wedge harnesses (MinIO-down, FUSE-hang,
NFS-shutdown). They all test what happens when something breaks.
Zero harnesses tested what happens when things work normally —
specifically, do bytes round-trip correctly?

Both classes are needed. The wedge harnesses catch resilience
regressions; the integrity harness catches correctness regressions.

The integrity harness must run BEFORE the wedge harnesses, not
after. If the happy-path is broken, the wedge results are
meaningless.

## Rule 7 — Per-iteration checklist additions

In addition to the loop's existing per-iteration checklist (read
STATE, pick goal, build, code-reviewer pass, etc), add:

  - For any commit touching nfs/, internal/nfs/, bridge/, cache/,
    metadata/, RUN `scripts/wedge-tests/write-integrity.sh` before
    commit. If it fails, the slice is not shippable.
  - For any commit introducing or modifying concurrency primitives
    (`go func`, channel, mutex, sync.WaitGroup, atomic.*), the
    code-reviewer pass MUST be prompted with Rule 4's template.

---

## Reference: scripts/wedge-tests/write-integrity.sh

Spec (to be implemented as a follow-up to this doc):

```
# write-integrity.sh — fail loud on any byte mismatch on the
# user-visible NFS mount path.
#
# Tests:
#   1. Small single-RPC write (512 KiB) — md5 match
#   2. Medium multi-RPC write (10 MiB) — md5 match
#   3. Large multi-RPC write (200 MiB) — md5 match
#   4. Concurrent writes (4 parallel cp's of 10 MiB to different
#      paths) — all 4 md5 match
#   5. cp with xattrs (cp -p of a file with com.apple.* xattrs) —
#      md5 match AND xattrs round-trip
#   6. Same as 1-5 but on a freshly-restarted mount (cold cache)
#
# Exit 0 only if ALL tests pass. Any byte-level mismatch is a
# blocking failure.
```

This belongs in the per-iteration checklist for every commit that
touches a request-path file.

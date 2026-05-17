# Loop C — Plan (open QAs from 2026-05-17 sessions)

Five real bugs surfaced by the user's live QA + the QA-14 corruption
discovered during the QA-13 fix validation. This loop closes all
of them, in severity order, with the new docs/QA-procedure.md
practices enforced (especially the write-integrity harness).

## Goal order (severity → priority)

### C.0 — `scripts/wedge-tests/write-integrity.sh` (BLOCKING DEP)

**Why first:** without this harness, every subsequent slice could
silently regress byte-integrity again. Per QA-procedure Rule 1,
this is the most fundamental correctness check; per Rule 6, it
runs before any other test.

**Spec:** see bottom of docs/QA-procedure.md. Tests 1-6 covering
small/medium/large single+concurrent writes, xattr round-trip, and
cold-cache restart. Exit non-zero on any byte mismatch.

**Acceptance:** running `./scripts/wedge-tests/write-integrity.sh`
reliably FAILS on the current binary (proves the harness is
detecting the QA-14 bug), then reliably PASSES after C.1 lands.

### C.1 — QA-14: fix NFS write corruption

**Severity:** highest. Writes through cp / Finder / cat produce
size-right but content-wrong files. This is the bug that triggered
the whole QA-procedure rewrite.

**Suspect:** `writeFile.Write(p)` at nfs/handler.go:1099 uses
non-positional `f.File.Write(p)` against a pooled fd shared
across parallel WRITE RPCs. Under iter-1's concurrent dispatch,
two WRITE RPCs at different offsets race the fd's internal
position counter. Bytes interleave wrong, file ends up at the
right size but with shuffled content.

**Investigation plan (Rule 2 — small repro first):**
  1. Confirm `writeFile.Write` is being called at all (add a log
     line; see who hits it during a cp).
  2. If yes: find the call site. Two candidates:
     - go-nfs library's WRITE RPC handler calling Write instead
       of WriteAt
     - Some path in our internal/nfs/nfs_onwrite.go using Write
  3. If no: the bug is in WriteAt (less likely; that one IS
     positional and should be safe).

**Fix candidates:**
  - (a) panic in writeFile.Write so we find the caller, then route
    that caller to WriteAt. Best — surfaces the design issue.
  - (b) make writeFile.Write thread-safe via Seek+Write under a
    mutex. Hides the design issue; not recommended.
  - (c) emergency mitigation: revert iter-1's concurrent dispatch
    (`691f550`). Forces sequential WRITE RPCs, race goes away,
    performance hit. Only worth doing if C.1 takes >2 hours.

**Acceptance:** write-integrity harness passes all 6 cases.

**Code-reviewer pass:** mandatory. Prompt with Rule 4's template
("what pre-existing code becomes unsafe under new concurrency?")
to make sure we don't miss adjacent unsafe call sites.

### C.2 — QA-12: open-time offline gate too aggressive

**Severity:** high. Defeats the cache value-proposition for the
"I just copied this" workflow. Even when bytes are locally cached,
the OPEN-time gate refuses non-pinned files when offline before
the read-time cache check ever runs.

**Fix path (per QA-12 entry):** at OpenFile (nfs/handler.go:745),
probe whether cacheReader can serve the first block before
returning ErrOfflineNotAvailable. If cache has the bytes, allow
the open; downstream ReadAt is already cache-priority-correct.

**Acceptance:** new test in write-integrity harness or a sibling:
pin nothing, write a file via the now-fixed write path, toggle
auto-offline (or SIGSTOP juicefs to kill FUSE), confirm the file
still opens and reads correctly. Adds a tier-1.7-tightening row.

### C.3 — QA-11: Start button silent no-op in .disconnected state

**Severity:** medium. UX dead-end — user clicks Start in a
.disconnected state, sees nothing happen, has no UI recovery path.

**Fix path (per QA-11 entry, recommendation b+c):**
  - Disable the Start button when state ∉ .idle, surface inline
    hint "Server is in <state> — use Stop everything to fully reset"
  - Show the Stop buttons in .disconnected too, so the user has
    a visible recovery path

No controller changes — pure popover wiring. Lowest risk slice.

### C.4 — QA-10: no notification on auto-offline engage

**Severity:** medium. Silent fail-safe feels like a silent
failure. User keeps clicking around unaware that writes are going
into the local journal.

**Fix path (per QA-10 entry):** in cbridge.go, the reachability
monitor's OnChange callback already fires when auto_offline flips.
Wire it to NSUserNotification (opt-in via Preferences). Notify on
both false→true (engaged) and true→false (recovered).

### C.5 — Validate QA-13 fix end-to-end after C.1

**Severity:** validation, not a new bug. QA-13's `._*` Stat-filter
fix landed during this session but couldn't be validated because
QA-14's write corruption was masking the result. After C.1 lands,
re-run a Finder cp with xattrs and confirm the dest md5 matches
AND the `._sidecar` file is created.

---

## Per-iteration checklist (delta from Loop B)

In addition to the standard loop checklist:

  - **Rule 1 enforcement:** for any commit touching nfs/,
    internal/nfs/, bridge/, cache/, metadata/, run
    `./scripts/wedge-tests/write-integrity.sh` BEFORE commit. If
    it fails, the slice is not shippable.
  - **Rule 2 enforcement:** when picking up a bug, the FIRST test
    is the smallest one that could prove the failure mode. No
    theory-first investigations.
  - **Rule 4 enforcement:** for concurrency-touching changes, the
    code-reviewer prompt MUST include the Rule 4 template.
  - **Rule 3 enforcement:** when closing a QA, document the
    adjacent-scenario matrix you tested. "Could not reproduce
    across N scenarios: X, Y, Z" — not just "could not reproduce."

## STOP conditions

  - All 5 goals (C.0–C.4) ✓ in STATE.md, AND C.5 validation passes.
  - OR three consecutive iterations produce no shippable progress.
  - OR user explicitly halts.
  - On stop: PushNotification with the new write-integrity harness
    status (pass/fail) + remaining QA counts.

## Non-negotiables (unchanged from prior loops)

  - No FileProviderExtension, ever.
  - No telemetry without explicit opt-in.
  - No proprietary deps for self-hosters.
  - One theme per commit; no bundled-PR scope creep.
  - SPECIFIC file paths to git add — NEVER `git add -A`.
  - Read docs/QA-procedure.md at session start. The rules there
    take precedence over older patterns in tier docs.

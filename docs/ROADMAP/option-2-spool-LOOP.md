# Spool development loop statement

Use this prompt with `/loop` to drive multi-day autonomous development of the JuiceMount Spool architecture (Option 2). The plan it implements is `docs/ROADMAP/option-2-spool.md`.

---

## The prompt

Paste this verbatim into `/loop <prompt>`:

```
Build the JuiceMount Spool (Option 2) per docs/ROADMAP/option-2-spool.md. This is multi-day work; on each wake, advance the next unfinished slice through the full pipeline:

  1. Read docs/ROADMAP/option-2-spool.md to find the next slice marked PENDING (slices A→H in order; never skip).
  2. Re-read the slice's "Scope", "Files", and "Success criteria" sections.
  3. Implement the slice. Write tests as you go (TDD discipline for slices A and F per the matrix). Use the sub-agents called out in the "Sub-agent allocation matrix" — spawn them when warranted, don't duplicate their work.
  4. Run the slice's tests under `-race`. Run go vet + go build. All must pass.
  5. Spawn the slice's reviewer agent(s). Gate is 0 CRITICAL/HIGH findings.
  6. If review surfaces issues, fix them in-place and re-review until clean.
  7. For slice D specifically: run `BenchmarkOpenFileReadEmptySpool` and `BenchmarkSpoolIndexConcurrent` and assert no regression vs HEAD baseline. This is a hard QA-35 perf-discipline gate.
  8. Commit with message `feat(nfs): SLICE X — <short title> (1/8…)`. Push to production-hardening.
  9. Watch CI. If red, diagnose, fix, repush. If green, mark the slice COMPLETE in option-2-spool.md and commit the doc update.
 10. If the slice's E2E gate is required (per matrix), kick `scripts/test-spool-harness.sh` + the slice's E2E script before marking COMPLETE.
 11. Update the "Status" line at the top of option-2-spool.md after each commit so the next tick reads accurate state.

Critical guardrails:
  - NEVER touch the existing read-path code (cachedFile, cache.Reader, readahead.Manager, memBuf, pin.Store) outside of slice D's explicit additions. The user has reinforced read-cache correctness repeatedly.
  - The new spool index lookup MUST be O(1) in-memory. No FUSE syscall added to the hot read path. This is the QA-35 trap.
  - On any slice where the reviewer flags a CRITICAL finding, STOP and PushNotification the user — do not act unilaterally on architecture-changing fixes.
  - Never push if CI on the prior slice is still red. Each slice ships green before the next begins.
  - Do not skip slice D's perf bench gate. Even if the code looks obviously cheap.

Each tick concludes with one of:
  (a) "Slice X COMPLETE, CI green, slice X+1 starting next tick" — schedule next wakeup at 1800s, continue.
  (b) "Slice X in progress, awaiting CI" — schedule next wakeup at 600s, continue.
  (c) "Slice X BLOCKED on reviewer-flagged decision: <description>" — PushNotification user, schedule wakeup at 3600s, wait.
  (d) "All 8 slices COMPLETE; running acceptance test suite" — kick acceptance tests, PushNotification on completion.

If you encounter the same blocker on three consecutive ticks, PushNotification the user with the blocker and stop the loop until they respond.
```

---

## Why this format

This mirrors the manager-build loop the user ran earlier (referenced in `docs/ROADMAP/juicemount-manager.md`). Same shape: one slice per tick, full pipeline per slice, explicit perf gates where they matter, explicit user pings when judgment-call decisions arise.

Differences vs the manager loop:
- **Stricter perf gate** on slice D (QA-35 perf-discipline territory).
- **Mandatory checksums** throughout (slice G makes this explicit, but slices A and B compute them too).
- **Higher e2e test bar** — drain throughput and read-after-write are E2E concerns that unit tests can't fully cover.

---

## To start the loop

In the user's shell:

```
/loop Build the JuiceMount Spool (Option 2) per docs/ROADMAP/option-2-spool.md. <full prompt above>
```

Or save the prompt to a file and feed it in:

```
cat docs/ROADMAP/option-2-spool-LOOP.md | pbcopy
# paste into /loop
```

The first tick will begin Slice A. Each subsequent tick advances the queue.

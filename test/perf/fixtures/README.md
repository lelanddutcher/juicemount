# Regression fixtures

Real captured log data, kept so an invariant can be proven against the bug it
was written for rather than against a hand-authored ideal.

## `scan_cadence_prefix_2026-08-16.log`

The clamp-ordering bug (fixed in 93321e4): a correctly-chosen 900s backstop was
truncated to 300s because the adaptive stretch applied its ceiling AFTER its
floor, so a full 420,699-entry SCAN ran every ~5 minutes and upserted nothing.

Contains the real `backstop changed -> new_sec 900` line plus the real
~300s-spaced `metadata sync complete` events from that window.

    JM_LOG=test/perf/fixtures/scan_cadence_prefix_2026-08-16.log \
      python3 test/perf/invariants.py --checks scan_cadence

Must report FAIL. Against the live post-fix mount the same check reports PASS
(observed 899s vs a 900s backstop). If the fixture ever passes, the invariant
has stopped detecting the thing it exists for.

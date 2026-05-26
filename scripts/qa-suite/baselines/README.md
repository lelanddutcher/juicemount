# Perf baselines

This directory stores the per-workload latency + throughput baselines
that `scripts/qa-suite/12-perf-regression.sh` compares against. One
file per `(workload, build-sha)` pair, plus a symlink `healthy.json`
pointing at the current accepted baseline for each workload.

Format: see `docs/PERFORMANCE_METHODOLOGY.md` § "Metrics contract" for
the JSON schema.

## Updating a baseline

Baselines do not auto-update. They are committed to git as the
declared "this is what good looks like" for a given SHA. Update only:

1. After a fix that legitimately improves throughput / reduces
   latency (commit the new baseline file in the same PR as the fix).
2. After an environmental change that legitimately changes the
   numbers (e.g., backend hardware upgrade — document the rationale
   in the commit message).

Never update to silence a regression. The regression is the signal
the threshold gate exists to catch.

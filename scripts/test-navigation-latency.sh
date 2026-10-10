#!/bin/bash
# Isolated local NFS/SQLite acceptance. No mount, Redis, sudo, or live volume.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <evidence-directory> [all|ram|sqlite|sql-contention|catalog|catalog-ingest]" >&2
  exit 2
fi
mode="${2:-all}"
case "$mode" in all|ram|sqlite|sql-contention|catalog|catalog-ingest) ;; *) echo "unknown mode: $mode" >&2; exit 2 ;; esac
root="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$1"
out="$(cd "$1" && pwd)"
if [[ -e "$out/results.tsv" ]]; then
  echo "refusing to overwrite an existing run: $out" >&2
  exit 2
fi
cd "$root"
mkdir -p "$out/tmp"
# Keep temporary databases and compiler scratch with the receipt.
export TMPDIR="$out/tmp/"
{
  printf 'mode=%s\n' "$mode"
  date -u '+%Y-%m-%dT%H:%M:%SZ'
  git rev-parse HEAD
  go version
  uname -srm
} > "$out/environment.txt"
git status --short > "$out/source-status.txt"
python3 - "$out/source-sha256.json" <<'PY'
import hashlib, json, pathlib, subprocess, sys
names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others',
    '--exclude-standard', '--', '*.go', 'go.mod', 'go.sum']).decode().split('\0')
names.extend(['scripts/test-navigation-latency.sh', 'test/perf/navigation-latency.md'])
paths = sorted(set(n for n in names if n and pathlib.Path(n).is_file()))
pathlib.Path(sys.argv[1]).write_text(json.dumps({n: hashlib.sha256(pathlib.Path(n).read_bytes()).hexdigest() for n in paths}, indent=2) + '\n')
PY
printf 'gate\texit_code\tlog\n' > "$out/results.tsv"
failed=0
require_tests() {
  local binary="$1" pattern="$2" manifest="$3"
  "$binary" -test.list "$pattern" > "$manifest" 2>&1
  python3 - "$manifest" "$pattern" <<'PY'
import pathlib, re, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
if not any(re.search(sys.argv[2], line) for line in lines):
    raise SystemExit('refusing an empty test selection: ' + sys.argv[1])
PY
}
run_gate() {
  local gate="$1"; shift
  local status=0
  printf '%q ' "$@" > "$out/$gate.command.txt"
  printf '\n' >> "$out/$gate.command.txt"
  "$@" > "$out/$gate.log" 2>&1 || status=$?
  local test_status="$status"
  python3 - "$out/$gate.log" "$gate" "$test_status" "$out/$gate.receipt.json" <<'PY' || status=1
import json, pathlib, re, sys
log, gate, status, receipt = sys.argv[1:]
text = pathlib.Path(log).read_text()
counts = {result: len(re.findall(r'^\s*--- ' + result + r': Test', text, re.M))
          for result in ['PASS', 'FAIL', 'SKIP']}
expected = {'nfs-ram': 6, 'nfs-sqlite': 6, 'sql-wire': 12,
            'sql-contention': 7, 'sql-absent-publication': 1,
            'sql-rename': 3, 'sql-mirror': 5, 'sql-intent': 6, 'sql-prune-commit': 1,
            'catalog-ram': 1, 'catalog-sqlite': 1,
            'catalog-ingest-ram': 1, 'catalog-ingest-sqlite': 1}[gate]
valid = int(status) == 0 and counts == {'PASS': expected, 'FAIL': 0, 'SKIP': 0}
pathlib.Path(receipt).write_text(json.dumps({'gate': gate, 'test_exit': int(status),
    'expected_pass_records': expected, 'actual': counts, 'qualified': valid}, indent=2) + '\n')
raise SystemExit(0 if valid else 1)
PY
  printf '%s\t%d\t%s\n' "$gate" "$status" "$gate.log" >> "$out/results.tsv"
  if [[ "$status" != 0 ]]; then failed=1; fi
  echo "$gate: exit $status ($out/$gate.log)"
}
go test -c -tags latency_acceptance ./nfs -o "$out/nfs-latency.test" > "$out/build-nfs.log" 2>&1
if [[ "$mode" == all || "$mode" == ram || "$mode" == sqlite ]]; then
  require_tests "$out/nfs-latency.test" '^TestNavigationLatency' "$out/nfs-tests.txt"
  if [[ "$mode" == all || "$mode" == ram ]]; then
    run_gate nfs-ram env JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/nfs-latency.test" -test.run '^TestNavigationLatency' -test.v -test.count=1 -test.timeout=60s
  fi
  if [[ "$mode" == all || "$mode" == sqlite ]]; then
    run_gate nfs-sqlite env JM_SERVE_FROM_SQLITE=1 JM_READDIR_PAGINATED=0 "$out/nfs-latency.test" -test.run '^TestNavigationLatency' -test.v -test.count=1 -test.timeout=60s
  fi
fi
if [[ "$mode" == all || "$mode" == catalog ]]; then
  require_tests "$out/nfs-latency.test" '^TestMetadataPressureRAMCatalogProductionOpenIsolation$' "$out/catalog-ram-tests.txt"
  require_tests "$out/nfs-latency.test" '^TestMetadataPressureSQLiteCatalogProductionOpenIsolation$' "$out/catalog-sqlite-tests.txt"
  run_gate catalog-ram env JM_METADATA_PRESSURE_ACCEPTANCE=ram-catalog-open JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/nfs-latency.test" -test.run '^TestMetadataPressureRAMCatalogProductionOpenIsolation$' -test.v -test.count=1 -test.timeout=120s
  run_gate catalog-sqlite env JM_METADATA_PRESSURE_ACCEPTANCE=sqlite-catalog-open JM_SERVE_FROM_SQLITE=1 JM_READDIR_PAGINATED=1 "$out/nfs-latency.test" -test.run '^TestMetadataPressureSQLiteCatalogProductionOpenIsolation$' -test.v -test.count=1 -test.timeout=120s
fi
if [[ "$mode" == all || "$mode" == catalog-ingest ]]; then
  require_tests "$out/nfs-latency.test" '^TestMetadataPressureRAMCatalogIngestProductionOpenIsolation$' "$out/catalog-ingest-ram-tests.txt"
  require_tests "$out/nfs-latency.test" '^TestMetadataPressureSQLiteCatalogIngestProductionOpenIsolation$' "$out/catalog-ingest-sqlite-tests.txt"
  run_gate catalog-ingest-ram env JM_METADATA_PRESSURE_ACCEPTANCE=ram-catalog-ingest JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/nfs-latency.test" -test.run '^TestMetadataPressureRAMCatalogIngestProductionOpenIsolation$' -test.v -test.count=1 -test.timeout=120s
  run_gate catalog-ingest-sqlite env JM_METADATA_PRESSURE_ACCEPTANCE=sqlite-catalog-ingest JM_SERVE_FROM_SQLITE=1 JM_READDIR_PAGINATED=1 "$out/nfs-latency.test" -test.run '^TestMetadataPressureSQLiteCatalogIngestProductionOpenIsolation$' -test.v -test.count=1 -test.timeout=120s
fi
if [[ "$mode" == all || "$mode" == sql-contention ]]; then
  require_tests "$out/nfs-latency.test" '^TestNavigationSQLContention$' "$out/sql-wire-tests.txt"
  run_gate sql-wire env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/nfs-latency.test" -test.run '^TestNavigationSQLContention$' -test.v -test.count=1 -test.timeout=15s
  go test -c -tags latency_acceptance ./metadata -o "$out/metadata-latency.test" > "$out/build-metadata.log" 2>&1
  require_tests "$out/metadata-latency.test" '^TestNavigationDuringBackgroundSQLWait$' "$out/metadata-tests.txt"
  run_gate sql-contention env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestNavigationDuringBackgroundSQLWait$' -test.v -test.count=1 -test.timeout=15s
  require_tests "$out/metadata-latency.test" '^TestAbsentPublicationSQLWaitKeepsCachedNavigationResponsive$' "$out/absent-publication-tests.txt"
  run_gate sql-absent-publication env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestAbsentPublicationSQLWaitKeepsCachedNavigationResponsive$' -test.v -test.count=1 -test.timeout=15s
  require_tests "$out/metadata-latency.test" '^TestRenameSQLWaitKeepsCachedNavigationResponsive$' "$out/rename-tests.txt"
  run_gate sql-rename env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestRenameSQLWaitKeepsCachedNavigationResponsive$' -test.v -test.count=1 -test.timeout=15s
  require_tests "$out/metadata-latency.test" '^TestMirrorSQLWaitKeepsCachedNavigationResponsive$' "$out/mirror-tests.txt"
  run_gate sql-mirror env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestMirrorSQLWaitKeepsCachedNavigationResponsive$' -test.v -test.count=1 -test.timeout=15s
  require_tests "$out/metadata-latency.test" '^TestIntentSQLWaitKeepsCachedNavigationResponsive$' "$out/intent-tests.txt"
  run_gate sql-intent env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestIntentSQLWaitKeepsCachedNavigationResponsive$' -test.v -test.count=1 -test.timeout=15s

  require_tests "$out/metadata-latency.test" '^TestConditionalPruneCommitWaitKeepsCachedNavigationResponsive$' "$out/prune-commit-tests.txt"
  run_gate sql-prune-commit env JM_FTS_DEFER=0 JM_SERVE_FROM_SQLITE=0 JM_READDIR_PAGINATED=0 "$out/metadata-latency.test" -test.run '^TestConditionalPruneCommitWaitKeepsCachedNavigationResponsive$' -test.v -test.count=1 -test.timeout=15s

fi
exit "$failed"

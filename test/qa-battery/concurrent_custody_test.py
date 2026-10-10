"""Host-stubbed category05 custody checks; never launch Finder or mount.

Run: /usr/bin/python3 -I -B test/qa-battery/concurrent_custody_test.py
"""
from pathlib import Path
import importlib.util
import json
import subprocess


HERE = Path(__file__).resolve().parent
SOURCE = (HERE / "05-concurrent-copies.sh").read_text()
LIB = (HERE / "lib.sh").read_text()
spec = importlib.util.spec_from_file_location("concurrent_evidence", HERE / "concurrent_evidence.py")
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


def function(source, name):
    start = source.index(name + "() {")
    return source[start:source.index("\n}\n", start) + 2]


def shell(source, body):
    # Stub every mutating host command. No source tree, temp dir or Finder
    # process is created; only the extracted production functions are loaded.
    command = """
calls=""
CC_BG_PIDS="101 102"
CC_COPY_PIDS="201 202"
CC_TMP=/nonexistent/owned
qa_offline() { calls="$calls offline"; }
rm() { calls="$calls rm"; }
_cc_kill_pids() { calls="$calls kill"; }
kill() { calls="$calls kill"; }
qa_finder_result_clean() {
  case "$1" in *error*) return 1 ;; *) return 0 ;; esac
}
source /dev/stdin
""" + body
    return subprocess.run(["/bin/bash", "-c", command], input=source, text=True,
                          capture_output=True, check=False)


def check_retention(source=SOURCE):
    functions = function(source, "_cc_teardown")
    for keep, pending, hold, expected in (
        (1, 0, 0, ""), (0, 1, 0, ""), (0, 0, 1, ""),
        (0, 0, 0, " offline rm")
    ):
        run = shell(functions, f"""
QA_KEEP_DATA={keep}
CC_PENDING={pending}
CC_HOLD={hold}
_cc_teardown
printf 'calls=%s\\n' "$calls"
""")
        assert run.returncode == 0 and run.stdout == "calls=" + expected + "\n", (
            keep, pending, hold, run.stdout, run.stderr
        )


def check_positive_reply(source=SOURCE):
    reply = function(source, "_cc_reply_positive")
    for rc, out, expected in (
        (0, "document file", 0), (0, "", 1), (0, "   ", 1),
        (1, "document file", 1), (0, "error -36", 1)
    ):
        run = shell(reply, f'_cc_reply_positive {rc} "{out}"')
        assert run.returncode == expected, (rc, out, run.returncode, run.stderr)


def check_unknown_reply(source=SOURCE):
    extracted = function(source, "_cc_reply_positive") + "\n" + function(source, "_cc_join_positive")
    for rc, out in (("1", "document file"), ("0", ""), ("0", "error -36")):
        body = f"""
RC={rc}
OUT="{out}"
CC_PENDING=1
function [ {{ if [[ "$1" == "!" && "$2" == "-f" ]]; then return 1; fi; builtin [ "$@"; }}
cat() {{ case "$1" in *.rc) echo "$RC";; *.out) echo "$OUT";; esac; }}
_cc_hold_unknown() {{ printf 'HOLD:%s\\n' "$1"; exit 88; }}
wait() {{ return 0; }}
_cc_wait_original() {{ return 0; }}
_cc_publish_joined() {{ return 0; }}
_cc_join_positive copy1 123 2
"""
        run = shell(extracted, body)
        assert run.returncode == 88 and run.stdout.startswith("HOLD:copy1"), (
            rc, out, run.returncode, run.stdout, run.stderr
        )


def check_late_reply(source=SOURCE, publish_fail=False):
    extracted = function(source, "_cc_reply_positive") + "\n" + function(source, "_cc_join_positive")
    body = f"""
CC_PENDING=1
CC_SOFT_RED=0
CC_HOLD=0
checks=0
marks=0
waits=0
writes=0
function [ {{
  if [[ "$1" == "!" && "$2" == "-f" ]]; then
    checks=$((checks+1))
    [[ "$checks" -eq 1 ]]
    return $?
  fi
  builtin [ "$@"
}}
date() {{ echo 3; }}
perl() {{ :; }}
cat() {{ case "$1" in *.rc) echo 0;; *.out) echo "document file";; esac; }}
_cc_soft_deadline() {{ marks=$((marks+1)); CC_SOFT_RED=1; CC_HOLD=1; }}
_cc_wait_original() {{ waits=$((waits+1)); return 0; }}
_cc_publish_joined() {{ writes=$((writes+1)); [[ "{int(publish_fail)}" = 1 && "$writes" = 1 ]] && return 1; return 0; }}
_cc_join_positive copy1 123 2
printf 'state=%s,%s,%s,%s,%s\\n' "$marks" "$waits" "$writes" "$CC_PENDING" "$CC_SOFT_RED"
"""
    run = shell(extracted, body)
    expected_writes = 2 if publish_fail else 1
    expected_marks = 2 if publish_fail else 1
    assert run.returncode == 0 and run.stdout == (
        f"state={expected_marks},1,{expected_writes},0,1\n"
    ), (run.returncode, run.stdout, run.stderr)


def check_observer_signal(source=SOURCE):
    line = next(line for line in source.splitlines() if line.startswith("trap 'CC_SIGNALLED=1;") and
                line.endswith(" TERM"))
    run = shell(line, """
CC_PENDING=0
CC_OBSERVERS=1
CC_HOLD=0
CC_SIGNALLED=0
qa_log() { calls="$calls logged"; }
kill() { builtin kill "$@"; }
kill -TERM $$
printf 'state=%s,%s\n' "$CC_SIGNALLED" "$calls"
""")
    assert run.returncode == 0 and run.stdout == "state=1, logged\n", (
        run.returncode, run.stdout, run.stderr
    )


def check_interrupted_wait(source=SOURCE):
    extracted = function(source, "_cc_wait_original")
    run = shell(extracted, """
CC_SIGNALLED=0
waits=0
marks=0
wait() {
  waits=$((waits+1))
  if [ "$waits" = 1 ]; then CC_SIGNALLED=1; return 143; fi
  return 0
}
_cc_soft_deadline() { marks=$((marks+1)); }
_cc_wait_original 123
rc=$?
printf 'state=%s,%s,%s\n' "$rc" "$waits" "$marks"
""")
    assert run.returncode == 0 and run.stdout == "state=0,2,1\n", (
        run.returncode, run.stdout, run.stderr
    )


def check_actual_cleanup_callers(source=SOURCE, caller="_cc_seed_gate"):
    extracted = (function(LIB, "qa_cleanup") + "\n" + function(LIB, "qa_end") +
                 "\n" + function(source, caller))
    for unproven, expected in ((1, ""), (0, " rm")):
        # Seed gate's positive path does not invoke qa_end; the final caller's
        # positive path does and retains the library's ordinary cleanup.
        if caller == "_cc_seed_gate" and unproven == 0:
            expected = ""
        if caller == "_cc_seed_gate":
            start = source.index("if qa_wait_drain; then", source.index("CASE B: pre-land"))
            end = source.index('if [ -f "$rb_landed" ]; then', start)
            branch = source[start:end]
        else:
            start = source.index("if qa_wait_drain; then", source.index('qa_sec "CASE D:'))
            end = source.index("# Surface any failed/quarantined", start)
            branch = source[start:end]
        body = f"""
unset QA_KEEP_DATA
MOUNT=/Volumes/zpool
QA_DEST_ROOT=/Volumes/zpool/JM_RELEASE_BATTERY
QA_STAGE=/tmp/jm-battery-stage-synthetic
QA_CAT_DIR=/nonexistent
QA_CAT=05-concurrent-copies
QA_T0=0
QA_PASS=0
QA_FAIL=1
QA_WARN=0
QA_CLEANUP_WORKERS=1
seed_ready=1
CC_DRAIN_UNPROVEN=0
rb_landed=/Volumes/zpool/JM_RELEASE_BATTERY/QA_owned/readback_src.dat
QA_DEST_ROOT=/Volumes/zpool/JM_RELEASE_BATTERY
qa_log() {{ :; }}
qa_warn() {{ :; }}
qa_pass() {{ :; }}
qa_fail() {{ QA_FAIL=$((QA_FAIL+1)); }}
qa_wait_drain() {{ return {1 if unproven else 0}; }}
find() {{ calls="$calls find"; }}
date() {{ echo 1; }}
{branch}
{caller} || :
printf 'cleanup=%s keep=%s\\n' "$calls" "${{QA_KEEP_DATA:-0}}"
"""
        run = shell(extracted, body)
        keep = 1 if unproven else 0
        assert run.returncode == 0 and run.stdout.endswith(
            f"cleanup={expected} keep={keep}\n"
        ), (caller, unproven, run.returncode, run.stdout, run.stderr)


class MemoryPath:
    def __init__(self, files, name=""):
        self.files, self.name = files, name

    def __truediv__(self, item):
        return MemoryPath(self.files, item)

    def read_text(self):
        return self.files[self.name]


def overlap_files():
    files = {"mount.path": "/Volumes/zpool", "parallel.count": "4",
             "readback.samples": "10.4 10.6 abcdef\n",
             "nav.samples": "10.4 10.6 /dir 3\n"}
    for i in range(1, 5):
        files[f"dest{i}.path"] = f"/Volumes/zpool/JM_RELEASE_BATTERY/QA_{i}"
        files[f"copy{i}.worker"] = f"{100+i} 9"
        files[f"copy{i}.reply_at"] = "12"
    path = lambda i: f"JM_RELEASE_BATTERY/QA_{i}/tree{i}/file"
    files["spool.samples"] = "\n".join(json.dumps(row) for row in (
        {"from": 10, "to": 10.2, "rows": {path(1): 1, path(2): 2}},
        {"from": 10.8, "to": 11, "rows": {path(1): 2, path(2): 3}},
    )) + "\n"
    return files


def check_overlap():
    base = overlap_files()
    assert evidence.verify(MemoryPath(base))["families"] == [1, 2]
    for changed in (
        {"readback.samples": ""},
        {"copy2.reply_at": "10.5"},
        {"dest2.path": "/Volumes/zpool/foreign/QA_2"},
        {"spool.samples": json.dumps({"from": 10, "to": 10.2, "rows": {
            "JM_RELEASE_BATTERY/QA_1/tree1/file": 1}}) + "\n" +
            json.dumps({"from": 10.8, "to": 11, "rows": {
                "JM_RELEASE_BATTERY/QA_1/tree1/file": 2,
                "JM_RELEASE_BATTERY/QA_2/tree2/file": 3}}) + "\n"},
    ):
        try:
            evidence.verify(MemoryPath(base | changed))
        except (ValueError, KeyError):
            continue
        raise AssertionError("invalid overlap admitted: " + repr(changed))


def neuter(label, source, old, new, check):
    assert old in source
    changed = source.replace(old, new, 1)
    syntax = subprocess.run(["/bin/bash", "-n"], input=changed, text=True,
                            capture_output=True, check=False)
    assert syntax.returncode == 0, (label, syntax.stderr)
    try:
        check(changed)
    except AssertionError:
        return label
    raise AssertionError("behavioral neuter did not fail: " + label)


check_retention()
check_positive_reply()
check_unknown_reply()
check_late_reply()
check_late_reply(publish_fail=True)
check_observer_signal()
check_interrupted_wait()
check_actual_cleanup_callers()
check_actual_cleanup_callers(caller="_cc_finish_category")
check_overlap()
mutants = [
    neuter("keep-data-before-teardown", SOURCE,
           '[ "${QA_KEEP_DATA:-0}" = 1 ] && return',
           ': # retention disabled', check_retention),
    neuter("pending-before-teardown", SOURCE,
           '[ "$CC_PENDING" -gt 0 ] && return',
           ': # unresolved cleanup enabled', check_retention),
    neuter("held-observer-before-teardown", SOURCE,
           '[ "$CC_HOLD" = 1 ] && return',
           ': # observer cleanup enabled', check_retention),
    neuter("nonempty-reply", SOURCE,
           '[[ "$2" =~ [^[:space:]] ]] && qa_finder_result_clean "$2"',
           'qa_finder_result_clean "$2"', check_positive_reply),
    neuter("original-reply-gate", SOURCE,
           '_cc_reply_positive "$rc" "$out" ||',
           ': ||', check_unknown_reply),
    neuter("soft-deadline", SOURCE,
           '_cc_soft_deadline "$id exceeded observation deadline"',
           ': # deadline ignored', check_late_reply),
    neuter("joined-publication", SOURCE,
           '_cc_publish_joined "$id" "$joined_rc"',
           'true', lambda changed: check_late_reply(changed, publish_fail=True)),
    neuter("observer-signal-custody", SOURCE,
           '[ "$CC_OBSERVERS" -gt 0 ] || [ "$CC_HOLD" = 1 ]; } && qa_log "CASE C: TERM',
           '[ "$CC_HOLD" = 1 ]; } && qa_log "CASE C: TERM', check_observer_signal),
    neuter("interrupted-original-wait", SOURCE,
           '[ "$rc" -gt 128 ] && continue',
           ': # interrupted wait not rejoined', check_interrupted_wait),
    neuter("joined-pid-signal", SOURCE,
           '# Every original child has already been joined.',
           '_cc_kill_pids "$CC_BG_PIDS"\n    # Every original child has already been joined.',
           check_retention),
    neuter("joined-copy-pid-signal", SOURCE,
           '# Every original child has already been joined.',
           '_cc_kill_pids "$CC_COPY_PIDS"\n    # Every original child has already been joined.',
           check_retention),
    neuter("seed-drain-retention", SOURCE,
           'export QA_KEEP_DATA=1 # unproven seed/drain: preserve source and destination',
           ': # seed cleanup permitted', check_actual_cleanup_callers),
    neuter("final-drain-retention", SOURCE,
           '[ "$CC_DRAIN_UNPROVEN" = 1 ] && export QA_KEEP_DATA=1',
           ': # failed final drain cleanup permitted',
           lambda changed: check_actual_cleanup_callers(changed, "_cc_finish_category")),
]
assert len(set(mutants)) == 13
print("PASS concurrent-custody: 10 actual-function groups, 13 intended behavioral mutants rejected")

"""Executable, host-stubbed checks for battery selection and retention.

Run with /usr/bin/python3 -I -B qa_isolation_test.py. No mount, control-plane,
Finder, pool generation, or filesystem cleanup command is allowed to run.
"""
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
LIB = (HERE / "lib.sh").read_text()
UNICODE = (HERE / "04-unicode-names.sh").read_text()
FILE_TYPES = (HERE / "01-file-types.sh").read_text()
SCRATCH = "/qa-isolation-synthetic-owned"


def shell(source, body, extra=None, prelude=""):
    env = os.environ.copy()
    for name in ("MOUNT", "QA_POOL", "QA_KEEP_DATA"):
        env.pop(name, None)
    env.update(QA_STAGE=SCRATCH + "/stage", QA_ARTIFACTS=SCRATCH + "/artifacts")
    if extra:
        env.update(extra)
    command = """
mount() { printf 'mount-called\\n' >&2; printf '127.0.0.1:/ on /Volumes/foreign (nfs)\\n'; }
mkdir() { :; }
dd() { printf 'FORBIDDEN-DD\\n' >&2; return 99; }
""" + prelude + "\nsource /dev/stdin\n" + body
    return subprocess.run(["/bin/bash", "-c", command], input=source,
                          text=True, capture_output=True, env=env, check=False)


def check_mount(source=LIB):
    explicit = shell(source, 'printf "selected=%s\\n" "$MOUNT"', {"MOUNT": "/Volumes/zpool"})
    assert explicit.returncode == 0 and explicit.stdout == "selected=/Volumes/zpool\n"
    assert "mount-called" not in explicit.stderr
    custom = shell(source, 'printf "selected=%s\\n" "$MOUNT"', {"MOUNT": "/Volumes/custom"})
    assert custom.returncode == 0 and custom.stdout == "selected=/Volumes/custom\n"
    assert "mount-called" not in custom.stderr
    auto = shell(source, 'printf "selected=%s\\n" "$MOUNT"')
    assert auto.returncode == 0 and auto.stdout == "selected=/Volumes/foreign\n"
    default_present = shell(source, 'printf "selected=%s\\n" "$MOUNT"',
                            prelude="mount() { printf '127.0.0.1:/ on /Volumes/zpool (nfs)\\n'; }")
    assert default_present.returncode == 0 and default_present.stdout == "selected=/Volumes/zpool\n"
    empty = shell(source, 'printf "SHOULD-NOT-REACH\\n"', {"MOUNT": ""})
    assert empty.returncode == 2 and "SHOULD-NOT-REACH" not in empty.stdout
    assert "mount-called" not in empty.stderr


def check_pool(source=LIB):
    default = shell(source, 'printf "pool=%s\\n" "$QA_POOL"', {"MOUNT": "/Volumes/zpool"})
    assert default.returncode == 0 and default.stdout == "pool=" + SCRATCH + "/stage/jm-battery-pool-64MiB\n"
    assert "FORBIDDEN-DD" not in default.stderr
    supplied = shell(source, 'printf "pool=%s\\n" "$QA_POOL"',
                     {"MOUNT": "/Volumes/zpool", "QA_POOL": SCRATCH + "/owned.pool"})
    assert supplied.returncode == 0 and supplied.stdout == "pool=" + SCRATCH + "/owned.pool\n"
    assert "FORBIDDEN-DD" not in supplied.stderr


def function(source, name):
    start = source.index(name + "() {")
    return source[start:source.index("\n}\n", start) + 2]


def check_lib_retention(source=LIB):
    cleanup = function(source, "qa_cleanup")
    prelude = """
find() { :; }
rm() { calls="$calls rm"; }
qa_warn() { :; }
calls=""
MOUNT=/Volumes/zpool
QA_DEST_ROOT="$MOUNT/JM_RELEASE_BATTERY"
QA_STAGE=/tmp/jm-battery-stage-probe
"""
    for keep, expected in (("1", ""), ("0", " rm")):
        ran = shell(cleanup, 'qa_cleanup; printf "calls=%s\\n" "$calls"',
                    {"QA_KEEP_DATA": keep}, prelude)
        assert ran.returncode == 0 and ran.stdout == "calls=" + expected + "\n"


def check_category_retention(source=UNICODE):
    on_exit = function(source, "_on_exit")
    prelude = """
calls=""
_cleanup_done=0
MOUNT=/Volumes/zpool
DEST="$MOUNT/JM_RELEASE_BATTERY/QA_owned"
qa_offline() { calls="$calls offline"; }
rm() { calls="$calls rm"; }
qa_cleanup() { calls="$calls cleanup"; }
"""
    for keep, expected in (("1", ""), ("0", " offline rm cleanup")):
        ran = shell(on_exit, '_on_exit; printf "calls=%s\\n" "$calls"',
                    {"QA_KEEP_DATA": keep}, prelude)
        assert ran.returncode == 0 and ran.stdout == "calls=" + expected + "\n"
    # Exercise EXIT itself, including an interrupted category exit status.
    trap_prelude = """
exec 3>&1
_cleanup_done=0
MOUNT=/Volumes/zpool
DEST="$MOUNT/JM_RELEASE_BATTERY/QA_owned"
qa_offline() { printf 'offline\\n' >&3; }
rm() { printf 'rm\\n' >&3; }
qa_cleanup() { printf 'cleanup\\n' >&3; }
"""
    for keep, expected in (("1", ""), ("0", "offline\nrm\ncleanup\n")):
        ran = shell(on_exit + "\ntrap _on_exit EXIT\n", 'exit 143',
                    {"QA_KEEP_DATA": keep}, trap_prelude)
        assert ran.returncode == 143 and ran.stdout == expected


def check_file_types_retention(source=FILE_TYPES):
    cleanup = function(source, "_ft_unlock_all") + "\n" + function(source, "_ft_cleanup")
    prelude = """
calls=""
MOUNT=/Volumes/zpool
QA_FT_DEST="$MOUNT/JM_RELEASE_BATTERY/QA_owned"
QA_FT_STAGE=/tmp/jm-battery-stage-owned
QA_FT_UCHG_PATHS=/dev/null
chflags() { calls="$calls unlock"; }
rm() { calls="$calls rm"; }
"""
    for keep, expected in (("1", ""), (None, " unlock rm rm")):
        extra = {"QA_KEEP_DATA": keep} if keep is not None else None
        ran = shell(cleanup, '_ft_cleanup; printf "calls=%s\\n" "$calls"', extra, prelude)
        assert ran.returncode == 0 and ran.stdout == "calls=" + expected + "\n"
    trap_prelude = prelude.replace('calls="$calls unlock"', "printf 'unlock\\n' >&3").replace(
        'calls="$calls rm"', "printf 'rm\\n' >&3") + "\nexec 3>&1\n"
    for keep, expected in (("1", ""), (None, "unlock\nrm\nrm\n")):
        extra = {"QA_KEEP_DATA": keep} if keep is not None else None
        ran = shell(cleanup + "\ntrap _ft_cleanup EXIT\n", "exit 143", extra, trap_prelude)
        assert ran.returncode == 143 and ran.stdout == expected


def neutered(label, changed, check):
    syntax = subprocess.run(["/bin/bash", "-n"], input=changed, text=True,
                            capture_output=True, check=False)
    assert syntax.returncode == 0, "neuter broke shell syntax: " + label
    try:
        check(changed)
    except AssertionError:
        return label
    raise AssertionError("behavioral neuter did not fail: " + label)


cases = []
check_mount(); cases.append("explicit-unset-empty-mount")
check_pool(); cases.append("pool-default-and-override-lazy")
check_lib_retention(); cases.append("library-cleanup-retention")
check_category_retention(); cases.append("category04-exit-retention")
check_file_types_retention(); cases.append("category01-exit-retention")
mutations = [
    neutered("mount-selection", LIB.replace('if [ "${MOUNT+x}" = x ]; then', 'if false; then', 1), check_mount),
    neutered("pool-override", LIB.replace('QA_POOL="${QA_POOL:-$QA_STAGE/jm-battery-pool-64MiB}"',
                                          'QA_POOL="/tmp/jm-battery-pool-64MiB"', 1), check_pool),
    neutered("library-retention", LIB.replace('[ "${QA_KEEP_DATA:-0}" = 1 ] && return 0', ': # retention disabled', 1), check_lib_retention),
    neutered("category04-retention", UNICODE.replace('[ "${QA_KEEP_DATA:-0}" = 1 ] && return',
                                                      ': # retention disabled', 1), check_category_retention),
    neutered("category01-retention", FILE_TYPES.replace('[ "${QA_KEEP_DATA:-0}" = 1 ] && return 0',
                                                       ': # retention disabled', 1), check_file_types_retention),
]
assert len(set(mutations)) == 5
print("PASS qa-isolation: 5 behavior groups; 5 intended behavioral neuters rejected")

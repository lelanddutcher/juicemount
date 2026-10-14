"""Focused host-free controls for the strict release seams. Root runs this.
No Finder, mount, provider, network or fixture cleanup is invoked.
"""
import os
import json
import tempfile
from pathlib import Path
import subprocess
import unittest

HERE = Path(__file__).resolve().parent
LIB = (HERE / "lib.sh").read_text()
STRICT = (HERE / "release_custody.sh").read_text()


def function(source, name):
    start = source.index(name + "() {")
    return source[start:source.index("\n}\n", start) + 2]


class ReleaseCustodyTests(unittest.TestCase):
    def test_cancel_requires_original_error_128(self):
        definition = function(STRICT, "qa_release_cancel_reply")
        cases = [(1, "execution error: User canceled. (-128)", 0),
                 (0, "execution error: User canceled. (-128)", 1),
                 (0, "document file copied", 1),
                 (1, "execution error: AppleEvent timed out. (-1712)", 1),
                 (143, "", 1), (1, "unrelated -128 text", 1)]
        for rc, reply, want in cases:
            with self.subTest(rc=rc, reply=reply):
                got = subprocess.run(["/bin/bash", "-c", definition +
                                      '\nqa_release_cancel_reply "$1" "$2"',
                                      "control", str(rc), reply], capture_output=True)
                self.assertEqual(got.returncode, want)

    def test_strict_missing_mount_refuses_before_effects(self):
        env = {"PATH": "/usr/bin:/bin", "QA_STRICT_RELEASE": "1", "QA_KEEP_DATA": "1"}
        prelude = "mount() { echo FORBIDDEN; }; mkdir() { echo FORBIDDEN; }; source /dev/stdin"
        got = subprocess.run(["/bin/bash", "-c", prelude], input=LIB,
                             text=True, capture_output=True, env=env)
        self.assertEqual(got.returncode, 2)
        self.assertNotIn("FORBIDDEN", got.stdout)
        self.assertIn("MOUNT", got.stderr)

    def test_mount_match_alone_requires_backend_hook(self):
        definition = function(LIB, "qa_verify_custody")
        source = definition + """
QA_STRICT_RELEASE=1
_qa_verify_mounted_custody() { return 0; }
qa_release_hook() { printf '%s\\n' "$*"; return 9; }
qa_release_hold() { exit 23; }
qa_verify_custody /owned/manifest /owned/mount/JM_RELEASE_BATTERY/case
"""
        got = subprocess.run(["/bin/bash", "-c", source], text=True, capture_output=True)
        self.assertEqual(got.returncode, 23)
        self.assertIn("backend-readback /owned/manifest", got.stdout)

    def test_drain_requires_one_typed_zero_snapshot(self):
        fields = "pending_files pending_symlinks pending_bytes in_progress capacity_used failed_files declined_files quarantined replacement_conflicts replacement_waiting stall_waiters stalled_files suspect_zero_tail".split()
        good = dict.fromkeys(fields, 0)
        good.update(enabled=True, offline=False, failed=99)
        cases = [(good, 0)]
        for field in fields:
            for value in (-1, "0", False, None):
                cases.append((dict(good, **{field: value}), 2))
            missing = good.copy()
            del missing[field]
            cases.append((missing, 2))
            cases.append((dict(good, **{field: 1}), 1))
        cases.extend([(dict(good, offline=True), 2),
                      (dict(good, error="unknown"), 2),
                      (dict(good, enabled=False), 2)])
        definition = function(STRICT, "qa_release_drain_state")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "snapshot.json"
            for observation, expected in cases:
                with self.subTest(observation=observation):
                    path.write_text(json.dumps(observation))
                    got = subprocess.run(["/bin/bash", "-c", definition +
                        '\nqa_release_drain_state "$1"', "control", str(path)], capture_output=True)
                    self.assertEqual(got.returncode, expected)
            path.write_text('{"in_progress":0,"in_progress":-1}')
            got = subprocess.run(["/bin/bash", "-c", definition +
                '\nqa_release_drain_state "$1"', "control", str(path)], capture_output=True)
            self.assertEqual(got.returncode, 2)

    def test_strict_operations_join_original_worker_inside_substitution(self):
        # A shell function stands in for osascript; no AppleEvent is sent.
        for operation in ("copy", "copy_replacing", "move", "delete"):
            for child_rc in (0, 1):
                with self.subTest(operation=operation, child_rc=child_rc), tempfile.TemporaryDirectory() as tmp:
                    source = r"""
source "$1/release_custody.sh"
QA_BATTERY_LIB_DIR="$1"; QA_CAT_DIR="$2"; QA_ARTIFACTS="$2"
FAKE_RC="$3"
osascript() { printf 'document file owned-fixture\n'; return "$FAKE_RC"; }
qa_finder_result_clean() { return 0; }
qa_release_hold() { exit 23; }
if [ "$4" = delete ]; then
    out="$(qa_finder_delete /owned/source 10)"; rc=$?
else
    out="$(qa_finder_$4 /owned/source /owned/dest 10)"; rc=$?
fi
printf '%s\n' "$out"
exit "$rc"
"""
                    got = subprocess.run(["/bin/bash", "-c", source, "control",
                        str(HERE), tmp, str(child_rc), operation], text=True, capture_output=True)
                    self.assertEqual(got.returncode, 0 if child_rc == 0 else 23, got.stderr)
                    records = list(Path(tmp).glob("original-finder.*"))
                    self.assertEqual(len(records), 1)
                    record = records[0]
                    self.assertEqual((record / "operation.rc").read_text().strip(), str(child_rc))
                    self.assertEqual((record / "operation.joined").read_text().split()[0], "0")
                    self.assertGreater(int((record / "operation.worker").read_text().split()[0]), 0)
                    self.assertEqual((record / "operation.out").read_text().strip(), "document file owned-fixture")
                    if child_rc == 0:
                        self.assertEqual(got.stdout.strip(), "document file owned-fixture")

    def test_category_cleanup_retains_failure_sources(self):
        for filename, name in [("02-file-sizes.sh", "_on_exit"),
                               ("03-deep-wide-trees.sh", "_cleanup_03"),
                               ("06-cancel-retry.sh", "_c06_cleanup"),
                               ("07-offline-spool.sh", "cleanup_07"),
                               ("08-chain-of-custody.sh", "_coc_cleanup")]:
            with self.subTest(category=filename):
                definition = function((HERE / filename).read_text(), name)
                got = subprocess.run(["/bin/bash", "-c", definition +
                    "\nQA_KEEP_DATA=1; rm() { echo FORBIDDEN; }; qa_offline() { echo FORBIDDEN; }; " + name],
                    text=True, capture_output=True)
                self.assertEqual(got.returncode, 0)
                self.assertEqual(got.stdout, "")


if __name__ == "__main__":
    unittest.main()

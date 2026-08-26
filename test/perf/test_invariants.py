#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("invariants.py")
SPEC = importlib.util.spec_from_file_location("jm_perf_invariants", MODULE_PATH)
INVARIANTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INVARIANTS)


def row(second):
    return {"time": "2026-08-26T18:00:%02d-04:00" % second}


class CurrentPushSessionTests(unittest.TestCase):
    def test_excludes_scan_events_from_prior_app_processes(self):
        starts = [row(5), row(30)]
        events = [row(10), row(20), row(31), row(40)]
        self.assertEqual(
            INVARIANTS._in_current_push_session(events, starts),
            [row(31), row(40)],
        )

    def test_refuses_without_a_session_boundary(self):
        self.assertEqual(INVARIANTS._in_current_push_session([row(10)], []), [])


if __name__ == "__main__":
    unittest.main()

"""Host-stubbed category09 controls; never mount, contact a server or use Finder.

Root runner supplies QA_TEST_ROOT (owned scratch, not /tmp).
Normal: /usr/bin/python3 -I -B test/qa-battery/snappiness_test.py
Behavior negatives: SNAP_NEUTER=<name> with the corresponding selected test.
"""
import copy
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SCRIPT = (HERE / '09-finder-snappiness.sh').read_text()
CUSTODY = (HERE / 'snappiness_custody.sh').read_text()
PYTHON = (HERE / 'snappiness_evidence.py').read_text()
NEUTER = os.environ.get('SNAP_NEUTER', '')
# Each negative is a source-level, syntactically valid guard removal. Root must
# see the intended behavioral assertion fail; syntax/fixture errors do not count.
if NEUTER == 'manifest-count':
    PYTHON = PYTHON.replace("require(len(rows) == expected, 'manifest count differs')", 'pass')
    PYTHON = PYTHON.replace("require(len(set(names)) == expected, 'duplicate manifest entry')", 'pass')
if NEUTER == 'metrics-reset':
    PYTHON = PYTHON.replace("require(all(a[k] >= b[k] for k in b), 'metrics reset/nonmonotonic')", 'pass')
if NEUTER == 'drain-overlap':
    PYTHON = PYTHON.replace("b['in_progress'] > 0 and a['in_progress'] > 0 and a['succeeded'] > b['succeeded']", 'True')
if NEUTER == 'offline':
    PYTHON = PYTHON.replace("state['offline'] is wanted", 'True')
if NEUTER == 'unknown-reply':
    CUSTODY = CUSTODY.replace('[ "$1" = 0 ] && [[ "$2" =~ [^[:space:]] ]] && qa_finder_result_clean "$2"', ':')
if NEUTER == 'keep-data':
    CUSTODY = CUSTODY.replace('[ "${QA_KEEP_DATA:-0}" != 1 ] || return 0', ':')
E = {'__name__': 'snappiness_evidence'}
exec(compile(PYTHON, str(HERE / 'snappiness_evidence.py'), 'exec'), E)


def function(source, name):
    start = source.index(name + '() {')
    return source[start:source.index('\n}\n', start) + 3]


def shell(functions, body):
    # Host-effect boundaries refuse unless individually replaced by a test.
    preamble = '''
set -uo pipefail
SNAP_PENDING=0 SNAP_HOLD=0 SNAP_SOFT_RED=0 SNAP_SIGNALLED=0 SNAP_FORCED_OFFLINE=0
SNAP_SOFT_REASON='' SNAP_COPY_PIDS='' QA_KEEP_DATA=0 QA_FAIL=0
SNAP_TMP=/nonexistent/owned SNAP_TIMEOUT=1 SNAP_FINDER_TIMEOUT=2
calls=''
qa_fail() { QA_FAIL=$((QA_FAIL+1)); }
qa_log() { :; }
qa_finder_result_clean() { case "$1" in *error*) return 1;; *) return 0;; esac; }
qa_cleanup() { calls="$calls cleanup"; }
curl() { echo UNEXPECTED_CURL >&2; return 99; }
osascript() { echo UNEXPECTED_FINDER >&2; return 99; }
rm() { echo UNEXPECTED_DELETE >&2; return 99; }
sleep() { :; }
perl() { :; }
source /dev/stdin
'''
    return subprocess.run(['/bin/bash', '-c', preamble + body], input=functions,
                          text=True, capture_output=True, timeout=5)


def metrics():
    d = {k: 0 for k in E['COUNTERS']}
    d.update(uptime_sec=10, rpcs={k: {'count': 0} for k in E['RPCS']})
    return d


def spool(active=1, succeeded=10):
    return dict(enabled=True, pending_files=3, in_progress=active,
                succeeded=succeeded, failed_files=0, capacity_used=12)


class EvidenceTests(unittest.TestCase):
    def test_manifest(self):
        self.assertEqual(E['manifest']('dir1/a\t' + 'a'*32 + '\n', 1), ['a'])
        for text, count in [('', 1), ('dir1/a\t'+'a'*32+'\n', 2),
                            ('dir1/a\t'+'a'*32+'\n'*2, 1), ('dir1/../a\t'+'a'*32, 1),
                            ('dir1/a\tbad', 1)]:
            with self.subTest(text=text, count=count), self.assertRaises(ValueError):
                E['manifest'](text, count)

    def test_tsv(self):
        good = '1\t/root/d/dir1\t/man\t/root/d\n'
        with patch.object(Path, 'read_text', return_value='dir1/a\t'+'a'*32+'\n'):
            self.assertEqual(len(E['rows'](good, [1])), 1)
            for bad in (good.replace('\t', '\\t'), '', good.replace('/man', ''), good + good):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    E['rows'](bad, [1])

    def test_names(self):
        result = E['listing_names'](['a', '._a', '.DS_Store'], ['a'])
        self.assertEqual(result, dict(primary_count=1, total_count=3, extras=['.DS_Store', '._a']))
        for names in ([], ['b'], ['a', 'b'], ['a', 'a']):
            with self.subTest(names=names), self.assertRaises(ValueError):
                E['listing_names'](names, ['a'])

    def test_metrics_reset(self):
        a, b = metrics(), metrics()
        a['rpc_total'] = 2
        with self.assertRaises(ValueError):
            E['metrics'](a, b)

    def test_metrics_schema_and_client_cache(self):
        a, b = metrics(), metrics()
        self.assertEqual(E['metrics'](a,b)['coverage'], 'client-cache-only-server-path-unproven')
        b['rpcs']['READDIRPLUS']['count'] = 1
        b['rpc_total'] = 1
        self.assertEqual(E['metrics'](a,b)['coverage'], 'server-activity-in-bracket')
        for value in (None, True, -1, 1.5, '0'):
            bad = copy.deepcopy(b); bad['readdir_mirror_hit'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                E['metrics'](a,bad)
        del b['rpcs']['READDIR']
        with self.assertRaises(ValueError): E['metrics'](a,b)

    def test_offline(self):
        E['offline']({'ok':True,'offline_mode':True}, {'offline':True}, True)
        E['offline']({'ok':True,'offline_mode':False}, {'offline':False}, False)
        for reply,state,wanted in [({'ok':True,'offline_mode':False},{'offline':True},False),
                                   ({'ok':True,'offline_mode':True},{'offline':False},True),
                                   ({'ok':False,'offline_mode':True},{'offline':True},True),
                                   ({'ok':True,'offline_mode':True},{},True)]:
            with self.subTest(state=state), self.assertRaises(ValueError):
                E['offline'](reply,state,wanted)

    def test_drain_overlap(self):
        self.assertEqual(E['overlap'](spool(), spool(1,11))['succeeded_delta'], 1)
        for before,after in [(spool(0),spool(0,11)),(spool(),spool(0,11)),(spool(),spool()),(spool(),spool(1,9))]:
            with self.subTest(before=before,after=after), self.assertRaises(ValueError):
                E['overlap'](before,after)


class CustodyTests(unittest.TestCase):
    def test_keep_data_and_pending_cleanup(self):
        f = function(CUSTODY,'snap_on_exit')
        for keep,pending,hold,offline,expected in [(1,0,0,0,''),(0,1,0,0,''),
                                                  (0,0,1,0,''),(0,0,0,1,''),(0,0,0,0,' cleanup')]:
            run = shell(f, f'QA_KEEP_DATA={keep}; SNAP_PENDING={pending}; SNAP_HOLD={hold}; SNAP_FORCED_OFFLINE={offline}; snap_on_exit; printf "%s" "$calls"')
            self.assertEqual((run.returncode,run.stdout), (0,expected),run.stderr)

    def test_unknown_reply(self):
        f = function(CUSTODY,'snap_reply_positive') + function(CUSTODY,'snap_join_positive')
        for rc,out in [('1','document file'),('0',''),('0','error -36')]:
            body = '''
SNAP_PENDING=1
function [ { if [[ "$1" == '!' && "$2" == '-f' ]]; then return 1; fi; builtin [ "$@"; }
cat() { case "$1" in *.rc) printf '%s' "$RC";; *.out) printf '%s' "$OUT";; esac; }
snap_hold_unknown() { echo HOLD; exit 88; }
snap_wait_original() { return 0; }
snap_publish_joined() { return 0; }
snap_join_positive original 123 2
'''
            run = shell(f, 'RC='+repr(rc)+'; OUT='+repr(out)+';\n'+body)
            self.assertEqual((run.returncode,run.stdout), (88,'HOLD\n'),run.stderr)

    def test_late_reply_and_reporting_retry(self):
        f = function(CUSTODY,'snap_reply_positive') + function(CUSTODY,'snap_join_positive')
        run = shell(f, '''
SNAP_PENDING=1; checks=0; marks=0; waits=0; writes=0
function [ { if [[ "$1" == '!' && "$2" == '-f' ]]; then checks=$((checks+1)); [[ "$checks" == 1 ]]; return $?; fi; builtin [ "$@"; }
date() { echo 3; }
cat() { case "$1" in *.rc) echo 0;; *.out) echo 'document file';; esac; }
snap_soft_deadline() { marks=$((marks+1)); SNAP_HOLD=1; SNAP_SOFT_RED=1; }
snap_wait_original() { waits=$((waits+1)); return 0; }
snap_publish_joined() { writes=$((writes+1)); [ "$writes" -gt 1 ]; }
snap_join_positive original 123 2
printf '%s,%s,%s,%s,%s' "$marks" "$waits" "$writes" "$SNAP_PENDING" "$SNAP_HOLD"
''')
        self.assertEqual((run.returncode,run.stdout),(0,'2,1,2,0,1'),run.stderr)

    def test_signal_wait_rejoins_original(self):
        f = function(CUSTODY,'snap_wait_original') + function(CUSTODY,'snap_signal')
        run = shell(f, '''
waits=0; ids=''
snap_soft_deadline() { SNAP_HOLD=1; SNAP_SOFT_RED=1; }
wait() { ids="$ids $1"; waits=$((waits+1)); if [ "$waits" == 1 ]; then snap_signal; return 143; fi; return 0; }
snap_wait_original 123
printf '%s,%s,%s' "$waits" "$ids" "$SNAP_HOLD"
''')
        self.assertEqual((run.returncode,run.stdout),(0,'2, 123 123,1'),run.stderr)

    def test_no_next_dispatch_after_soft_red(self):
        f = function(CUSTODY,'snap_admit_next') + function(SCRIPT,'snap_copy')
        run = shell(f, '''
SNAP_HOLD=1; SNAP_SOFT_RED=1
snap_start_copy() { echo BAD_DISPATCH; }
snap_copy id src dest man
''')
        self.assertEqual((run.returncode,run.stdout),(1,''),run.stderr)

    def test_finish_retains_failure_before_shared_cleanup(self):
        f = function(SCRIPT,'snap_finish')
        run = shell(f, '''
SNAP_HOLD=1; QA_FAIL=1; QA_CAT=test; QA_CAT_DIR=owned
snap_publish_terminal() { return 0; }
qa_end() { printf 'keep=%s\n' "$QA_KEEP_DATA"; }
snap_finish
''')
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertIn('keep=1',run.stdout)

    def test_failed_restore_retains_uncertainty(self):
        # Redirect into /dev/null through a harmless owned scratch directory.
        root = os.environ.get('QA_TEST_ROOT')
        self.assertTrue(root, 'root runner must supply owned QA_TEST_ROOT')
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            f = function(CUSTODY,'snap_admit_next') + function(SCRIPT,'snap_offline')
            run = shell(f, '''
QA_CAT_DIR='''+repr(work)+'''
qa_offline() { echo '{"ok":true,"offline_mode":false}'; }
snap_get() { return 0; }
snap_evidence() { return 1; }
snap_offline off
printf '%s,%s,%s' "$SNAP_FORCED_OFFLINE" "$SNAP_HOLD" "$QA_FAIL"
''')
            self.assertEqual((run.returncode,run.stdout),(0,'1,1,1'),run.stderr)

    def test_tsv_writer_actual_tabs(self):
        root = os.environ.get('QA_TEST_ROOT')
        self.assertTrue(root, 'root runner must supply owned QA_TEST_ROOT')
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            run = shell(function(SCRIPT,'snap_row'), 'QA_CAT_DIR='+repr(work)+'; snap_row 1 /d/dir1 /manifest /d')
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertEqual((Path(work)/'snap_dirs.tsv').read_text(),'1\t/d/dir1\t/manifest\t/d\n')


    def test_empty_manifest_never_calls_shared_custody(self):
        root = os.environ.get('QA_TEST_ROOT')
        self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-', dir=root) as work:
            run = shell(function(SCRIPT, 'snap_custody'), """
snap_evidence() { return 1; }
qa_verify_custody() { echo BAD_SHARED_CUSTODY; }
snap_custody /absent /unused 1 """ + repr(work+'/proof'))
            self.assertEqual((run.returncode,run.stdout),(1,''),run.stderr)

    def test_initial_drain_failure_stops_measurement(self):
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            f = function(SCRIPT,'snap_run') + function(SCRIPT,'snap_row')
            run = shell(f, """
QA_CAT_DIR="""+repr(work)+"""
QA_STAGE="$QA_CAT_DIR/stage"; QA_DEST_ROOT="$QA_CAT_DIR/dest"; SNAP_TMP="$QA_CAT_DIR/ipc"
SNAP_SIZES=1; SNAP_FILE_BYTES=1; SNAP_DRAIN_PROBE_FILES=1
qa_preflight() { return 0; }; qa_unique_tag() { echo unique; }; qa_log_mark() { echo 0; }
snap_admit_next() { return 0; }; qa_stage_tree() { echo /unused/manifest; }
snap_evidence() { echo '["a"]'; }; snap_copy() { return 0; }; snap_validate_rows() { return 0; }
snap_settled() { return 1; }; snap_measure() { echo BAD_MEASURE; }
snap_run
""")
            self.assertEqual((run.returncode,run.stdout),(1,''),run.stderr)

    def test_final_drain_failure_joins_before_refusal(self):
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            run = shell(function(SCRIPT,'snap_drain_phase'), """
QA_CAT_DIR="""+repr(work)+"""
QA_STAGE="$QA_CAT_DIR"; SNAP_DEST=/unused; SNAP_DRAIN_PROBE_FILES=1; SNAP_FILE_BYTES=1
SNAP_BIG_N=1; SNAP_BIG_LEAF=/unused; SNAP_BIG_NAMES=/unused
snap_admit_next() { return 0; }; qa_unique_tag() { echo x; }; qa_stage_tree() { echo /unused; }
snap_evidence() { return 0; }; snap_get() { return 0; }
snap_start_copy() { SNAP_LAST_PID=123; }; snap_measure() { return 0; }
snap_join_positive() { echo "joined:$2"; }; snap_flush_soft_reason() { :; }
snap_settled() { echo drain-failed; return 1; }; snap_custody() { echo BAD_CUSTODY; }
snap_drain_phase
""")
            self.assertEqual((run.returncode,run.stdout),(1,'joined:123\ndrain-failed\n'),run.stderr)


    def test_terminal_publication_failure_retains_original_result(self):
        run = shell(function(SCRIPT,'snap_finish'), """
QA_CAT=test; QA_CAT_DIR=owned; writes=0; marks=0
snap_publish_terminal() { writes=$((writes+1)); [ "$writes" -gt 1 ]; }
snap_soft_deadline() { marks=$((marks+1)); SNAP_HOLD=1; SNAP_SOFT_RED=1; QA_FAIL=1; }
qa_end() { printf 'receipt=%s,marks=%s,keep=%s\n' "$writes" "$marks" "$QA_KEEP_DATA"; }
snap_finish
""")
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertIn('receipt=2,marks=1,keep=1',run.stdout)

    def test_worker_publication_failure_never_reissues_finder(self):
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            run = shell(function(CUSTODY,'snap_bg_copy'), """
SNAP_TMP="""+repr(work)+"""
issued=0; moves=0
osascript() { issued=$((issued+1)); printf 'document file'; }
perl() { echo 123.5; }
mv() { moves=$((moves+1)); [ "$moves" != 1 ] || return 1; command mv "$@"; }
snap_bg_copy /unused /unused copy 1
printf 'issued=%s,moves=%s' "$issued" "$moves"
""")
            self.assertEqual((run.returncode,run.stdout),(0,'issued=1,moves=4'),run.stderr)
            self.assertEqual((Path(work)/'copy.rc').read_text(),'0')
            self.assertEqual((Path(work)/'copy.out').read_text(),'document file')

    def test_measure_table_compatibility_and_client_cache_label(self):
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-',dir=root) as work:
            run = shell(function(SCRIPT,'snap_measure'), """
QA_CAT_DIR="""+repr(work)+"""
SNAP_SEQ=0; QA_SNAPPY_MS=200
snap_get() { return 0; }
snap_evidence() {
    case "$1" in list) echo '{}';; interval) printf '1\t1.000\tclient-cache-only-server-path-unproven';; *) return 99;; esac
}
qa_pass() { printf '%s' "$1"; }
snap_measure warm flat_1 /unused /unused
""")
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertIn('client-cache-only-server-path-unproven',run.stdout)
            self.assertEqual((Path(work)/'latency-table.tsv').read_text(),'warm\tflat_1\t1\t1.000\tPASS\n')


    def run_drain_measurement(self, mode, reply='positive'):
        # Actual phase/custody functions; stub only host-effect boundaries.
        # A real original shell child exits and the actual wait consumes it.
        # The strict HOLD boundary exits this test instead of waiting forever.
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-drain-', dir=root) as work:
            functions = ''.join(function(SCRIPT, name) for name in
                                ('snap_measurement_fail', 'snap_measure', 'snap_drain_phase'))
            functions += ''.join(function(CUSTODY, name) for name in
                                 ('snap_intent', 'snap_start_copy', 'snap_hold_unknown',
                                  'snap_reply_positive', 'snap_join_positive',
                                  'snap_wait_original', 'snap_publish_joined', 'snap_admit_next'))
            strict = (HERE / 'release_custody.sh').read_text()
            functions += function(strict, 'qa_fail')
            body = 'QA_CAT_DIR='+repr(work)+'; MODE='+repr(mode)+'; REPLY='+repr(reply)+r'''
QA_STAGE="$QA_CAT_DIR"; SNAP_TMP="$QA_CAT_DIR"; SNAP_DEST=/unused
SNAP_DRAIN_PROBE_FILES=100; SNAP_FILE_BYTES=4096; SNAP_SEQ=0; QA_SNAPPY_MS=200
SNAP_BIG_N=5000; SNAP_BIG_LEAF=/unused; SNAP_BIG_NAMES=/unused
TRACE="$QA_CAT_DIR/trace"
qa_unique_tag() { echo s_1_2_3; }
qa_stage_tree() { echo "$QA_CAT_DIR/manifest"; }
qa_pass() { printf 'pass\n' >> "$TRACE"; }
date() { printf '1\n'; }
perl() { printf '1.5\n'; }
snap_bg_copy() {
    printf 'worker\n' >> "$TRACE"
    if [ "$REPLY" = missing ]; then : > "$SNAP_TMP/$3.out"
    elif [ "$REPLY" = negative ]; then printf 'execution error -36' > "$SNAP_TMP/$3.out"
    else printf 'folder original' > "$SNAP_TMP/$3.out"; fi
    printf '2.0' > "$SNAP_TMP/$3.reply_at"
    printf '0' > "$SNAP_TMP/$3.rc"
}
wait() { printf 'wait:%s\n' "$1" >> "$TRACE"; builtin wait "$@"; }
qa_release_hold() {
    printf 'hold:%s\n' "$SNAP_PENDING" >> "$TRACE"
    [ "$SNAP_PENDING" = 0 ] && exit 1
    exit 88
}
snap_get() { printf '{}\n' > "$2"; }
snap_evidence() {
    case "$1" in
        manifest) printf '["file1.dat"]\n';;
        spool) [ "$MODE" != uncovered ];;
        list) printf '{"names":[],"elapsed_ns":1}\n';;
        interval)
            printf '{"retained":"%s"}\n' "$MODE" > "$2.result.json"
            printf '5000\t1.000\tserver-activity-in-bracket'
            [ "$MODE" != invalid ];;
        *) return 99;;
    esac
}
snap_flush_soft_reason() { :; }
snap_settled() {
    [ "$SNAP_PENDING" = 0 ] && [ -s "$SNAP_TMP/drain.joined" ] || exit 92
    printf 'settled\n' >> "$TRACE"
}
snap_custody() {
    [ "$SNAP_PENDING" = 0 ] && [ -s "$SNAP_TMP/drain.joined" ] || exit 93
    printf 'mounted+sdk-custody\n' >> "$TRACE"
}
snap_drain_phase
rc=$?
printf 'returned:%s,fail:%s,pending:%s,keep:%s\n' "$rc" "$QA_FAIL" "$SNAP_PENDING" "$QA_KEEP_DATA" >> "$TRACE"
[ "$rc" = 0 ] || qa_fail 'category09 required phase failed or uncovered; no replacement operation'
'''
            run = shell(functions, body)
            path = Path(work)
            evidence = {name: (path/name).read_text() if (path/name).exists() else None
                        for name in ('trace', 'drain.worker', 'drain.joined',
                                     'interval-1-under-drain.result.json', 'latency-table.tsv')}
            return run, evidence

    def test_uncovered_drain_joins_and_custody_before_red(self):
        run, evidence = self.run_drain_measurement('uncovered')
        self.assertEqual(run.returncode, 1, run.stderr)
        trace = evidence['trace'].splitlines(); pid = evidence['drain.worker'].split()[0]
        self.assertEqual(trace.count('worker'), 1)
        self.assertEqual(trace.count('wait:'+pid), 1)
        self.assertTrue(evidence['drain.joined'].startswith('0 '))
        self.assertLess(trace.index('wait:'+pid), trace.index('settled'))
        self.assertLess(trace.index('settled'), trace.index('mounted+sdk-custody'))
        self.assertLess(trace.index('mounted+sdk-custody'), trace.index('hold:0'))
        self.assertIn('returned:1,fail:1,pending:0,keep:1', trace)
        self.assertNotIn('pass', trace)

    def test_invalid_drain_sample_joins_and_retains_raw_red(self):
        run, evidence = self.run_drain_measurement('invalid')
        self.assertEqual(run.returncode, 1, run.stderr)
        trace = evidence['trace'].splitlines(); pid = evidence['drain.worker'].split()[0]
        self.assertEqual(trace.count('worker'), 1)
        self.assertEqual(trace.count('wait:'+pid), 1)
        self.assertLess(trace.index('mounted+sdk-custody'), trace.index('hold:0'))
        self.assertIn('returned:1,fail:1,pending:0,keep:1', trace)
        self.assertIn('invalid', evidence['interval-1-under-drain.result.json'])
        self.assertTrue(evidence['latency-table.tsv'].endswith('\tFAIL\n'))
        self.assertNotIn('pass', trace)

    def test_measurement_red_never_admits_unknown_finder_reply(self):
        for reply in ('missing', 'negative'):
            with self.subTest(reply=reply):
                run, evidence = self.run_drain_measurement('uncovered', reply)
                self.assertEqual(run.returncode, 88, run.stderr)
                trace = evidence['trace'].splitlines()
                self.assertEqual(trace.count('worker'), 1)
                self.assertIn('hold:1', trace)
                self.assertIsNone(evidence['drain.joined'])
                self.assertNotIn('settled', trace)
                self.assertNotIn('mounted+sdk-custody', trace)

    def test_earlier_measurement_failure_stays_strict(self):
        root = os.environ.get('QA_TEST_ROOT'); self.assertTrue(root)
        with tempfile.TemporaryDirectory(prefix='snappiness-earlier-', dir=root) as work:
            functions = function(SCRIPT, 'snap_measurement_fail') + function(SCRIPT, 'snap_measure')
            run = shell(functions, 'QA_CAT_DIR='+repr(work)+r'''
SNAP_SEQ=0; QA_SNAPPY_MS=200
snap_get() { return 0; }
snap_evidence() { [ "$1" = list ] && printf '{}' && return 0; return 1; }
qa_fail() { printf 'STRICT_EARLIER\n'; exit 87; }
snap_measure warm flat_10 /unused /unused
''')
            self.assertEqual((run.returncode, run.stdout), (87, 'STRICT_EARLIER\n'), run.stderr)


if __name__ == '__main__':
    unittest.main()

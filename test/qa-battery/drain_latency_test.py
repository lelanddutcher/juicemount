"""Root-run pure controls: actual shell callers under host stubs, no Finder/mount/network."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
SOURCE=(HERE/'11-drain-latency.sh').read_text()
EVIDENCE=(HERE/'drain_latency_evidence.py').read_text()
NEUTER=os.environ.get('DRAIN_NEUTER','')
if NEUTER == 'active-floor':
    EVIDENCE=EVIDENCE.replace("require(len(active) >= minimum,", "require(True,")
elif NEUTER == 'active-tail':
    EVIDENCE=EVIDENCE.replace("b['in_progress'] > 0 and a['in_progress'] > 0", "b['in_progress'] > 0")
elif NEUTER == 'active-p99':
    EVIDENCE=EVIDENCE.replace("(('whole-window',samples),('active-only',active))", "(('whole-window',samples),)")
elif NEUTER == 'window-cap':
    SOURCE=SOURCE.replace("snap_soft_deadline 'drain measurement window cap'; rc=1; break", "break")
elif NEUTER == 'original-join':
    SOURCE=SOURCE.replace('snap_join_positive load "$pid" "$deadline"', ': # disabled original join')
elif NEUTER == 'pending-cleanup':
    SOURCE=SOURCE.replace("[ \"$SNAP_PENDING\" = 0 ] || snap_hold_unknown 'category11 exits with original work pending'", ': # disabled pending refusal')
else:
    assert NEUTER == ''
module=types.ModuleType('drain_test_evidence'); module.__file__=str(HERE/'drain_latency_evidence.py')
exec(compile(EVIDENCE,module.__file__,'exec'),module.__dict__)


class DrainTests(unittest.TestCase):
    def setUp(self):
        base=os.environ.get('QA_TEST_ROOT'); self.assertTrue(base,'owned QA_TEST_ROOT required')
        self.temp=tempfile.TemporaryDirectory(dir=base,prefix='drain-test-');self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)

    def metrics(self,n=0):
        return dict({k:n for k in module.shared.COUNTERS},uptime_sec=n,
                    rpcs={k:{'count':n} for k in module.shared.RPCS})

    def spool(self,active=1,succeeded=0):
        return {'enabled':True,'pending_files':1 if active else 0,'in_progress':active,
                'succeeded':succeeded,'failed_files':0,'capacity_used':1 if active else 0}

    def sample(self,n,active=True,ns=1000000):
        return {'active':active,'dir_ns':ns,'stat_ns':ns,
                'spool_before':{'succeeded':n},'spool_after':{'succeeded':n+1}}

    def pair_files(self,after_active=1):
        prefix=str(self.root/'pair')
        docs={'list':{'kind':'list','ok':True,'names':['file1.dat','._file1.dat'],'elapsed_ns':1},
              'stat':{'kind':'stat','ok':True,'regular':True,'size':4096,'elapsed_ns':2},
              'spool-before':self.spool(), 'spool-after':self.spool(after_active,1),
              'metrics-before':self.metrics(), 'metrics-middle':self.metrics(),'metrics-after':self.metrics()}
        for suffix,doc in docs.items():Path(prefix+'.'+suffix+'.json').write_text(json.dumps(doc))
        return prefix

    def shell(self,body,main=False):
        source=SOURCE.replace('source "$DRAIN_HERE/lib.sh"','').replace(
            'source "$DRAIN_HERE/snappiness_custody.sh"', 'source '+repr(str(HERE/'snappiness_custody.sh')))
        prefix,tail=source.rsplit('qa_begin "$QA_CAT"',1)
        pre='''
QA_STAGE="$CASE/stage"; QA_CAT_DIR="$CASE/report"; QA_DEST_ROOT="$CASE/dest"
QA_DRAIN_TIMEOUT=2 QA_SNAPPY_MS=200 QA_HARD_STALL_MS=1000 QA_KEEP_DATA=1
CP_BASE=http://unused; QA_FAIL=0
mkdir -p "$QA_STAGE" "$QA_CAT_DIR" "$QA_DEST_ROOT"
qa_fail() { QA_FAIL=$((QA_FAIL+1)); echo "FAIL:$*" >> "$CASE/events"; }
qa_log() { :; }; qa_cleanup() { echo CLEANUP >> "$CASE/events"; }
qa_end() { [ "$QA_KEEP_DATA" = 1 ] || qa_cleanup; }
qa_finder_result_clean() { [ "$1" = positive ]; }
curl() { echo UNEXPECTED_NETWORK >&2; return 99; }
osascript() { echo UNEXPECTED_FINDER >&2; return 99; }
qa_timeout() { echo UNEXPECTED_HOST_CALL >&2; return 99; }
sleep() { :; }; perl() { :; }
'''
        text=pre+prefix+'\n'+body+'\n'+('qa_begin "$QA_CAT"'+tail if main else '')
        path=self.root/'control.sh';path.write_text(text)
        env=dict(os.environ,CASE=str(self.root))
        result=subprocess.run(['/bin/bash',str(path)],env=env,capture_output=True,text=True,timeout=10)
        return result,(self.root/'events').read_text() if (self.root/'events').exists() else ''

    def test_exact_manifest(self):
        text=''.join(f'dir{d}/file{f}.dat\t'+('a'*32)+'\n' for d in range(1,9) for f in range(1,41))
        self.assertEqual(len(module.manifest(text,8,40)),320)
        for bad in ('',text.replace('dir8/file40.dat','dir8/file39.dat'),text[:-34],text.replace('\t',' ')):
            with self.assertRaises(ValueError):module.manifest(bad,8,40)

    def test_pair_names_stat_and_client_cache(self):
        prefix=self.pair_files()
        result=module.pair(prefix,['dir1/file1.dat'],4096)
        self.assertEqual(result['stat_metrics']['coverage'],'client-cache-only-server-path-unproven')
        for field,value in [('regular',False),('size',4095),('size',True),('elapsed_ns',-1)]:
            path=Path(prefix+'.stat.json');doc=json.loads(path.read_text());old=dict(doc);doc[field]=value;path.write_text(json.dumps(doc))
            with self.assertRaises(ValueError):module.pair(prefix,['dir1/file1.dat'],4096)
            path.write_text(json.dumps(old))
        with self.assertRaises(ValueError):module.pair(prefix,['dir1/other.dat'],4096)

    def test_active_requires_both_boundaries(self):
        self.assertFalse(module.pair(self.pair_files(0),['dir1/file1.dat'],4096)['active'])

    def test_invalid_metrics_and_spool(self):
        for bad in ({}, {'enabled':True,'pending_files':True},dict(self.spool(),in_progress=-1)):
            with self.assertRaises(ValueError):module.settled(bad)
        self.assertTrue(module.settled(self.spool(0)))
        with self.assertRaises(ValueError):module.shared.metrics(self.metrics(1),self.metrics(0))
        for v in (True,-1,None,'0'):
            bad=self.metrics();bad['rpc_total']=v
            with self.assertRaises(ValueError):module.shared.metrics(bad,self.metrics())

    def test_active_floor_no_padding(self):
        samples=[self.sample(i,i<39) for i in range(100)]
        with self.assertRaisesRegex(ValueError,'insufficient active'):module.summarize(samples,40,200,1000,True)

    def test_active_p99_not_diluted(self):
        samples=[self.sample(i,i<40,300000000 if i==39 else 1000000) for i in range(400)]
        self.assertFalse(module.summarize(samples,40,200,1000,True)['within_budget'])

    def test_summary_progress_cap_worst_and_reset(self):
        samples=[self.sample(i) for i in range(40)]
        self.assertTrue(module.summarize(samples,40,200,1000,True)['within_budget'])
        with self.assertRaises(ValueError):module.summarize(samples,40,200,1000,False)
        with self.assertRaises(ValueError):module.summarize([],40,200,1000,True)
        broken=copy.deepcopy(samples);broken[20]['spool_before']['succeeded']=0
        with self.assertRaises(ValueError):module.summarize(broken,40,200,1000,True)
        broken=copy.deepcopy(samples)
        for s in broken:s['spool_before']['succeeded']=s['spool_after']['succeeded']=0
        with self.assertRaises(ValueError):module.summarize(broken,40,200,1000,True)
        samples[-1]['stat_ns']=1000000000
        self.assertFalse(module.summarize(samples,40,200,1000,True)['within_budget'])

    def test_actual_main_reaches_first_dispatch(self):
        result,events=self.shell('''
qa_begin() { :; }; qa_preflight() { return 0; }; qa_unique_tag() { echo "${1}_owned"; }
qa_log_mark() { echo 0; }
qa_stage_tree() { printf x > "$QA_CAT_DIR/probe.manifest"; echo "$QA_CAT_DIR/probe.manifest"; }
drain_evidence() { [ "$1" = manifest ] && { echo '["dir1/file1.dat"]'; return 0; }; return 1; }
snap_start_copy() { echo "DISPATCH:$1:$CACHE_PROBE_FILES:$DRAIN_DIRS:$DRAIN_FILES_PER_DIR:$DRAIN_FILE_BYTES" >> "$CASE/events"; return 1; }
''',main=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('DISPATCH:probe:200:8:40:8388608',events)
        self.assertNotIn('CLEANUP',events)
        self.assertTrue((self.root/'report/terminal.tsv').exists())

    def test_observer_failure_still_joins_original(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; LOAD_CEIL=2
snap_join_positive() { echo "JOIN:$1:$2" >> "$CASE/events"; SNAP_PENDING=0; }
drain_settle() { echo SETTLE >> "$CASE/events"; return 0; }
drain_pair() { printf 0 > "$SNAP_TMP/load.rc"; printf positive > "$SNAP_TMP/load.out"; return 1; }
drain_get() { printf '{}' > "$2"; return 0; }; drain_evidence() { return 0; }
SNAP_PENDING=1
drain_window 123 9999999999; echo "RC:$?" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('JOIN:load:123',events)
        self.assertIn('RC:1',events);self.assertIn('SETTLE',events)
        self.assertIn('invalid',(self.root/'report/pairs.tsv').read_text())

    def test_window_cap_sticky_red_and_join(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; LOAD_CEIL=2
snap_join_positive() { echo JOIN >> "$CASE/events"; }
drain_settle() { return 0; }
drain_get() { echo '{}' > "$2"; }
drain_window 123 0; echo "RC:$?:$SNAP_SOFT_RED" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('JOIN',events)
        self.assertIn('RC:1:1',events)

    def test_negative_reply_holds_without_cleanup(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; printf 1 > "$SNAP_TMP/load.rc"; printf error > "$SNAP_TMP/load.out"
snap_hold_unknown() { echo HOLD >> "$CASE/events"; exit 77; }
drain_window 123 9999999999
''')
        self.assertEqual(result.returncode,77,result.stderr);self.assertIn('HOLD',events);self.assertNotIn('CLEANUP',events)

    def test_pending_finish_refuses_cleanup(self):
        result,events=self.shell('''
QA_KEEP_DATA=0; SNAP_PENDING=1
snap_hold_unknown() { echo HOLD >> "$CASE/events"; exit 77; }
drain_finish
''')
        self.assertEqual(result.returncode,77,result.stderr);self.assertNotIn('CLEANUP',events)

    def test_finish_keepdata_and_publication_failure(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; N=0
drain_publish_terminal() { N=$((N+1)); [ "$N" -gt 1 ]; }
drain_finish; echo "KEEP:$QA_KEEP_DATA:$SNAP_SOFT_RED" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('KEEP:1:1',events);self.assertNotIn('CLEANUP',events)

    def test_settle_timeout_is_failure(self):
        result,events=self.shell('''
drain_get() { echo '{}' > "$2"; }
drain_evidence() { [ "$1" = spool ]; }
drain_settle "$QA_CAT_DIR/settle" 0; echo "RC:$?" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('RC:1',events);self.assertIn('deadline',events)



    def test_observer_retains_failure_and_monotonic_duration(self):
        with patch.object(module.os,'listdir',side_effect=OSError('owned failure')):
            observed=module.observe('list',str(self.root))
        self.assertFalse(observed['ok']); self.assertIn('owned failure',observed['error'])
        self.assertGreaterEqual(observed['elapsed_ns'],0)

    def test_signal_keeps_original_custody(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; SNAP_PENDING=1; QA_KEEP_DATA=0
snap_signal
echo "SIGNAL:$SNAP_PENDING:$SNAP_HOLD:$SNAP_SIGNALLED:$QA_KEEP_DATA" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('SIGNAL:1:1:1:1',events);self.assertNotIn('CLEANUP',events)

    def test_original_negative_reply_never_joins_as_success(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; SNAP_PENDING=1
printf 1 > "$SNAP_TMP/load.rc"; printf error > "$SNAP_TMP/load.out"
snap_hold_unknown() { echo HOLD >> "$CASE/events"; exit 77; }
snap_wait_original() { echo WRONG_WAIT >> "$CASE/events"; }
snap_join_positive load 123 0
''')
        self.assertEqual(result.returncode,77,result.stderr)
        self.assertIn('HOLD',events);self.assertNotIn('WRONG_WAIT',events)


    def test_failed_request_with_valid_json_is_sticky(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; LOAD_CEIL=2; N=0
printf 0 > "$SNAP_TMP/load.rc"; printf positive > "$SNAP_TMP/load.out"
drain_get() { printf '%s' '{"enabled":true,"pending_files":0,"in_progress":0,"succeeded":3,"failed_files":0,"capacity_used":0}' > "$2"; N=$((N+1)); [ "$N" -gt 1 ]; }
drain_evidence() { return 0; }; drain_pair() { return 0; }
snap_join_positive() { echo JOIN >> "$CASE/events"; }; drain_settle() { return 0; }
drain_window 123 9999999999; echo "RC:$?" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('RC:1',events);self.assertIn('request failed',events);self.assertIn('JOIN',events)

    def test_cli_summary_rejects_gap_and_window_errors(self):
        (self.root/'pairs.tsv').write_text(''.join(f'{i}\tvalid\n' for i in range(1,41)))
        (self.root/'window-end').write_text('copy-and-spool-complete')
        for side in ('before','after'):(self.root/f'window-{side}.json').write_text(json.dumps(self.metrics()))
        for i in range(1,41):
            (self.root/f'pair-{i}.result.json').write_text(json.dumps(self.sample(i)))
            for side in ('before','after'):(self.root/f'pair-{i}.metrics-{side}.json').write_text(json.dumps(self.metrics()))
        args=['summary',str(self.root),'40','200','1000'];module.main(args)
        (self.root/'window-before.json').write_text(json.dumps(self.metrics(1)))
        (self.root/'window-after.json').write_text(json.dumps(self.metrics(1)))
        with self.assertRaisesRegex(ValueError,'nonmonotonic'):module.main(args)
        for side in ('before','after'):(self.root/f'window-{side}.json').write_text(json.dumps(self.metrics()))
        for i in range(2,41):
            for side in ('before','after'):
                doc=self.metrics();doc['rpc_errors']=1
                (self.root/f'pair-{i}.metrics-{side}.json').write_text(json.dumps(doc))
        with self.assertRaisesRegex(ValueError,'inter-pair RPC error'):module.main(args)
        doc=self.metrics();doc['rpc_errors']=1
        (self.root/'window-after.json').write_text(json.dumps(doc))
        with self.assertRaisesRegex(ValueError,'window RPC error'):module.main(args)

    def test_actual_main_through_load_summary_and_finish(self):
        result,events=self.shell('''
qa_begin() { :; }; qa_preflight() { return 0; }; qa_unique_tag() { echo "${1}_owned"; }
qa_log_mark() { echo 0; }; qa_drain_ceiling() { echo 2; }
qa_stage_tree() { local p="$QA_CAT_DIR/$(basename "$1").manifest"; printf x > "$p"; echo "$p"; }
qa_verify_custody() { return 0; }; qa_error_scan() { return 0; }
qa_pass() { echo PASS >> "$CASE/events"; }
drain_get() { echo '{}' > "$2"; }
drain_evidence() {
    echo "EVIDENCE:$1" >> "$CASE/events"
    case "$1" in
        manifest) echo '["dir1/file1.dat"]' ;;
        settled) case "$2" in */end-0.json) return 1 ;; esac ;;
        summary) echo SUMMARY >> "$CASE/events" ;;
    esac
    return 0
}
snap_start_copy() {
    echo "DISPATCH:$1" >> "$CASE/events"; SNAP_PENDING=$((SNAP_PENDING+1)); SNAP_LAST_PID=123
    printf 0 > "$SNAP_TMP/$1.rc"; printf positive > "$SNAP_TMP/$1.out"
}
snap_wait_original() { echo JOIN >> "$CASE/events"; return 0; }
''',main=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('DISPATCH:probe',events);self.assertIn('DISPATCH:load',events)
        self.assertEqual(events.count('JOIN'),2);self.assertIn('SUMMARY',events);self.assertIn('PASS',events)
        self.assertNotIn('FAIL:',events);self.assertNotIn('CLEANUP',events)
        self.assertEqual((self.root/'report/window-end').read_text().strip(),'copy-and-spool-complete')


    def test_window_settle_failure_retains_hold_after_join(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; LOAD_CEIL=2; SNAP_PENDING=1
printf 0 > "$SNAP_TMP/load.rc"; printf positive > "$SNAP_TMP/load.out"
drain_get() { echo '{}' > "$2"; }; drain_evidence() { return 0; }
snap_join_positive() { echo JOIN >> "$CASE/events"; SNAP_PENDING=0; }
drain_settle() { return 1; }
drain_window 123 9999999999; echo "RC:$?:$SNAP_HOLD:$SNAP_PENDING" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('JOIN',events);self.assertIn('RC:1:1:0',events);self.assertNotIn('CLEANUP',events)


    def test_spool_cli_rejects_failure_and_observed_reset(self):
        first=self.root/'first.json'; second=self.root/'second.json'
        first.write_text(json.dumps(self.spool(1,5)))
        second.write_text(json.dumps(self.spool(0,4)))
        with self.assertRaisesRegex(ValueError,'reset'):module.main(['spool',str(second),str(first)])
        second.write_text(json.dumps(dict(self.spool(0,5),failed_files=1)))
        with self.assertRaisesRegex(ValueError,'failure'):module.main(['spool',str(second),str(first)])

    def test_end_transient_spool_failure_stays_red(self):
        result,events=self.shell('''
mkdir -p "$SNAP_TMP"; LOAD_CEIL=2; N=0
printf 0 > "$SNAP_TMP/load.rc"; printf positive > "$SNAP_TMP/load.out"
drain_get() { echo '{}' > "$2"; }
drain_evidence() {
    if [ "$1" = spool ]; then N=$((N+1)); [ "$N" -gt 1 ] || return 1; fi
    return 0
}
drain_pair() { return 0; }; drain_settle() { return 0; }
snap_join_positive() { echo JOIN >> "$CASE/events"; }
drain_window 123 9999999999; echo "RC:$?" >> "$CASE/events"
''')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('RC:1',events);self.assertIn('JOIN',events);self.assertIn('invalid',events)

if __name__ == '__main__':unittest.main()

"""Root-run owned scratch controls; no Finder/network/mount. Actual source functions."""
import copy,json,os,subprocess,tempfile,time,types,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
SOURCE=(HERE/'10-index-stability.sh').read_text();EVIDENCE=(HERE/'index_stability_evidence.py').read_text()
NEUTER=os.environ.get('INDEX_NEUTER','')
if NEUTER=='overlap-tail':EVIDENCE=EVIDENCE.replace("covered=before_active and after_active","covered=before_active")
elif NEUTER=='coverage-floor':EVIDENCE=EVIDENCE.replace("len(covered)>=12 and", "True and")
elif NEUTER=='copy-overlap':EVIDENCE=EVIDENCE.replace("require(any(copy_start-lower<=s['before_ns']<=s['after_ns']<=copy_end-upper for s in covered),", "require(True,")
elif NEUTER=='push-real':EVIDENCE=EVIDENCE.replace("delta['events_applied']>0", "True")
elif NEUTER=='joined-sync':EVIDENCE=EVIDENCE.replace("type(r.get('joined')) is bool and r['joined']", "True")
elif NEUTER=='logloss':EVIDENCE=EVIDENCE.replace("all(v==0 for v in values.values())", "all(v==0 for k,v in values.items() if k!='LOGLOSS')")
elif NEUTER=='pending-exit':SOURCE=SOURCE.replace('[ "$SNAP_PENDING" = 0 ] && [ "$INDEX_SYNC_PENDING" = 0 ] || snap_hold_unknown', ': || snap_hold_unknown')
else:assert NEUTER==''
m=types.ModuleType('index_controls');m.__file__=str(HERE/'index_stability_evidence.py');exec(compile(EVIDENCE,m.__file__,'exec'),m.__dict__)
class IndexTests(unittest.TestCase):
    def setUp(self):
        root=os.environ.get('QA_TEST_ROOT');self.assertTrue(root)
        self.tmp=tempfile.TemporaryDirectory(dir=root,prefix='ix-');self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
    def metric(self,n=0):
        return dict({k:0 for k in m.shared.COUNTERS},uptime_sec=n,rpcs={k:{'count':0} for k in m.shared.RPCS},keyspace={'verdict':'working','events_applied':n,'self_write_events':n,'published_mutations':n,'scan_promotions':0})
    def write(self,name,value):p=self.root/name;p.write_text(json.dumps(value));return p
    def sync(self):
        db=self.root/'data';db.mkdir(exist_ok=True);d=self.root/'ipc';d.mkdir(mode=0o700,exist_ok=True)
        arm={'clock_domain':'posix-clock-monotonic-ns','schema':1,'method':'sync','request_nonce':'a'*32,'service_instance':'instance','metadata_db_path':str(db/'metadata.db'),'mount_point':str(self.root/'volume'),'isolation_root':str(self.root)}
        who={'app':'JuiceMount',**{k:arm[k] for k in ('metadata_db_path','mount_point')},'instance_id':'instance'}
        (d/'arm.json').write_text(json.dumps(arm));return d,who
    def test_arm_request_reply_and_no_reissue(self):
        d,who=self.sync();m.arm_directory(d,who);q=m.request(d,who)
        with self.assertRaises(ValueError):m.request(d,who)
        r=dict(q,dispatched_ns=q['requested_ns'],completed_ns=q['requested_ns']+1,joined=True,text='ok');(d/'reply.json').write_text(json.dumps(r));self.assertEqual(m.reply(d),r)
        r['joined']=False;(d/'reply.json').write_text(json.dumps(r))
        with self.assertRaises(ValueError):m.reply(d)
    def test_wrong_clock_domain_refused(self):
        d,who=self.sync();arm=json.loads((d/'arm.json').read_text());arm['clock_domain']='python-per-process-origin';(d/'arm.json').write_text(json.dumps(arm))
        with self.assertRaises(ValueError):m.arm_directory(d,who)
    def test_actual_crossprocess_clock_brackets(self):
        # Exercise exactly the interpreter used by shell helpers, not an in-process fake.
        for _ in range(3):
            before=time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            value=subprocess.check_output(['/usr/bin/python3','-I','-B',str(HERE/'index_stability_evidence.py'),'clock'],text=True,timeout=5)
            after=time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            self.assertLessEqual(before,int(value));self.assertLessEqual(int(value),after)
    def test_wrong_identity_and_malformed_mailbox(self):
        d,who=self.sync();who['instance_id']='other'
        with self.assertRaises(ValueError):m.arm_directory(d,who)
        (d/'arm.json').write_text('{}')
        with self.assertRaises(ValueError):m.arm_directory(d,who)
    def test_manifest_exact_nonempty(self):
        self.assertEqual(m.manifest('dir1/file1.dat\t'+'0'*32,1,1),['dir1/file1.dat'])
        for text in ('','dir1/file2.dat\t'+'0'*32):
            with self.assertRaises(ValueError):m.manifest(text,1,1)
    def test_activity_not_busy_substitute(self):
        for doc in ({'busy':True,'operations':[{'kind':'drain','active':True}]},{'operations':[{'kind':'reconcile','active':1}]}):
            with self.assertRaises(ValueError):m.active(doc)
        self.assertFalse(m.active({'operations':[{'kind':'reconcile','active':False}]}))
    def sample_files(self,tail=True):
        p=str(self.root/'sample')
        docs={'listing':{'ok':True,'extras_regular':True,'names':['file1.dat'],'start_ns':2,'end_ns':3,'elapsed_ns':1},'clock':{'before_ns':1,'after_ns':4,'wall_before_ns':1,'wall_after_ns':4},'metrics-before':self.metric(),'metrics-after':self.metric(),'activity-before':{'operations':[{'kind':'reconcile','active':True}]},'activity-after':{'operations':[{'kind':'reconcile','active':tail}]}}
        for key,value in docs.items():Path(p+'.'+key+'.json').write_text(json.dumps(value))
        return p
    def test_inactive_before_still_validates_after(self):
        p=self.sample_files(False);Path(p+'.activity-before.json').write_text(json.dumps({'operations':[{'kind':'reconcile','active':False}]}));Path(p+'.activity-after.json').write_text('{}')
        with self.assertRaises(ValueError):m.sample(p,1,'driver')
    def test_half_overlap_rejected(self):self.assertFalse(m.sample(self.sample_files(False),1,'driver')['covered'])
    def samples(self,count=12):return [{'covered':True,'namespace':'driver' if i%2==0 else 'small','elapsed_ns':10,'before_ns':2,'after_ns':3,'wall_before_ns':2,'wall_after_ns':3} for i in range(count)]
    def test_coverage_floor_no_idle_padding(self):
        with self.assertRaises(ValueError):m.summary(self.samples(2),1,4,{'dispatched_ns':1,'completed_ns':4},200)
        self.assertEqual(m.summary(self.samples(),1,4,{'dispatched_ns':1,'completed_ns':4},200)['covered'],12)
    def test_actual_copy_completion_not_observation(self):
        with self.assertRaises(ValueError):m.summary(self.samples(),0,1,{'dispatched_ns':1,'completed_ns':4},200)
    def test_clock_offset_inconsistency_refused(self):
        samples=self.samples();samples[0]['wall_before_ns']+=100
        with self.assertRaises(ValueError):m.summary(samples,1,4,{'dispatched_ns':1,'completed_ns':4},200)
    def test_missing_slow_or_idle_samples(self):
        for sample in ([],[dict(x,covered=False) for x in self.samples()],[dict(x,elapsed_ns=200000000) for x in self.samples()]):
            with self.assertRaises(ValueError):m.summary(sample,1,4,{'dispatched_ns':1,'completed_ns':4},200)
    def test_push_real_not_selfwrite(self):
        b=self.metric();a=self.metric(1);a['keyspace']['events_applied']=0
        with self.assertRaises(ValueError):m.push(b,a)
        self.assertEqual(m.push(b,self.metric(1))['events_applied'],1)
    def test_counter_gaps_reset_and_errors(self):
        a=self.metric(1);b=self.metric();
        with self.assertRaises(ValueError):m.clean_metrics(a,b)
        b['rpc_errors']=1
        with self.assertRaises(ValueError):m.clean_metrics(self.metric(),b)
        del b['rpcs']['READDIR']
        with self.assertRaises(ValueError):m.clean_metrics(self.metric(),b)
    def test_logloss_and_missing_tally(self):
        text='errscan STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0 LOGLOSS=0';m.error_scan(text)
        for bad in ('',text.replace('LOGLOSS=0','LOGLOSS=1')):
            with self.assertRaises(ValueError):m.error_scan(bad)
    def shell(self,tail):
        body='\n'.join(x for x in SOURCE.splitlines() if not x.startswith('source ') and not x.startswith('INDEX_HERE='))
        body=body.split('\nqa_begin "$QA_CAT"\n',1)[0]
        pre='QA_STAGE='+str(self.root)+'\nQA_CAT_DIR='+str(self.root)+'\nQA_FAIL=0\nQA_SNAPPY_MS=200\nQA_DRAIN_TIMEOUT=1\nQA_DEST_ROOT='+str(self.root/'dest')+'\n'
        return subprocess.run(['/bin/bash','-c',pre+body+'\ntrap - EXIT\n'+tail],text=True,capture_output=True,timeout=45)
    def test_actual_finish_pending_refuses(self):
        p=self.shell('INDEX_SYNC_PENDING=1\nsnap_hold_unknown(){ exit 42; }\nqa_end(){ :; }\nindex_finish')
        self.assertEqual(p.returncode,42,p.stderr)
    def test_actual_finish_keep_data(self):
        p=self.shell('SNAP_HOLD=1\nqa_end(){ [ "$QA_KEEP_DATA" = 1 ] || exit 9; }\nindex_finish')
        self.assertEqual(p.returncode,0,p.stderr)
    def test_actual_unarmed_main_no_finder(self):
        p=self.shell('qa_preflight(){ :; }\nsnap_start_copy(){ exit 9; }\nqa_fail(){ :; }\nindex_run; rc=$?; [ "$rc" = 1 ]')
        self.assertEqual(p.returncode,0,p.stderr)
    def test_actual_window_joins_original_before_summary(self):
        d,who=self.sync();(d/'reply.json').write_text('{}')
        tail='''QA_INDEX_SYNC_DIR="'''+str(d)+'''"
qa_fail(){ :; }
index_sync_join(){ echo sync >> "$QA_CAT_DIR/order"; INDEX_SYNC_PENDING=0; }
snap_flush_soft_reason(){ :; }
index_settle(){ echo settle >> "$QA_CAT_DIR/order"; }
index_get(){ echo '{}' > "$2"; }
index_evidence(){ :; }
index_window 9999999999
[ "$(cat "$QA_CAT_DIR/order")" = "$(printf 'sync\\nsettle')" ]
'''
        p=self.shell(tail);self.assertEqual(p.returncode,0,p.stderr)
    def test_actual_sync_unknown_keeps_pending(self):
        d,who=self.sync();(d/'reply.json').write_text('{}')
        helper=self.root/'evidence.py';helper.write_text(EVIDENCE.replace("Path(__file__).with_name('snappiness_evidence.py')",repr(str(HERE/'snappiness_evidence.py'))))
        tail='QA_INDEX_SYNC_DIR="'+str(d)+'"\nINDEX_SYNC_PENDING=1\nindex_evidence(){ /usr/bin/python3 -I -B "'+str(helper)+'" "$@"; }\nsnap_hold_unknown(){ [ "$INDEX_SYNC_PENDING" = 1 ] || exit 7;exit 42; }\nindex_sync_join 9999999999'
        result=self.shell(tail);self.assertEqual(result.returncode,42,result.stderr)
    def test_exit_pending_never_cleans(self):
        result=self.shell('INDEX_SYNC_PENDING=1\nsnap_on_exit(){ exit 9; }\nindex_exit')
        self.assertEqual(result.returncode,0,result.stderr)
    def test_original_copy_clock_bound(self):
        with self.assertRaises(ValueError):m.summary(self.samples(),0,45000000000,{'dispatched_ns':1,'completed_ns':4},200)
    def test_actual_main_happy_dispatches_once_and_parser_identity(self):
        d,who=self.sync();self.write('who.json',who);self.write('before.json',self.metric());self.write('after.json',self.metric(1))
        helper=self.root/'evidence.py';helper.write_text(EVIDENCE.replace("Path(__file__).with_name('snappiness_evidence.py')",repr(str(HERE/'snappiness_evidence.py'))))
        host=self.root/'host.py'
        host.write_text('''import json,sys,time,pathlib,shutil
r=pathlib.Path(__file__).parent;mode=sys.argv[1]
if mode=='get':
 endpoint,out=sys.argv[2:];out=pathlib.Path(out)
 if endpoint=='whoami':value=json.loads((r/'who.json').read_text())
 elif endpoint=='metrics':
  if 'push-after' in out.name:(r/'push-done').write_text('1')
  value=json.loads((r/('after.json' if (r/'push-done').exists() else 'before.json')).read_text())
 elif endpoint=='spool':value={'enabled':True,'pending_files':0,'in_progress':0,'failed_files':0,'capacity_used':0,'succeeded':1}
 elif endpoint=='activity':
  counter=r/'observations';n=int(counter.read_text())+1 if counter.exists() else 1;counter.write_text(str(n))
  custody=next(r.glob('index-ipc-*'))
  if n>=18 and (custody/'small.worker').exists() and not (custody/'small.rc').exists():
   (custody/'small.out').write_text('folder copied');(custody/'small.rc').write_text('0');(custody/'small.reply_at').write_text(str(time.time()))
  if n>=60:
   q=json.loads((r/'ipc/request.json').read_text());q.update(dispatched_ns=q['requested_ns'],completed_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC),joined=True,text='ok');(r/'ipc/reply.json').write_text(json.dumps(q))
  value={'operations':[{'kind':'reconcile','active':True}]}
 else:raise AssertionError(endpoint)
 out.write_text(json.dumps(value))
elif mode=='copy':
 ident,source,dest,custody=sys.argv[2:];source=pathlib.Path(source);custody=pathlib.Path(custody)
 shutil.copytree(source,pathlib.Path(dest)/source.name)
 (custody/(ident+'.worker')).write_text('123 '+str(time.time()))
 if ident=='driver':
  (custody/(ident+'.rc')).write_text('0');(custody/(ident+'.out')).write_text('copied');(custody/(ident+'.reply_at')).write_text(str(time.time()))
''')
        tail='''QA_INDEX_SYNC_DIR="%s"
INDEX_DIRS=1 INDEX_FILES=1 INDEX_BYTES=1
qa_preflight(){ :; };qa_unique_tag(){ echo "$1"; };qa_log_mark(){ echo 0; }
index_evidence(){ /usr/bin/python3 -I -B "%s" "$@"; }
index_get(){ /usr/bin/python3 -I -B "%s" get "$1" "$2"; }
qa_stage_tree(){ mkdir -p "$1"; f="$1.manifest"; : > "$f"; for ((d=1;d<=$2;d++));do for ((n=1;n<=$3;n++));do mkdir -p "$1/dir$d"; printf x > "$1/dir$d/file$n.dat"; printf 'dir%%s/file%%s.dat\\t00000000000000000000000000000000\\n' "$d" "$n" >> "$f";done;done;echo "$f"; }
snap_admit_next(){ return 0; }
snap_start_copy(){ echo "$1" >> "$QA_CAT_DIR/dispatch";SNAP_LAST_PID=123; /usr/bin/python3 -I -B "%s" copy "$1" "$2" "$3" "$SNAP_TMP"; }
snap_join_positive(){ [ -f "$SNAP_TMP/$1.rc" ] || exit 8; printf '0 0\\n' > "$SNAP_TMP/$1.joined"; }
snap_reply_positive(){ [ "$1" = 0 ] && [ -n "$2" ]; }
snap_flush_soft_reason(){ :; };qa_verify_custody(){ :; }
snap_hold_unknown(){ exit 42; };snap_soft_deadline(){ exit 43; }
qa_error_scan(){ echo 'errscan STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0 LOGLOSS=0'; }
qa_pass(){ echo pass > "$QA_CAT_DIR/pass"; };qa_fail(){ exit 9; }
index_run
[ "$(cat "$QA_CAT_DIR/dispatch")" = "$(printf 'driver\\nsmall')" ] && [ -s "$QA_CAT_DIR/pass" ]
''' % (d,helper,host,host)
        result=self.shell(tail);self.assertEqual(result.returncode,0,result.stderr)
        self.assertGreaterEqual(json.loads((self.root/'summary.json').read_text())['covered'],12)
if __name__=='__main__':unittest.main()

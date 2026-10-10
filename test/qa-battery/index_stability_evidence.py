"""Category10 evidence and one-shot sync mailbox. No implicit runtime actions."""
import importlib.util,json,os,re,signal,stat,sys,time
from decimal import Decimal
from pathlib import Path
_spec=importlib.util.spec_from_file_location('snappiness_evidence',Path(__file__).with_name('snappiness_evidence.py'))
shared=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(shared)
require=shared.require;integer=shared.integer

def load(p):return json.loads(Path(p).read_text())
def mailbox_load(p):
    fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd) as f:
        st=os.fstat(f.fileno());require(stat.S_ISREG(st.st_mode) and st.st_uid==os.geteuid() and st.st_nlink==1 and st.st_size<=65536,'invalid mailbox file')
        return json.load(f)
def write(p,value):Path(p).write_text(json.dumps(value,sort_keys=True)+'\n')
def manifest(text,dirs,files):
    expected={f'dir{d}/file{f}.dat' for d in range(1,dirs+1) for f in range(1,files+1)};seen=set()
    require(dirs>0 and files>0,'empty dimensions')
    for line in text.splitlines():
        row=line.split('\t');require(len(row)==2 and row[0] in expected and row[0] not in seen and re.fullmatch('[0-9a-f]{32}',row[1]),'invalid manifest');seen.add(row[0])
    require(seen==expected,'empty/incomplete manifest');return sorted(seen)

CLOCK_DOMAIN='posix-clock-monotonic-ns'

def identity(arm,who):
    require(arm.get('clock_domain')==CLOCK_DOMAIN,'wrong clock domain')
    require(arm.get('schema')==1 and type(arm.get('schema')) is int and arm.get('method')=='sync','invalid sync schema')
    require(type(arm.get('request_nonce')) is str and re.fullmatch('[0-9a-f]{32,64}',arm['request_nonce']),'invalid nonce')
    require(type(arm.get('service_instance')) is str and arm['service_instance'],'missing identity')
    require(who.get('app')=='JuiceMount' and who.get('instance_id')==arm['service_instance'],'wrong service identity')
    for key in ('metadata_db_path','mount_point'):
        require(type(arm.get(key)) is str and Path(arm[key]).is_absolute() and who.get(key)==arm[key],'identity binding differs')
    return arm

def arm_directory(directory,who):
    p=Path(directory);require(p.is_absolute() and p.resolve()==p,'sync directory not literal')
    for x in (p,*p.parents):require(stat.S_ISDIR(x.lstat().st_mode),'sync ancestor not directory')
    st=p.stat();require(st.st_dev==Path.home().stat().st_dev and '/Volumes' not in [str(x) for x in p.parents],'mailbox must stay on primary home device')
    require(st.st_uid==os.geteuid() and stat.S_IMODE(st.st_mode)==0o700,'sync directory not private')
    arm=identity(mailbox_load(p/'arm.json'),who)
    root=Path(arm['isolation_root']);require(root.is_absolute() and root.resolve()==root and root in p.parents,'sync root differs')
    db=Path(arm['metadata_db_path']);require(root in db.parents and st.st_dev==root.stat().st_dev==db.parent.stat().st_dev,'sync directory not primary product device')
    require(not (p/'request.json').exists() and not (p/'reply.json').exists(),'sync already consumed')
    return arm

def request(directory,who):
    p=Path(directory);arm=arm_directory(p,who)
    value={k:arm[k] for k in ('schema','method','request_nonce','service_instance','clock_domain')};value['requested_ns']=time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    # No replacement request: O_EXCL before write, durable file then directory.
    fd=os.open(p/'request.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as f:json.dump(value,f);f.write('\n');f.flush();os.fsync(f.fileno())
    fd=os.open(p,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return value

def reply(directory):
    p=Path(directory);a=mailbox_load(p/'arm.json');q=mailbox_load(p/'request.json');r=mailbox_load(p/'reply.json')
    require(r.get('clock_domain')==CLOCK_DOMAIN,'wrong reply clock domain')
    require(type(r.get('schema')) is int and type(q.get('schema')) is int and r['schema']==1 and r.get('method')=='sync','invalid reply schema')
    for key in ('schema','method','request_nonce','service_instance','clock_domain'):
        require(r.get(key)==q.get(key)==a.get(key),'sync reply identity differs')
    require(type(r.get('joined')) is bool and r['joined'] and r.get('text')=='ok','sync original not positively joined')
    require(integer(r.get('requested_ns'))==integer(q.get('requested_ns')),'request clock differs')
    require(q['requested_ns']<=integer(r.get('dispatched_ns'))<=integer(r.get('completed_ns')),'sync clocks invalid')
    return r

def active(doc):
    require(type(doc) is dict and type(doc.get('operations')) is list,'activity missing')
    rows=[r for r in doc['operations'] if type(r) is dict and r.get('kind')=='reconcile']
    require(len(rows)==1 and type(rows[0].get('active')) is bool,'reconcile state missing')
    if 'files' in rows[0]:integer(rows[0]['files'])
    return rows[0]['active']

def clean_metrics(before,after):
    value=shared.metrics(before,after)
    require(not any(value['delta'][k] for k in ('rpc_errors','read_retries','read_fails')),'RPC error in observation window')
    return value

def push(before,after):
    clean_metrics(before,after);b=before.get('keyspace');a=after.get('keyspace')
    require(type(a) is dict and type(b) is dict,'keyspace unavailable')
    keys=('events_applied','self_write_events','published_mutations','scan_promotions')
    delta={k:integer(a.get(k))-integer(b.get(k)) for k in keys};require(all(v>=0 for v in delta.values()),'keyspace reset')
    require(a.get('verdict')=='working' and delta['events_applied']>0 and delta['published_mutations']>0,'real push/published progress unproven')
    return delta

def observe(path):
    def expired(*_):raise TimeoutError('listing deadline')
    signal.signal(signal.SIGALRM,expired);signal.alarm(30);start=time.clock_gettime_ns(time.CLOCK_MONOTONIC);out={'start_ns':start,'ok':False}
    try:
        out['names']=os.listdir(path);out['extras_regular']=all(stat.S_ISREG(os.stat(Path(path)/n,follow_symlinks=False).st_mode) for n in out['names'] if n=='.DS_Store' or n.startswith('._'));out['ok']=True
    except (OSError,TimeoutError) as e:out['error']=str(e)
    finally:out['end_ns']=time.clock_gettime_ns(time.CLOCK_MONOTONIC);out['elapsed_ns']=out['end_ns']-start;signal.alarm(0)
    return out

def sample(prefix,files,namespace):
    p=str(prefix);listing=load(p+'.listing.json');clock=load(p+'.clock.json')
    require(listing.get('ok') is True and listing.get('extras_regular') is True,'listing observer failed')
    names=shared.listing_names(listing.get('names'),[f'file{x}.dat' for x in range(1,files+1)])
    require(integer(clock.get('before_ns'))<=integer(listing.get('start_ns'))<=integer(listing.get('end_ns'))<=integer(clock.get('after_ns')),'sample clocks')
    require(integer(clock.get('wall_before_ns'))<=integer(clock.get('wall_after_ns')),'wall clock reversed')
    elapsed=integer(listing.get('elapsed_ns'));require(elapsed==listing['end_ns']-listing['start_ns'],'elapsed differs')
    metrics=clean_metrics(load(p+'.metrics-before.json'),load(p+'.metrics-after.json'))
    before_active=active(load(p+'.activity-before.json'));after_active=active(load(p+'.activity-after.json'))
    covered=before_active and after_active
    return dict(namespace=namespace,names=names,elapsed_ns=elapsed,covered=covered,before_ns=clock['before_ns'],after_ns=clock['after_ns'],wall_before_ns=integer(clock.get('wall_before_ns')),wall_after_ns=integer(clock.get('wall_after_ns')),metrics=metrics)

def summary(samples,copy_start,copy_end,sync,budget):
    require(bool(samples) and all(s['elapsed_ns']<budget*1000000 for s in samples),'listing too slow or missing')
    covered=[s for s in samples if s['covered'] and sync['dispatched_ns']<=s['before_ns']<=s['after_ns']<=sync['completed_ns']]
    require(len(covered)>=12 and {s['namespace'] for s in covered}=={'driver','small'},'reconcile listing coverage unproven')
    require(copy_end>=copy_start and copy_end-copy_start<45_000_000_000,'parallel copy deadline')
    lower=max(s['wall_before_ns']-s['before_ns'] for s in samples)
    upper=min(s['wall_after_ns']-s['after_ns'] for s in samples)
    require(lower<=upper and upper-lower<=1000000000,'wall/monotonic clock binding inconsistent')
    require(any(copy_start-lower<=s['before_ns']<=s['after_ns']<=copy_end-upper for s in covered),'copy reconcile overlap unproven')
    return {'samples':len(samples),'covered':len(covered),'worst_ms':max(s['elapsed_ns'] for s in samples)/1e6,'copy_ms':(copy_end-copy_start)/1e6,'scope':'sampled active boundaries; global RPC descriptive, no exclusive/continuous/SQL/WAN/paint claim'}

def error_scan(text):
    keys=('STALE','PHANTOM','E100070','E100060','E48','E36','E5000','PERM','LOGLOSS')
    lines=[x for x in text.splitlines() if x.startswith('errscan ')]
    require(len(lines)==1,'error tally absent/ambiguous')
    rows=lines[0].split()[1:];require(len(rows)==len(keys),'error tally fields')
    values={}
    for item in rows:
        key,value=item.split('=');require(key in keys and key not in values and value.isdecimal(),'malformed tally');values[key]=int(value)
    require(set(values)==set(keys) and all(v==0 for v in values.values()),'error or LOGLOSS observed')

def main(args):
    mode=args[0]
    if mode=='identity':identity(load(args[1]),load(args[2]))
    elif mode=='arm':arm_directory(args[1],load(args[2]))
    elif mode=='request':print(json.dumps(request(args[1],load(args[2]))))
    elif mode=='reply':print(json.dumps(reply(args[1])))
    elif mode=='active':require(active(load(args[1])),'reconcile not active')
    elif mode=='manifest':print(json.dumps(manifest(Path(args[1]).read_text(),int(args[2]),int(args[3]))))
    elif mode=='observe':print(json.dumps(observe(args[1])))
    elif mode=='clock':print(time.clock_gettime_ns(time.CLOCK_MONOTONIC))
    elif mode=='wallclock':print(time.time_ns())
    elif mode=='clockpair':print(json.dumps({'before_ns':int(args[1]),'after_ns':int(args[2]),'wall_before_ns':int(args[3]),'wall_after_ns':int(args[4])}))
    elif mode=='sample':print(json.dumps(sample(args[1],int(args[2]),args[3])))
    elif mode=='push':print(json.dumps(push(load(args[1]),load(args[2]))))
    elif mode=='settled':
        s=shared.spool(load(args[1]));require(all(s[k]==0 for k in ('pending_files','in_progress','failed_files','capacity_used')),'spool not settled')
    elif mode=='spool':require(shared.spool(load(args[1]))['failed_files']==0,'spool failed')
    elif mode=='metrics':clean_metrics(load(args[1]),load(args[2]))
    elif mode=='errors':error_scan(Path(args[1]).read_text())
    elif mode=='summary':
        d=Path(args[1]);samples=[];previous=d/'window-before.json'
        for number,line in enumerate((d/'samples.tsv').read_text().splitlines(),1):
            fields=line.split('\t');require(len(fields)==4 and fields[0]==str(number) and fields[1]=='valid' and fields[2] in ('driver','small'),'invalid observed sample')
            expected=load(d/(fields[2]+'.expected.json'));require(isinstance(expected,list) and expected and int(fields[3])==sum(1 for name in expected if name.startswith('dir1/')),'sample manifest count differs')
            prefix=d/('sample-'+fields[0]);clean_metrics(load(previous),load(str(prefix)+'.metrics-before.json'))
            s=sample(prefix,int(fields[3]),fields[2]);samples.append(s);previous=Path(str(prefix)+'.metrics-after.json')
        clean_metrics(load(previous),load(d/'window-after.json'))
        r=reply(args[2]);custody=Path((d/'custody.path').read_text().strip())
        worker=(custody/'small.worker').read_text().split();require(len(worker)==2,'worker timestamp missing')
        start=int(Decimal(worker[1])*1000000000);end=int(Decimal((custody/'small.reply_at').read_text().strip())*1000000000)
        dispatch=integer(int((d/'small-start-wall-ns').read_text()))
        require(dispatch<=start<=end and end-dispatch<45000000000,'actual copy dispatch deadline')
        result=summary(samples,start,end,r,int(args[3]))
        # Actual worker timestamps, never delayed parent observation, bound copy overlap.
        write(d/'summary.json',result);print(json.dumps(result))
    else:raise ValueError('unknown mode')
if __name__=='__main__':main(sys.argv[1:])

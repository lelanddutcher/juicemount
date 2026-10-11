#!/usr/bin/env python3
"""Read-only inspection of immutable published binaries; never starts the image."""
import hashlib, json, os, pathlib, re, secrets, signal, subprocess
assert os.environ.get('GITHUB_ACTIONS')=='true' and os.environ.get('RUNNER_OS')=='Linux'
root=pathlib.Path(os.environ['RUNNER_TEMP']).resolve(strict=True)/('jm-storage-security-'+os.environ['GITHUB_RUN_ID']+'-'+os.environ['GITHUB_RUN_ATTEMPT'])
root.mkdir(mode=0o700); ev=root/'evidence';ev.mkdir(mode=0o700);bins=root/'bin';bins.mkdir(mode=0o700)
empty=root/'empty-data';empty.mkdir(mode=0o700)
image='docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46'
expected='sha256:983b34aec255b1ef414506989ec0106b319aef815a4f6fbaddd57e4d2bf2494f'
name='jm-security-'+os.environ['GITHUB_RUN_ID']+'-'+os.environ['GITHUB_RUN_ATTEMPT'];nonce=secrets.token_hex(24)
cid=None;attempted=False
result={'workflow_sha':os.environ['GITHUB_SHA'],'image':image,'scope':'amd64 published binary inspection, no image execution or credentials','status':'RUNNING','binaries':{},'cleanup':{}}
def run(args,timeout=120,check=True,env=None):
    child=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,start_new_session=True)
    try:out,err=child.communicate(timeout=timeout)
    except BaseException:
        if child.poll() is None:os.killpg(child.pid,signal.SIGTERM)
        try:child.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGKILL);child.communicate(timeout=5)
        raise
    r=subprocess.CompletedProcess(args,child.returncode,out,err)
    if check and r.returncode:raise RuntimeError('command failed: '+args[0]+' '+r.stderr.decode(errors='replace')[:1500])
    return r

def events(raw):
    decoder=json.JSONDecoder();text=raw.decode();out=[]
    while text.strip():
        obj,n=decoder.raw_decode(text.lstrip());out.append(obj);text=text.lstrip()[n:]
    assert out and out[0].get('config',{}).get('scan_mode')=='binary'
    assert out[0]['config'].get('scan_level')=='symbol'
    assert any(x.get('SBOM',{}).get('modules') and x.get('SBOM',{}).get('go_version') for x in out)
    return out

try:
    run(['docker','pull',image],timeout=180)
    meta=json.loads(run(['docker','image','inspect',image]).stdout)[0]
    assert meta['Id']==expected and meta['Architecture']=='amd64'
    assert set(meta['Config'].get('Volumes',{}))=={'/data'}
    assert meta['Config']['Labels']['org.opencontainers.image.revision']=='2a4d51406b7ed87af5fe6fe0f801f3290f96eb3c'
    absent=run(['docker','inspect',name],check=False);assert absent.returncode and b'No such' in absent.stderr
    attempted=True
    out=run(['docker','create','--name',name,'--label','juicemount.security-trial='+nonce,'--mount','type=bind,src='+str(empty)+',dst=/data,readonly',image]).stdout.decode().strip()
    assert re.fullmatch('[a-f0-9]{64}',out);cid=out
    obj=json.loads(run(['docker','inspect',cid]).stdout)[0]
    assert obj['Image']==expected and obj['Config']['Labels'].get('juicemount.security-trial')==nonce and not obj['State']['Running']
    assert len(obj['Mounts'])==1 and obj['Mounts'][0]['Type']=='bind' and obj['Mounts'][0]['Source']==str(empty) and obj['Mounts'][0]['Destination']=='/data'
    for binary in ['silo','mcli']:
        dest=bins/binary
        run(['docker','cp',cid+':/usr/bin/'+binary,str(dest)])
        assert 0<dest.stat().st_size<300_000_000
        result['binaries'][binary]={'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'bytes':dest.stat().st_size}
        (ev/(binary+'-build.txt')).write_bytes(run(['go','version','-m',str(dest)]).stdout)
    tools=root/'tools';tools.mkdir(mode=0o700)
    run(['go','install','golang.org/x/vuln/cmd/govulncheck@v1.8.0'],timeout=300,env=dict(os.environ,GOBIN=str(tools),GOTOOLCHAIN='local'))
    scanner=tools/'govulncheck'
    (ev/'scanner-build.txt').write_bytes(run(['go','version','-m',str(scanner)]).stdout)
    for binary in ['silo','mcli']:
        scan=run([str(scanner),'-mode=binary','-format=json',str(bins/binary)],timeout=300,check=False)
        (ev/(binary+'-govulncheck.jsonl')).write_bytes(scan.stdout)
        (ev/(binary+'-govulncheck.stderr')).write_bytes(scan.stderr)
        result['binaries'][binary]['scanner_exit']=scan.returncode
        assert scan.returncode==0, 'JSON scanner did not complete'
        # JSON mode returns zero even with findings; inspect protocol messages.
        stream=events(scan.stdout)
        findings=[x['finding'] for x in stream if 'finding' in x]
        result['binaries'][binary].update(
            scanner_config=stream[0]['config'],
            module_findings=sorted({x['osv'] for x in findings}),
            symbol_findings=sorted({x['osv'] for x in findings if any(f.get('function') for f in x.get('trace',[]))}))
    result['status']='HOLD_FINDINGS' if any(x['module_findings'] for x in result['binaries'].values()) else 'PASS'

except BaseException as e:result.update(status='FAIL',error=str(e))
finally:
    try:
        if attempted:
            q=run(['docker','inspect',cid or name],check=False)
            if q.returncode==0:
                obj=json.loads(q.stdout)[0]
                assert cid is None or obj['Id']==cid
                assert obj['Image']==expected and obj['Config']['Labels'].get('juicemount.security-trial')==nonce and not obj['State']['Running']
                assert len(obj['Mounts'])==1 and obj['Mounts'][0]['Type']=='bind' and obj['Mounts'][0]['Source']==str(empty) and obj['Mounts'][0]['Destination']=='/data'
                original=obj['Id'];run(['docker','rm',original])
                q=run(['docker','inspect',original],check=False);assert q.returncode and b'No such' in q.stderr
                result['cleanup']['never_started_container_removed']=True
            else:assert b'No such' in q.stderr
        else:result['cleanup']['no_container_attempted']=True
    except BaseException as e:result.update(status='FAIL',cleanup_error=str(e))
    (ev/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if result['status']!='PASS':raise SystemExit(1)

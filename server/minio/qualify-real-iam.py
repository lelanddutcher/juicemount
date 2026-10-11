#!/usr/bin/env python3
"""Isolated cloud-only qualification of real MinIO provisioning and keyd IAM.

Uses empty owned storage and generated credentials. Never uses a saved deployment.
Only the redacted result is suitable for an uploaded CI artifact.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import time
import urllib.parse
import urllib.request

assert os.environ.get('GITHUB_ACTIONS') == 'true', 'cloud runner required'
assert os.environ.get('RUNNER_OS') == 'Linux', 'Linux cloud runner required'
ROOT = Path(os.environ['MINIO_TRIAL_ROOT']).resolve(strict=True)
assert ROOT.parent == Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
PRIVATE = ROOT / 'private'
PRIVATE.mkdir(mode=0o700)
EVIDENCE = ROOT / 'evidence'
EVIDENCE.mkdir(mode=0o700)
POLICY = Path('server/minio').resolve(strict=True)
BIN = ROOT / 'bin'
MC = str(BIN / 'mc')
BASH = os.environ['JM_TEST_KEYD_BASH']
URL = 'http://127.0.0.1:19000'
CREDENTIALS = {name: ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') for _ in range(40)) for name in ['root', 'client', 'gc', 'keyadmin']}
REDACT = list(CREDENTIALS.values())
ENV = {k: os.environ[k] for k in ['PATH', 'LANG'] if k in os.environ}
ENV.update(TMPDIR=str(PRIVATE), MC_CONFIG_DIR=str(PRIVATE / 'mc'))
RESULT = {'candidate': os.environ['CANDIDATE'], 'workflow_sha': os.environ['GITHUB_SHA'], 'scope': 'amd64 source build and real IAM; no image/deployment/Manager/Link acceptance', 'checks': [], 'cleanup': {}, 'status': 'RUNNING'}
STEP = 'setup'
server = None
server_log = None

def scrub(text):
    for secret in REDACT:
        text = text.replace(secret, '[REDACTED]')
    text = re.sub(r'https?://[^\s"\x27<>]*@', 'http://[REDACTED]@', text)
    return text

def call(args, *, env=None, data=None, expected=0, timeout=45):
    child = subprocess.Popen(args, env=env or ENV, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        out, err = child.communicate(data, timeout=timeout)
    except BaseException:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
        try:
            child.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.communicate(timeout=5)
        raise
    if expected is not None and child.returncode != expected:
        raise RuntimeError(f'command {Path(args[0]).name} failed ({child.returncode}): '+scrub((out+err).decode(errors='replace'))[:2000])
    return child.returncode, out, err

def auth(name, secret):
    return dict(ENV, MC_HOST_trial='http://'+urllib.parse.quote(name, safe='')+':'+urllib.parse.quote(secret, safe='')+'@127.0.0.1:19000')

def mc(args, name='juicemount', secret=None, **kw):
    return call([MC]+args, env=auth(name, secret or CREDENTIALS['root']), **kw)

def passed(name):
    RESULT['checks'].append(name)
    print('PASS '+name, flush=True)

def denied(args, name, secret, revoked=False):
    rc, out, err = mc(['--json']+args, name, secret, expected=None)
    text=(out+err).decode(errors='replace')
    markers=r'InvalidAccessKeyId|InvalidAccessKeyID|Access Key Id you provided does not exist' if revoked else r'AccessDenied|Access [Dd]enied'
    assert rc != 0 and re.search(markers, text), 'not a proven permission refusal: '+scrub(text)[:1000]

def keyd(seq, wanted, remove=None, needs=False):
    desired={'seq':seq,'keys':[{'name':NAME,'state':'active','needs_secret':needs}] if wanted else [],'remove':remove or []}
    tmp=KEYS/'desired.next'
    tmp.write_text(json.dumps(desired));tmp.chmod(0o600);tmp.replace(KEYS/'desired.json')
    env=dict(ENV, MC=MC, JM_OBJ_PER_MAC_SERVER='1', JM_OBJ_KEYADMIN_SECRET_KEY=CREDENTIALS['keyadmin'], JM_MINIO_ENDPOINT=URL, JM_OBJKEYS_DIR=str(KEYS), JM_KEYD_ONCE='1')
    call([BASH, str(POLICY/'keyd.sh')], env=env, timeout=65)
    applied=json.loads((KEYS/'applied.json').read_text())
    assert applied['seq']==seq and not applied.get('error'), 'keyd did not verify desired state: '+scrub(json.dumps(applied))
    return applied

try:
    os.umask(0o077)
    data=PRIVATE/'data';data.mkdir()
    KEYS=PRIVATE/'objkeys';KEYS.mkdir()
    NAME='mac-0123456789ab-1'
    payload=b'JuiceMount real MinIO IAM qualification\n'*32
    env=dict(ENV, MINIO_ROOT_USER='juicemount', MINIO_ROOT_PASSWORD=CREDENTIALS['root'], MINIO_BROWSER='off', MINIO_UPDATE='off')
    server_log=(PRIVATE/'server.log').open('wb')
    server=subprocess.Popen([str(BIN/'minio'),'server','--address','127.0.0.1:19000','--console-address','127.0.0.1:19001',str(data)],env=env,stdout=server_log,stderr=subprocess.STDOUT,start_new_session=True)
    STEP='owned server readiness'
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        assert server.poll() is None, 'owned MinIO exited before readiness'
        try:
            with urllib.request.urlopen(URL+'/minio/health/ready',timeout=1) as r:
                if r.status==200:break
        except (OSError,urllib.error.URLError):pass
        time.sleep(0.2)
    else:raise RuntimeError('owned MinIO did not become ready')
    # These generated root credentials bind the response to this fresh server.
    mc(['--json','admin','info','trial'])
    passed(STEP)
    STEP='real provision and idempotent reprovision'
    mc(['mb','trial/zpool'])
    penv=dict(ENV, MC=MC, MINIO_ROOT_USER='juicemount', MINIO_ROOT_PASSWORD=CREDENTIALS['root'], JM_MINIO_ENDPOINT=URL, JM_MINIO_POLICY_DIR=str(POLICY), JM_OBJ_CLIENT_SECRET_KEY=CREDENTIALS['client'], JM_OBJ_GC_SECRET_KEY=CREDENTIALS['gc'], JM_OBJ_KEYADMIN_SECRET_KEY=CREDENTIALS['keyadmin'], JM_OBJ_PER_MAC_SERVER='1')
    for _ in range(2):
        call([BASH,str(POLICY/'provision-object-keys.sh')],env=penv)
        for name,policy in [('jm-client','jm-client'),('jm-gc','jm-gc'),('jm-macs','jm-client'),('jm-keyadmin','jm-keyadmin')]:
            _,out,_=mc(['--json','admin','user','info','trial',name]);info=json.loads(out)
            assert info['policyName']==policy and info['userStatus']=='enabled',(name,info)
    passed(STEP)
    STEP='client write/read/list; delete refused; GC deletion'
    mc(['pipe','trial/zpool/base-object'],'jm-client',CREDENTIALS['client'],data=payload)
    assert mc(['cat','trial/zpool/base-object'],'jm-client',CREDENTIALS['client'])[1]==payload
    listing=mc(['--json','ls','trial/zpool/'],'jm-client',CREDENTIALS['client'])[1]
    assert b'base-object' in listing
    denied(['rm','trial/zpool/base-object'],'jm-client',CREDENTIALS['client'])
    assert mc(['cat','trial/zpool/base-object'],'jm-client',CREDENTIALS['client'])[1]==payload
    mc(['rm','trial/zpool/base-object'],'jm-gc',CREDENTIALS['gc'])
    assert b'base-object' not in mc(['--json','ls','trial/zpool/'])[1]
    passed(STEP)
    STEP='real keyd creates usable per-Mac key'
    a=keyd(1,True,needs=True)
    match=[k for k in a['keys'] if k['name']==NAME]
    assert len(match)==1 and match[0]['exists'] and match[0]['parent']=='jm-macs' and match[0]['status']=='on' and match[0]['implied_policy'] is True
    delivery=KEYS/'results'/f'{NAME}.json'
    key=json.loads(delivery.read_text());secret=key['secret_key'];REDACT.append(secret)
    assert key['name']==NAME and re.fullmatch(r'[A-Za-z0-9+/]{32,40}',secret)
    mc(['pipe','trial/zpool/per-mac-object'],NAME,secret,data=payload)
    assert mc(['cat','trial/zpool/per-mac-object'],NAME,secret)[1]==payload
    denied(['rm','trial/zpool/per-mac-object'],NAME,secret)
    passed(STEP)
    STEP='keyd restart preserves delivered secret'
    keyd(1,True,needs=True)
    assert json.loads(delivery.read_text())['secret_key']==secret
    keyd(2,True)
    delivery.unlink()
    keyd(2,True)
    assert not delivery.exists()
    assert mc(['cat','trial/zpool/per-mac-object'],NAME,secret)[1]==payload
    passed(STEP)
    STEP='lost-secret recovery rotates without deleting objects'
    keyd(3,True,needs=True)
    replacement=json.loads(delivery.read_text())['secret_key'];REDACT.append(replacement)
    assert replacement!=secret
    # Same access-key name now has a different secret, so the old signature must fail.
    rc,out,err=mc(['--json','stat','trial/zpool/per-mac-object'],NAME,secret,expected=None)
    assert rc!=0 and re.search(r'SignatureDoesNotMatch|signature we calculated does not match',(out+err).decode(errors='replace'))
    assert mc(['cat','trial/zpool/per-mac-object'],NAME,replacement)[1]==payload
    passed(STEP)
    STEP='explicit revoke rejects credential and preserves content'
    keyd(4,True);delivery.unlink()
    applied=keyd(5,False,remove=[NAME])
    assert not any(k['name']==NAME and k.get('exists') for k in applied['keys'])
    denied(['stat','trial/zpool/per-mac-object'],NAME,replacement,revoked=True)
    assert mc(['cat','trial/zpool/per-mac-object'])[1]==payload
    passed(STEP)
    RESULT['status']='PASS'
except BaseException as exc:
    RESULT.update(status='FAIL',failed_step=STEP,error=scrub(f'{type(exc).__name__}: {exc}'))
finally:
    if server is not None:
        try:
            if server.poll() is None:os.killpg(server.pid,signal.SIGTERM)
            try:server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid,signal.SIGKILL);server.wait(timeout=5)
                RESULT['cleanup']['forced_after_timeout']=True
            RESULT['cleanup'].update(owned_server_joined=True,exit_code=server.returncode)
            try:os.killpg(server.pid,0)
            except ProcessLookupError:RESULT['cleanup']['process_group_absent']=True
            else:raise RuntimeError('owned process group still exists')
        except BaseException as exc:
            RESULT.update(status='FAIL',cleanup_error=scrub(str(exc)))
    if server_log:server_log.close()
    for name in ['minio','mc']:
        p=BIN/name
        if p.exists():RESULT.setdefault('binary_sha256',{})[name]=hashlib.sha256(p.read_bytes()).hexdigest()
    if RESULT['status']!='PASS' and (PRIVATE/'server.log').exists():
        RESULT['server_log_tail']=scrub((PRIVATE/'server.log').read_text(errors='replace')[-3000:])
    (EVIDENCE/'result.json').write_text(json.dumps(RESULT,indent=2)+'\n')
    print(json.dumps(RESULT,indent=2))
if RESULT['status']!='PASS':raise SystemExit(1)

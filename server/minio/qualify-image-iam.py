#!/usr/bin/env python3
"""Isolated cloud-only qualification of a maintained storage image and credential transport.

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
EVIDENCE.mkdir(mode=0o700, exist_ok=True)
POLICY = Path('server/minio').resolve(strict=True)
IMAGE = os.environ['STORAGE_TRIAL_IMAGE']
assert IMAGE == 'docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46'
MC = '/usr/bin/mc'
BASH = '/bin/bash'
CONTAINER_NAME = 'jm-storage-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT']
CONTAINER_LABEL = 'juicemount.storage-trial=' + secrets.token_hex(24)
CONTAINER_CREATE_ATTEMPTED = False
IMAGE_ID = None
CONTAINER = None
URL = 'http://127.0.0.1:19000'
CREDENTIALS = {name: ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') for _ in range(40)) for name in ['root', 'client', 'gc', 'keyadmin']}
CREDENTIALS['root'] = '+/:@%?&#=\"\\ ' + CREDENTIALS['root']
REDACT = list(CREDENTIALS.values())
ENV = {k: os.environ[k] for k in ['PATH', 'LANG'] if k in os.environ}
ENV.update(TMPDIR=str(PRIVATE), MC_CONFIG_DIR=str(PRIVATE / 'mc'))
RESULT = {'candidate': os.environ['CANDIDATE'], 'workflow_sha': os.environ['GITHUB_SHA'], 'scope': 'amd64 immutable image, production provisioning and keyd IAM; no migration/Manager/Mac acceptance', 'image': IMAGE, 'checks': [], 'cleanup': {}, 'status': 'RUNNING'}
STEP = 'setup'
server = None
server_log = None

def scrub(text):
    for secret in REDACT:
        for spelling in sorted({secret, json.dumps(secret)[1:-1], urllib.parse.quote(secret,safe='')},key=len,reverse=True):
            text = text.replace(spelling, '[REDACTED]')
    text = re.sub(r'https?://[^\s"\x27<>]*@', 'http://[REDACTED]@', text)
    return text

def call(args, *, env=None, data=None, expected=0, timeout=45):
    actual_env = env or ENV
    if args[0] in [MC, BASH]:
        assert CONTAINER is not None
        # docker exec inherits server container variables. Clear server-only
        # credentials unless this exact call explicitly supplies them.
        inherited = ['MINIO_ROOT_PASSWORD', 'MINIO_ROOT_USER', 'MINIO_BROWSER', 'MINIO_UPDATE']
        clear = [key for key in inherited if key not in actual_env]
        launch = ['/bin/bash', '-c', 'unset ' + ' '.join(clear) + '; exec \"$@\"', '--'] + args if clear else args
        args = ['docker', 'exec', '-i'] + [part for key in sorted(actual_env) if key not in ['PATH', 'LANG'] for part in ['--env', key]] + [CONTAINER] + launch
    child = subprocess.Popen(args, env=actual_env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
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
    assert re.fullmatch(r'[A-Za-z0-9_-]+', name)
    cfg = PRIVATE / ('mc-' + name)
    cfg.mkdir(mode=0o700, exist_ok=True)
    conf = cfg / 'config.json'
    conf.write_text(json.dumps({'version':'10', 'aliases':{'trial':{'url':URL, 'accessKey':name, 'secretKey':secret, 'api':'S3v4', 'path':'auto'}}}))
    conf.chmod(0o600)
    return dict(ENV, MC_CONFIG_DIR=str(cfg))

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
    STEP='exact image capability and identity'
    call(['docker','pull',IMAGE],timeout=180)
    _,out,_=call(['docker','image','inspect',IMAGE])
    inspected=json.loads(out)[0]
    assert inspected['Architecture']=='amd64' and inspected['Os']=='linux'
    assert inspected['Config']['Labels']['org.opencontainers.image.revision']=='2a4d51406b7ed87af5fe6fe0f801f3290f96eb3c'
    IMAGE_ID=inspected['Id']
    RESULT['image_id']=IMAGE_ID
    inherited_keys={entry.split('=',1)[0] for entry in inspected['Config'].get('Env',[])}
    assert not any(key.startswith('MC_HOST_') or key in {'MC_CONFIG_ENV_FILE','MINIO_ROOT_PASSWORD','MINIO_ROOT_USER','JM_OBJ_CLIENT_SECRET_KEY','JM_OBJ_GC_SECRET_KEY'} for key in inherited_keys), 'image has credential overrides'
    rc,out,err=call(['docker','inspect',CONTAINER_NAME],expected=None)
    assert rc!=0 and b'No such' in err, 'container name is not proven unused'
    CONTAINER_CREATE_ATTEMPTED=True
    # Detached server is bound to a unique name/label and owned runner directories.
    # No credentials appear on argv; --env NAME reads this process's environment.
    call(['docker','create','--name',CONTAINER_NAME,'--label',CONTAINER_LABEL,'--network','host',
          '--user',str(os.getuid())+':'+str(os.getgid()),
          '--mount','type=bind,src='+str(Path.cwd())+',dst='+str(Path.cwd())+',readonly',
          '--mount','type=bind,src='+str(PRIVATE)+',dst='+str(PRIVATE),
          '--env','MINIO_ROOT_USER','--env','MINIO_ROOT_PASSWORD','--env','MINIO_BROWSER','--env','MINIO_UPDATE',
          IMAGE,'server','--address','127.0.0.1:19000','--console-address','127.0.0.1:19001',str(data)],env=env)
    _,out,_=call(['docker','inspect',CONTAINER_NAME])
    obj=json.loads(out)[0]
    assert obj['Config']['Labels'].get('juicemount.storage-trial')==CONTAINER_LABEL.split('=',1)[1]
    assert obj['Image']==IMAGE_ID
    assert any(m.get('Source')==str(PRIVATE) and m.get('Destination')==str(PRIVATE) and m.get('RW') for m in obj['Mounts'])
    CONTAINER=obj['Id']
    call(['docker','start',CONTAINER])
    call([BASH,'-ec','for c in mc curl tr head mkdir chmod rm; do command -v "$c" >/dev/null; done; test -r /dev/urandom; mc --version; bash --version'])
    passed(STEP)
    STEP='owned server readiness'
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        _,state,_=call(['docker','inspect','--format','{{.State.Running}}',CONTAINER])
        assert state.strip()==b'true', 'owned storage container exited before readiness'
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
    STEP='old credential transport rejects punctuated password'
    old=PRIVATE/'old-provision-object-keys.sh'
    old.write_bytes(subprocess.check_output(['git','show',os.environ['CANDIDATE']+':server/minio/provision-object-keys.sh']))
    rc,out,err=call([BASH,str(old)],env=penv,expected=None)
    assert rc!=0 and re.search(r'SignatureDoesNotMatch|signature we calculated does not match|InvalidAccessKeyId|Access Key Id you provided does not exist',(out+err).decode(errors='replace')), 'old transport did not fail for authentication'
    mc(['--json','admin','info','trial'])
    passed(STEP)
    STEP='real provision and idempotent reprovision'
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
    # Recover the owned container even if create succeeded but its reply was lost.
    try:
        if not CONTAINER_CREATE_ATTEMPTED:
            RESULT['cleanup']['no_create_attempted']=True
            rc,out,err=1,b'',b'No such container: no create attempted'
        else:
            rc,out,err=call(['docker','inspect',CONTAINER or CONTAINER_NAME],expected=None)
        if rc==0:
            obj=json.loads(out)[0]
            assert obj['Config']['Labels'].get('juicemount.storage-trial')==CONTAINER_LABEL.split('=',1)[1], 'foreign container name collision'
            assert CONTAINER is None or obj['Id']==CONTAINER, 'original container ID changed'
            assert obj['Image']==IMAGE_ID, 'owned image identity changed'
            assert any(m.get('Source')==str(PRIVATE) and m.get('Destination')==str(PRIVATE) and m.get('RW') for m in obj['Mounts']), 'owned private bind changed'
            owned=obj['Id']
            if obj['State']['Running']:
                call(['docker','stop','--time','10',owned],timeout=20)
            _,out,_=call(['docker','inspect',owned]); stopped=json.loads(out)[0]
            assert not stopped['State']['Running']
            RESULT['cleanup'].update(owned_container_stopped=True,exit_code=stopped['State']['ExitCode'])
            if stopped['State']['ExitCode']!=0:
                RESULT.update(status='FAIL',cleanup_error='storage did not exit normally')
            call(['docker','rm',owned])
            rc,out,err=call(['docker','inspect',owned],expected=None)
            assert rc!=0 and b'No such' in err
            RESULT['cleanup']['owned_container_absent']=True
        else:
            assert b'No such' in err, 'container ownership check failed'
            RESULT['cleanup']['no_container_created']=True
    except BaseException as exc:
        RESULT.update(status='FAIL',cleanup_error=scrub(str(exc)))
    (EVIDENCE/'result.json').write_text(json.dumps(RESULT,indent=2)+'\n')
    print(json.dumps(RESULT,indent=2))
if RESULT['status']!='PASS':raise SystemExit(1)

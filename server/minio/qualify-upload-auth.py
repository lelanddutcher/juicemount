#!/usr/bin/env python3
"""Cloud-only pinned-source upload auth regression and three mutation controls."""

import collections
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

if sys.flags.optimize:
    raise RuntimeError('optimized Python disables qualification assertions')

BASE = '40208c76fd7175987f307b972cef156d785b4163'
URL = 'https://github.com/pgsty/silo.git'
REV = '2a4d51406b7ed87af5fe6fe0f801f3290f96eb3c'
TREE = '5d4ad13b3ff4f23b38d346b0b4a19fb8ed97b686'
MODULES = {
    'before': {'go.mod': '61cdae6906fa55c92028a898e32ff8f704e941c8a9b0f7c5e84688e2854055fb',
               'go.sum': 'da1b142985760aea3d8d1570df5f2133c4cc1fa5fea6221103d8e60c36dc3417'},
    'after': {'go.mod': '15678b8fbc78e839da7b24fecd4ac9c42ae0921f68d4b57612e09a1118b15613',
              'go.sum': '140d249124a1a2658ce80206229ea4c21fdddef4a0aa47452ea9411e5993f2ad'},
}
CONTROLS = {'cmd/streaming-v4-unsigned.go': 'f6f7a92188a8d6cae0ee1dbfd1f89a6b53e3f111db87c484f5abfd198ca20d60',
            'cmd/object-handlers.go': '61897a260f3f5f660f41edcb50956c60e914ef98f9a987da824f16d78171fde2'}
TEST = 'TestJuiceMountUnsignedTrailerAuth'
ROUTES = ('put', 'part', 'snowball')
MODES = ('valid_header', 'forged_header', 'valid_query', 'forged_query')
BACKENDS = {'ErasureSD', 'Erasure'}
assert os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('RUNNER_OS') == 'Linux'
runner = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
root = runner / ('jm-silo-auth-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT'])
root.mkdir(mode=0o700)
private, evidence = root/'private', root/'evidence'
private.mkdir(mode=0o700); evidence.mkdir(mode=0o700)
os.umask(0o077)
env = {key: os.environ[key] for key in ('PATH', 'LANG') if key in os.environ}
env.update(HOME=str(private), TMPDIR=str(private), GOTMPDIR=str(private), GOPATH=str(private/'go'),
           GOCACHE=str(private/'cache'), GOENV='off', GOWORK='off', GOTOOLCHAIN='local',
           CGO_ENABLED='0', GOOS='linux', GOARCH='amd64', GOPROXY='https://proxy.golang.org',
           GOSUMDB='sum.golang.org', GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null',
           GIT_TERMINAL_PROMPT='0')
checkout = private/'silo'
overlay = Path('server/minio/juicemount_upload_auth_test.go')
result = {'state': 'RUNNING', 'workflow_sha': os.environ['GITHUB_SHA'], 'base': BASE,
          'source': REV, 'tree': TREE, 'target': 'linux/amd64 CGO_ENABLED=0 Go1.27.2',
          'overlay': {}, 'module_hashes': {}, 'controls': {}, 'tests': {},
          'commands': [], 'started': time.time()}


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def combine_output(earlier, later):
    return later if later.startswith(earlier) else earlier + later


def run(argv, *, cwd=None, timeout=120, expected=0):
    number = len(result['commands']) + 1
    started = time.monotonic()
    child = None
    out = err = b''
    failure_type = None
    try:
        child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        try:
            out, err = child.communicate(timeout=timeout)
        except BaseException as error:
            failure_type = type(error).__name__
            if isinstance(error, subprocess.TimeoutExpired):
                out, err = error.output or b'', error.stderr or b''
            try:
                try: os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                try:
                    later_out, later_err = child.communicate(timeout=5)
                    out, err = combine_output(out, later_out), combine_output(err, later_err)
                except subprocess.TimeoutExpired as late:
                    out = combine_output(out, late.output or b'')
                    err = combine_output(err, late.stderr or b'')
                    try: os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    later_out, later_err = child.communicate(timeout=5)
                    out, err = combine_output(out, later_out), combine_output(err, later_err)
                child.wait(timeout=5)
            finally:
                if child.poll() is None:
                    try: os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    child.wait(timeout=5)
            raise
    except BaseException as error:
        failure_type = failure_type or type(error).__name__
        raise
    finally:
        stem = f'command-{number:02d}'
        (evidence/(stem+'-stdout.txt')).write_bytes(out)
        (evidence/(stem+'-stderr.txt')).write_bytes(err)
        result['commands'].append({'number': number, 'argv': argv, 'timeout_seconds': timeout,
                                   'duration_seconds': round(time.monotonic()-started, 3),
                                   'returncode': child.returncode if child else None,
                                   'joined': child is None or child.poll() is not None,
                                   'failure_type': failure_type,
                                   'stdout_sha256': hashlib.sha256(out).hexdigest(),
                                   'stderr_sha256': hashlib.sha256(err).hexdigest()})
    if expected is not None and child.returncode != expected:
        raise RuntimeError(Path(argv[0]).name + ' exited ' + str(child.returncode))
    return child.returncode, out, err


def git(*args):
    return run(['git', '-C', str(checkout), *args])[1].decode().strip()


def hashes():
    return {name: sha(checkout/name) for name in CONTROLS}


def parse_test(label, code, stdout, stderr, wanted, mutant=False):
    (evidence/(label+'.jsonl')).write_bytes(stdout)
    (evidence/(label+'-stderr.txt')).write_bytes(stderr)
    events = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    assert events and code == (1 if mutant else 0), label + ' process exit/JSON missing'
    package = [e for e in events if e.get('Action') == ('fail' if mutant else 'pass') and not e.get('Test')]
    assert len(package) == 1, label + ' package result missing/duplicated'
    root_result = [e for e in events if e.get('Test') == TEST and e.get('Action') in ('pass', 'fail', 'skip')]
    assert len(root_result) == 1 and root_result[0]['Action'] == ('fail' if mutant else 'pass'), label + ' root test result differs'
    leaves = {}
    instances = collections.defaultdict(set)
    for event in events:
        name = event.get('Test', '')
        if not name.startswith(TEST+'/'):
            continue
        bits = name.split('/')
        if len(bits) < 4 or not '/'.join(bits[1:-2]):
            continue
        route, mode = bits[-2:]
        assert route in ROUTES and mode in MODES, label + ' unexpected leaf name'
        backend = '/'.join(bits[1:-2])
        assert backend in BACKENDS, label + ' unexpected backend'
        instances[(route, mode)].add(backend)
        leaf = leaves.setdefault(name, {'route': route, 'mode': mode, 'terminal': [], 'output': []})
        if event.get('Action') in ('pass', 'fail', 'skip'):
            leaf['terminal'].append(event['Action'])
        if event.get('Action') == 'output':
            leaf['output'].append(event.get('Output', ''))
    count = collections.Counter()
    for name, leaf in leaves.items():
        route_mode = (leaf['route'], leaf['mode'])
        assert route_mode in wanted, label + ' ran an unselected leaf'
        assert leaf['terminal'] == (['fail'] if mutant else ['pass']), label + ' wrong leaf result'
        if mutant:
            output = ''.join(leaf['output'])
            assert 'JM_AUTH_UNEXPECTED_STATUS: got=200' in output, label + ' wrong failure witness'
            assert 'JM_AUTH_READBACK_CHANGED' in output, label + ' canonical readback witness absent'
            assert 'JM_AUTH_BYPASS_WITNESS: write=200 read=200 exact_attempted_bytes=true' in output, label + ' exact forged-byte bypass witness absent'
        count[route_mode] += 1
    assert set(count) == wanted and all(count[item] == 2 for item in wanted), label + ' leaf coverage differs'
    assert all(instances[item] == BACKENDS for item in wanted), label + ' backend coverage differs'
    assert not any(e.get('Action') == 'skip' and e.get('Test', '').startswith(TEST) for e in events), label + ' skipped test'
    assert all(e['Test'] == TEST or e['Test'] in leaves or any(name.startswith(e['Test']+'/') for name in leaves)
               for e in events if e.get('Action') == 'fail' and e.get('Test')), label + ' unrelated test failed'
    return {'exit': code, 'leaf_count': len(leaves), 'counts': {r+'/'+m: n for (r, m), n in sorted(count.items())},
            'json_sha256': hashlib.sha256(stdout).hexdigest()}


def go_test(label, selector, wanted, *, mutant=False, timeout=240):
    args = ['go', 'test', '-json', '-mod=readonly', '-count=1', '-timeout=90s', './cmd', '-run', selector]
    code, out, err = run(args, cwd=checkout, timeout=timeout, expected=None)
    result['tests'][label] = parse_test(label, code, out, err, wanted, mutant)
    (evidence/'result.json').write_text(json.dumps(result, indent=2)+'\n')


def mutation(label, relative, needle, wanted, selector):
    target = checkout/relative
    original = target.read_bytes()
    assert sha(target) == CONTROLS[relative], label + ' source preimage differs'
    assert original.count(needle) == 1, label + ' patch target not unique'
    edited = original.replace(needle, b'', 1)
    patch = ''.join(difflib.unified_diff(original.decode().splitlines(True), edited.decode().splitlines(True),
                                         fromfile=relative, tofile=relative+' (mutant)'))
    (evidence/(label+'.patch')).write_text(patch)
    result['controls'][label] = {'source': relative, 'original_sha256': sha(target),
                                 'mutant_sha256': hashlib.sha256(edited).hexdigest(),
                                 'patch_sha256': hashlib.sha256(patch.encode()).hexdigest()}
    try:
        target.write_bytes(edited)
        assert sha(target) == result['controls'][label]['mutant_sha256']
        go_test(label, selector, wanted, mutant=True)
    finally:
        target.write_bytes(original)
        assert sha(target) == CONTROLS[relative], label + ' source restoration failed'
        result['controls'][label]['restored_sha256'] = sha(target)


try:
    assert os.uname().machine == 'x86_64'
    assert run(['git', 'rev-parse', 'HEAD'])[1].decode().strip() == os.environ['GITHUB_SHA']
    assert run(['git', 'rev-parse', 'HEAD^'])[1].decode().strip() == BASE
    changed = set(run(['git', 'diff', '--name-only', 'HEAD^', 'HEAD'])[1].decode().splitlines())
    assert changed == {'.github/workflows/ci.yml', 'server/minio/qualify-upload-auth.py',
                       'server/minio/juicemount_upload_auth_test.go'}
    assert run(['go', 'version'])[1].decode().startswith('go version go1.27.2 linux/amd64')
    overlay_bytes = overlay.read_bytes()
    result['overlay'] = {'source_path': str(overlay), 'sha256': hashlib.sha256(overlay_bytes).hexdigest(),
                         'bytes': len(overlay_bytes), 'workflow_sha': os.environ['GITHUB_SHA']}
    checkout.mkdir(mode=0o700)
    run(['git', 'init', str(checkout)])
    run(['git', '-C', str(checkout), 'remote', 'add', 'origin', URL])
    run(['git', '-C', str(checkout), '-c', 'credential.helper=', 'fetch', '--depth=1', 'origin', REV], timeout=180)
    run(['git', '-C', str(checkout), 'checkout', '--detach', REV], timeout=180)
    assert git('rev-parse', 'HEAD') == REV and git('rev-parse', 'HEAD^{tree}') == TREE
    assert git('remote', 'get-url', 'origin') == URL and not git('status', '--porcelain')
    assert {name: sha(checkout/name) for name in CONTROLS} == CONTROLS, 'guard source hashes differ'
    assert {name: sha(checkout/name) for name in MODULES['before']} == MODULES['before']
    original_mod = (checkout/'go.mod').read_bytes()
    assert original_mod.count(b'go 1.27.1\n') == 1 and original_mod.count(b'golang.org/x/net v0.59.0') == 1
    run(['go', 'mod', 'edit', '-go=1.27.2'], cwd=checkout)
    run(['go', 'get', 'golang.org/x/net@v0.60.0'], cwd=checkout, timeout=180)
    (evidence/'module.patch').write_bytes(run(['git', '-C', str(checkout), 'diff', '--', 'go.mod', 'go.sum'])[1])
    expected_mod = original_mod.replace(b'go 1.27.1\n', b'go 1.27.2\n', 1).replace(
        b'golang.org/x/net v0.59.0', b'golang.org/x/net v0.60.0', 1)
    assert (checkout/'go.mod').read_bytes() == expected_mod
    assert {name: sha(checkout/name) for name in MODULES['after']} == MODULES['after']
    assert set(git('diff', '--name-only').splitlines()) == {'go.mod', 'go.sum'}
    result['module_hashes'] = MODULES
    test_copy = checkout/'cmd/juicemount_upload_auth_test.go'
    assert not os.path.lexists(test_copy)
    with test_copy.open('xb') as stream: stream.write(overlay_bytes)
    assert sha(test_copy) == result['overlay']['sha256']
    wanted = {(route, mode) for route in ROUTES for mode in MODES}
    go_test('normal', '^'+TEST+'$', wanted, timeout=600)
    unsigned = checkout/'cmd/streaming-v4-unsigned.go'
    mutation('presigned_guard', 'cmd/streaming-v4-unsigned.go',
             b'\tif isRequestPresignedSignatureV4(req) {\n\t\treturn nil, ErrSignatureVersionNotSupported\n\t}\n',
             {(route, 'forged_query') for route in ROUTES}, '^'+TEST+'$/.*/(put|part|snowball)/forged_query$')
    mutation('header_guard', 'cmd/streaming-v4-unsigned.go',
             b'\tif isRequestSignatureV4(req) {\n\t\tif errCode := reqSignatureV4Verify(req, globalSite.Region(), serviceS3); errCode != ErrNone {\n\t\t\treturn nil, errCode\n\t\t}\n\t}\n',
             {(route, 'forged_header') for route in ('put', 'part')}, '^'+TEST+'$/.*/(put|part)/forged_header$')
    handler = checkout/'cmd/object-handlers.go'
    source = handler.read_bytes()
    start = source.index(b'func (api objectAPIHandlers) PutObjectExtractHandler(')
    end = source.find(b'\nfunc ', start+1)
    scope = source[start:end if end >= 0 else len(source)]
    case = (b'\tcase authTypeStreamingUnsignedTrailer:\n'
            b'\t\t// Initialize stream chunked reader with optional trailers.\n'
            b'\t\treader, s3Err = newUnsignedV4ChunkedReader(r, true)\n'
            b'\t\tif s3Err != ErrNone {\n'
            b'\t\t\twriteErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)\n'
            b'\t\t\treturn\n'
            b'\t\t}\n')
    assert scope.count(case) == 1 and source.count(case) == 1, 'extract guard target ambiguous'
    mutation('snowball_guard', 'cmd/object-handlers.go', case,
             {('snowball', 'forged_header')}, '^'+TEST+'$/.*/snowball/forged_header$')
    assert hashes() == CONTROLS and {name: sha(checkout/name) for name in MODULES['after']} == MODULES['after']
    assert git('rev-parse', 'HEAD') == REV and git('rev-parse', 'HEAD^{tree}') == TREE
    assert set(git('diff', '--name-only').splitlines()) == {'go.mod', 'go.sum'}
    assert git('ls-files', '--others', '--exclude-standard') == 'cmd/juicemount_upload_auth_test.go'
    result.update(state='PASS', finished=time.time())
except BaseException as error:
    result.update(state='RED', finished=time.time(), error_type=type(error).__name__,
                  failed_check=str(error)[:160] if isinstance(error, AssertionError) else '')
finally:
    if checkout.is_dir() and (checkout/'.git').is_dir():
        try:
            post = {'head': git('rev-parse', 'HEAD'), 'tree': git('rev-parse', 'HEAD^{tree}'),
                    'control_sha256': hashes(),
                    'module_sha256': {name: sha(checkout/name) for name in MODULES['after']},
                    'tracked_changes': sorted(git('diff', '--name-only').splitlines()),
                    'untracked': git('ls-files', '--others', '--exclude-standard').splitlines()}
            result['postflight'] = post
            if post['head'] != REV or post['tree'] != TREE or post['control_sha256'] != CONTROLS or \
               post['module_sha256'] != MODULES['after'] or post['tracked_changes'] != ['go.mod', 'go.sum'] or \
               post['untracked'] != ['cmd/juicemount_upload_auth_test.go']:
                result.update(state='RED', postflight_error='source/module/overlay custody mismatch')
        except BaseException as error:
            result.update(state='RED', postflight_error=type(error).__name__)
    (evidence/'hashes.json').write_text(json.dumps({'overlay': result['overlay'], 'modules': result['module_hashes'],
                                                   'controls': result['controls'],
                                                   'postflight': result.get('postflight')}, indent=2)+'\n')
    (evidence/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({'state': result['state'], 'tests': sorted(result['tests']),
                      'controls': sorted(result['controls'])}), flush=True)
if result['state'] != 'PASS':
    raise SystemExit(1)

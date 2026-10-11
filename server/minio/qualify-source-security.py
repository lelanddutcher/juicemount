#!/usr/bin/env python3
"""Cloud-only source/binary security diagnostic; never runs either product binary.

The only candidate edits are go.mod/go.sum in fresh pinned upstream checkouts.
This does not produce or select a production image. Evidence excludes binaries.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess


BASE = '40208c76fd7175987f307b972cef156d785b4163'
SOURCES = {
    'mcli': ('https://github.com/pgsty/mc.git', 'e952aa78f10a2b77dd525a2b7e3143bcda0cd377', 'github.com/minio/mc'),
    'silo': ('https://github.com/pgsty/silo.git', '2a4d51406b7ed87af5fe6fe0f801f3290f96eb3c', 'github.com/minio/minio'),
}
SCANNER = 'golang.org/x/vuln/cmd/govulncheck@v1.8.0'
GO_VERSION = 'go1.27.2'
X_NET = 'v0.60.0'

assert os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('RUNNER_OS') == 'Linux', 'GitHub Linux only'
runner_temp = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
root = runner_temp / ('jm-source-security-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT'])
root.mkdir(mode=0o700)  # Exclusive: no adoption of pre-existing trial state.
evidence = root / 'evidence'
private = root / 'private'
tools = private / 'tools'
for path in (evidence, private, tools):
    path.mkdir(mode=0o700)
os.umask(0o077)
env = {k: os.environ[k] for k in ('PATH', 'LANG') if k in os.environ}
env.update(HOME=str(private), XDG_CONFIG_HOME=str(private), TMPDIR=str(private),
           GOPATH=str(private / 'go'), GOCACHE=str(private / 'go-cache'), GOBIN=str(tools),
           GOTOOLCHAIN='local', GOWORK='off', GOENV='off', CGO_ENABLED='0', GOOS='linux', GOARCH='amd64', GOPROXY='https://proxy.golang.org', GOSUMDB='sum.golang.org',
           GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null', GIT_TERMINAL_PROMPT='0')
result = {'status': 'RUNNING', 'workflow_sha': os.environ['GITHUB_SHA'], 'base': BASE,
          'go_version_required': GO_VERSION, 'x_net_required': X_NET,
          'scanner': SCANNER, 'target': {'GOOS': 'linux', 'GOARCH': 'amd64', 'CGO_ENABLED': '0', 'GOWORK': 'off'},
          'scope': 'pinned source root package and built binary; diagnostic only, no image or credentials',
          'sources': {}, 'scanner_findings': {}}


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def run(args, *, cwd=None, timeout=120, expected=0):
    child = subprocess.Popen(args, cwd=cwd, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, start_new_session=True)
    try:
        out, err = child.communicate(timeout=timeout)
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
        raise RuntimeError(Path(args[0]).name + ' exited ' + str(child.returncode) + ': ' +
                           err.decode(errors='replace')[:1200])
    return child.returncode, out, err


def git(path, *args):
    return run(['git', '-C', str(path), *args])[1].decode().strip()


def json_stream(raw):
    text = raw.decode()
    decoder = json.JSONDecoder()
    messages = []
    while text.strip():
        text = text.lstrip()
        message, used = decoder.raw_decode(text)
        messages.append(message)
        text = text[used:]
    assert messages and isinstance(messages[0].get('config'), dict), 'scanner JSON config missing'
    return messages


def package_graph(checkout):
    root_package = run(['go', 'list', '-mod=readonly', '-f', '{{.ImportPath}}', '.'], cwd=checkout, timeout=180)[1].decode().strip()
    objects = json_stream_packages(run(['go', 'list', '-mod=readonly', '-deps', '-json', '.'], cwd=checkout, timeout=180)[1])
    packages = {p['ImportPath']: p for p in objects}
    assert root_package in packages
    from collections import deque
    pending = deque([(root_package, [root_package])])
    visited = set()
    chains = []
    while pending:
        name, path = pending.popleft()
        if name in visited:
            continue
        visited.add(name)
        if name == 'golang.org/x/crypto/openpgp' or name.startswith('golang.org/x/crypto/openpgp/'):
            module = packages[name].get('Module') or {}
            chains.append({'path': path, 'module': {'path': module.get('Path'), 'version': module.get('Version')}})
        for imported in packages[name].get('Imports', []):
            if imported in packages and imported not in visited:
                pending.append((imported, path + [imported]))
    return {'root_package': root_package, 'package_count': len(packages), 'openpgp_import_chains': chains}


def json_stream_packages(raw):
    text = raw.decode()
    decoder = json.JSONDecoder()
    packages = []
    while text.strip():
        text = text.lstrip()
        package, used = decoder.raw_decode(text)
        packages.append(package)
        text = text[used:]
    return packages


def scan(kind, name, checkout_or_binary, expected_root):
    assert kind in ('source', 'binary')
    args = [str(tools / 'govulncheck'), '-mode=' + kind, '-format=json']
    if kind == 'source':
        args.append('.')
        cwd = checkout_or_binary
    else:
        args.append(str(checkout_or_binary))
        cwd = None
    code, out, err = run(args, cwd=cwd, timeout=300, expected=None)
    stem = name + '-' + kind + '-govulncheck'
    (evidence / (stem + '.jsonl')).write_bytes(out)
    (evidence / (stem + '.stderr')).write_bytes(err)
    assert code == 0, name + ' ' + kind + ' scanner failed or did not complete'
    stream = json_stream(out)
    assert stream[0]['config'].get('scan_mode') == kind and stream[0]['config'].get('scan_level') == 'symbol'
    sboms = [m['SBOM'] for m in stream if 'SBOM' in m]
    assert len(sboms) == 1, kind + ' complete SBOM absent or duplicated'
    sbom = sboms[0]
    assert sbom.get('go_version') == GO_VERSION and sbom.get('modules'), kind + ' SBOM toolchain/modules mismatch'
    assert sbom.get('roots') == [expected_root], kind + ' SBOM root mismatch'
    findings = [m['finding'] for m in stream if 'finding' in m]
    ids = sorted({f['osv'] for f in findings})
    symbol_ids = sorted({f['osv'] for f in findings if any(t.get('function') for t in f.get('trace', []))})
    return {'finding_messages': len(findings), 'ids': ids, 'symbol_trace_ids': symbol_ids,
            'without_symbol_trace_ids': sorted(set(ids) - set(symbol_ids)),
            'sbom': sbom, 'scanner_exit': code,
            'note': 'Symbol traces can include synthesized wildcard frames; IDs are not proof of reachable vulnerable functions.'}


try:
    assert os.uname().machine == 'x86_64', 'expected linux/amd64'
    assert run(['git', 'rev-parse', 'HEAD'])[1].decode().strip() == os.environ['GITHUB_SHA']
    assert run(['git', 'rev-parse', 'HEAD^'])[1].decode().strip() == BASE
    changed = set(run(['git', 'diff', '--name-only', 'HEAD^', 'HEAD'])[1].decode().splitlines())
    assert changed == {'.github/workflows/ci.yml', 'server/minio/qualify-source-security.py'}, 'diagnostic commit changed other paths'
    assert run(['go', 'version'])[1].decode().startswith('go version ' + GO_VERSION + ' linux/amd64'), 'wrong Go toolchain'
    run(['go', 'install', SCANNER], timeout=300)
    scanner = tools / 'govulncheck'
    assert scanner.is_file()
    result['scanner_sha256'] = sha(scanner)
    (evidence / 'scanner-build.txt').write_bytes(run(['go', 'version', '-m', str(scanner)])[1])
    for name, (url, revision, module_path) in SOURCES.items():
        checkout = private / name
        checkout.mkdir(mode=0o700)
        run(['git', 'init', str(checkout)])
        run(['git', '-C', str(checkout), 'remote', 'add', 'origin', url])
        run(['git', '-C', str(checkout), '-c', 'credential.helper=', 'fetch', '--depth=1', 'origin', revision], timeout=180)
        run(['git', '-C', str(checkout), 'checkout', '--detach', revision], timeout=180)
        assert git(checkout, 'rev-parse', 'HEAD') == revision, name + ' source revision changed'
        assert git(checkout, 'remote', 'get-url', 'origin') == url, name + ' source URL changed'
        assert not git(checkout, 'status', '--porcelain'), name + ' source checkout dirty'
        source_tree = git(checkout, 'rev-parse', 'HEAD^{tree}')
        original_mod_bytes = (checkout / 'go.mod').read_bytes()
        before = {'go.mod': sha(checkout / 'go.mod'), 'go.sum': sha(checkout / 'go.sum')}
        original_mod = run(['go', 'mod', 'edit', '-json'], cwd=checkout)[1]
        assert json.loads(original_mod)['Module']['Path'] == module_path, name + ' module path changed'
        assert json.loads(original_mod)['Go'] == '1.27.1', name + ' original Go directive changed'
        run(['go', 'mod', 'edit', '-go=1.27.2'], cwd=checkout)
        run(['go', 'get', 'golang.org/x/net@' + X_NET], cwd=checkout, timeout=180)
        changed_files = set(git(checkout, 'diff', '--name-only').splitlines())
        patch = run(['git', '-C', str(checkout), 'diff', '--', 'go.mod', 'go.sum'])[1]
        (evidence / (name + '-module.patch')).write_bytes(patch)
        for filename in ('go.mod', 'go.sum'):
            shutil.copy2(checkout / filename, evidence / (name + '-' + filename))
        after = {'go.mod': sha(checkout / 'go.mod'), 'go.sum': sha(checkout / 'go.sum')}
        assert original_mod_bytes.count(b'go 1.27.1\n') == 1, name + ' original Go directive is not unique'
        assert original_mod_bytes.count(b'golang.org/x/net v0.59.0') == 1, name + ' original x/net pin is not unique'
        expected_mod = original_mod_bytes.replace(b'go 1.27.1\n', b'go 1.27.2\n', 1)
        expected_mod = expected_mod.replace(b'golang.org/x/net v0.59.0', b'golang.org/x/net v0.60.0', 1)
        assert (checkout / 'go.mod').read_bytes() == expected_mod, name + ' go.mod changed beyond the two reviewed lines'
        assert changed_files <= {'go.mod', 'go.sum'} and 'go.mod' in changed_files, name + ' unexpected source edit'
        assert not git(checkout, 'ls-files', '--others', '--exclude-standard'), name + ' untracked source files'
        modified_mod = run(['go', 'mod', 'edit', '-json'], cwd=checkout)[1]
        edited = json.loads(modified_mod)
        assert edited['Go'] == '1.27.2', name + ' Go directive changed unexpectedly'
        toolchain = edited.get('Toolchain')
        assert toolchain in (None, GO_VERSION), name + ' toolchain directive requests a different Go version'
        selected = json.loads(run(['go', 'list', '-mod=readonly', '-m', '-json', 'golang.org/x/net'], cwd=checkout)[1])
        assert selected['Version'] == X_NET, name + ' x/net selection changed unexpectedly'
        (evidence / (name + '-modules.json')).write_bytes(run(['go', 'list', '-mod=readonly', '-m', '-json', 'all'], cwd=checkout, timeout=180)[1])
        run(['go', 'mod', 'verify'], cwd=checkout, timeout=180)
        graph = package_graph(checkout)
        result['sources'][name] = {'url': url, 'revision': revision, 'tree': source_tree,
                                   'original_hashes': before, 'modified_hashes': after,
                                   'modified_files': sorted(changed_files), 'go_directive': edited['Go'],
                                   'toolchain_directive': toolchain, 'selected_x_net': selected['Version'],
                                   'dependency_graph': graph}
        binary = private / (name + '.bin')
        run(['go', 'build', '-mod=readonly', '-trimpath', '-buildvcs=true', '-o', str(binary), '.'], cwd=checkout, timeout=300)
        assert binary.is_file() and binary.stat().st_size > 0
        buildinfo = run(['go', 'version', '-m', str(binary)])[1]
        lines = buildinfo.decode().splitlines()
        assert re.fullmatch(r'.+: ' + re.escape(GO_VERSION), lines[0]), name + ' binary Go version changed'
        net_deps = [line for line in lines if line.startswith('\tdep\tgolang.org/x/net\t')]
        assert len(net_deps) == 1 and net_deps[0].split('\t')[3] == X_NET, name + ' binary x/net dependency differs'
        assert '\tbuild\tvcs.revision=' + revision in lines, name + ' binary source revision differs'
        assert '\tbuild\tvcs.modified=true' in lines, name + ' binary must identify the module patch'
        assert '\tbuild\tCGO_ENABLED=0' in lines and '\tbuild\tGOOS=linux' in lines and '\tbuild\tGOARCH=amd64' in lines
        (evidence / (name + '-build.txt')).write_bytes(buildinfo)
        result['sources'][name]['binary_sha256'] = sha(binary)
        result['sources'][name]['binary_bytes'] = binary.stat().st_size
        result['scanner_findings'][name] = {'source': scan('source', name, checkout, module_path),
                                            'binary': scan('binary', name, binary, module_path)}
        assert git(checkout, 'rev-parse', 'HEAD') == revision and git(checkout, 'rev-parse', 'HEAD^{tree}') == source_tree
        assert set(git(checkout, 'diff', '--name-only').splitlines()) == changed_files, name + ' build or scan changed tracked source'
        assert {'go.mod': sha(checkout / 'go.mod'), 'go.sum': sha(checkout / 'go.sum')} == after, name + ' module files changed after capture'
    result['status'] = ('HOLD_FINDINGS' if any(mode['ids'] for source in result['scanner_findings'].values()
                                                 for mode in source.values()) else 'PASS_DIAGNOSTIC_SCANS')
except BaseException as exc:
    result.update(status='FAIL', error=f'{type(exc).__name__}: {exc}')
finally:
    (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2), flush=True)
if result['status'] != 'PASS_DIAGNOSTIC_SCANS':
    raise SystemExit(1)

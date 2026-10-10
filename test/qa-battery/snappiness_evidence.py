"""Category09 evidence validation. No network calls; listing is explicit only.

The measured operation is directory enumeration, never Finder rendering.
"""
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import sys
import time


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def integer(value):
    require(type(value) is int and value >= 0, 'missing/invalid nonnegative counter')
    return value


def manifest(text, expected):
    require(type(expected) is int and expected > 0, 'nonpositive expected count')
    rows = text.splitlines()
    require(len(rows) == expected, 'manifest count differs')
    names = []
    for row in rows:
        fields = row.split('\t')
        require(len(fields) == 2, 'malformed manifest')
        path, digest = fields
        p = PurePosixPath(path)
        require(p.as_posix() == path and len(p.parts) == 2 and p.parts[0] == 'dir1'
                and p.name not in ('.', '..') and not p.name.startswith('.')
                and re.fullmatch('[0-9a-f]{32}', digest), 'invalid manifest entry')
        names.append(p.name)
    require(len(set(names)) == expected, 'duplicate manifest entry')
    return names


def rows(text, sizes):
    lines = text.splitlines()
    require(len(lines) == len(sizes) and len(lines) > 0, 'directory row count differs')
    result = []
    for line, size in zip(lines, sizes):
        fields = line.split('\t')
        require(len(fields) == 4 and all(fields), 'malformed directory TSV')
        n, leaf, man, dest = fields
        require(n == str(size) and size > 0, 'directory size differs')
        require(all(Path(p).is_absolute() for p in (leaf, man, dest))
                and leaf == dest + '/dir1', 'invalid directory binding')
        manifest(Path(man).read_text(), size)
        result.append(fields)
    require(len({r[1] for r in result}) == len(result), 'duplicate destination')
    return result


def listing_names(names, expected):
    require(isinstance(expected, list) and len(expected) == len(set(expected)) > 0, 'invalid expected names')
    require(isinstance(names, list) and all(type(n) is str and n and '/' not in n
            and n not in ('.', '..') for n in names), 'invalid enumeration')
    require(len(names) == len(set(names)), 'duplicate returned entry')
    primary = [n for n in names if n != '.DS_Store' and not n.startswith('._')]
    extras = [n for n in names if n not in primary]
    require(set(primary) == set(expected) and len(primary) == len(expected) > 0,
            'enumerated primary names/count differ')
    return {'primary_count': len(primary), 'total_count': len(names), 'extras': sorted(extras)}


COUNTERS = ('rpc_total', 'rpc_errors', 'read_retries', 'read_fails',
            'readdir_mirror_hit', 'readdir_empty_refill', 'readdir_cold_shed',
            'readdir_fs_readdir', 'readdir_verifier_hit')
RPCS = ('READDIR', 'READDIRPLUS', 'LOOKUP', 'GETATTR', 'READ', 'WRITE')


def metric_values(doc):
    require(type(doc) is dict, 'invalid metrics object')
    values = {key: integer(doc.get(key)) for key in COUNTERS}
    values['uptime_sec'] = integer(doc.get('uptime_sec'))
    require(type(doc.get('rpcs')) is dict, 'missing RPC schema')
    for name in RPCS:
        require(type(doc['rpcs'].get(name)) is dict, 'missing RPC cell')
        values[name] = integer(doc['rpcs'][name].get('count'))
    return values


def metrics(before, after):
    b, a = metric_values(before), metric_values(after)
    require(all(a[k] >= b[k] for k in b), 'metrics reset/nonmonotonic')
    delta = {k: a[k] - b[k] for k in b}
    return {'delta': delta, 'coverage': 'server-activity-in-bracket' if
            delta['READDIR'] + delta['READDIRPLUS'] > 0 else
            'client-cache-only-server-path-unproven'}


def offline(reply, state, wanted):
    require(type(reply) is dict and reply.get('ok') is True
            and type(reply.get('offline_mode')) is bool
            and reply['offline_mode'] is wanted, 'offline toggle not acknowledged')
    require(type(state) is dict and type(state.get('offline')) is bool
            and state['offline'] is wanted, 'effective offline state differs')


def spool(doc):
    require(type(doc) is dict and doc.get('enabled') is True, 'spool unavailable')
    return {k: integer(doc.get(k)) for k in
            ('pending_files', 'in_progress', 'succeeded', 'failed_files', 'capacity_used')}


def overlap(before, after):
    b, a = spool(before), spool(after)
    require(b['in_progress'] > 0 and a['in_progress'] > 0 and a['succeeded'] > b['succeeded'],
            'no observed active drain progress across enumeration bracket')
    require(b['failed_files'] == a['failed_files'] == 0, 'actionable drain failures')
    return {'succeeded_delta': a['succeeded'] - b['succeeded'],
            'scope': 'active at both bracket boundaries with progress across bracket; not continuous or exclusive overlap'}


def load(path):
    return json.loads(Path(path).read_text())


def main(args):
    mode = args[0]
    if mode == 'manifest':
        print(json.dumps(manifest(Path(args[1]).read_text(), int(args[2]))))
    elif mode == 'top':
        lines = Path(args[1]).read_text().splitlines()
        require(lines and all(len(line.split('\t')) == 4 for line in lines), 'invalid top rows')
        print(json.dumps([Path(line.split('\t')[3]).name for line in lines]))
    elif mode == 'rows':
        rows(Path(args[1]).read_text(), [int(n) for n in args[2:]])
    elif mode == 'offline':
        offline(load(args[1]), load(args[2]), args[3] == 'on')
    elif mode == 'spool':
        value = spool(load(args[1]))
        require(value['in_progress'] > 0, 'drain not active')
    elif mode == 'settled':
        value = spool(load(args[1]))
        require(all(value[k] == 0 for k in ('pending_files', 'in_progress', 'failed_files', 'capacity_used')),
                'spool not settled')
    elif mode == 'list':
        # The original child is synchronously waited by its category owner.
        # A failed/blocked observer does not authorize Finder cleanup.
        def expired(_sig, _frame):
            raise TimeoutError('enumeration exceeded 30 seconds')
        signal.signal(signal.SIGALRM, expired)
        signal.alarm(30)
        start = time.monotonic_ns()
        names = os.listdir(args[1])
        end = time.monotonic_ns()
        signal.alarm(0)
        print(json.dumps({'names': names, 'elapsed_ns': end - start,
                          'scope': 'opendir/readdir/closedir; no per-entry stat or Finder paint'}))
    elif mode == 'interval':
        prefix, expected_path, budget, phase = args[1:]
        observed = load(prefix + '.listing.json')
        found = listing_names(observed.get('names'), load(expected_path))
        ns = integer(observed.get('elapsed_ns'))
        result = dict(found, elapsed_ns=ns, phase=phase,
                      metrics=metrics(load(prefix + '.before.json'), load(prefix + '.after.json')))
        if phase == 'under-drain':
            result['drain'] = overlap(load(prefix + '.spool-before.json'), load(prefix + '.spool-after.json'))
        result['within_budget'] = ns < int(budget) * 1000000
        result['counter_errors'] = any(result['metrics']['delta'][k] for k in
                                      ('rpc_errors', 'read_retries', 'read_fails'))
        Path(prefix + '.result.json').write_text(json.dumps(result, indent=2) + '\n')
        # Emit latency only after names/schema/overlap validity is established.
        print('%s\t%.3f\t%s' % (found['primary_count'], ns / 1000000,
                                result['metrics']['coverage']))
        require(result['within_budget'] and not result['counter_errors'], 'latency or RPC error gate failed')
    else:
        raise ValueError('unknown evidence mode')


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (ValueError, KeyError, TypeError, OSError, TimeoutError) as error:
        print('INVALID: ' + str(error), file=sys.stderr)
        sys.exit(1)

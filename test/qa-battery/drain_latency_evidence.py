"""Category11 paired syscall evidence; no Finder, network or implicit main calls."""
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import sys
import time

_spec = importlib.util.spec_from_file_location('snappiness_evidence', Path(__file__).with_name('snappiness_evidence.py'))
shared = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(shared)
require = shared.require
integer = shared.integer


def manifest(text, dirs, files):
    require(type(dirs) is int and dirs > 0 and type(files) is int and files > 0, 'invalid manifest dimensions')
    expected = {f'dir{d}/file{f}.dat' for d in range(1, dirs+1) for f in range(1, files+1)}
    seen = set()
    for line in text.splitlines():
        fields = line.split('\t')
        require(len(fields) == 2 and fields[0] in expected and fields[0] not in seen
                and re.fullmatch('[0-9a-f]{32}', fields[1]), 'invalid/duplicate manifest row')
        seen.add(fields[0])
    require(seen == expected, 'empty/incomplete manifest')
    return sorted(expected)


def settled(doc):
    s = shared.spool(doc)
    require(s['failed_files'] == 0, 'spool failures')
    return all(s[k] == 0 for k in ('pending_files', 'in_progress', 'capacity_used'))


def observe(kind, path):
    result = {'kind': kind, 'ok': False}
    def expired(_signum, _frame):
        raise TimeoutError('syscall observer exceeded 30 seconds')
    signal.signal(signal.SIGALRM, expired)
    signal.alarm(30)
    start = time.monotonic_ns()
    try:
        if kind == 'list':
            result['names'] = os.listdir(path)
        elif kind == 'stat':
            value = os.stat(path, follow_symlinks=False)
            result['regular'] = stat.S_ISREG(value.st_mode)
            result['size'] = value.st_size
        else:
            raise ValueError('unknown observer kind')
        result['ok'] = True
    except (OSError, TimeoutError, ValueError) as error:
        result['error'] = str(error)
    finally:
        result['elapsed_ns'] = time.monotonic_ns() - start
        signal.alarm(0)
    return result


def load(path):
    return json.loads(Path(path).read_text())


def pair(prefix, expected, size):
    listing, value = load(prefix+'.list.json'), load(prefix+'.stat.json')
    require(listing.get('ok') is True and listing.get('kind') == 'list', 'enumeration failed')
    require(value.get('ok') is True and value.get('kind') == 'stat', 'stat failed')
    found = shared.listing_names(listing.get('names'), [Path(n).name for n in expected])
    require(value.get('regular') is True and type(value.get('size')) is int and value['size'] == size,
            'stat target is not expected regular file/size')
    b, a = shared.spool(load(prefix+'.spool-before.json')), shared.spool(load(prefix+'.spool-after.json'))
    require(a['succeeded'] >= b['succeeded'] and b['failed_files'] == a['failed_files'] == 0,
            'spool reset/failure')
    listing_metrics = shared.metrics(load(prefix+'.metrics-before.json'), load(prefix+'.metrics-middle.json'))
    stat_metrics = shared.metrics(load(prefix+'.metrics-middle.json'), load(prefix+'.metrics-after.json'))
    for observed in (listing_metrics, stat_metrics):
        require(not any(observed['delta'][k] for k in ('rpc_errors','read_retries','read_fails')), 'RPC error during pair')
    # Global RPC increments describe brackets, not attribution to these syscalls.
    stat_metrics['coverage'] = ('server-activity-in-bracket' if stat_metrics['delta']['GETATTR'] > 0
                                else 'client-cache-only-server-path-unproven')
    return {'listing': found, 'dir_ns': integer(listing.get('elapsed_ns')),
            'stat_ns': integer(value.get('elapsed_ns')), 'active': b['in_progress'] > 0 and a['in_progress'] > 0,
            'spool_before': b, 'spool_after': a,
            'listing_metrics': listing_metrics, 'stat_metrics': stat_metrics,
            'scope': 'active at both acquisition boundaries; not continuous or exclusive overlap'}


def percentile(values, fraction):
    require(bool(values), 'no valid samples')
    return sorted(values)[max(0, math.ceil(len(values)*fraction)-1)]


def summarize(samples, minimum, budget_ms, hard_ms, complete):
    require(complete is True, 'measurement window incomplete or capped')
    require(type(minimum) is int and minimum >= 40, 'active pair floor below40')
    require(bool(samples), 'no valid pairs')
    active = [s for s in samples if s['active'] is True]
    require(len(active) >= minimum, 'insufficient active pairs; no post-drain padding')
    previous = None
    for sample in samples:
        for boundary in ('spool_before','spool_after'):
            now = sample[boundary]['succeeded']
            require(previous is None or now >= previous, 'spool counter reset across pairs')
            previous = now
    require(active[-1]['spool_after']['succeeded'] > active[0]['spool_before']['succeeded'],
            'no succeeded progress across covered window')
    result = {'pairs':len(samples), 'active_pairs':len(active), 'scopes':{}}
    for label, subset in (('whole-window',samples),('active-only',active)):
        result['scopes'][label] = {}
        for op, key in (('dir-open','dir_ns'),('stat','stat_ns')):
            values=[integer(s[key]) for s in subset]
            result['scopes'][label][op]={'samples':len(values),'p50_ns':percentile(values,.5),
                                         'p99_ns':percentile(values,.99),'worst_ns':max(values)}
    result['within_budget'] = all(v['p99_ns'] < budget_ms*1000000 and v['worst_ns'] < hard_ms*1000000
                                 for scope in result['scopes'].values() for v in scope.values())
    return result


def main(args):
    mode = args[0]
    if mode == 'manifest':
        print(json.dumps(manifest(Path(args[1]).read_text(), int(args[2]), int(args[3]))))
    elif mode == 'settled':
        require(settled(load(args[1])), 'spool not settled')
    elif mode == 'spool':
        value = shared.spool(load(args[1]))
        require(value['failed_files'] == 0, 'actionable spool failure')
        if len(args) > 2 and args[2]:
            previous = shared.spool(load(args[2]))
            require(value['succeeded'] >= previous['succeeded'], 'spool reset between observations')
    elif mode == 'observe':
        result = observe(args[1], args[2]); print(json.dumps(result))
        require(result['ok'], 'observer failed; original evidence retained')
    elif mode == 'pair':
        result = pair(args[1], load(args[2]), int(args[3]))
        Path(args[1]+'.result.json').write_text(json.dumps(result,indent=2)+'\n')
    elif mode == 'summary':
        directory = Path(args[1]); lines=(directory/'pairs.tsv').read_text().splitlines()
        require(lines and all(line.split('\t') == [str(i),'valid'] for i,line in enumerate(lines,1)),
                'missing/failed pair; timings not reportable')
        samples=[load(str(directory/f'pair-{i}')+'.result.json') for i in range(1,len(lines)+1)]
        # Include acquisition gaps and dispatch/completion boundaries. Positive
        # errors anywhere in this encompassing window are a conservative RED.
        window = shared.metrics(load(directory/'window-before.json'), load(directory/'window-after.json'))
        require(not any(window['delta'][k] for k in ('rpc_errors','read_retries','read_fails')), 'window RPC error')
        gaps = [(directory/'window-before.json', directory/'pair-1.metrics-before.json')]
        gaps += [(directory/f'pair-{i}.metrics-after.json', directory/f'pair-{i+1}.metrics-before.json')
                 for i in range(1,len(lines))]
        gaps += [(directory/f'pair-{len(lines)}.metrics-after.json', directory/'window-after.json')]
        for before, after in gaps:
            gap = shared.metrics(load(before), load(after))
            require(not any(gap['delta'][k] for k in ('rpc_errors','read_retries','read_fails')), 'inter-pair RPC error')
        result=summarize(samples,int(args[2]),int(args[3]),int(args[4]),
                         (directory/'window-end').read_text().strip() == 'copy-and-spool-complete')
        (directory/'drain-latency-result.json').write_text(json.dumps(result,indent=2)+'\n')
        with (directory/'drain-latency-table.tsv').open('w') as out:
            out.write('metric\tsamples\tp50_ms\tp99_ms\tworst_ms\tbudget_ms\tover_budget\terrors\n')
            for scope,ops in result['scopes'].items():
                for op,v in ops.items():
                    out.write('%s/%s\t%d\t%.3f\t%.3f\t%.3f\t%s\t%d\t0\n' %
                              (scope,op,v['samples'],v['p50_ns']/1e6,v['p99_ns']/1e6,v['worst_ns']/1e6,args[3],
                               int(v['p99_ns'] >= int(args[3])*1000000)))
        require(result['within_budget'], 'whole-window or active-only p99/worst failed')
    else:
        raise ValueError('unknown evidence mode')


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (ValueError, KeyError, TypeError, OSError, TimeoutError) as error:
        print('INVALID: '+str(error), file=sys.stderr)
        sys.exit(1)

'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createLibraryClient, createTransport, validate, renderRecord, renderInspectorNotice, node } = require('../../internal/manager/static/library.js');
const lib = 'lib_' + '1'.repeat(32), A = 'asset_' + '1'.repeat(32), B = 'asset_' + '2'.repeat(32);
const bearer = digit => 'jmc1_' + digit.repeat(32) + '.' + digit.repeat(64);
const token = digit => 'sel1_' + digit.repeat(64);
const pageToken = digit => 'page1_' + digit.repeat(64);
const asset = (id, label = id, reasons = ['stable_id']) => ({ asset_id: id, label, head_revision_id: null, reasons });
const collection = (items = [], options = {}) => ({ version: 2, kind: 'search', items, matched_count: items.length, count_scope: 'frozen_window', window_complete: true, next_page_token: null, sort_version: 'discovery2', ...options });
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } });
const error = (code, status) => json({ version: 1, error: code }, status);
const deferred = () => { let resolve; const promise = new Promise(r => resolve = r); return { promise, resolve }; };
async function ticks() { for (let i = 0; i < 12; i++) await Promise.resolve(); }
function fixture(handler) {
  const calls = []; let now = 1800000000, n = 0;
  const client = createLibraryClient({ base: '/manager', now: () => now, crypto: { getRandomValues: v => { v.fill(++n); return v; } }, fetch: async (url, init) => {
    const call = { route: url.split('/continuity/')[1], body: JSON.parse(init.body), raw: init.body, auth: init.headers.Authorization, init };
    calls.push(call); return handler ? handler(call) : json(collection());
  } });
  return { client, calls, advance: seconds => { now += seconds; } };
}
function ordinary(call, current = null) {
  const id = call.body.asset_id || A;
  if (call.route.endsWith('/preference')) return json({ version: 2, asset_id: id, selection_token: current });
  if (call.route === 'v1/asset') return json({ version: 1, asset: asset(id) });
  if (call.route === 'v1/counts') return json({ version: 1, asset_id: id, revision_count: null, relationship_count: null, location_count: null });
  if (call.route.startsWith('v1/')) return json(collection([], { version: 1, kind: call.route.slice(3), sort_version: 'discovery1' }));
  return json(collection([asset(A), asset(B)]));
}
async function connected(f) { await f.client.setActive(true); await f.client.connect(lib, bearer('a')); }

test('transport owns prefix, bearer headers, deadlines and bounded streamed decoding', async () => {
  let options;
  const transport = createTransport('/manager', { fetch: async (url, init) => { assert.equal(url, '/manager/api/continuity/v2/search'); options = init; return json(collection()); } });
  await transport('v2/search', '{}', bearer('a'));
  assert.equal(options.credentials, 'omit'); assert.equal(options.cache, 'no-store'); assert.equal(options.redirect, 'error'); assert.ok(options.signal); assert.equal(options.headers.Authorization, 'Bearer ' + bearer('a'));
  assert.throws(() => createTransport('https://other.invalid'), /Check/);
  for (const response of [new Response('<private>', { headers: { 'content-type': 'text/html' } }), new Response(new Uint8Array([255]), { headers: { 'content-type': 'application/json' } }), json({ private: 'body' }), new Response('{', { headers: { 'content-type': 'application/json' } })]) {
    await assert.rejects(createTransport('', { fetch: async () => response })('v2/search', '{}', ''), /unreadable/);
  }
  let reads = 0, cancelled = false;
  const big = { ok: true, redirected: false, headers: new Headers({ 'content-type': 'application/json' }), body: { getReader: () => ({ read: async () => { reads++; return { done: false, value: new Uint8Array(1024 * 1024) }; }, cancel: async () => { cancelled = true; }, releaseLock() {} }) } };
  await assert.rejects(createTransport('', { fetch: async () => big })('v2/search', '{}', ''), /unreadable/); assert.equal(reads, 9); assert.equal(cancelled, true);
  const held = deferred(); let abort;
  const deadline = createTransport('', { setTimeout: fn => { abort = fn; return 1; }, clearTimeout() {}, fetch: async () => { await held.promise; return json(collection()); } });
  const request = deadline('v2/search', '{}', ''); await ticks(); abort(); held.resolve(); await assert.rejects(request, /timed out/);
});

// Model an already-buffered response that cannot be reclaimed by cancellation.
// Keep this override local to serial tests and restore it even on assertion fail;
// the real transport/client ownership checks remain unchanged.
async function ignoringCancellation(run) {
  const Original = globalThis.AbortController;
  globalThis.AbortController = class extends Original { abort() {} };
  try { await run(); } finally { globalThis.AbortController = Original; }
}
for (const kind of ['search', 'detail']) {
  test('connection epoch rejects an explicitly admitted same-generation late A ' + kind, { concurrency: false }, async () => ignoringCancellation(async () => {
    const held = deferred(), entered = deferred(); let admitOld = false;
    const f = fixture(call => {
      const old = call.auth === 'Bearer ' + bearer('a');
      if (admitOld && old && (kind === 'search' ? call.route === 'v2/search' && call.body.text === 'same-generation' : call.route === 'v1/asset')) { entered.resolve(); return held.promise; }
      if (call.route === 'v2/search') return json(collection([asset(A, 'Current B search')]));
      if (call.route === 'v1/asset') return json({ version: 1, asset: asset(A, 'Current B inspector') });
      return ordinary(call);
    });
    await connected(f); admitOld = true;
    const old = kind === 'search' ? f.client.search('same-generation') : f.client.inspect(A); await entered.promise;
    await f.client.connect(lib, bearer('b'));
    if (kind === 'search') await f.client.search('same-generation'); else await f.client.inspect(A);
    held.resolve(kind === 'search' ? json(collection([asset(A, 'OLD SECRET SEARCH')])) : json({ version: 1, asset: asset(A, 'OLD SECRET DETAIL') })); await old;
    if (kind === 'search') assert.equal(f.client.snapshot().items[0].label, 'Current B search', 'old connection search overwrote current connection');
    else assert.equal(f.client.snapshot().sections.asset.value.asset.label, 'Current B inspector', 'old connection inspector overwrote current connection');
    assert.equal(f.client.snapshot().connected, true); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD SECRET|jmc1_/);
  }));
}

test('stale detail and status cannot repopulate a newer asset inspector', async () => {
  const detail = deferred(), status = deferred(); let hold = true;
  const f = fixture(call => { if (hold && call.body.asset_id === A && call.route === 'v1/asset') return detail.promise; if (hold && call.body.asset_id === A && call.route === 'v2/preference') return status.promise; return ordinary(call, null); });
  await connected(f); const first = f.client.inspect(A); await ticks(); hold = false; await f.client.inspect(B); detail.resolve(json({ version: 1, asset: asset(A, 'OLD DETAIL') })); status.resolve(json({ version: 2, asset_id: A, selection_token: token('a') })); await first;
  assert.equal(f.client.snapshot().selected, B); assert.equal(f.client.snapshot().preference, false); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD DETAIL/);
});

test('reading and temporary context never send personal mutations; query pages invalidate', async () => {
  const f = fixture(call => call.route === 'v2/search' ? json(collection([asset(A)], { next_page_token: pageToken('a'), matched_count: 2 })) : ordinary(call));
  await connected(f); await f.client.context(asset(A)); await f.client.inspect(A);
  assert.equal(f.calls.some(c => /select|clear/.test(c.route)), false); assert.equal(f.client.snapshot().next, false);
  await f.client.search('new'); assert.equal(f.calls.at(-1).body.text, 'new');
});

test('deactivation aborts reads and clears scoped DOM state; return revalidates first', async () => {
  const hold = deferred(), searchEntered = deferred(), restoreEntered = deferred(); let block = false;
  const f = fixture(call => {
    if (!block) return ordinary(call);
    if (call.route === 'v2/search' && call.body.page_size === 100) restoreEntered.resolve();
    if (call.route === 'v2/search' && call.body.text === 'held' && call.body.page_size === 50) searchEntered.resolve();
    return hold.promise.then(() => json(collection([asset(A, 'Fresh context', ['context'])])));
  }); await connected(f); await f.client.context(asset(A, 'Context label'));
  block = true; const search = f.client.search('held'); await searchEntered.promise; await f.client.setActive(false);
  assert.equal(f.client.snapshot().items.length, 0); assert.equal(f.client.snapshot().contexts.length, 0); assert.equal(f.client.snapshot().selected, '');
  const returning = f.client.setActive(true); assert.equal(f.client.snapshot().connected, false); assert.equal(f.client.snapshot().items.length, 0);
  await restoreEntered.promise;
  assert.equal(f.client.snapshot().contexts.length, 0);
  block = false; hold.resolve(); await search; await returning;
  assert.equal(f.client.snapshot().contexts[0].label, 'Fresh context');
  f.client.pagehide(); assert.equal(f.client.snapshot().hasConnection, false); assert.equal(f.client.snapshot().contexts.length, 0);
});

for (const [code, status] of [['unavailable', 503], ['deadline', 504], ['restart_query', 409]]) {
  test('valid ' + code + ' after simulated commit preserves exact retry identity', async () => {
    let selected = null, attempts = 0;
    const f = fixture(call => { if (call.route === 'v2/select') { selected = token('a'); if (++attempts === 1) return error(code, status); return json({ version: 2, operation_id: call.body.operation_id, kind: 'select', selection_token: token('a'), receipt_scope: 'operation_result' }); } return ordinary(call, selected); });
    await connected(f); await f.client.inspect(A); await f.client.remember(); assert.equal(f.client.snapshot().pending, true);
    await f.client.status(); assert.equal(f.client.snapshot().preference, true); assert.equal(f.client.snapshot().pending, true);
    f.advance(30); await f.client.retry(); const mutations = f.calls.filter(c => c.route === 'v2/select'); assert.equal(mutations.length, 2); assert.equal(mutations[1].raw, mutations[0].raw); assert.equal(f.client.snapshot().pending, false);
  });
}

test('malformed receipt, exact retry and historical receipt do not assert current selection', async () => {
  let attempts = 0, current = token('b');
  const f = fixture(call => { if (call.route === 'v2/select') return ++attempts === 1 ? json({ private: true }) : json({ version: 2, operation_id: call.body.operation_id, kind: 'select', selection_token: token('a'), receipt_scope: 'operation_result' }); if (call.route === 'v2/clear') { current = null; return json({ version: 2, operation_id: call.body.operation_id, kind: 'clear', selection_token: null, receipt_scope: 'operation_result' }); } return ordinary(call, current); });
  await connected(f); await f.client.inspect(A); await f.client.remember(); assert.equal(f.client.snapshot().pending, true); await f.client.retry();
  assert.equal(f.client.snapshot().preference, true); assert.equal(f.client.snapshot().pending, false); await f.client.clear();
  const clear = f.calls.find(c => c.route === 'v2/clear'); assert.equal(clear.body.expected_token, token('b')); assert.equal(f.client.snapshot().preference, false); assert.equal(f.client.snapshot().pending, false);
});

test('expiry and expired_request preserve unresolved outcome until fresh status and acknowledgement', async () => {
  let expireError = false;
  const f = fixture(call => call.route === 'v2/select' ? expireError ? error('expired_request', 409) : error('unavailable', 503) : ordinary(call));
  await connected(f); await f.client.inspect(A); await f.client.remember(); expireError = true; await f.client.retry();
  assert.equal(f.client.snapshot().pending, false); assert.equal(f.client.snapshot().warning.acknowledged, false); assert.equal(f.client.snapshot().canMutate, false);
  const before = f.calls.length; await f.client.retry(); assert.equal(f.calls.length, before); f.client.acknowledge(); assert.equal(f.client.snapshot().warning.acknowledged, false);
  await f.client.status(); f.client.acknowledge(); assert.equal(f.client.snapshot().canMutate, true);
  expireError = false; await f.client.remember(); f.advance(601); f.client.snapshot(); assert.equal(f.client.snapshot().pending, false); assert.equal(f.client.snapshot().warning.acknowledged, false);
});

test('401 clears a dispatched mutation with a nonsensitive may-have-been-accepted notice', async () => {
  const f = fixture(call => call.route === 'v2/select' ? error('unauthenticated', 401) : ordinary(call)); await connected(f); await f.client.inspect(A); await f.client.remember();
  const state = f.client.snapshot(); assert.equal(state.hasConnection, false); assert.equal(state.items.length, 0); assert.equal(state.pending, false); assert.match(state.message, /may have been accepted/);
});

test('context revalidation proves the entire64-ID prefix across bounded pages with escaped labels', async () => {
  const contexts = Array.from({ length: 64 }, (_, i) => asset('asset_' + (i + 1).toString(16).padStart(32, '0'), '\u0001'.repeat(4096), ['context']));
  let proof = false, page = 0;
  const f = fixture(call => {
    if (!proof) return json(collection());
    if (call.route === 'v2/search' && call.body.page_size === 50) return json(collection());
    const index = page++; const values = contexts.slice(index * 15, index * 15 + 15);
    return json(collection(values, { matched_count: 64, next_page_token: index < 4 ? pageToken(String(index + 1)) : null }));
  });
  await connected(f); for (const a of contexts) await f.client.context(a); const before = f.calls.length; await f.client.context(asset('asset_' + 'f'.repeat(32))); assert.equal(f.calls.length, before); assert.equal(f.client.snapshot().contexts.length, 64); assert.equal(f.calls.filter(c => c.route === 'v2/select').length, 0);
  await f.client.setActive(false); proof = true; await f.client.setActive(true); assert.equal(page, 5); assert.equal(f.client.snapshot().contexts.length, 64); assert.equal(f.client.snapshot().contexts[63].label, contexts[63].label);
});

test('denied contexts prune only after complete proof; interrupted continuation publishes nothing', async () => {
  for (const failure of [false, true]) {
    let proof = false;
    const f = fixture(call => { if (!proof) return json(collection()); if (call.route === 'v2/page') return failure ? error('restart_query', 409) : json(collection([asset('asset_' + '3'.repeat(32))], { next_page_token: null })); if (call.body.page_size === 100) return json(collection([asset(A, 'Fresh', ['context'])], { matched_count: 2, next_page_token: pageToken('a') })); return json(collection()); });
    await connected(f); await f.client.context(asset(A)); await f.client.context(asset(B)); await f.client.setActive(false); proof = true; await f.client.setActive(true);
    if (failure) { assert.equal(f.client.snapshot().contexts.length, 0); assert.equal(f.client.snapshot().items.length, 0); assert.match(f.client.snapshot().message, /expired|changed/); }
    else assert.deepEqual(f.client.snapshot().contexts.map(a => a.asset_id), [A]);
  }
});

test('public shape validation rejects unknown fields and preserves unsafe numbers honestly', () => {
  assert.throws(() => validate('v2/search', { ...collection(), private: 1 }), /unreadable/);
  const revision = { revision_id: 'rev1_' + 'a'.repeat(64), asset_id: A, content: { sha256: 'b'.repeat(64), size: 2 ** 60 }, parents: [] };
  assert.equal(validate('v1/revisions', collection([revision], { version: 1, kind: 'revisions', sort_version: 'discovery1' })).items.length, 1);
});

test('metadata renderer emits inert text with bounded expandable declarations', () => {
  class Element { constructor(tag) { this.tagName = tag; this.children = []; this.textContent = ''; } append(...children) { this.children.push(...children); } set innerHTML(_) { throw new Error('unsafe HTML'); } }
  const document = { createElement: tag => new Element(tag) };
  const text = '<img src=x onerror=alert(1)>';
  const element = node(document, 'p', text); assert.equal(element.textContent, text); assert.equal(element.children.length, 0);
  const record = renderRecord(document, 'locations', { location_id: 'place', asset_id: A, revision_id: null, source: text, locator: 'javascript:alert(1)' });
  const all = []; const walk = e => { all.push(e); e.children.forEach(walk); }; walk(record);
  assert.equal(all.some(e => e.tagName === 'a' || e.tagName === 'img'), false); assert.equal(all.some(e => e.textContent === 'javascript:alert(1)'), true);
});

test('late search and interrupted context continuation never publish into a newer connection', async () => {
  let mode = 'initial'; const held = deferred(), continuationEntered = deferred();
  const f = fixture(call => {
    if (mode === 'initial') return json(collection());
    if (mode === 'proof' && call.route === 'v2/search') return json(collection([asset(A, 'Partial context', ['context'])], { matched_count: 2, next_page_token: pageToken('a') }));
    if (mode === 'proof' && call.route === 'v2/page') { continuationEntered.resolve(); return held.promise; }
    return json(collection([asset(B, 'Fresh connection B')]));
  });
  await connected(f); await f.client.context(asset(A)); await f.client.context(asset(B)); await f.client.setActive(false); mode = 'proof';
  const resuming = f.client.setActive(true); await continuationEntered.promise; assert.equal(f.client.snapshot().contexts.length, 0);
  mode = 'new'; await f.client.connect(lib, bearer('b')); held.resolve(json(collection([asset(B, 'OLD CONTINUATION', ['context'])]))); await resuming;
  assert.equal(f.client.snapshot().items[0].label, 'Fresh connection B'); assert.equal(f.client.snapshot().contexts.length, 0);
});

test('same-connection late searches cannot overwrite a later explicit query', async () => {
  const old = deferred();
  const f = fixture(call => call.body.text === 'old' ? old.promise : ordinary(call)); await connected(f);
  const first = f.client.search('old'); await ticks(); await f.client.search('current'); old.resolve(json(collection([asset(A, 'OLD QUERY')]))); await first;
  assert.equal(f.client.snapshot().query, 'current'); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD QUERY/);
});

test('one unsettled action blocks duplicate IDs and retains uncertainty through later conflict', async () => {
  let attempts = 0; const committed = deferred(), entered = deferred();
  const f = fixture(call => { if (call.route === 'v2/select') { if (++attempts === 1) { entered.resolve(); return committed.promise; } return error('conflict', 409); } return ordinary(call); }); await connected(f); await f.client.inspect(A);
  const action = f.client.remember(); await entered.promise; await f.client.remember(); committed.resolve(error('unavailable', 503)); await action;
  assert.equal(attempts, 1); await f.client.retry(); assert.equal(f.client.snapshot().pending, true);
  assert.equal(f.calls.filter(c => c.route === 'v2/select')[0].raw, f.calls.filter(c => c.route === 'v2/select')[1].raw);
  f.client.disconnect(); assert.equal(f.client.snapshot().pending, false); assert.match(f.client.snapshot().message, /may have been accepted/);
});

test('shared two-read admission completes all inspector reads without self-induced server budget errors', async () => {
  let inFlight = 0, maximum = 0, overBudget = 0, completed = 0; const gates = [], twoEntered = deferred();
  const f = fixture(call => {
    if (call.route.startsWith('v1/') || call.route === 'v2/preference') {
      // Model the qualified server's concurrent construction budget, not remote
      // latency. A third admitted build gets a real fixed503 shape, no retry.
      if (inFlight >= 2) { overBudget++; completed++; return error('busy', 503); }
      inFlight++; maximum = Math.max(maximum, inFlight); const gate = deferred();
      gates.push(() => { inFlight--; completed++; gate.resolve(ordinary(call)); });
      if (inFlight === 2) twoEntered.resolve();
      return gate.promise;
    }
    return ordinary(call);
  });
  await connected(f); const inspecting = f.client.inspect(A); await twoEntered.promise;
  assert.equal(gates.length, 2, 'inspector did not retain two demonstrably admitted reads');
  for (let turn = 0; completed < 7 && turn < 100; turn++) {
    if (gates.length) gates.splice(0).forEach(done => done());
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(completed, 7); await inspecting;
  assert.equal(overBudget, 0, 'client exceeded the qualified concurrent server construction budget');
  assert.equal(maximum, 2); assert.equal(inFlight, 0);
  const state = f.client.snapshot();
  for (const kind of ['asset', 'counts', 'revisions', 'relationships', 'locations', 'history']) {
    assert.equal(state.sections[kind].busy, false, kind + ' stayed busy');
    assert.equal(state.sections[kind].error, undefined, kind + ' failed from self-induced fanout');
  }
  assert.equal(state.statusFresh, true);
});

test('explicit location denial remains unavailable rather than an empty inspector section', async () => {
  const f = fixture(call => call.route === 'v1/locations' ? error('not_found', 404) : ordinary(call));
  await connected(f); await f.client.inspect(A);
  assert.match(f.client.snapshot().sections.locations.error, /Unavailable/);
  assert.equal(f.client.snapshot().sections.locations.items, undefined);
});

test('unavailable search stays an error rather than an empty successful collection', async () => {
  let unavailable = false; const f = fixture(call => unavailable ? error('unavailable', 503) : ordinary(call)); await connected(f); unavailable = true; await f.client.search('failing');
  assert.equal(f.client.snapshot().searchError, true); assert.match(f.client.snapshot().message, /unavailable/); assert.equal(f.client.snapshot().items.length, 0);
});

test('explicit More uses the owned v2 cursor, blocks duplicate loads and caps displayed items', async () => {
  const held = deferred(), entered = deferred(); let pages = 0;
  const cursor = n => 'page1_' + n.toString(16).padStart(64, '0');
  const result = index => collection(Array.from({ length: 50 }, (_, i) => asset('asset_' + (index * 50 + i + 1).toString(16).padStart(32, '0'))), { matched_count: 1000, window_complete: false, next_page_token: index < 19 ? cursor(index + 1) : null });
  const f = fixture(call => { if (call.route === 'v2/page') { pages++; assert.equal(call.body.version, 2); assert.deepEqual(Object.keys(call.body), ['version', 'library_id', 'token']); if (pages === 1) { entered.resolve(); return held.promise; } return json(result(pages)); } return json(result(0)); });
  await connected(f); assert.equal(f.client.snapshot().items.length, 50);
  const more = f.client.search(undefined, true); await entered.promise; await f.client.search(undefined, true); assert.equal(pages, 1); held.resolve(json(result(1))); await more;
  for (let i = 0; i < 18; i++) await f.client.search(undefined, true);
  assert.equal(f.client.snapshot().items.length, 1000); assert.equal(f.client.snapshot().next, false); assert.equal(pages, 19);
  const before = f.calls.length; await f.client.search('😀'.repeat(65)); assert.equal(f.calls.length, before); assert.match(f.client.snapshot().message, /256 UTF-8/);
  await f.client.search('fresh'); assert.equal(f.client.snapshot().items.length, 50); assert.equal(f.calls.at(-1).route, 'v2/search');
});

test('context prefix proof stops at five pages and never publishes a partial proof', async () => {
  let proof = false, pages = 0;
  const f = fixture(call => { if (proof) { pages++; return json(collection([asset(A, 'Only A', ['context'])], { matched_count: 2, next_page_token: pageToken('a') })); } return json(collection()); });
  await connected(f); await f.client.context(asset(A)); await f.client.context(asset(B)); await f.client.setActive(false); proof = true; await f.client.setActive(true);
  assert.equal(pages, 5); assert.equal(f.client.snapshot().contexts.length, 0); assert.equal(f.client.snapshot().connected, false); assert.match(f.client.snapshot().message, /fully revalidated/);
});

for (const outcome of ['receipt', 'uncertain']) {
  test('personal ' + outcome + ' invalidates a held old ranked page before fresh status', { concurrency: false }, async () => ignoringCancellation(async () => {
    const oldPage = deferred(), pageEntered = deferred(), freshStatus = deferred(), statusEntered = deferred(), mutationEntered = deferred(), mutationReply = deferred();
    let committed = false, action, checked, operationID;
    const f = fixture(call => {
      if (call.route === 'v2/page') { pageEntered.resolve(); return oldPage.promise; }
      if (call.route === 'v2/select') { committed = true; mutationEntered.resolve(call.body.operation_id); return mutationReply.promise; }
      if (committed && call.route === 'v2/preference') { statusEntered.resolve(); return freshStatus.promise; }
      if (call.route === 'v2/search') return json(collection([asset(A, committed ? 'Current ranking' : 'Previous ranking')], { matched_count: 2, next_page_token: pageToken('a') }));
      return ordinary(call);
    });
    await connected(f); await f.client.inspect(A); await f.client.search('ranked');
    const more = f.client.search(undefined, true); await pageEntered.promise;
    const stale = () => json(collection([asset(B, 'OLD RANKED PAGE')], { matched_count: 2 }));
    const current = () => json({ version: 2, asset_id: A, selection_token: token('a') });
    try {
      action = f.client.remember(); operationID = await mutationEntered.promise;
      assert.equal(f.client.snapshot().next, false, 'dispatch retained an old ranked cursor');
      assert.equal(f.client.snapshot().items.length, 0, 'dispatch retained an old ranked window');
      const beforeReply = f.calls.length; await f.client.search('blocked before receipt');
      assert.equal(f.calls.length, beforeReply, 'ranked query admitted before mutation reply');
      mutationReply.resolve(outcome === 'uncertain' ? error('unavailable', 503) : json({ version: 2, operation_id: operationID, kind: 'select', selection_token: token('a'), receipt_scope: 'operation_result' }));
      if (outcome === 'uncertain') { await action; checked = f.client.status(); }
      await statusEntered.promise;
      assert.equal(f.client.snapshot().next, false, 'personal action retained the old ranked cursor');
      assert.equal(f.client.snapshot().items.length, 0, 'personal action retained the old ranked window');
      if (outcome === 'receipt') {
        assert.equal(f.client.snapshot().mutationBusy, true);
        const admitted = f.calls.length; await f.client.search('blocked while settling'); await f.client.search(undefined, true);
        assert.equal(f.calls.length, admitted, 'new ranked request admitted while mutation was settling');
      }
      oldPage.resolve(stale()); await more;
      assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD RANKED PAGE/, 'old ranked page appended after personal action');
      if (outcome === 'uncertain') {
        assert.equal(f.client.snapshot().pending, true); await f.client.search('explicit fresh search');
        assert.equal(f.client.snapshot().items[0].label, 'Current ranking');
      }
      freshStatus.resolve(current()); await action; if (checked) await checked;
      assert.equal(f.client.snapshot().items[0].label, 'Current ranking');
      assert.equal(f.client.snapshot().pending, outcome === 'uncertain', 'current status resolved an uncertain historical outcome');
    } finally {
      mutationReply.resolve(error('unavailable', 503)); oldPage.resolve(stale()); freshStatus.resolve(current());
      await Promise.all([more, action, checked].filter(Boolean));
      f.client.disconnect();
    }
  }));
}

test('failed asset read has an honest inspector gate and working local retry without old metadata', async () => {
  let denied = false;
  const f = fixture(call => call.route === 'v1/asset' ? denied ? error('not_found', 404) : json({ version: 1, asset: asset(call.body.asset_id, call.body.asset_id === A ? 'Previous private label' : 'Fresh B label') }) : ordinary(call));
  await connected(f); await f.client.inspect(A); denied = true; await f.client.inspect(B);
  const state = f.client.snapshot(); assert.equal(state.inspector, 'error'); assert.equal(state.selected, ''); assert.match(state.inspectorError, /Unavailable/);
  class Element {
    constructor(tag) { this.tagName = tag; this.children = []; this.textContent = ''; }
    append(...children) { this.children.push(...children); }
    addEventListener(name, run) { assert.equal(name, 'click'); this.activate = run; }
    set innerHTML(_) { throw new Error('unsafe renderer'); }
  }
  const document = { createElement: tag => new Element(tag) }, parent = new Element('aside');
  assert.equal(renderInspectorNotice(document, parent, state, () => f.client.loadSection('asset')), true);
  const all = []; const walk = value => { all.push(value); value.children.forEach(walk); }; walk(parent);
  assert.equal(all.some(value => /Choose an asset|Previous private label/.test(value.textContent)), false);
  assert.equal(all.some(value => value.textContent === 'Asset unavailable'), true);
  const retry = all.find(value => value.tagName === 'button'); assert.equal(retry.textContent, 'Retry asset read');
  denied = false; await retry.activate();
  assert.equal(f.client.snapshot().inspector, 'ready'); assert.equal(f.client.snapshot().selected, B); assert.equal(f.client.snapshot().sections.asset.value.asset.label, 'Fresh B label');
  const loading = new Element('aside'); renderInspectorNotice(document, loading, { inspector: 'loading' }, () => {});
  assert.match(loading.children[0].textContent, /Reading permitted asset/);
});

test('uncertain retry cancels a held context proof without leaving access recovery busy', { concurrency: false }, async () => ignoringCancellation(async () => {
  const heldProof = deferred(), proofEntered = deferred(); let holdRestoration = false, proofs = 0, returning;
  const f = fixture(call => {
    if (call.route === 'v2/select') return error('unavailable', 503);
    if (holdRestoration && call.route === 'v2/search' && call.body.page_size === 100) {
      proofs++;
      if (proofs === 1) { proofEntered.resolve(); return heldProof.promise; }
      return json(collection([asset(A, 'Recovered fresh context', ['context'])]));
    }
    return ordinary(call);
  });
  const stale = () => json(collection([asset(A, 'OLD cancelled proof', ['context'])]));
  try {
    await connected(f); await f.client.context(asset(A, 'Initial context')); await f.client.inspect(A); await f.client.remember();
    assert.equal(f.client.snapshot().pending, true);
    await f.client.setActive(false); holdRestoration = true; returning = f.client.setActive(true); await proofEntered.promise;
    assert.equal(f.client.snapshot().restoring, true); assert.equal(f.client.snapshot().connected, false); assert.equal(f.client.snapshot().contexts.length, 0);
    await f.client.retry();
    const state = f.client.snapshot();
    assert.equal(state.restoring, false, 'cancelled context proof left the restore busy flag set');
    assert.equal(state.active && state.hasConnection && !state.connected && !state.restoring, true, 'Retry access remains hidden after uncertain retry');
    assert.equal(state.contexts.length, 0, 'cancelled proof published partial context'); assert.equal(state.items.length, 0); assert.equal(state.pending, true);
    const mutations = f.calls.filter(call => call.route === 'v2/select'); assert.equal(mutations.length, 2); assert.equal(mutations[0].raw, mutations[1].raw);
    await f.client.refresh();
    assert.equal(proofs, 2); assert.equal(f.client.snapshot().connected, true); assert.equal(f.client.snapshot().contexts[0].label, 'Recovered fresh context'); assert.equal(f.client.snapshot().pending, true);
    heldProof.resolve(stale()); await returning;
    assert.equal(f.client.snapshot().contexts[0].label, 'Recovered fresh context'); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD cancelled proof/);
  } finally { heldProof.resolve(stale()); if (returning) await returning; f.client.disconnect(); }
}));

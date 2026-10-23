'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createLibraryClient, createTransport, validate, collectionCanonical, randomCollectionID } = require('../../internal/manager/static/library.js');
const hex = (n, width = 32) => n.toString(16).padStart(width, '0');
const lib = 'lib_' + hex(1), C = 'col_' + hex(1), D = 'col_' + hex(2), A = 'asset_' + hex(1), B = 'asset_' + hex(2);
const entry = n => 'cent_' + hex(n), grant = n => 'cgr_' + hex(n), subject = n => 'sub_' + hex(n), cv = n => 'cv_' + hex(n), gv = n => 'cgv_' + hex(n), token = n => 'page1_' + hex(n, 64);
const bearer = n => 'jmc1_' + hex(n) + '.' + hex(n, 64);
const rights = ['collections.admin', 'collections.edit', 'collections.read'];
const summary = (overrides = {}) => ({ collection_id: C, kind: 'manual', name: 'Working set', archived: false, definition_version: cv(1), creator: 'you', permissions: { read: true, edit: true, admin: true }, ...overrides });
const member = (n, removed = false) => ({ entry_id: entry(n), asset_id: 'asset_' + hex(n), label: 'Member ' + n, head_revision_id: null, removed });
const access = (n, overrides = {}) => ({ grant_id: grant(n), expected: { value: { subject_id: subject(n), scope: { kind: 'collection', collection_id: C }, actions: rights.slice() }, version: gv(n), removed: false, ...overrides } });
const window = (kind, items = [], overrides = {}) => ({ version: 1, kind, collection: kind === 'directory' ? null : summary(), ...(kind === 'directory' ? { can_create: true } : {}), ...(kind === 'members' ? { state: 'active' } : {}), items, matched_count: items.length, count_scope: kind === 'members' ? 'frozen_visible_membership' : 'frozen_window', window_complete: true, next_page_token: null, sort_version: 'collections1', snapshot_scope: kind === 'members' ? 'frozen_membership_current_page' : 'frozen_snapshot', ...overrides });
const assets = (items = []) => ({ version: 2, kind: 'search', items, matched_count: items.length, count_scope: 'frozen_window', window_complete: true, next_page_token: null, sort_version: 'discovery2' });
const asset = (n, reasons = ['stable_id']) => ({ asset_id: 'asset_' + hex(n), label: 'Asset ' + n, head_revision_id: null, reasons });
const json = (v, status = 200) => new Response(JSON.stringify(v), { status, headers: { 'content-type': 'application/json' } });
const error = (code, status) => json({ version: 1, error: code }, status);
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
async function ticks() { for (let i = 0; i < 16; i++) await Promise.resolve(); }
function fixture(handler, options = {}) {
  const calls = []; let sequence = 32, now = 1800000000;
  const client = createLibraryClient({ ...options, base: '/manager', now: () => now, crypto: { getRandomValues: a => { a.fill(++sequence); return a; } }, fetch: async (url, init) => {
    const call = { route: url.split('/continuity/')[1], body: JSON.parse(init.body), raw: init.body, auth: init.headers.Authorization, init }; calls.push(call);
    const response = handler && await handler(call); return response || ordinary(call);
  } });
  return { client, calls, advance: n => { now += n; } };
}
function ordinary(call, current = summary()) {
  if (call.route === 'v1/collections/directory') return json(window('directory', [current]));
  if (call.route === 'v1/collections/open') return json({ version: 1, kind: 'collection', collection: { ...current, collection_id: call.body.collection_id } });
  if (call.route === 'v1/collections/members') return json(window('members', [member(1), member(2)], { collection: { ...current, collection_id: call.body.collection_id }, state: call.body.state, items: [member(1, call.body.state === 'removed'), member(2, call.body.state === 'removed')] }));
  if (call.route === 'v1/collections/history') return json(window('history', [], { collection: current }));
  if (call.route === 'v1/collections/access') return json(window('access', [access(1)], { collection: current }));
  if (call.route === 'v1/collections/mutate') return json({ version: 1, operation_id: call.body.operation_id, collection_id: call.body.collection_id, result_version: cv(2) });
  if (call.route === 'v1/asset') return json({ version: 1, asset: { ...asset(1), asset_id: call.body.asset_id } });
  if (call.route === 'v1/counts') return json({ version: 1, asset_id: call.body.asset_id, revision_count: null, relationship_count: null, location_count: null });
  if (call.route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: null });
  if (call.route.startsWith('v1/')) return json({ ...assets(), version: 1, kind: call.route.slice(3), sort_version: 'discovery1' });
  return json(assets([asset(1), asset(2)]));
}
async function connected(f, open = true) { await f.client.setActive(true); await f.client.switchView('collections'); await f.client.connect(lib, bearer(1)); if (open) await f.client.openCollection(C); }
async function ignoringCancellation(run) { const Original = globalThis.AbortController; globalThis.AbortController = class extends Original { abort() {} }; try { await run(); } finally { globalThis.AbortController = Original; } }
const operation = (kind, value) => ({ version: 1, library_id: lib, operation_id: 'cop_' + hex(1), collection_id: C, expected_version: kind === 'create' ? '' : cv(1), kind, value });

test('collection canonical bytes use independent literals, all kinds and Go Unicode escaping', () => {
  const prefix = '{"version":1,"library_id":"lib_00000000000000000000000000000001","operation_id":"cop_00000000000000000000000000000001","collection_id":"col_00000000000000000000000000000001","expected_version":';
  const expectedCV = '"cv_00000000000000000000000000000001"';
  assert.equal(collectionCanonical(operation('create', { name: '<>&"\u2028\u2029😀', creator_grant_id: grant(1), entries: [] })), prefix + '"","kind":"create","value":{"name":"\\u003c\\u003e\\u0026\\"\\u2028\\u2029😀","creator_grant_id":"cgr_00000000000000000000000000000001","entries":[]}}');
  assert.equal(collectionCanonical(operation('rename', { name: ' Edited ' })), prefix + expectedCV + ',"kind":"rename","value":{"name":" Edited "}}');
  assert.equal(collectionCanonical(operation('archive', { archived: true })), prefix + expectedCV + ',"kind":"archive","value":{"archived":true}}');
  assert.equal(collectionCanonical(operation('entries', { changes: [{ kind: 'remove', entry_id: entry(1), asset_id: A }, { kind: 'reactivate', entry_id: entry(2), asset_id: B }] })), prefix + expectedCV + ',"kind":"entries","value":{"changes":[{"kind":"remove","entry_id":"cent_00000000000000000000000000000001","asset_id":"asset_00000000000000000000000000000001"},{"kind":"reactivate","entry_id":"cent_00000000000000000000000000000002","asset_id":"asset_00000000000000000000000000000002"}]}}');
  const newGrant = { kind: 'put', grant_id: grant(1), expected: null, value: { subject_id: subject(2), scope: { kind: 'collection', collection_id: C }, actions: ['collections.edit', 'collections.read'] } };
  assert.equal(collectionCanonical(operation('grants', { changes: [newGrant] })), prefix + expectedCV + ',"kind":"grants","value":{"changes":[{"kind":"put","grant_id":"cgr_00000000000000000000000000000001","expected":null,"value":{"subject_id":"sub_00000000000000000000000000000002","scope":{"kind":"collection","collection_id":"col_00000000000000000000000000000001"},"actions":["collections.edit","collections.read"]}}]}}');
  for (const name of ['\ud800', '\udc00', 'a\ud800b', '\u0000', '', '😀'.repeat(65)]) assert.throws(() => collectionCanonical(operation('rename', { name })), /Check/);
  assert.doesNotThrow(() => collectionCanonical(operation('rename', { name: '😀'.repeat(64) })));
  assert.throws(() => collectionCanonical({ ...operation('rename', { name: 'x' }), actor: subject(1) }), /Check/);
  assert.throws(() => collectionCanonical(operation('entries', { changes: [{ kind: 'add', entry_id: entry(1), asset_id: A }, { kind: 'add', entry_id: entry(2), asset_id: A }] })), /Check/);
  const sorted = JSON.parse(collectionCanonical(operation('create', { name: 'Two', creator_grant_id: grant(1), entries: [{ entry_id: entry(2), asset_id: B }, { entry_id: entry(1), asset_id: A }] })));
  assert.deepEqual(sorted.value.entries.map(e => e.entry_id), [entry(1), entry(2)]);
});

test('grant canonical correction preserves exact expected binding and rejects retarget or no-op', () => {
  const old = access(1).expected;
  const change = { kind: 'put', grant_id: grant(1), expected: old, value: { ...old.value, actions: ['collections.read'] } };
  const raw = collectionCanonical(operation('grants', { changes: [change] }));
  assert.equal(raw, '{"version":1,"library_id":"lib_00000000000000000000000000000001","operation_id":"cop_00000000000000000000000000000001","collection_id":"col_00000000000000000000000000000001","expected_version":"cv_00000000000000000000000000000001","kind":"grants","value":{"changes":[{"kind":"put","grant_id":"cgr_00000000000000000000000000000001","expected":{"value":{"subject_id":"sub_00000000000000000000000000000001","scope":{"kind":"collection","collection_id":"col_00000000000000000000000000000001"},"actions":["collections.admin","collections.edit","collections.read"]},"version":"cgv_00000000000000000000000000000001","removed":false},"value":{"subject_id":"sub_00000000000000000000000000000001","scope":{"kind":"collection","collection_id":"col_00000000000000000000000000000001"},"actions":["collections.read"]}}]}}');
  assert.deepEqual(JSON.parse(collectionCanonical(operation('grants', { changes: [{ ...change, kind: 'remove', value: null }] }))).value.changes[0].expected, old);
  assert.doesNotThrow(() => collectionCanonical(operation('grants', { changes: [{ ...change, expected: { ...old, removed: true }, value: old.value }] })));
  for (const value of [{ ...old.value, subject_id: subject(2) }, { ...old.value, scope: { kind: 'collection', collection_id: D } }, old.value, { ...old.value, actions: [] }, { ...old.value, actions: ['collections.read', 'collections.edit'] }]) assert.throws(() => collectionCanonical(operation('grants', { changes: [{ ...change, value }] })), /Check/);
});

test('secure collection identities reject zero entropy with a finite attempt bound', () => {
  let calls = 0; assert.throws(() => randomCollectionID({ getRandomValues(a) { calls++; a.fill(0); } }, 'operation'), /unavailable/); assert.equal(calls, 4);
  assert.equal(randomCollectionID({ getRandomValues(a) { a.fill(1); } }, 'entry'), 'cent_' + '01'.repeat(16));
  assert.throws(() => randomCollectionID(null, 'collection'), /unavailable/);
});

test('collection DTO validation keeps every union strict and each kind capacity independent', () => {
  const create = { kind: 'create', recorded_unix_nano: '9223372036854775807', actor: 'you', value: { name: 'Initial', entries: [] } };
  for (const [route, response] of [['directory', window('directory', [summary()])], ['open', { version: 1, kind: 'collection', collection: summary() }], ['members', window('members', [member(1)])], ['access', window('access', [access(1)])], ['history', window('history', [create, { ...create, kind: 'rename', value: { name: 'New' } }, { ...create, kind: 'archive', value: { archived: true } }, { ...create, kind: 'entries', value: { changes: [{ kind: 'remove', entry_id: entry(1), asset_id: A }] } }])]]) assert.equal(validate('v1/collections/' + route, response), response);
  assert.doesNotThrow(() => validate('v1/collections/members', window('members', [member(1)], { matched_count: 8192 })));
  assert.doesNotThrow(() => validate('v1/collections/access', window('access', [access(1)], { matched_count: 128 })));
  for (const [kind, count] of [['directory', 1025], ['members', 8193], ['access', 129], ['history', 1001]]) assert.throws(() => validate('v1/collections/' + kind, window(kind, [], { matched_count: count })), /unreadable/);
  const invalid = [{ ...window('members'), items: null }, window('members', Array.from({ length: 101 }, (_, i) => member(i + 1))), window('members', [member(1)], { state: 'removed' }), window('members', [{ ...member(1), label: 'x'.repeat(4097) }]), window('access', [{ ...access(1), extra: true }]), window('history', [{ ...create, recorded_unix_nano: 1 }]), window('history', [{ ...create, recorded_unix_nano: '9223372036854775808' }]), { ...window('directory'), private: true }, window('directory', [], { collection: summary() }), window('history', [{ ...create, value: { name: 'x', entries: null } }])];
  for (const response of invalid) assert.throws(() => validate('v1/collections/page', response), /unreadable/);
  assert.throws(() => validate('v1/collections/page', window('members', [member(1)], { snapshot_scope: 'frozen_snapshot' })), /unreadable/);
});

test('collection transport uses exact prefixed route family and a separate one MiB stream cap', async () => {
  let options, url; const transport = createTransport('/manager', { fetch: async (u, init) => { url = u; options = init; return json(window('directory')); } });
  await transport('v1/collections/directory', '{}', bearer(1));
  assert.equal(url, '/manager/api/continuity/v1/collections/directory'); assert.equal(options.headers.Authorization, 'Bearer ' + bearer(1)); assert.equal(options.credentials, 'omit'); assert.equal(options.redirect, 'error'); assert.equal(options.cache, 'no-store');
  await assert.rejects(transport('v2/collections/members', '{}', bearer(1)), /Check/); // v2 is saved-query-only; its valid routes have independent exact DTO tests.
  let chunks = 0, cancelled = false;
  const large = { ok: true, redirected: false, headers: new Headers({ 'content-type': 'application/json' }), body: { getReader: () => ({ read: async () => { chunks++; return { done: false, value: new Uint8Array(512 * 1024) }; }, cancel: async () => { cancelled = true; }, releaseLock() {} }) } };
  await assert.rejects(createTransport('', { fetch: async () => large })('v1/collections/members', '{}', bearer(1)), /unreadable/); assert.equal(chunks, 3); assert.equal(cancelled, true);
});

for (const total of [130, 8192]) test('member traversal retains only the current page through ' + total + ' identities', async () => {
  let page = 0;
  const f = fixture(call => {
    if (call.route === 'v1/collections/members' || call.route === 'v1/collections/page') {
      if (call.route.endsWith('/members')) page = 0;
      const start = page * 100, values = Array.from({ length: Math.min(100, total - start) }, (_, i) => member(start + i + 1)); page++;
      return json(window('members', values, { matched_count: total, next_page_token: page * 100 < total ? token(page) : null }));
    }
  });
  await connected(f); let seen = 0;
  do { const w = f.client.snapshot().collections.windows.members; seen += w.items.length; assert.ok(w.items.length <= 100); assert.equal(w.count, total); if (!w.next) break; await f.client.collectionNext('members'); } while (true);
  assert.equal(seen, total, 'member traversal must not truncate at the legacy 1000-asset bound');
  const state = f.client.snapshot(); assert.equal(state.collections.windows.members.items.at(-1).entry_id, entry(total)); assert.ok(!JSON.stringify(state).includes('Member 1"'));
  assert.equal(f.calls.filter(c => c.route === 'v1/collections/page').length, Math.ceil(total / 100) - 1);
  assert.ok(f.calls.filter(c => c.route.startsWith('v1/collections/')).every(c => !Object.hasOwn(c.body, 'kind')));
});

for (const flaw of ['duplicate-entry', 'cycle', 'changed-version', 'changed-count', 'empty-successor']) test('collection page rejects ' + flaw + ' without retaining stale names', async () => {
  const f = fixture(call => {
    if (call.route === 'v1/collections/members') return json(window('members', [member(1)], { matched_count: 3, next_page_token: token(1) }));
    if (call.route === 'v1/collections/page') return json(window('members', flaw === 'empty-successor' ? [] : [member(flaw === 'duplicate-entry' ? 1 : 2)], { matched_count: flaw === 'changed-count' ? 4 : 3, next_page_token: flaw === 'cycle' ? token(1) : flaw === 'empty-successor' ? token(2) : null, collection: summary({ definition_version: flaw === 'changed-version' ? cv(2) : cv(1) }) }));
  }); await connected(f); await f.client.collectionNext('members');
  assert.equal(f.client.snapshot().collections.current, null); assert.match(f.client.snapshot().collections.error, /unreadable/); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /Member 1/);
});

test('128-entry roster pages preserve private CAS while exposing only current rights', async () => {
  let current = summary(), edits = 0;
  const f = fixture(call => {
    if (call.route === 'v1/collections/access') return json(window('access', Array.from({ length: 100 }, (_, i) => access(i + 1)), { collection: current, matched_count: 128, next_page_token: token(1) }));
    if (call.route === 'v1/collections/page') return json(window('access', Array.from({ length: 28 }, (_, i) => access(i + 101)), { collection: current, matched_count: 128 }));
    if (call.route === 'v1/collections/mutate') { edits++; current = summary({ definition_version: cv(2) }); return json({ version: 1, operation_id: call.body.operation_id, collection_id: C, result_version: cv(2) }); }
    return ordinary(call, current);
  }); await connected(f); await f.client.collectionTab('access'); await f.client.collectionNext('access');
  assert.equal(f.client.snapshot().collections.windows.access.items.length, 28); assert.equal(f.client.snapshot().collections.windows.access.count, 128);
  assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /cgv_|page1_|jmc1_|serialized|definition_version/);
  await f.client.changeAccess(subject(128), ['collections.read'], grant(128));
  const change = f.calls.find(c => c.route.endsWith('/mutate')).body.value.changes[0]; assert.deepEqual(change.expected, access(128).expected); assert.equal(change.grant_id, grant(128)); assert.equal(edits, 1);
});

test('entry remove and restore use only the visible immutable entry, not a filtered replacement', async () => {
  const f = fixture(); await connected(f); await f.client.changeMember(entry(2), 'remove');
  const first = f.calls.find(c => c.route.endsWith('/mutate')); assert.deepEqual(first.body.value, { changes: [{ kind: 'remove', entry_id: entry(2), asset_id: B }] });
  await f.client.collectionTab('members', 'removed'); await f.client.changeMember(entry(2), 'reactivate');
  const second = f.calls.filter(c => c.route.endsWith('/mutate'))[1]; assert.deepEqual(second.body.value, { changes: [{ kind: 'reactivate', entry_id: entry(2), asset_id: B }] });
  assert.equal(f.calls.some(c => /v2\/(select|clear)$/.test(c.route)), false);
});

test('explicit add batches give the same asset independent entries in two collections', async () => {
  const f = fixture(); await connected(f); await f.client.startPicker(); f.client.selectionToggle(A); await f.client.addPicked();
  await f.client.openCollection(D); await f.client.startPicker(); f.client.selectionToggle(A); await f.client.addPicked();
  const mutations = f.calls.filter(c => c.route.endsWith('/mutate')); assert.equal(mutations.length, 2); assert.equal(mutations[0].body.collection_id, C); assert.equal(mutations[1].body.collection_id, D);
  assert.equal(mutations[0].body.value.changes[0].asset_id, A); assert.equal(mutations[1].body.value.changes[0].asset_id, A); assert.notEqual(mutations[0].body.value.changes[0].entry_id, mutations[1].body.value.changes[0].entry_id);
});

test('collection context is one explicit bounded batch followed by full current context proof', async () => {
  const f = fixture(call => {
    if (call.route === 'v1/collections/members') return json(window('members', Array.from({ length: 65 }, (_, i) => member(i + 1))));
    if (call.route === 'v2/search') return json(assets(call.body.context_asset_ids.map(id => ({ asset_id: id, label: '<current>', head_revision_id: null, reasons: ['context'] }))));
  }); await connected(f);
  for (let i = 1; i <= 65; i++) f.client.selectionToggle('asset_' + hex(i), entry(i));
  assert.equal(f.client.snapshot().collections.selectedMembers.length, 64); const before = f.calls.length; await f.client.useCollectionContext();
  const calls = f.calls.slice(before); assert.equal(calls.filter(c => c.route === 'v2/search' && c.body.page_size === 100).length, 1); assert.equal(calls[0].body.context_asset_ids.length, 64); assert.equal(f.client.snapshot().contexts.length, 64); assert.equal(f.client.snapshot().libraryView, 'assets'); assert.ok(!calls.some(c => /select|clear/.test(c.route)));
});

test('Collections connection works without asset search and a refusal is not an empty directory', async () => {
  const f = fixture(call => call.route === 'v2/search' ? error('not_found', 404) : undefined); await connected(f, false); assert.equal(f.client.snapshot().connected, true); assert.equal(f.calls.some(c => c.route === 'v2/search'), false);
  for (const code of ['not_found', 'unavailable']) { const denied = fixture(call => call.route.startsWith('v1/collections/') ? error(code, code === 'not_found' ? 404 : 503) : undefined); await connected(denied, false); const state = denied.client.snapshot(); assert.equal(state.connected, false); assert.ok(state.collections.windows.directory.error); assert.equal(state.collections.windows.directory.canCreate, false); const before = denied.calls.length; await denied.client.createCollection('No'); assert.equal(denied.calls.length, before); await denied.client.switchView('assets'); assert.equal(denied.client.snapshot().connected, true); }
});

test('create, archive and direct rights controls follow permissions, never creator attribution', async () => {
  const viewer = summary({ creator: 'you', permissions: { read: true, edit: false, admin: true } });
  const f = fixture(call => ordinary(call, viewer)); await connected(f); assert.equal(f.client.snapshot().collections.canChange, false); await f.client.renameCollection('Denied'); await f.client.changeAccess(subject(2), rights); assert.equal(f.calls.some(c => c.route.endsWith('/mutate')), false);
  const editor = summary({ creator: 'another_authorized_person', permissions: { read: true, edit: true, admin: false } }); const e = fixture(call => ordinary(call, editor)); await connected(e); assert.equal(e.client.snapshot().collections.canChange, true); await e.client.archiveCollection(true); assert.equal(e.calls.some(c => c.route.endsWith('/mutate')), false); await e.client.collectionTab('access'); assert.equal(e.calls.some(c => c.route.endsWith('/access')), false);
  const noCreate = fixture(call => call.route.endsWith('/directory') ? json(window('directory', [], { can_create: false })) : undefined); await connected(noCreate, false); await noCreate.client.createCollection('Denied create'); assert.equal(noCreate.calls.some(c => c.route.endsWith('/mutate')), false);
  const owner = fixture(); await connected(owner, false); await owner.client.createCollection('Shared work'); const create = owner.calls.find(c => c.route.endsWith('/mutate')); assert.equal(create.body.kind, 'create'); assert.deepEqual(create.body.value.entries, []); assert.equal(create.body.expected_version, '');
  await owner.client.openCollection(C); await owner.client.archiveCollection(true); assert.equal(owner.calls.filter(c => c.route.endsWith('/mutate')).at(-1).body.value.archived, true);
});

test('uncertain collection retry has no preference expiry and fresh current CV beats historical receipt', async () => {
  let attempts = 0, current = summary();
  const f = fixture(call => {
    if (call.route === 'v1/collections/mutate') { attempts++; current = summary({ definition_version: cv(9), name: 'Current after collaborator' }); if (attempts === 1) return error('unavailable', 503); return json({ version: 1, operation_id: call.body.operation_id, collection_id: C, result_version: cv(2) }); }
    return ordinary(call, current);
  }); await connected(f); await f.client.renameCollection('Uncertain'); assert.equal(f.client.snapshot().pendingCollection, true); assert.equal(f.client.snapshot().collections.current, null);
  f.advance(10000000); assert.equal(f.client.snapshot().pending, true); assert.equal(f.client.snapshot().warning, null); await f.client.renameCollection('Must not send'); await f.client.remember(); assert.equal(attempts, 1);
  await f.client.checkActionState(); assert.equal(f.client.snapshot().pending, true); await f.client.retry(); assert.equal(f.client.snapshot().pending, false); assert.equal(f.client.snapshot().collections.current.name, 'Current after collaborator');
  const mutations = f.calls.filter(c => c.route.endsWith('/mutate')); assert.equal(mutations[1].raw, mutations[0].raw, 'exact retry must retain canonical bytes and every identity');
  await f.client.renameCollection('Next'); assert.equal(f.calls.filter(c => c.route.endsWith('/mutate')).at(-1).body.expected_version, cv(9), 'historical receipt must not install current CV');
});

test('first conflict rejects only its attempt; conflict after an uncertain receipt keeps exact retry', async () => {
  for (const uncertain of [false, true]) {
    let attempt = 0; const f = fixture(call => call.route.endsWith('/mutate') ? ++attempt === 1 && uncertain ? json({ version: 1, operation_id: 'cop_' + hex(99), collection_id: C, result_version: cv(2) }) : error('conflict', 409) : undefined);
    await connected(f); await f.client.renameCollection('Change'); if (uncertain) { await f.client.retry(); assert.equal(f.client.snapshot().pending, true); assert.equal(f.calls.filter(c => c.route.endsWith('/mutate'))[0].raw, f.calls.filter(c => c.route.endsWith('/mutate'))[1].raw); }
    else { assert.equal(f.client.snapshot().pending, false); await f.client.renameCollection('Explicit new attempt'); assert.equal(attempt, 2); }
  }
});

test('successful historical retry refreshes downgraded permissions; denied retry remains unresolved', async () => {
  for (const deny of [false, true]) {
    let attempt = 0, current = summary();
    const f = fixture(call => { if (call.route.endsWith('/mutate')) { attempt++; current = summary({ definition_version: cv(2), permissions: { read: true, edit: true, admin: false } }); return attempt === 1 ? error('unavailable', 503) : deny ? error('not_found', 404) : json({ version: 1, operation_id: call.body.operation_id, collection_id: C, result_version: cv(2) }); } return ordinary(call, current); });
    await connected(f); await f.client.collectionTab('access'); await f.client.changeAccess(subject(2), ['collections.read']); await f.client.retry();
    assert.equal(f.client.snapshot().pending, deny); assert.equal(f.client.snapshot().collections.windows.access, undefined);
    if (!deny) { assert.equal(f.client.snapshot().collections.current.permissions.admin, false); assert.equal(f.client.snapshot().collections.section, 'members'); }
  }
});

for (const kind of ['directory', 'members', 'history', 'access']) test('late A ' + kind + ' cannot publish into B even when cancellation is ignored', { concurrency: false }, async () => ignoringCancellation(async () => {
  const held = deferred(), entered = deferred(); let hold = false;
  const f = fixture(call => { if (hold && call.auth === 'Bearer ' + bearer(1) && call.route === 'v1/collections/' + kind) { entered.resolve(); return held.promise; } return ordinary(call, summary({ name: 'B current' })); }); await connected(f); hold = true;
  const old = kind === 'directory' ? f.client.collectionDirectory() : kind === 'members' ? f.client.collectionStart('members') : f.client.collectionTab(kind); await entered.promise;
  await f.client.connect(lib, bearer(2)); await f.client.openCollection(C);
  // Repeat the same scope admissions in B. A removed epoch predicate cannot
  // hide behind a coincidental per-scope generation mismatch in this oracle.
  if (kind === 'directory') await f.client.collectionDirectory(); else if (kind === 'members') await f.client.collectionStart('members'); else await f.client.collectionTab(kind);
  const secret = kind === 'directory' ? window('directory', [summary({ name: 'OLD SECRET' })]) : kind === 'members' ? window('members', [{ ...member(1), label: 'OLD SECRET' }]) : kind === 'access' ? window('access', [access(99)]) : window('history', [{ kind: 'rename', recorded_unix_nano: '1', actor: 'you', value: { name: 'OLD SECRET' } }]);
  held.resolve(json(secret)); await old; const state = f.client.snapshot(); assert.equal(state.collections.current && state.collections.current.name, 'B current', 'old connection collection response changed current state'); assert.doesNotMatch(JSON.stringify(state), /OLD SECRET|cgv_|jmc1_/); assert.ok(!JSON.stringify(state).includes(subject(99)));
}));

for (const outcome of ['receipt', 'error']) test('late A mutation ' + outcome + ' cannot settle or contaminate B connection', { concurrency: false }, async () => ignoringCancellation(async () => {
  const held = deferred(), entered = deferred(); let oldBody;
  const f = fixture(call => { if (call.route.endsWith('/mutate')) { oldBody = call.body; entered.resolve(); return held.promise; } }); await connected(f);
  const old = f.client.renameCollection('Old action'); await entered.promise; await f.client.connect(lib, bearer(2)); await f.client.openCollection(C);
  held.resolve(outcome === 'receipt' ? json({ version: 1, operation_id: oldBody.operation_id, collection_id: C, result_version: cv(2) }) : error('unauthenticated', 401)); await old;
  assert.equal(f.client.snapshot().hasConnection, true); assert.equal(f.client.snapshot().pending, false); assert.equal(f.client.snapshot().collections.current.name, 'Working set'); assert.doesNotMatch(f.client.snapshot().message, /Access ended/);
}));

test('hidden return clears collection and access names until fresh revalidation completes', async () => {
  const held = deferred(), entered = deferred(); let hold = false;
  const f = fixture(call => { if (hold && call.route === 'v1/collections/directory') { entered.resolve(); return held.promise; } }); await connected(f); await f.client.collectionTab('access'); await f.client.setActive(false);
  assert.equal(f.client.snapshot().collections.current, null); assert.deepEqual(f.client.snapshot().collections.windows, {});
  hold = true; const resume = f.client.setActive(true); await entered.promise; const waiting = f.client.snapshot();
  hold = false; held.resolve(json(window('directory', [summary()]))); await resume;
  assert.equal(waiting.collections.current, null); assert.deepEqual(waiting.collections.windows.directory.items, []); assert.equal(f.client.snapshot().collections.current.name, 'Working set');
  f.client.disconnect(); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /Working set|sub_/);
});

test('asset and collection readers share two owners and cancellation drains queued requests', async () => {
  let live = 0, peak = 0; const f = fixture(async call => { live++; peak = Math.max(peak, live); await ticks(); live--; return ordinary(call); }); await connected(f);
  await Promise.all([f.client.inspect(A), f.client.collectionStart('members'), f.client.collectionDirectory()]); assert.equal(peak, 2); assert.equal(live, 0);
  const held = deferred(), entered = deferred(); let hold = false, admitted = 0;
  const g = fixture(async call => { if (hold) { if (++admitted === 2) entered.resolve(); await held.promise; } return ordinary(call); }); await connected(g); hold = true;
  const inspect = g.client.inspect(A); await entered.promise; g.client.disconnect(); held.resolve(); await inspect; assert.equal(admitted, 2, 'canceled queued work must never acquire a transport owner'); assert.equal(g.client.snapshot().hasConnection, false);
});

test('byte-limited member pages and incomplete history remain distinct from a complete collection', async () => {
  const f = fixture(call => {
    if (call.route === 'v1/collections/members') return json(window('members', [{ ...member(1), label: '<'.repeat(4096) }], { matched_count: 2, next_page_token: token(1) }));
    if (call.route === 'v1/collections/page') return json(window('members', [{ ...member(2), label: '>'.repeat(4096) }], { matched_count: 2 }));
    if (call.route === 'v1/collections/history') return json(window('history', [], { window_complete: false }));
  }); await connected(f); assert.equal(f.client.snapshot().collections.windows.members.items[0].label.length, 4096); await f.client.collectionNext('members'); assert.equal(f.client.snapshot().collections.windows.members.items.length, 1); assert.equal(f.client.snapshot().collections.windows.members.items[0].entry_id, entry(2));
  await f.client.collectionTab('history'); assert.equal(f.client.snapshot().collections.windows.history.next, false); assert.equal(f.client.snapshot().collections.windows.history.complete, false);
});

test('asset picker keeps an explicit 64-ID selection across search pages and sends no unshown assets', async () => {
  const f = fixture(call => {
    if (call.route === 'v2/search') return json({ ...assets(Array.from({ length: 50 }, (_, i) => asset(i + 1))), matched_count: 100, next_page_token: token(99) });
    if (call.route === 'v2/page') return json({ ...assets(Array.from({ length: 50 }, (_, i) => asset(i + 51))), matched_count: 100 });
  }); await connected(f); await f.client.startPicker(); for (let i = 1; i <= 50; i++) f.client.selectionToggle('asset_' + hex(i));
  await f.client.search(undefined, true); for (let i = 51; i <= 65; i++) f.client.selectionToggle('asset_' + hex(i)); assert.equal(f.client.snapshot().collections.pickedAssets.length, 64);
  await f.client.addPicked(); const change = f.calls.find(c => c.route === 'v1/collections/mutate'); assert.equal(change.body.value.changes.length, 64); assert.deepEqual(change.body.value.changes.map(e => e.asset_id), Array.from({ length: 64 }, (_, i) => 'asset_' + hex(i + 1)));
  assert.equal(new Set(change.body.value.changes.map(e => e.entry_id)).size, 64); assert.equal(f.calls.some(c => /^v2\/(select|clear)$/.test(c.route)), false);
});

test('current access edits share, remove and reactivate separate entries without collapsing a person', async () => {
  let removed = false;
  const f = fixture(call => {
    if (call.route === 'v1/collections/access') return json(window('access', [access(1), { ...access(2, { removed }), expected: { ...access(2).expected, removed, value: { ...access(2).expected.value, subject_id: subject(1), actions: ['collections.read'] } } }]));
    if (call.route === 'v1/collections/mutate' && call.body.kind === 'grants') { const c = call.body.value.changes[0]; if (c.grant_id === grant(2)) removed = c.kind === 'remove'; return ordinary(call); }
  }); await connected(f); await f.client.collectionTab('access'); assert.equal(f.client.snapshot().collections.windows.access.items.filter(x => x.subject_id === subject(1)).length, 2);
  await f.client.changeAccess(subject(1), ['collections.read'], grant(2), true); const removal = f.calls.filter(c => c.route.endsWith('/mutate')).at(-1).body.value.changes[0]; assert.equal(removal.kind, 'remove'); assert.equal(removal.value, null); assert.equal(removal.expected.removed, false); assert.equal(removal.grant_id, grant(2));
  await f.client.changeAccess(subject(1), ['collections.read'], grant(2)); const restoration = f.calls.filter(c => c.route.endsWith('/mutate')).at(-1).body.value.changes[0]; assert.equal(restoration.kind, 'put'); assert.equal(restoration.expected.removed, true); assert.equal(restoration.grant_id, grant(2)); assert.equal(restoration.value.subject_id, subject(1));
  await f.client.changeAccess(subject(3), ['collections.edit', 'collections.read']); const shared = f.calls.filter(c => c.route.endsWith('/mutate')).at(-1).body.value.changes[0]; assert.equal(shared.expected, null); assert.notEqual(shared.grant_id, grant(1)); assert.notEqual(shared.grant_id, grant(2)); assert.deepEqual(shared.value.actions, ['collections.edit', 'collections.read']);
});

test('late collection open, member filter and hidden view cannot restore superseded state', { concurrency: false }, async () => ignoringCancellation(async () => {
  for (const scenario of ['open', 'filter', 'view']) {
    const held = deferred(), entered = deferred(); let hold = false;
    const f = fixture(call => { if (hold && (scenario === 'open' ? call.route === 'v1/collections/open' && call.body.collection_id === C : call.route === 'v1/collections/members' && call.body.state === 'active')) { entered.resolve(); return held.promise; } });
    await connected(f); hold = true;
    const old = scenario === 'open' ? f.client.openCollection(C) : f.client.collectionStart('members'); await entered.promise;
    if (scenario === 'open') await f.client.openCollection(D); else if (scenario === 'filter') await f.client.collectionTab('members', 'removed'); else await f.client.switchView('assets');
    held.resolve(scenario === 'open' ? json({ version: 1, kind: 'collection', collection: summary({ name: 'OLD OPEN' }) }) : json(window('members', [{ ...member(1), label: 'OLD MEMBER' }]))); await old;
    assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD OPEN|OLD MEMBER/);
    if (scenario === 'open') assert.equal(f.client.snapshot().collections.current.collection_id, D); else if (scenario === 'filter') assert.equal(f.client.snapshot().collections.memberState, 'removed'); else assert.equal(f.client.snapshot().collections.current, null);
  }
}));

test('collection 401 teardown forgets exact bytes and leaves only an honest nonsensitive notice', async () => {
  const f = fixture(call => call.route.endsWith('/mutate') ? error('unauthenticated', 401) : undefined); await connected(f); await f.client.renameCollection('Private new name');
  const state = f.client.snapshot(); assert.equal(state.hasConnection, false); assert.equal(state.pending, false); assert.equal(state.collections.current, null); assert.deepEqual(state.collections.windows, {}); assert.match(state.message, /may have been accepted/); assert.doesNotMatch(JSON.stringify(state), /Private new name|Working set|cop_|cgv_/);
});

test('settled receipt and first conflict refresh chains yield to newer user intent', { concurrency: false }, async () => ignoringCancellation(async () => {
  for (const outcome of ['receipt', 'first-conflict']) for (const intent of ['assets', 'directory-and-D', 'directory-refresh']) {
    const held = deferred(), entered = deferred(); let armed = false, heldOnce = false, original;
    const current = target => summary({ collection_id: target || C, name: target === D ? 'Chosen D' : 'Settled C', definition_version: cv(2) });
    const f = fixture(call => {
      if (call.route === 'v1/collections/mutate') { armed = true; return outcome === 'receipt' ? json({ version: 1, operation_id: call.body.operation_id, collection_id: C, result_version: cv(2) }) : error('conflict', 409); }
      if (armed && !heldOnce && call.route === 'v1/collections/directory') { heldOnce = true; entered.resolve(); return held.promise; }
      return ordinary(call, current(call.body.collection_id));
    });
    try {
      await connected(f); original = f.client.renameCollection('Requested C'); await entered.promise;
      assert.equal(f.client.snapshot().pending, false, 'matching receipt or first rejection must settle before its refresh');
      if (intent === 'assets') await f.client.switchView('assets');
      else { await f.client.collectionDirectory('all'); if (intent === 'directory-and-D') await f.client.openCollection(D); }
      const before = f.client.snapshot(), opens = f.calls.filter(c => c.route === 'v1/collections/open').length;
      held.resolve(json(window('directory', [summary({ name: 'OLD SETTLEMENT DIRECTORY' })]))); await original;
      const after = f.client.snapshot();
      assert.equal(f.calls.filter(c => c.route === 'v1/collections/open').length, opens, 'superseded settlement must not admit another collection open');
      assert.equal(after.pending, false, 'new intent must not undo a settled receipt or first rejection');
      assert.equal(after.libraryView, before.libraryView); assert.deepEqual(after.collections.current, before.collections.current, 'superseded settlement must not replace the chosen collection');
      assert.equal(after.message, before.message, 'superseded settlement must not overwrite newer status');
      assert.doesNotMatch(JSON.stringify(after), /OLD SETTLEMENT DIRECTORY/);
      if (intent === 'assets') assert.equal(after.collections.current, null);
      if (intent === 'directory-and-D') assert.equal(after.collections.current.collection_id, D);
    } finally { held.resolve(json(window('directory', [current(C)]))); if (original) await original; f.client.disconnect(); }
  }
}));

test('settlement open completion cannot overwrite a newer explicit refresh status', { concurrency: false }, async () => ignoringCancellation(async () => {
  const held = deferred(), entered = deferred(); let armed = false, heldOnce = false, original, current = summary();
  const f = fixture(call => {
    if (call.route === 'v1/collections/mutate') { armed = true; current = summary({ name: 'Receipt-era C', definition_version: cv(2) }); return json({ version: 1, operation_id: call.body.operation_id, collection_id: C, result_version: cv(2) }); }
    if (armed && call.route === 'v1/collections/open') {
      if (!heldOnce) { heldOnce = true; entered.resolve(); return held.promise; }
      current = summary({ name: 'New explicit C', definition_version: cv(3) });
    }
    return ordinary(call, current);
  });
  try {
    await connected(f); original = f.client.renameCollection('Requested C'); await entered.promise;
    assert.equal(f.client.snapshot().pending, false);
    await f.client.openCollection(C); const before = f.client.snapshot(), reads = f.calls.length;
    held.resolve(json({ version: 1, kind: 'collection', collection: summary({ name: 'OLD SETTLEMENT OPEN', definition_version: cv(2) }) })); await original;
    const after = f.client.snapshot();
    assert.equal(f.calls.length, reads, 'superseded settlement open must not admit more section reads');
    assert.equal(after.collections.current.name, 'New explicit C'); assert.equal(after.pending, false);
    assert.equal(after.message, before.message, 'superseded settlement completion must not restore old status');
    assert.doesNotMatch(JSON.stringify(after), /OLD SETTLEMENT OPEN/);
  } finally { held.resolve(json({ version: 1, kind: 'collection', collection: summary() })); if (original) await original; f.client.disconnect(); }
}));

test('terminal collection windows reject first-page and continuation truncation including incomplete history', async () => {
  for (const kind of ['directory', 'members', 'access', 'history']) for (const stage of ['first', 'continuation']) {
    const item = n => kind === 'directory' ? summary({ collection_id: n === 1 ? C : D, name: 'Truncated directory ' + n }) : kind === 'members' ? { ...member(n), label: 'Truncated member ' + n } : kind === 'access' ? access(n) : { kind: 'rename', recorded_unix_nano: String(n), actor: 'you', value: { name: 'Truncated history ' + n } };
    const response = (items, next) => window(kind, items, { matched_count: stage === 'first' ? 2 : 3, next_page_token: next, window_complete: kind !== 'history' });
    let armed = false;
    const f = fixture(call => {
      if (!armed) return undefined;
      if (call.route === 'v1/collections/' + kind) return json(response([item(1)], stage === 'first' ? null : token(1)));
      if (call.route === 'v1/collections/page') return json(response([item(2)], null));
    });
    try {
      await connected(f); armed = true;
      if (kind === 'directory') await f.client.backToCollections(); else if (kind === 'members') await f.client.collectionStart('members'); else await f.client.collectionTab(kind);
      if (stage === 'continuation') {
        assert.equal(f.client.snapshot().collections.windows[kind].next, true, 'nonterminal progress must remain readable');
        await f.client.collectionNext(kind);
      }
      const state = f.client.snapshot(), result = state.collections.windows[kind];
      assert.match(result.error, /unreadable/, 'terminal row deficit must reject the frozen window');
      assert.deepEqual(result.items, [], 'terminal row deficit must discard previously displayed rows');
      assert.equal(result.next, false); assert.equal(state.collections.current, null);
      assert.doesNotMatch(JSON.stringify(state), /Truncated directory|Truncated member|Truncated history/);
    } finally { f.client.disconnect(); }
  }
});

test('incomplete history may end only after all rows of its bounded window', async () => {
  const first = { kind: 'rename', recorded_unix_nano: '1', actor: 'you', value: { name: 'First bounded event' } };
  const second = { kind: 'archive', recorded_unix_nano: '2', actor: 'another_authorized_person', value: { archived: false } };
  const f = fixture(call => call.route === 'v1/collections/history' ? json(window('history', [first], { matched_count: 2, next_page_token: token(1), window_complete: false })) : call.route === 'v1/collections/page' ? json(window('history', [second], { matched_count: 2, window_complete: false })) : undefined);
  try {
    await connected(f); await f.client.collectionTab('history'); assert.equal(f.client.snapshot().collections.windows.history.next, true);
    await f.client.collectionNext('history'); const result = f.client.snapshot().collections.windows.history;
    assert.equal(result.error, ''); assert.deepEqual(result.items, [second]); assert.equal(result.count, 2); assert.equal(result.next, false); assert.equal(result.complete, false); assert.equal(f.client.snapshot().collections.current.collection_id, C);
  } finally { f.client.disconnect(); }
});

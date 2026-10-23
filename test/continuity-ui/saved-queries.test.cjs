'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createLibraryClient, createTransport, validate, savedCanonical } = require('../../internal/manager/static/library.js');
const hex = (n, w = 32) => n.toString(16).padStart(w, '0');
const L = 'lib_' + hex(1), Q = 'col_' + hex(2), Q2 = 'col_' + hex(3), A = 'asset_' + hex(1), B = 'asset_' + hex(2);
const cv = n => 'cv_' + hex(n), op = n => 'cop_' + hex(n), grant = n => 'cgr_' + hex(n), gv = n => 'cgv_' + hex(n), person = n => 'sub_' + hex(n), page = n => 'page1_' + hex(n, 64), sel = n => 'sel1_' + hex(n, 64), bearer = n => 'jmc1_' + hex(n) + '.' + hex(n, 64);
const rights = ['collections.admin', 'collections.edit', 'collections.read'];
const D = { version: 1, kind: 'ranked_assets', text: ' Alpha ', context_asset_ids: [A] };
const summary = (extra = {}) => ({ collection_id: Q, kind: 'saved_query', name: 'Shared context', archived: false, definition_version: cv(1), creator: 'you', permissions: { read: true, edit: true, admin: true }, ...extra });
const result = (assetID, collectionID = Q, label = assetID === A ? 'Alpha' : 'Alphabet') => ({ result_id: { collection_id: collectionID, asset_id: assetID }, asset_id: assetID, label, head_revision_id: null, reasons: ['label_prefix'] });
const roster = (n = 1, actions = rights) => ({ grant_id: grant(n), expected: { value: { subject_id: person(n), scope: { kind: 'collection', collection_id: Q }, actions: actions.slice() }, version: gv(n), removed: false } });
const event = (extra = {}) => ({ kind: 'create', recorded_unix_nano: '1800000000000000000', actor: 'you', value: { name: 'Shared context', definition: D }, ...extra });
const recent = (assetID = A) => ({ asset_id: assetID, label: assetID === A ? 'Alpha' : 'Alphabet', head_revision_id: null, reasons: ['declared_selection'], selected_unix: '1800000000', expires_unix: '1802592000' });
const window = (kind, items, extra = {}) => ({ version: 2, kind, collection: kind === 'directory' ? null : summary(), ...(kind === 'directory' ? { can_create: true } : {}), ...(kind === 'execute' ? { query_version: cv(1) } : {}), items, matched_count: items.length, count_scope: 'frozen_window', window_complete: true, next_page_token: null, sort_version: kind === 'execute' ? 'discovery2' : 'saved_queries2', snapshot_scope: 'frozen_snapshot', ...extra });
const recents = (items = [recent()], extra = {}) => ({ version: 2, kind: 'recents', items, matched_count: items.length, count_scope: 'frozen_authorized_recent', window_complete: true, next_page_token: null, sort_version: 'recents1', snapshot_scope: 'frozen_snapshot', ...extra });
const search = (items = [{ asset_id: A, label: 'Alpha', head_revision_id: null, reasons: ['stable_id'] }, { asset_id: B, label: 'Alphabet', head_revision_id: null, reasons: ['stable_id'] }]) => ({ version: 2, kind: 'search', items, matched_count: items.length, count_scope: 'frozen_window', window_complete: true, next_page_token: null, sort_version: 'discovery2' });
const go = v => JSON.stringify(v).replace(/[<>&\u2028\u2029]/g, c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'));
const json = (value, status = 200) => new Response(go(value), { status, headers: { 'content-type': 'application/json' } });
const error = (code, status = 404) => json({ version: 1, error: code }, status);
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
async function ticks() { for (let i = 0; i < 24; i++) await Promise.resolve(); }
function fixture(handler, base = '/manager') {
  const calls = []; let n = 30, now = 1800000000, current = summary(), preference = null;
  function ordinary(call) {
    const route = call.route;
    if (route === 'v2/collections/directory') return json(window('directory', [current]));
    if (route === 'v2/collections/open') return json({ version: 2, kind: 'collection', collection: { ...current, collection_id: call.body.collection_id } });
    if (route === 'v2/collections/definition') return json({ version: 2, kind: 'definition', collection: current, definition: D });
    if (route === 'v2/collections/execute') return json(window('execute', [result(A, current.collection_id), result(B, current.collection_id)], { collection: current, query_version: current.definition_version }));
    if (route === 'v2/collections/history') return json(window('history', [event()], { collection: current }));
    if (route === 'v2/collections/access') return json(window('access', [roster()], { collection: current }));
    if (route === 'v2/collections/mutate') { current = summary({ collection_id: call.body.collection_id, definition_version: cv(2) }); return json({ version: 2, operation_id: call.body.operation_id, collection_id: call.body.collection_id, result_version: cv(2) }); }
    if (route === 'v2/recents/list') return json(recents());
    if (route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: preference });
    if (route === 'v2/select' || route === 'v2/clear') { preference = route.endsWith('/select') ? sel(1) : null; return json({ version: 2, operation_id: call.body.operation_id, kind: route.split('/')[1], selection_token: preference, receipt_scope: 'operation_result' }); }
    if (route === 'v1/asset') return json({ version: 1, asset: { asset_id: call.body.asset_id, label: call.body.asset_id === A ? 'Alpha' : 'Alphabet', head_revision_id: null, reasons: ['stable_id'] } });
    if (route === 'v1/counts') return json({ version: 1, asset_id: call.body.asset_id, revision_count: null, relationship_count: null, location_count: null });
    if (route.startsWith('v1/')) return json({ ...search([]), version: 1, kind: route.slice(3), sort_version: 'discovery1' });
    return json(search());
  }
  const client = createLibraryClient({ base, now: () => now, crypto: { getRandomValues(bytes) { bytes.fill(++n); return bytes; } }, fetch: async (url, init) => {
    const call = { route: url.split('/continuity/')[1], body: JSON.parse(init.body), raw: init.body, auth: init.headers.Authorization, init, url }; calls.push(call);
    return handler && await handler(call, ordinary) || ordinary(call);
  } });
  return { client, calls, advance(seconds) { now += seconds; }, setCurrent(value) { current = value; } };
}
async function connected(f, view = 'saved', open = true) { await f.client.setActive(true); await f.client.switchView(view); await f.client.connect(L, bearer(1)); if (view === 'saved' && open) await f.client.openSavedQuery(Q); }
async function ignoringAbort(run) { const Original = globalThis.AbortController; globalThis.AbortController = class extends Original { abort() {} }; try { await run(); } finally { globalThis.AbortController = Original; } }
const request = (kind, value) => ({ version: 2, library_id: L, operation_id: op(1), collection_id: Q, expected_version: kind === 'create' ? '' : cv(1), kind, value });

test('saved mutation independent exact literals preserve context/text and Go escaping', () => {
  const prefix = '{"version":2,"library_id":"lib_00000000000000000000000000000001","operation_id":"cop_00000000000000000000000000000001","collection_id":"col_00000000000000000000000000000002","expected_version":';
  assert.equal(savedCanonical(request('create', { name: 'Work', creator_grant_id: grant(1), definition: { version: 1, kind: 'ranked_assets', text: ' <>&\u2028😀 ', context_asset_ids: [A, B] } })), prefix + '"","kind":"create","value":{"name":"Work","creator_grant_id":"cgr_00000000000000000000000000000001","definition":{"version":1,"kind":"ranked_assets","text":" \\u003c\\u003e\\u0026\\u2028😀 ","context_asset_ids":["asset_00000000000000000000000000000001","asset_00000000000000000000000000000002"]}}}');
  assert.equal(savedCanonical(request('replace_definition', { definition: { version: 1, kind: 'ranked_assets', text: '', context_asset_ids: [] } })), prefix + '"cv_00000000000000000000000000000001","kind":"replace_definition","value":{"definition":{"version":1,"kind":"ranked_assets","text":"","context_asset_ids":[]}}}');
  assert.equal(savedCanonical(request('rename', { name: 'Renamed' })), prefix + '"cv_00000000000000000000000000000001","kind":"rename","value":{"name":"Renamed"}}');
  assert.equal(savedCanonical(request('archive', { archived: true })), prefix + '"cv_00000000000000000000000000000001","kind":"archive","value":{"archived":true}}');
  const grantChange = { kind: 'remove', grant_id: grant(1), expected: roster().expected, value: null };
  assert.equal(savedCanonical(request('grants', { changes: [grantChange] })), prefix + '"cv_00000000000000000000000000000001","kind":"grants","value":{"changes":[{"kind":"remove","grant_id":"cgr_00000000000000000000000000000001","expected":{"value":{"subject_id":"sub_00000000000000000000000000000001","scope":{"kind":"collection","collection_id":"col_00000000000000000000000000000002"},"actions":["collections.admin","collections.edit","collections.read"]},"version":"cgv_00000000000000000000000000000001","removed":false},"value":null}]}}');
});

test('saved definition codec rejects unknown family, unsorted/duplicate/65 anchors and invalid scalar bytes', () => {
  for (const value of [{ ...D, context_asset_ids: [B, A] }, { ...D, context_asset_ids: [A, A] }, { ...D, context_asset_ids: Array.from({ length: 65 }, (_, i) => 'asset_' + hex(i + 1)) }, { ...D, private: true }, { ...D, text: '\ud800' }, { ...D, text: '\u0000' }, { ...D, text: '😀'.repeat(65) }, { ...D, kind: 'manual' }, { ...D, context_asset_ids: null }]) assert.throws(() => savedCanonical(request('replace_definition', { definition: value })), /Check/);
  assert.throws(() => savedCanonical(request('entries', { changes: [] })), /Check/);
  assert.throws(() => savedCanonical({ ...request('rename', { name: 'x' }), version: 1 }), /Check/);
  assert.doesNotThrow(() => savedCanonical(request('replace_definition', { definition: { ...D, text: '😀'.repeat(64), context_asset_ids: Array.from({ length: 64 }, (_, i) => 'asset_' + hex(i + 1)) } })));
});

test('saved/recents DTO unions preserve exact fields, typed identities and decimal precision', () => {
  for (const [route, dto] of [['open', { version: 2, kind: 'collection', collection: summary() }], ['definition', { version: 2, kind: 'definition', collection: summary(), definition: D }], ['directory', window('directory', [summary()])], ['execute', window('execute', [result(A)])], ['history', window('history', [event(), event({ kind: 'replace_definition', value: { definition: D } }), event({ kind: 'rename', value: { name: 'x' } }), event({ kind: 'archive', value: { archived: true } })])], ['access', window('access', [roster()])], ['mutate', { version: 2, operation_id: op(1), collection_id: Q, result_version: cv(2) }]]) assert.equal(validate('v2/collections/' + route, dto), dto);
  assert.equal(validate('v2/recents/list', recents()).items[0].selected_unix, '1800000000');
  for (const dto of [window('execute', [result(A, Q2)]), window('execute', [{ ...result(A), result_id: { collection_id: Q, asset_id: B } }]), window('execute', [result(A)], { query_version: cv(2) }), window('directory', [summary({ kind: 'manual' })]), window('history', [event({ value: { name: 'secret', definition: { ...D, context_asset_ids: null } } })]), window('history', [event({ recorded_unix_nano: '9223372036854775808' })]), window('history', []), { ...window('directory', []), extra: 1 }]) assert.throws(() => validate('v2/collections/page', dto), /unreadable/);
  for (const dto of [recents([{ ...recent(), selection_token: sel(1) }]), recents([{ ...recent(), subject_id: person(2) }]), recents([{ ...recent(), selected_unix: 1800000000 }]), recents([{ ...recent(), expires_unix: '1802592001' }]), recents([{ ...recent(), selected_unix: '01800000000' }]), recents([], { window_complete: false }), recents([], { matched_count: 257 }), recents([], { sort_version: 'discovery2' }), recents([{ ...recent(), reasons: ['context'] }])]) assert.throws(() => validate('v2/recents/page', dto), /unreadable/);
  assert.throws(() => validate('v1/collections/open', { version: 1, kind: 'collection', collection: summary() }), /unreadable/);
});

for (const base of ['', '/manager']) test('every saved query and recents transport route has exact prefix and closed DTO at ' + (base || 'root'), async () => {
  const vectors = { directory: window('directory', []), open: { version: 2, kind: 'collection', collection: summary() }, definition: { version: 2, kind: 'definition', collection: summary(), definition: D }, execute: window('execute', []), history: window('history', [event()]), access: window('access', []), page: window('execute', []), mutate: { version: 2, operation_id: op(1), collection_id: Q, result_version: cv(2) } };
  for (const [kind, dto] of Object.entries(vectors)) {
    let called = 0; const send = createTransport(base, { fetch: async (url, init) => { called++; assert.equal(url, base + '/api/continuity/v2/collections/' + kind); assert.equal(init.headers.Authorization, 'Bearer ' + bearer(1)); assert.equal(init.credentials, 'omit'); assert.equal(init.redirect, 'error'); return json(dto); } });
    await send('v2/collections/' + kind, '{}', bearer(1)); assert.equal(called, 1);
  }
  for (const kind of ['list', 'page']) await createTransport(base, { fetch: async url => { assert.equal(url, base + '/api/continuity/v2/recents/' + kind); return json(recents([])); } })('v2/recents/' + kind, '{}', bearer(1));
  await assert.rejects(createTransport(base)('v2/collections/members', '{}', bearer(1)), /Check/);
  await assert.rejects(createTransport(base)('v2/recents/search', '{}', bearer(1)), /Check/);
});

test('new family stream cap, duplicate JSON keys, partial bodies and noncanonical responses fail closed', async () => {
  let reads = 0, cancelled = false;
  const huge = { ok: true, redirected: false, headers: new Headers({ 'content-type': 'application/json' }), body: { getReader: () => ({ read: async () => { reads++; return { done: false, value: new Uint8Array(524288) }; }, cancel: async () => { cancelled = true; }, releaseLock() {} }) } };
  await assert.rejects(createTransport('', { fetch: async () => huge })('v2/recents/list', '{}', ''), /unreadable/); assert.equal(reads, 3); assert.equal(cancelled, true);
  for (const raw of [go(recents()).slice(0, -1), go(recents()).replace('"version":2', '"version":2,"version":2'), go(recents()).replace('"version":2', '"version":2.0'), go(recents()) + '\n']) await assert.rejects(createTransport('', { fetch: async () => new Response(raw, { headers: { 'content-type': 'application/json' } }) })('v2/recents/list', '{}', ''), /unreadable/);
});

test('Save search copies only visible chosen context and never sends Remember', async () => {
  const f = fixture(); await connected(f, 'assets', false); await f.client.context({ asset_id: A, label: 'Visible Alpha' }); await f.client.search(' Alpha '); await f.client.startSavedCreate('search');
  assert.deepEqual(f.client.snapshot().saved.editor.anchors, [{ asset_id: A, label: 'Visible Alpha' }]); assert.equal(f.client.snapshot().saved.editor.text, ' Alpha ');
  f.client.updateSavedDraft('name', 'My context'); await f.client.saveSavedDraft();
  const call = f.calls.find(x => x.route === 'v2/collections/mutate'); assert.equal(call.body.version, 2); assert.equal(call.body.kind, 'create'); assert.deepEqual(call.body.value.definition, D); assert.equal(call.body.value.name, 'My context'); assert.equal(call.body.expected_version, ''); assert.equal(Object.hasOwn(call.body.value, 'entries'), false); assert.equal(f.calls.some(x => /v2\/(select|clear)$/.test(x.route)), false);
});

test('definition editor preserves deliberate text/anchors and Cancel writes nothing', async () => {
  const f = fixture(); await connected(f); await f.client.editSavedDefinition(); assert.deepEqual(f.client.snapshot().saved.editor.anchors, [{ asset_id: A, label: 'Alpha' }]);
  f.client.updateSavedDraft('text', '  changed  '); await f.client.savedDirectory(); assert.equal(f.client.snapshot().saved.editor.text, '  changed  ');
  f.client.cancelSavedEdit(); assert.equal(f.client.snapshot().saved.editor, null); assert.equal(f.calls.some(x => x.route.endsWith('/mutate')), false);
  await f.client.editSavedDefinition(); f.client.updateSavedDraft('text', 'new'); await f.client.saveSavedDraft(); const mutation = f.calls.find(x => x.route.endsWith('/mutate')); assert.equal(mutation.body.kind, 'replace_definition'); assert.equal(mutation.body.expected_version, cv(1)); assert.deepEqual(mutation.body.value.definition, { ...D, text: 'new' });
});

test('summary survives unavailable definition and explicit blank whole-definition repair uses summary CAS', async () => {
  const f = fixture(call => call.route === 'v2/collections/definition' || call.route === 'v2/collections/execute' || call.route === 'v2/collections/history' ? error('not_found') : undefined); await connected(f); await f.client.savedTab('definition');
  const state = f.client.snapshot(); assert.equal(state.saved.current.name, 'Shared context'); assert.equal(state.saved.definition, null); assert.deepEqual(state.saved.anchors, []); assert.equal(state.saved.editor, null);
  await f.client.editSavedDefinition(true); assert.equal(f.client.snapshot().saved.editor.mode, 'repair'); assert.deepEqual(f.client.snapshot().saved.editor.anchors, []); assert.equal(f.client.snapshot().saved.editor.text, '');
  f.client.updateSavedDraft('text', 'repair'); await f.client.saveSavedDraft(); const mutation = f.calls.find(x => x.route.endsWith('/mutate')); assert.equal(mutation.body.expected_version, cv(1)); assert.deepEqual(mutation.body.value, { definition: { version: 1, kind: 'ranked_assets', text: 'repair', context_asset_ids: [] } }); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /partial definition/);
});

for (const permissions of [{ read: true, edit: false, admin: false }, { read: true, edit: true, admin: false }]) test('saved query current permissions bound authoring and administration ' + JSON.stringify(permissions), async () => {
  const f = fixture(); f.setCurrent(summary({ creator: 'another_authorized_person', permissions })); await connected(f);
  await f.client.archiveSaved(true); await f.client.changeSavedAccess(person(2), ['collections.read']); await f.client.savedTab('access'); assert.equal(f.calls.some(x => x.route.endsWith('/access') || x.body.kind === 'archive' || x.body.kind === 'grants'), false);
  await f.client.renameSaved('Changed'); assert.equal(f.calls.filter(x => x.route.endsWith('/mutate')).length, permissions.edit ? 1 : 0);
});

test('saved access delta retains full per-entry CAS and independent same-person grants', async () => {
  const f = fixture(call => call.route === 'v2/collections/access' ? json(window('access', [roster(1), { ...roster(2), expected: { ...roster(2).expected, value: { ...roster(2).expected.value, subject_id: person(1) } } }])) : undefined); await connected(f); await f.client.savedTab('access');
  assert.equal(f.client.snapshot().saved.windows.access.items.length, 2); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /cgv_|page1_|jmc1_|definition_version/);
  await f.client.changeSavedAccess(person(1), ['collections.read'], grant(2), true); const change = f.calls.find(x => x.route.endsWith('/mutate')).body.value.changes[0]; assert.equal(change.grant_id, grant(2)); assert.equal(change.kind, 'remove'); assert.equal(change.value, null); assert.deepEqual(change.expected, { ...roster(2).expected, value: { ...roster(2).expected.value, subject_id: person(1) } });
});

test('Continue work uses real own Recents, exact asset resume, and no read-driven activity', async () => {
  const f = fixture(); await connected(f, 'continue', false); assert.equal(f.calls.some(x => x.route === 'v2/recents/list'), true); assert.equal(f.calls.some(x => x.route === 'v2/search'), false);
  assert.deepEqual(f.client.snapshot().recents.items.map(x => x.asset_id), [A]); await f.client.resumeRecent(A); assert.equal(f.client.snapshot().selected, A); assert.equal(f.client.snapshot().inspector, 'ready'); assert.equal(f.calls.some(x => /v2\/(select|clear)$/.test(x.route)), false);
  await f.client.remember(); assert.equal(f.calls.some(x => x.route === 'v2/select'), true); assert.equal(f.calls.at(-1).route, 'v2/recents/list');
});

test('dynamic execution revalidates the selected typed pair across reorder and clears a missing asset', async () => {
  let stage = 0; const f = fixture(call => call.route === 'v2/collections/execute' ? json(window('execute', stage === 0 ? [result(A), result(B)] : stage === 1 ? [result(B), result(A, Q, 'Alpha current')] : [result(B)])) : undefined);
  await connected(f); await f.client.inspectSavedResult(Q, A); assert.deepEqual(f.client.snapshot().saved.selected, { collection_id: Q, asset_id: A }); stage = 1; await f.client.executeSaved(); assert.equal(f.client.snapshot().selected, A); assert.equal(f.client.snapshot().saved.windows.execute.items[0].asset_id, B);
  stage = 2; await f.client.executeSaved(); assert.equal(f.client.snapshot().selected, ''); assert.equal(f.client.snapshot().saved.selected, null); assert.match(f.client.snapshot().message, /no longer in this query/); assert.equal(f.calls.some(x => x.body.kind === 'entries'), false);
});

for (const kind of ['directory', 'execute', 'history', 'access', 'recents']) test('typed traversal rejects terminal deficits/cycles/duplicates for ' + kind, async () => {
  for (const flaw of ['deficit', 'cycle', 'duplicate']) {
    let armed = false; const item = kind === 'directory' ? summary() : kind === 'execute' ? result(A) : kind === 'history' ? event() : kind === 'access' ? roster() : recent();
    const second = flaw === 'duplicate' ? item : kind === 'directory' ? summary({ collection_id: Q2 }) : kind === 'execute' ? result(B) : kind === 'history' ? event({ kind: 'rename', value: { name: 'Second' } }) : kind === 'access' ? roster(2) : recent(B);
    const value = (items, next) => kind === 'recents' ? recents(items, { matched_count: 3, next_page_token: next }) : window(kind, items, { matched_count: 3, next_page_token: next });
    const f = fixture(call => { if (!armed) return undefined; if (call.route === 'v2/collections/' + kind || kind === 'recents' && call.route === 'v2/recents/list') return json(value([item], page(1))); if (call.route === 'v2/collections/page' || call.route === 'v2/recents/page') return json(value([second], flaw === 'cycle' ? page(1) : flaw === 'duplicate' ? page(2) : null)); });
    await connected(f, kind === 'recents' ? 'continue' : 'saved', kind !== 'directory' && kind !== 'recents'); armed = true;
    if (kind === 'directory') await f.client.backToSaved(); else if (kind === 'recents') await f.client.recentSelections(); else await f.client.savedTab(kind);
    if (kind === 'recents') await f.client.recentSelections(true); else await f.client.savedNext(kind);
    const w = kind === 'recents' ? f.client.snapshot().recents : f.client.snapshot().saved.windows[kind];
    if (kind === 'history' && flaw === 'duplicate') { assert.equal(w.error, '', 'equal history items remain distinct source rows'); assert.equal(w.next, true); }
    else { assert.match(w.error, /unreadable/); assert.deepEqual(w.items, []); assert.equal(w.next, false); }
  }
});

test('saved query continuation keeps exact query-kind/id and history does not deduplicate equal rows', async () => {
  const f = fixture(call => call.route === 'v2/collections/history' ? json(window('history', [event()], { matched_count: 2, next_page_token: page(1) })) : call.route === 'v2/collections/page' ? json(window('history', [event()], { matched_count: 2 })) : undefined); await connected(f); await f.client.savedTab('history'); await f.client.savedNext('history');
  const call = f.calls.find(x => x.route === 'v2/collections/page'); assert.deepEqual(call.body, { version: 2, library_id: L, query_kind: 'history', collection_id: Q, token: page(1) }); assert.equal(f.client.snapshot().saved.windows.history.page, 2); assert.equal(f.client.snapshot().saved.windows.history.count, 2); assert.equal(f.client.snapshot().saved.windows.history.items.length, 1);
});

test('uncertain query retry retains identical bytes and fresh CAS without preference expiry', async () => {
  let attempts = 0; const f = fixture(call => { if (call.route === 'v2/collections/mutate') return ++attempts === 1 ? error('unavailable', 503) : json({ version: 2, operation_id: call.body.operation_id, collection_id: Q, result_version: cv(2) }); }); await connected(f); await f.client.renameSaved('Private requested name'); assert.equal(f.client.snapshot().pendingSavedQuery, true);
  await f.client.renameSaved('Must not replace'); assert.equal(attempts, 1); f.advance(10000000); assert.equal(f.client.snapshot().pendingSavedQuery, true, 'saved query retry must outlive the personal action expiry window'); assert.equal(f.client.snapshot().warning, null); f.setCurrent(summary({ name: 'Current collaborator', definition_version: cv(9) })); await f.client.checkActionState(); assert.equal(f.client.snapshot().pending, true); assert.equal(f.client.snapshot().saved.current.name, 'Current collaborator'); await f.client.retry();
  const calls = f.calls.filter(x => x.route.endsWith('/mutate')); assert.equal(calls.length, 2); assert.equal(calls[0].raw, calls[1].raw); assert.equal(f.client.snapshot().pending, false); assert.equal(f.client.snapshot().saved.current.name, 'Current collaborator');
  await f.client.renameSaved('Next explicit'); assert.equal(f.calls.filter(x => x.route.endsWith('/mutate')).at(-1).body.expected_version, cv(9));
});

for (const kind of ['execute', 'definition', 'recents']) test('late old connection ' + kind + ' is discarded even when abort is ignored', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, first;
  const f = fixture(call => { if (armed && call.auth === 'Bearer ' + bearer(1) && call.route === (kind === 'recents' ? 'v2/recents/list' : 'v2/collections/' + kind)) { entered.resolve(); return held.promise; } });
  await connected(f, kind === 'recents' ? 'continue' : 'saved'); armed = true;
  try {
    first = kind === 'recents' ? f.client.recentSelections() : kind === 'definition' ? f.client.readDefinition() : f.client.executeSaved(); await entered.promise; await f.client.connect(L, bearer(2)); if (kind !== 'recents') await f.client.openSavedQuery(Q);
    // Re-admit the same scope in B to match A's generation deliberately.
    // The old response must be rejected by connection identity, not luck.
    if (kind === 'recents') await f.client.recentSelections(); else if (kind === 'definition') await f.client.readDefinition(); else await f.client.executeSaved();
    const old = kind === 'recents' ? recents([{ ...recent(), label: 'OLD PRIVATE' }]) : kind === 'definition' ? { version: 2, kind: 'definition', collection: summary(), definition: { ...D, text: 'OLD PRIVATE' } } : window('execute', [result(A, Q, 'OLD PRIVATE')]);
    held.resolve(json(old)); await first; assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD PRIVATE|jmc1_|page1_|cgv_/);
  } finally { held.resolve(json(kind === 'recents' ? recents([]) : window('execute', []))); if (first) await first; f.client.disconnect(); }
}));

test('hide and cross-view changes discard draft, rows and late query reads', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, reading;
  const f = fixture(call => armed && call.route === 'v2/collections/execute' ? (entered.resolve(), held.promise) : undefined); await connected(f); await f.client.editSavedDefinition(); f.client.updateSavedDraft('text', 'Private draft'); await f.client.setActive(false); assert.equal(f.client.snapshot().saved.editor, null); assert.equal(f.client.snapshot().saved.current, null);
  await f.client.setActive(true); armed = true;
  try { reading = f.client.executeSaved(); await entered.promise; await f.client.switchView('assets'); held.resolve(json(window('execute', [result(A, Q, 'OLD VIEW')]))); await reading; assert.equal(f.client.snapshot().libraryView, 'assets'); assert.equal(f.client.snapshot().saved.current, null); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD VIEW|Private draft/); }
  finally { held.resolve(json(window('execute', []))); if (reading) await reading; f.client.disconnect(); }
}));

test('new query/recents/inspector families share two owners and a bounded waiter queue', async () => {
  let live = 0, peak = 0; const f = fixture(async (call, ordinary) => { live++; peak = Math.max(peak, live); await ticks(); live--; return ordinary(call); }); await connected(f);
  await Promise.all([f.client.inspect(A), f.client.savedDirectory(), f.client.savedTab('history'), f.client.recentSelections()]); assert.equal(peak, 2); assert.equal(live, 0);
  const held = deferred(), entered = deferred(); let armed = false, count = 0;
  const g = fixture(async (call, ordinary) => { if (armed) { if (++count === 2) entered.resolve(); await held.promise; } return ordinary(call); }); await connected(g); armed = true;
  const pending = [g.client.inspect(A), ...Array.from({ length: 30 }, () => g.client.savedDirectory())]; await entered.promise; g.client.disconnect(); held.resolve(); await Promise.all(pending); assert.equal(count, 2, 'cancelled waiters must never acquire a new transport'); assert.equal(g.client.snapshot().hasConnection, false);
});

test('saved mutation reply cannot settle a new credential or retarget a newer view', { concurrency: false }, async () => ignoringAbort(async () => {
  for (const change of ['credential', 'view']) {
    const held = deferred(), entered = deferred(); let requestBody, writing;
    const f = fixture(call => { if (call.route === 'v2/collections/mutate') { requestBody = call.body; entered.resolve(); return held.promise; } }); await connected(f);
    try { writing = f.client.renameSaved('Old action'); await entered.promise; if (change === 'credential') await f.client.connect(L, bearer(2)); else await f.client.switchView('continue'); const calls = f.calls.length; held.resolve(json({ version: 2, operation_id: requestBody.operation_id, collection_id: Q, result_version: cv(2) })); await writing; assert.equal(f.calls.length, calls, 'old mutation settlement must not admit a fresh open'); assert.equal(f.client.snapshot().libraryView, change === 'credential' ? 'saved' : 'continue'); assert.equal(f.client.snapshot().pending, false); }
    finally { held.resolve(error('unavailable', 503)); if (writing) await writing; f.client.disconnect(); }
  }
}));

test('static UI places inspector outside Assets and uses inert accessible authoring controls', () => {
  const fs = require('node:fs'), path = require('node:path'); const html = fs.readFileSync(path.join(__dirname, '../../internal/manager/static/index.html'), 'utf8'), js = fs.readFileSync(path.join(__dirname, '../../internal/manager/static/library.js'), 'utf8');
  for (const name of ['saved', 'continue']) assert.match(html, new RegExp('id="library-view-' + name + '"'));
  const start = html.lastIndexOf('<section', html.indexOf('id="library-assets-panel"')); let depth = 0, end = -1; const tags = /<\/?section\b[^>]*>/g; tags.lastIndex = start; let tag; while ((tag = tags.exec(html))) { depth += tag[0].startsWith('</') ? -1 : 1; if (!depth) { end = tags.lastIndex; break; } }
  assert.ok(end > start && html.indexOf('aria-labelledby="library-inspector-title"') > end, 'query/Recents resume inspector must be outside the hidden Assets section');
  assert.match(js, /retained up to 30 days; not included in catalog exports/); assert.match(js, /This replaces the entire query and ranking context/); assert.match(js, /Saving a query does not Remember/); assert.match(js, /input\.addEventListener\('input'/); assert.match(js, /setSelectionRange/);
  assert.doesNotMatch(js, /\.innerHTML\s*=|localStorage|sessionStorage|eval\(/);
});

test('each new family enforces its own full-window, page and label cap', () => {
  for (const [kind, cap] of [['directory', 1024], ['execute', 1000], ['history', 1000], ['access', 128], ['recents', 256]]) {
    const make = extra => kind === 'recents' ? recents([recent()], extra) : window(kind, [kind === 'directory' ? summary() : kind === 'execute' ? result(A) : kind === 'history' ? event() : roster()], extra);
    const route = kind === 'recents' ? 'v2/recents/list' : 'v2/collections/' + kind;
    assert.doesNotThrow(() => validate(route, make({ matched_count: cap })));
    assert.throws(() => validate(route, make({ matched_count: cap + 1 })), /unreadable/);
    assert.throws(() => validate(route, make({ items: Array.from({ length: 101 }, () => make({}).items[0]), matched_count: Math.max(101, cap) })), /unreadable/);
  }
  for (const label of ['x'.repeat(4097), '\ud800']) { assert.throws(() => validate('v2/collections/execute', window('execute', [result(A, Q, label)])), /unreadable/); assert.throws(() => validate('v2/recents/list', recents([{ ...recent(), label }])), /unreadable/); }
});

test('explicit anchor picker refuses the 65th choice and preserves all64 chosen labels across async renders', async () => {
  const values = Array.from({ length: 70 }, (_, i) => ({ asset_id: 'asset_' + hex(i + 1), label: 'Chosen ' + (i + 1), head_revision_id: null, reasons: ['stable_id'] }));
  const f = fixture(call => call.route === 'v2/search' ? json(search(values)) : undefined); await connected(f, 'saved', false); await f.client.startSavedCreate(); f.client.updateSavedDraft('name', 'My explicit context'); f.client.updateSavedDraft('text', 'local draft survives'); await f.client.pickSavedAnchors();
  for (const value of values.slice(0, 65)) f.client.chooseSavedAnchor(value.asset_id);
  assert.equal(f.client.snapshot().saved.editor.anchors.length, 64); assert.match(f.client.snapshot().message, /limited to 64/); assert.equal(f.client.snapshot().saved.editor.anchors[63].label, 'Chosen 64'); assert.equal(f.client.snapshot().saved.editor.anchors.some(x => x.asset_id === values[64].asset_id), false);
  await f.client.savedDirectory('all'); assert.equal(f.client.snapshot().saved.editor.name, 'My explicit context'); assert.equal(f.client.snapshot().saved.editor.text, 'local draft survives'); f.client.finishSavedPicker(); await f.client.saveSavedDraft();
  const mutation = f.calls.find(x => x.route.endsWith('/mutate')); assert.deepEqual(mutation.body.value.definition.context_asset_ids, values.slice(0, 64).map(x => x.asset_id)); assert.equal(f.calls.some(x => /v2\/(select|clear)$/.test(x.route)), false);
});

test('manual context authoring requires explicit bounded selection and does not copy membership', async () => {
  const manual = { ...summary(), kind: 'manual', name: 'Manual source' };
  const members = Array.from({ length: 70 }, (_, i) => ({ entry_id: 'cent_' + hex(i + 1), asset_id: 'asset_' + hex(i + 1), label: 'Manual ' + (i + 1), head_revision_id: null, removed: false }));
  const f = fixture(call => {
    if (call.route === 'v1/collections/directory') return json({ version: 1, kind: 'directory', collection: null, can_create: true, items: [manual], matched_count: 1, count_scope: 'frozen_window', window_complete: true, next_page_token: null, sort_version: 'collections1', snapshot_scope: 'frozen_snapshot' });
    if (call.route === 'v1/collections/open') return json({ version: 1, kind: 'collection', collection: manual });
    if (call.route === 'v1/collections/members') return json({ version: 1, kind: 'members', collection: manual, state: 'active', items: members, matched_count: 70, count_scope: 'frozen_visible_membership', window_complete: true, next_page_token: null, sort_version: 'collections1', snapshot_scope: 'frozen_membership_current_page' });
  }); await connected(f, 'collections', false); await f.client.openCollection(Q);
  await f.client.startSavedCreate('manual'); assert.equal(f.client.snapshot().saved.editor, null); assert.match(f.client.snapshot().message, /explicitly/);
  f.client.selectionToggle(A, 'cent_' + hex(1)); f.client.selectionToggle(B, 'cent_' + hex(2)); await f.client.startSavedCreate('manual'); assert.deepEqual(f.client.snapshot().saved.editor.anchors, [{ asset_id: A, label: 'Manual 1' }, { asset_id: B, label: 'Manual 2' }]); f.client.updateSavedDraft('name', 'Explicit two'); await f.client.saveSavedDraft();
  const mutation = f.calls.find(x => x.route === 'v2/collections/mutate'); assert.deepEqual(mutation.body.value.definition.context_asset_ids, [A, B]); assert.equal(Object.hasOwn(mutation.body.value, 'entries'), false); assert.equal(f.calls.some(x => x.route === 'v1/collections/mutate' || /v2\/(select|clear)$/.test(x.route)), false);
});

test('fresh execution adopts a fresh authorized query version but never mixes its continuation baseline', async () => {
  let changed = false;
  const f = fixture(call => { if (changed && call.route === 'v2/collections/execute') return json(window('execute', [result(B)], { collection: summary({ definition_version: cv(2), name: 'Collaborator current' }), query_version: cv(2), matched_count: 2, next_page_token: page(1) })); if (call.route === 'v2/collections/page') return json(window('execute', [result(A)], { matched_count: 2 })); }); await connected(f); changed = true; await f.client.executeSaved();
  assert.equal(f.client.snapshot().saved.current.name, 'Collaborator current'); assert.equal(f.client.snapshot().saved.windows.execute.items[0].asset_id, B); await f.client.savedNext('execute'); assert.match(f.client.snapshot().saved.windows.execute.error, /unreadable/); assert.deepEqual(f.client.snapshot().saved.windows.execute.items, []);
});

test('late personal acknowledgement cannot reopen its earlier asset after a newer exact selection', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let operationID, writing;
  const f = fixture(call => { if (call.route === 'v2/select') { operationID = call.body.operation_id; entered.resolve(); return held.promise; } }); await connected(f); await f.client.inspectSavedResult(Q, A);
  try { writing = f.client.remember(); await entered.promise; await f.client.inspect(B); const reads = f.calls.length; held.resolve(json({ version: 2, operation_id: operationID, kind: 'select', selection_token: sel(1), receipt_scope: 'operation_result' })); await writing; assert.equal(f.calls.length, reads, 'older action must not admit status/search/inspect for its old asset'); assert.equal(f.client.snapshot().selected, B); assert.equal(f.client.snapshot().statusFresh, true, 'older receipt must not erase the newer selection status'); assert.equal(f.client.snapshot().pending, false); }
  finally { held.resolve(error('unavailable', 503)); if (writing) await writing; f.client.disconnect(); }
}));

test('settled query refresh yields to a newer non-navigating intent even if its open ignores abort', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, writing;
  const f = fixture((call, ordinary) => { if (call.route === 'v2/collections/mutate') { armed = true; return ordinary(call); } if (armed && call.route === 'v2/collections/open') { entered.resolve(); return held.promise; } }); await connected(f);
  try { writing = f.client.renameSaved('Requested'); await entered.promise; assert.equal(f.client.snapshot().pending, false); await f.client.status(); const reads = f.calls.length; held.resolve(json({ version: 2, kind: 'collection', collection: summary({ name: 'OLD SETTLEMENT' }) })); await writing; assert.equal(f.calls.length, reads, 'superseded settlement open must not admit an execute after a newer status intent'); assert.equal(f.client.snapshot().saved.current, null); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD SETTLEMENT/); }
  finally { held.resolve(error('not_found')); if (writing) await writing; f.client.disconnect(); }
}));

test('query-result Remember and Clear refresh order and restore only the original typed selection after fresh execute', async () => {
  let remembered = false;
  const f = fixture(call => {
    if (call.route === 'v2/collections/execute') return json(window('execute', remembered ? [{ ...result(B), reasons: ['declared_selection'] }, result(A)] : [result(A), result(B)]));
    if (call.route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: call.body.asset_id === B && remembered ? sel(9) : null });
    if (call.route === 'v2/select' || call.route === 'v2/clear') { remembered = call.route === 'v2/select'; return json({ version: 2, operation_id: call.body.operation_id, kind: remembered ? 'select' : 'clear', selection_token: remembered ? sel(9) : null, receipt_scope: 'operation_result' }); }
  });
  try {
    await connected(f); await f.client.inspectSavedResult(Q, B);
    const initialExecutes = f.calls.filter(x => x.route === 'v2/collections/execute').length;
    await f.client.remember();
    let state = f.client.snapshot();
    assert.equal(f.calls.filter(x => x.route === 'v2/collections/execute').length, initialExecutes + 1, 'query-result Remember must admit one fresh execute');
    assert.deepEqual(state.saved.windows.execute.items.map(x => x.asset_id), [B, A]);
    assert.deepEqual(state.saved.selected, { collection_id: Q, asset_id: B }); assert.equal(state.selected, B); assert.equal(state.inspector, 'ready'); assert.equal(state.statusFresh, true); assert.equal(state.preference, true);
    await f.client.clear(); state = f.client.snapshot();
    assert.equal(f.calls.filter(x => x.route === 'v2/collections/execute').length, initialExecutes + 2, 'query-result Clear must admit one fresh execute');
    assert.deepEqual(state.saved.windows.execute.items.map(x => x.asset_id), [A, B]);
    assert.deepEqual(state.saved.selected, { collection_id: Q, asset_id: B }); assert.equal(state.selected, B); assert.equal(state.statusFresh, true); assert.equal(state.preference, false); assert.equal(state.pending, false);
    const writes = f.calls.filter(x => /v2\/(select|clear)$/.test(x.route));
    assert.deepEqual(writes.map(x => [x.route, x.body.asset_id, x.body.expected_token]), [['v2/select', B, null], ['v2/clear', B, sel(9)]]);
    assert.equal(f.calls.some(x => x.route === 'v2/collections/mutate' || x.route.startsWith('v2/recents/')), false);
    for (const mutation of writes) {
      const later = f.calls.slice(f.calls.indexOf(mutation) + 1), execute = later.findIndex(x => x.route === 'v2/collections/execute'), inspector = later.findIndex(x => x.route === 'v1/asset' && x.body.asset_id === B), preference = later.findIndex(x => x.route === 'v2/preference' && x.body.asset_id === B);
      assert.ok(execute >= 0 && inspector > execute && preference > execute, 'fresh execution must precede restoring inspector/current preference for the original pair');
    }
    assert.doesNotMatch(JSON.stringify(state), /sel1_|jmc1_|page1_|serialized/);
  } finally { f.client.disconnect(); }
});

for (const action of ['remember', 'clear']) test('query-result ' + action + ' clears the original pair when fresh execution no longer admits it', async () => {
  let absent = false;
  const f = fixture(call => {
    if (call.route === 'v2/collections/execute') return json(window('execute', absent ? [result(A)] : [result(A), result(B)]));
    if (call.route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: action === 'clear' && !absent ? sel(9) : null });
    if (call.route === 'v2/select' || call.route === 'v2/clear') { absent = true; return json({ version: 2, operation_id: call.body.operation_id, kind: action === 'remember' ? 'select' : 'clear', selection_token: action === 'remember' ? sel(9) : null, receipt_scope: 'operation_result' }); }
  });
  try {
    await connected(f); await f.client.inspectSavedResult(Q, B); const before = f.calls.length;
    await f.client[action](); const state = f.client.snapshot(), later = f.calls.slice(before), execute = later.findIndex(x => x.route === 'v2/collections/execute');
    assert.equal(later.filter(x => x.route === 'v2/collections/execute').length, 1, 'successful query activity must reexecute even though dispatch intentionally cleared selected');
    assert.deepEqual(state.saved.windows.execute.items.map(x => x.asset_id), [A]); assert.equal(state.saved.selected, null); assert.equal(state.selected, ''); assert.equal(state.statusFresh, false); assert.equal(state.pending, false); assert.match(state.message, /no longer in this query/);
    assert.equal(later.slice(execute + 1).some(x => x.body.asset_id === B && ['v1/asset', 'v2/preference'].includes(x.route)), false, 'missing original pair must never restore an inspector or preference read');
  } finally { f.client.disconnect(); }
});

for (const intent of ['view', 'selection']) test('held post-activity execute yields to newer ' + intent + ' intent without old follow-on reads', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, writing;
  const f = fixture(call => {
    if (call.route === 'v2/select') { armed = true; return json({ version: 2, operation_id: call.body.operation_id, kind: 'select', selection_token: sel(9), receipt_scope: 'operation_result' }); }
    if (armed && call.route === 'v2/collections/execute') { entered.resolve(); return held.promise; }
  });
  try {
    await connected(f); await f.client.inspectSavedResult(Q, B); writing = f.client.remember();
    const barrier = await Promise.race([entered.promise.then(() => 'entered'), writing.then(() => 'completed')]);
    assert.equal(barrier, 'entered', 'matching query activity must admit its fresh execute before settling');
    assert.equal(f.client.snapshot().pending, false);
    if (intent === 'view') await f.client.switchView('continue'); else await f.client.inspect(A);
    const reads = f.calls.length;
    held.resolve(json(window('execute', [result(B, Q, 'OLD ACTION RESULT'), result(A)]))); await writing;
    assert.equal(f.calls.length, reads, 'superseded activity execution must not admit original asset/status follow-ons');
    const state = f.client.snapshot(); assert.equal(state.libraryView, intent === 'view' ? 'continue' : 'saved'); if (intent === 'selection') assert.equal(state.selected, A);
    assert.doesNotMatch(JSON.stringify(state), /OLD ACTION RESULT/); assert.equal(state.pending, false);
  } finally { held.resolve(error('not_found')); if (writing) await writing; f.client.disconnect(); }
}));

for (const stage of ['definition-error', 'definition-success', 'anchor-error', 'anchor-success']) test('blank repair owns its draft against older ' + stage + ' continuation', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, reading;
  const f = fixture(call => {
    if (armed && stage.startsWith('definition') && call.route === 'v2/collections/definition') { entered.resolve(); return held.promise; }
    if (armed && stage.startsWith('anchor') && call.route === 'v1/asset' && call.body.asset_id === A) { entered.resolve(); return held.promise; }
  });
  try {
    await connected(f); armed = true; reading = f.client.savedTab('definition'); await entered.promise;
    await f.client.editSavedDefinition(true); f.client.updateSavedDraft('text', 'Deliberate new repair'); await f.client.pickSavedAnchors(); f.client.chooseSavedAnchor(B); f.client.finishSavedPicker();
    const newer = f.client.snapshot().saved.editor;
    assert.deepEqual(newer, { mode: 'repair', name: 'Shared context', text: 'Deliberate new repair', anchors: [{ asset_id: B, label: 'Alphabet' }] });
    const reads = f.calls.length;
    held.resolve(stage.endsWith('error') ? error('not_found') : stage.startsWith('definition') ? json({ version: 2, kind: 'definition', collection: summary(), definition: { ...D, text: 'OLD DEFINITION' } }) : json({ version: 1, asset: { asset_id: A, label: 'OLD ANCHOR LABEL', head_revision_id: null, reasons: ['stable_id'] } }));
    await reading;
    assert.equal(f.calls.length, reads, 'superseded definition chain must admit no old summary or anchor-label follow-on');
    const state = f.client.snapshot(); assert.deepEqual(state.saved.editor, newer, 'older definition/error must not erase or populate the deliberate blank repair'); assert.equal(state.saved.current.name, 'Shared context'); assert.equal(state.saved.definition, null); assert.deepEqual(state.saved.anchors, []); assert.doesNotMatch(JSON.stringify(state), /OLD DEFINITION|OLD ANCHOR LABEL/);
    await f.client.saveSavedDraft();
    const mutation = f.calls.find(x => x.route === 'v2/collections/mutate'); assert.equal(mutation.body.expected_version, cv(1), 'repair must retain its original fresh summary CAS'); assert.deepEqual(mutation.body.value, { definition: { version: 1, kind: 'ranked_assets', text: 'Deliberate new repair', context_asset_ids: [B] } });
  } finally { held.resolve(error('not_found')); if (reading) await reading; f.client.disconnect(); }
}));

for (const origin of ['create', 'edit']) test('saved-query ' + origin + ' picker Remember and Clear refresh ordinary status/search while preserving the deliberate editor', async () => {
  let preference = null;
  const f = fixture(call => {
    if (call.route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: preference });
    if (call.route === 'v2/select' || call.route === 'v2/clear') { preference = call.route === 'v2/select' ? sel(9) : null; return json({ version: 2, operation_id: call.body.operation_id, kind: call.route === 'v2/select' ? 'select' : 'clear', selection_token: preference, receipt_scope: 'operation_result' }); }
  });
  try {
    await connected(f, 'saved', origin === 'edit');
    if (origin === 'create') await f.client.startSavedCreate(); else await f.client.editSavedDefinition();
    f.client.updateSavedDraft('name', 'Deliberate ' + origin); f.client.updateSavedDraft('text', '  Keep this exact editor text  ');
    await f.client.pickSavedAnchors(); await f.client.search('Al'); if (origin === 'create') f.client.chooseSavedAnchor(A);
    const editor = f.client.snapshot().saved.editor;
    assert.deepEqual(editor, { mode: origin === 'create' ? 'create' : 'replace', name: 'Deliberate ' + origin, text: '  Keep this exact editor text  ', anchors: [{ asset_id: A, label: 'Alpha' }] });
    await f.client.inspect(B);
    assert.equal(f.calls.some(x => /v2\/(select|clear)$/.test(x.route)), false, 'picker search, anchor choice and inspection must not record activity');
    const start = f.calls.length;
    for (const action of ['remember', 'clear']) {
      const before = f.calls.length;
      await f.client[action]();
      const state = f.client.snapshot(), later = f.calls.slice(before), route = action === 'remember' ? 'v2/select' : 'v2/clear';
      assert.equal(state.pending, false); assert.equal(state.selected, B); assert.equal(state.inspector, 'ready');
      assert.equal(state.statusFresh, true, 'ordinary picker activity must refresh current PreferenceState'); assert.equal(state.preference, action === 'remember');
      assert.deepEqual(state.saved.editor, editor, 'ordinary picker activity must preserve deliberate name, text and chosen anchors'); assert.equal(state.saved.picker, true); assert.equal(state.saved.selected, null);
      assert.equal(later.filter(x => x.route === route).length, 1); assert.equal(later.filter(x => x.route === 'v2/preference' && x.body.asset_id === B).length, 2, 'preflight and post-receipt current status must both address the inspected picker asset');
      const write = later.findIndex(x => x.route === route), refreshed = later.findIndex((x, i) => i > write && x.route === 'v2/preference'), searched = later.findIndex(x => x.route === 'v2/search');
      assert.ok(write >= 0 && refreshed > write && searched > refreshed, 'matching picker activity must refresh current status before its search');
      assert.equal(later.filter(x => x.route === 'v2/search').length, 1, 'ordinary picker activity must refresh exactly its picker search'); assert.equal(later[searched].body.text, 'Al');
      assert.deepEqual(state.items.map(x => x.asset_id), [A, B], 'picker results must remain usable after activity');
      f.client.chooseSavedAnchor(B); assert.deepEqual(f.client.snapshot().saved.editor.anchors, [{ asset_id: A, label: 'Alpha' }, { asset_id: B, label: 'Alphabet' }]);
      f.client.chooseSavedAnchor(B); assert.deepEqual(f.client.snapshot().saved.editor, editor, 'refreshed picker must still add/remove explicit anchors without changing the editor');
    }
    const later = f.calls.slice(start), writes = later.filter(x => /v2\/(select|clear)$/.test(x.route));
    assert.deepEqual(writes.map(x => [x.route, x.body.asset_id, x.body.expected_token]), [['v2/select', B, null], ['v2/clear', B, sel(9)]], 'Clear must use the current token read after Remember');
    assert.equal(later.some(x => x.route === 'v2/collections/execute' || x.route.endsWith('/mutate') || x.route.startsWith('v2/recents/')), false, 'ordinary picker activity must not execute/mutate a query or refresh another view');
    assert.equal(f.calls.filter(x => /v2\/(select|clear)$/.test(x.route)).length, 2, 'only the two explicit personal actions may write activity');
    assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /sel1_|jmc1_|page1_|serialized|pickerDraft/);
  } finally { f.client.disconnect(); }
});

for (const stage of ['status', 'search']) test('held ordinary picker ' + stage + ' yields to newer ' + (stage === 'status' ? 'view' : 'status') + ' intent without old follow-on state', { concurrency: false }, async () => ignoringAbort(async () => {
  const held = deferred(), entered = deferred(); let armed = false, heldOnce = false, writing;
  const f = fixture(call => {
    if (call.route === 'v2/select') { armed = true; return json({ version: 2, operation_id: call.body.operation_id, kind: 'select', selection_token: sel(9), receipt_scope: 'operation_result' }); }
    if (armed && !heldOnce && (stage === 'status' ? call.route === 'v2/preference' && call.body.asset_id === B : call.route === 'v2/search')) { heldOnce = true; entered.resolve(); return held.promise; }
    if (call.route === 'v2/preference') return json({ version: 2, asset_id: call.body.asset_id, selection_token: armed && call.body.asset_id === B ? sel(9) : null });
  });
  try {
    await connected(f, 'saved', false); await f.client.startSavedCreate(); f.client.updateSavedDraft('name', 'Owned picker'); f.client.updateSavedDraft('text', 'Keep deliberate text');
    await f.client.pickSavedAnchors(); await f.client.search('Al'); f.client.chooseSavedAnchor(A); await f.client.inspect(B);
    const editor = f.client.snapshot().saved.editor; writing = f.client.remember();
    assert.equal(await Promise.race([entered.promise.then(() => 'entered'), writing.then(() => 'completed')]), 'entered', 'matching picker activity must admit its owned ' + stage + ' before settling');
    assert.equal(f.client.snapshot().mutationBusy, stage === 'status'); assert.equal(f.client.snapshot().pending, false);
    if (stage === 'status') await f.client.switchView('continue'); else await f.client.status();
    const state = f.client.snapshot(), reads = f.calls.length;
    if (stage === 'status') { assert.equal(state.libraryView, 'continue'); assert.ok(state.recents); assert.equal(state.mutationBusy, false); }
    else { assert.equal(state.selected, B); assert.equal(state.statusFresh, true); assert.equal(state.preference, true); assert.deepEqual(state.saved.editor, editor); }
    held.resolve(stage === 'status' ? json({ version: 2, asset_id: B, selection_token: sel(8) }) : json(search([{ asset_id: A, label: 'OLD PICKER SEARCH', head_revision_id: null, reasons: ['stable_id'] }]))); await writing;
    assert.equal(f.calls.length, reads, 'cancelled picker settlement must admit no old follow-on reads');
    assert.deepEqual(f.client.snapshot(), state, 'late picker settlement must leave the newer view/status and deliberate editor exact'); assert.doesNotMatch(JSON.stringify(f.client.snapshot()), /OLD PICKER SEARCH/);
  } finally { held.resolve(error('not_found')); if (writing) await writing; f.client.disconnect(); }
}));

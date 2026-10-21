/* Library uses its own scoped bearer and never the Manager admin transport.
 * Browser credentials, cursors and retry bodies live only in this closure. */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.ManagerLibrary = { init: api.init, setActive: api.setActive };
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';
  const MAX_BODY = 8 * 1024 * 1024;
  // Qualified server accounting: each new collection reserves 21 MiB plus a
  // shared 4 MiB base. Three builds exceed 64 MiB; two shared reads avoid that
  // self-induced fanout, without promising capacity across other clients.
  const READ_LIMIT = 2;
  const encoder = new TextEncoder();
  const ids = { asset: /^asset_(?!0{32}$)[0-9a-f]{32}$/, library: /^lib_(?!0{32}$)[0-9a-f]{32}$/, revision: /^rev1_[0-9a-f]{64}$/, selection: /^sel1_[0-9a-f]{64}$/, page: /^page1_[0-9a-f]{64}$/ };
  const messages = {
    invalid: 'Check the request and try again.', unauthenticated: 'Access expired or was revoked. Connect again.',
    not_found: 'Unavailable with this access.', unavailable: 'Library is unavailable. Try again later.',
    restart_query: 'This result expired or changed. Run a fresh search.', conflict: 'This attempt conflicts with current state or an earlier request.',
    expired_request: 'The original action expired. Its earlier outcome may still be unknown.',
    work_budget: 'This query exceeds the bounded search window. Narrow your query or context.',
    busy: 'Library is busy. Try again.', deadline: 'The request timed out. Try again.',
    transport: 'The request could not be completed.', invalid_response: 'Library returned an unreadable response.',
    context: 'Context could not be fully revalidated. Retry to show fresh results.'
  };
  class LibraryError extends Error { constructor(code) { super(messages[code] || messages.unavailable); this.code = code; } }
  function requireValue(ok) { if (!ok) throw new LibraryError('invalid_response'); }
  function object(v, keys) { return v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === keys.length && keys.every(k => Object.prototype.hasOwnProperty.call(v, k)); }
  function text(v, max = 4096) { return typeof v === 'string' && !v.includes('\u0000') && encoder.encode(v).length <= max; }
  function id(v, kind) { return typeof v === 'string' && ids[kind].test(v); }
  function nullableRevision(v) { return v === null || id(v, 'revision'); }
  function integer(v) { return typeof v === 'number' && Number.isFinite(v) && Number.isInteger(v) && v >= 0; }
  function safeCount(v) { return Number.isSafeInteger(v) && v >= 0 && v <= 1000; }
  const reasons = new Set(['context', 'declared_selection', 'label_exact', 'label_prefix', 'label_contains', 'stable_id']);
  function asset(v) {
    return object(v, ['asset_id', 'label', 'head_revision_id', 'reasons']) && id(v.asset_id, 'asset') && text(v.label) && nullableRevision(v.head_revision_id) && Array.isArray(v.reasons) && v.reasons.length <= 6 && v.reasons.every(r => reasons.has(r));
  }
  function revision(v) {
    return object(v, ['revision_id', 'asset_id', 'content', 'parents']) && id(v.revision_id, 'revision') && id(v.asset_id, 'asset') && object(v.content, ['sha256', 'size']) && typeof v.content.sha256 === 'string' && /^[0-9a-f]{64}$/.test(v.content.sha256) && integer(v.content.size) && Array.isArray(v.parents) && v.parents.length <= 64 && v.parents.every(p => id(p, 'revision'));
  }
  function edge(v, removed = false) {
    const keys = ['relationship_id', 'source_asset_id', 'source_revision_id', 'target_asset_id', 'target_revision_id', 'predicate', 'provenance', 'source'];
    if (removed) keys.push('removed');
    return object(v, keys) && text(v.relationship_id, 128) && id(v.source_asset_id, 'asset') && id(v.target_asset_id, 'asset') && nullableRevision(v.source_revision_id) && nullableRevision(v.target_revision_id) && text(v.predicate, 128) && ['claimed', 'observed', 'inferred'].includes(v.provenance) && text(v.source, 1024) && (!removed || typeof v.removed === 'boolean');
  }
  function location(v, removed = false) {
    const keys = ['location_id', 'asset_id', 'revision_id', 'source', 'locator'];
    if (removed) keys.push('removed');
    return object(v, keys) && text(v.location_id, 128) && id(v.asset_id, 'asset') && nullableRevision(v.revision_id) && text(v.source, 1024) && text(v.locator, 8192) && (!removed || typeof v.removed === 'boolean');
  }
  function history(v) {
    if (!object(v, ['kind', 'recorded_unix_nano', 'actor_claim', 'before', 'after']) || !integer(v.recorded_unix_nano) || !text(v.actor_claim, 1024)) return false;
    const label = x => object(x, ['asset_id', 'label']) && id(x.asset_id, 'asset') && text(x.label);
    const head = x => object(x, ['asset_id', 'revision_id']) && id(x.asset_id, 'asset') && nullableRevision(x.revision_id);
    switch (v.kind) {
      case 'create_asset': return v.before === null && label(v.after);
      case 'rename_asset': return label(v.before) && label(v.after);
      case 'record_revision': return v.before === null && revision(v.after);
      case 'advance_head': return head(v.before) && head(v.after);
      case 'put_relationship': return (v.before === null || edge(v.before, true)) && edge(v.after, true);
      case 'remove_relationship': return edge(v.before, true) && edge(v.after, true);
      case 'put_location': return (v.before === null || location(v.before, true)) && location(v.after, true);
      case 'remove_location': return location(v.before, true) && location(v.after, true);
      default: return false;
    }
  }
  function validate(route, v) {
    if (route.startsWith('v1/collections/')) return validateCollection(route.slice(15), v);
    const [versionPart, kind] = route.split('/'); const version = Number(versionPart.slice(1));
    if (kind === 'select' || kind === 'clear') {
      requireValue(object(v, ['version', 'operation_id', 'kind', 'selection_token', 'receipt_scope']) && v.version === 2 && text(v.operation_id, 128) && v.kind === kind && v.receipt_scope === 'operation_result' && (kind === 'select' ? id(v.selection_token, 'selection') : v.selection_token === null));
    } else if (kind === 'preference') {
      requireValue(object(v, ['version', 'asset_id', 'selection_token']) && v.version === 2 && id(v.asset_id, 'asset') && (v.selection_token === null || id(v.selection_token, 'selection')));
    } else if (kind === 'asset') {
      requireValue(object(v, ['version', 'asset']) && v.version === 1 && asset(v.asset));
    } else if (kind === 'counts') {
      requireValue(object(v, ['version', 'asset_id', 'revision_count', 'relationship_count', 'location_count']) && v.version === 1 && id(v.asset_id, 'asset') && ['revision_count', 'relationship_count', 'location_count'].every(k => v[k] === null || Number.isSafeInteger(v[k]) && v[k] >= 0 && v[k] <= 8192));
    } else {
      requireValue(object(v, ['version', 'kind', 'items', 'matched_count', 'count_scope', 'window_complete', 'next_page_token', 'sort_version']) && v.version === version && ['search', 'revisions', 'relationships', 'locations', 'history'].includes(v.kind) && (kind === 'page' || v.kind === kind) && v.sort_version === 'discovery' + version && v.count_scope === 'frozen_window' && safeCount(v.matched_count) && typeof v.window_complete === 'boolean' && (v.next_page_token === null || id(v.next_page_token, 'page')) && Array.isArray(v.items) && v.items.length <= 1000 && v.items.length <= v.matched_count);
      const check = { search: asset, revisions: revision, relationships: edge, locations: location, history }[v.kind];
      requireValue(v.items.every(x => check(x)));
    }
    return v;
  }
  const collectionIDs = { collection: /^col_(?!0{32}$)[0-9a-f]{32}$/, entry: /^cent_(?!0{32}$)[0-9a-f]{32}$/, grant: /^cgr_(?!0{32}$)[0-9a-f]{32}$/, operation: /^cop_(?!0{32}$)[0-9a-f]{32}$/, subject: /^sub_(?!0{32}$)[0-9a-f]{32}$/, version: /^cv_(?!0{32}$)[0-9a-f]{32}$/, grantVersion: /^cgv_(?!0{32}$)[0-9a-f]{32}$/ };
  const collectionActions = ['collections.admin', 'collections.edit', 'collections.read'];
  const collectionCaps = { directory: 1024, members: 8192, access: 128, history: 1000 };
  function cid(value, kind) { return typeof value === 'string' && collectionIDs[kind].test(value); }
  function scalar(value) {
    if (typeof value !== 'string') return false;
    for (let i = 0; i < value.length; i++) {
      const c = value.charCodeAt(i);
      if (c >= 0xd800 && c <= 0xdbff) { const n = value.charCodeAt(++i); if (!(n >= 0xdc00 && n <= 0xdfff)) return false; }
      else if (c >= 0xdc00 && c <= 0xdfff) return false;
    }
    return true;
  }
  function collectionText(value, max, nonempty = false) { return scalar(value) && text(value, max) && (!nonempty || value.length > 0); }
  function goJSON(value) { return JSON.stringify(value).replace(/[<>&\u2028\u2029]/g, c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0')); }
  function summary(value) {
    return object(value, ['collection_id', 'kind', 'name', 'archived', 'definition_version', 'creator', 'permissions']) && cid(value.collection_id, 'collection') && value.kind === 'manual' && collectionText(value.name, 256, true) && typeof value.archived === 'boolean' && cid(value.definition_version, 'version') && ['you', 'another_authorized_person'].includes(value.creator) && object(value.permissions, ['read', 'edit', 'admin']) && ['read', 'edit', 'admin'].every(k => typeof value.permissions[k] === 'boolean');
  }
  function grantValue(value, collectionID) {
    return object(value, ['subject_id', 'scope', 'actions']) && cid(value.subject_id, 'subject') && object(value.scope, ['kind', 'collection_id']) && value.scope.kind === 'collection' && cid(value.scope.collection_id, 'collection') && (!collectionID || value.scope.collection_id === collectionID) && Array.isArray(value.actions) && value.actions.length > 0 && value.actions.length <= 3 && value.actions.every((a, i) => collectionActions.includes(a) && (!i || value.actions[i - 1] < a));
  }
  function expectedGrant(value, collectionID) { return object(value, ['value', 'version', 'removed']) && grantValue(value.value, collectionID) && cid(value.version, 'grantVersion') && typeof value.removed === 'boolean'; }
  function entryRefs(values, changes = false, empty = true) {
    if (!Array.isArray(values) || values.length > 64 || (!empty && !values.length)) return false;
    const entries = new Set(), assets = new Set();
    return values.every(v => {
      if (!object(v, changes ? ['kind', 'entry_id', 'asset_id'] : ['entry_id', 'asset_id']) || !cid(v.entry_id, 'entry') || !id(v.asset_id, 'asset') || entries.has(v.entry_id) || assets.has(v.asset_id) || changes && !['add', 'remove', 'reactivate'].includes(v.kind)) return false;
      entries.add(v.entry_id); assets.add(v.asset_id); return true;
    });
  }
  function collectionHistory(value) {
    if (!object(value, ['kind', 'recorded_unix_nano', 'actor', 'value']) || !['you', 'another_authorized_person'].includes(value.actor) || typeof value.recorded_unix_nano !== 'string' || !/^[1-9][0-9]{0,18}$/.test(value.recorded_unix_nano) || BigInt(value.recorded_unix_nano) > 9223372036854775807n) return false;
    const v = value.value;
    if (value.kind === 'create') return object(v, ['name', 'entries']) && collectionText(v.name, 256, true) && entryRefs(v.entries);
    if (value.kind === 'rename') return object(v, ['name']) && collectionText(v.name, 256, true);
    if (value.kind === 'archive') return object(v, ['archived']) && typeof v.archived === 'boolean';
    return value.kind === 'entries' && object(v, ['changes']) && entryRefs(v.changes, true, false);
  }
  function validateCollection(route, value) {
    if (route === 'mutate') {
      requireValue(object(value, ['version', 'operation_id', 'collection_id', 'result_version']) && value.version === 1 && cid(value.operation_id, 'operation') && cid(value.collection_id, 'collection') && cid(value.result_version, 'version')); return value;
    }
    if (route === 'open') { requireValue(object(value, ['version', 'kind', 'collection']) && value.version === 1 && value.kind === 'collection' && summary(value.collection)); return value; }
    const kind = value && value.kind, cap = collectionCaps[kind];
    const keys = ['version', 'kind', 'collection', 'items', 'matched_count', 'count_scope', 'window_complete', 'next_page_token', 'sort_version', 'snapshot_scope'];
    if (kind === 'directory') keys.push('can_create'); if (kind === 'members') keys.push('state');
    requireValue(cap && object(value, keys) && value.version === 1 && (route === 'page' || route === kind) && value.sort_version === 'collections1' && value.count_scope === (kind === 'members' ? 'frozen_visible_membership' : 'frozen_window') && value.snapshot_scope === (kind === 'members' ? 'frozen_membership_current_page' : 'frozen_snapshot') && Number.isSafeInteger(value.matched_count) && value.matched_count >= 0 && value.matched_count <= cap && typeof value.window_complete === 'boolean' && (value.next_page_token === null || id(value.next_page_token, 'page')) && Array.isArray(value.items) && value.items.length <= 100 && value.items.length <= value.matched_count);
    requireValue(kind === 'directory' ? value.collection === null && typeof value.can_create === 'boolean' : summary(value.collection));
    if (kind === 'members') requireValue(['active', 'removed'].includes(value.state));
    for (const item of value.items) {
      requireValue(encoder.encode(goJSON(item)).length <= 32768);
      if (kind === 'directory') requireValue(summary(item));
      if (kind === 'members') requireValue(object(item, ['entry_id', 'asset_id', 'label', 'head_revision_id', 'removed']) && cid(item.entry_id, 'entry') && id(item.asset_id, 'asset') && collectionText(item.label, 4096) && nullableRevision(item.head_revision_id) && typeof item.removed === 'boolean' && item.removed === (value.state === 'removed'));
      if (kind === 'access') requireValue(object(item, ['grant_id', 'expected']) && cid(item.grant_id, 'grant') && expectedGrant(item.expected, value.collection.collection_id));
      if (kind === 'history') requireValue(collectionHistory(item));
    }
    return value;
  }
  function collectionCanonical(request) {
    const invalid = ok => { if (!ok) throw new LibraryError('invalid'); };
    invalid(object(request, ['version', 'library_id', 'operation_id', 'collection_id', 'expected_version', 'kind', 'value']) && request.version === 1 && id(request.library_id, 'library') && cid(request.operation_id, 'operation') && cid(request.collection_id, 'collection') && (request.kind === 'create' ? request.expected_version === '' : cid(request.expected_version, 'version')));
    const v = request.value; let value;
    if (request.kind === 'create') {
      invalid(object(v, ['name', 'creator_grant_id', 'entries']) && collectionText(v.name, 256, true) && cid(v.creator_grant_id, 'grant') && entryRefs(v.entries));
      value = { name: v.name, creator_grant_id: v.creator_grant_id, entries: v.entries.map(e => ({ entry_id: e.entry_id, asset_id: e.asset_id })).sort((a, b) => a.entry_id < b.entry_id ? -1 : a.entry_id > b.entry_id ? 1 : 0) };
    } else if (request.kind === 'rename') { invalid(object(v, ['name']) && collectionText(v.name, 256, true)); value = { name: v.name }; }
    else if (request.kind === 'archive') { invalid(object(v, ['archived']) && typeof v.archived === 'boolean'); value = { archived: v.archived }; }
    else if (request.kind === 'entries') { invalid(object(v, ['changes']) && entryRefs(v.changes, true, false)); value = { changes: v.changes.map(e => ({ kind: e.kind, entry_id: e.entry_id, asset_id: e.asset_id })) }; }
    else if (request.kind === 'grants') {
      invalid(object(v, ['changes']) && Array.isArray(v.changes) && v.changes.length > 0 && v.changes.length <= 16); const seen = new Set();
      const copyValue = x => ({ subject_id: x.subject_id, scope: { kind: 'collection', collection_id: x.scope.collection_id }, actions: x.actions.slice() });
      const changes = v.changes.map(c => {
        invalid(object(c, ['kind', 'grant_id', 'expected', 'value']) && cid(c.grant_id, 'grant') && !seen.has(c.grant_id) && (c.expected === null || expectedGrant(c.expected, request.collection_id))); seen.add(c.grant_id);
        const e = c.expected;
        if (c.kind === 'put') invalid(grantValue(c.value, request.collection_id) && (!e || e.value.subject_id === c.value.subject_id && (e.removed || JSON.stringify(e.value.actions) !== JSON.stringify(c.value.actions))));
        else invalid(c.kind === 'remove' && e && !e.removed && c.value === null);
        return { kind: c.kind, grant_id: c.grant_id, expected: e === null ? null : { value: copyValue(e.value), version: e.version, removed: e.removed }, value: c.value === null ? null : copyValue(c.value) };
      }).sort((a, b) => a.grant_id < b.grant_id ? -1 : a.grant_id > b.grant_id ? 1 : 0); value = { changes };
    } else invalid(false);
    const result = goJSON({ version: 1, library_id: request.library_id, operation_id: request.operation_id, collection_id: request.collection_id, expected_version: request.expected_version, kind: request.kind, value });
    invalid(encoder.encode(result).length <= 32768); return result;
  }
  function randomCollectionID(random, kind) {
    const prefix = { collection: 'col_', entry: 'cent_', grant: 'cgr_', operation: 'cop_' }[kind];
    if (!prefix || !random || typeof random.getRandomValues !== 'function') throw new LibraryError('unavailable');
    for (let i = 0; i < 4; i++) { const bytes = new Uint8Array(16); random.getRandomValues(bytes); if (bytes.some(b => b !== 0)) return prefix + Array.from(bytes, b => b.toString(16).padStart(2, '0')).join(''); }
    throw new LibraryError('unavailable');
  }
  function createTransport(base, options = {}) {
    if (typeof base !== 'string' || !/^(?:\/[a-zA-Z0-9_-]+)*$/.test(base)) throw new LibraryError('invalid');
    const fetcher = options.fetch || globalThis.fetch;
    const later = options.setTimeout || setTimeout, cancelTimer = options.clearTimeout || clearTimeout;
    return async function transport(route, body, credential, signal) {
      const collectionRoute = /^v1\/collections\/(directory|open|members|history|access|page|mutate)$/.test(route);
      if (!collectionRoute && !/^v[12]\/(search|page|asset|counts|revisions|relationships|locations|history|select|clear|preference)$/.test(route)) throw new LibraryError('invalid');
      const controller = new AbortController();
      const abort = () => controller.abort();
      if (signal) { if (signal.aborted) abort(); else signal.addEventListener('abort', abort, { once: true }); }
      const timer = later(abort, 10000);
      let reader;
      try {
        const response = await fetcher(base + '/api/continuity/' + route, { method: 'POST', redirect: 'error', credentials: 'omit', cache: 'no-store', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + credential }, body, signal: controller.signal });
        if (response.status === 401) { if (response.body) reader = response.body.getReader(); throw new LibraryError('unauthenticated'); }
        if (response.redirected || !/^application\/json(?:\s*;|\s*$)/i.test(response.headers.get('content-type') || '') || !response.body) throw new LibraryError('invalid_response');
        reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8', { fatal: true }); let size = 0, decoded = '';
        while (true) {
          if (controller.signal.aborted) throw new LibraryError('deadline');
          const chunk = await reader.read(); if (chunk.done) break;
          requireValue(chunk.value instanceof Uint8Array); size += chunk.value.byteLength;
          if (size > (collectionRoute ? 1024 * 1024 : MAX_BODY)) throw new LibraryError('invalid_response');
          try { decoded += decoder.decode(chunk.value, { stream: true }); } catch (_) { throw new LibraryError('invalid_response'); }
        }
        try { decoded += decoder.decode(); } catch (_) { throw new LibraryError('invalid_response'); }
        if (controller.signal.aborted) throw new LibraryError('deadline');
        let value; try { value = JSON.parse(decoded); } catch (_) { throw new LibraryError('invalid_response'); }
        if (!response.ok) {
          const statuses = { invalid: 400, unauthenticated: 401, not_found: 404, restart_query: 409, conflict: 409, expired_request: 409, work_budget: 422, busy: 503, unavailable: 503, deadline: 504 };
          if (object(value, ['version', 'error']) && value.version === 1 && statuses[value.error] === response.status) throw new LibraryError(value.error);
          throw new LibraryError('invalid_response');
        }
        return validate(route, value);
      } catch (error) {
        if (error instanceof LibraryError) throw error;
        throw new LibraryError(controller.signal.aborted ? 'deadline' : 'transport');
      } finally {
        controller.abort();
        if (reader) { try { await reader.cancel(); } catch (_) { /* response is discarded */ } reader.releaseLock(); }
        cancelTimer(timer); if (signal) signal.removeEventListener('abort', abort);
      }
    };
  }

  function createLibraryClient(options = {}) {
    const send = createTransport(options.base || '', options);
    const clock = options.now || (() => Math.floor(Date.now() / 1000));
    const random = options.crypto || globalThis.crypto;
    const onChange = options.onChange || (() => {});
    let credential = '', library = '', epoch = 0, active = false, connected = false;
    let notice = '', searchError = false;
    let query = '', contextIDs = [], contexts = [], items = [], next = null, count = 0, complete = true;
    let selected = '', sections = {}, preference = null, statusFresh = false;
    let expiryTimer = null;
    let pending = null, warning = null, message = '', searchBusy = false, restoring = false, mutationBusy = false;
    let libraryView = 'assets', directoryState = 'active', collectionID = '', collectionSection = 'members', memberState = 'active';
    let collectionCurrent = null, collectionWindows = {}, collectionError = '', collectionBusy = false, picker = false;
    let intentGeneration = 0, settlementOwner = null;
    const memberSelection = new Map(), pickerSelection = new Map();
    const collectionScopes = ['directory', 'collection', 'members', 'history', 'access'];
    const generations = { search: 0, detail: 0, status: 0, restore: 0, directory: 0, collection: 0, members: 0, history: 0, access: 0 };
    const requests = new Set(); let readCount = 0; const waiters = [];
    function clearExpiry() { if (expiryTimer !== null) { clearTimeout(expiryTimer); expiryTimer = null; } }
    function expire() {
      if (pending && pending.family !== 'collection' && (pending.expired || clock() >= pending.expires)) {
        clearExpiry(); invalidate('status');
        warning = { assetID: pending.assetID, acknowledged: false, message: 'An earlier action may have been accepted. Its retry window ended. Check current state, then acknowledge before a new action.' };
        pending = null; statusFresh = false;
      }
    }
    function view() {
      expire();
      return { active, hasConnection: !!credential, connected: active && connected, library: active && connected ? library : '', message, notice, searchError,
        query, contexts: active ? contexts.slice() : [], items: active ? items.slice() : [], next: active && !!next,
        count, complete, searchBusy, restoring, inspector: active && connected && selected ? sections.asset && sections.asset.value ? 'ready' : sections.asset && sections.asset.error ? 'error' : 'loading' : 'idle', inspectorError: active && connected && sections.asset && sections.asset.error || '', selected: active && connected && sections.asset && sections.asset.value ? selected : '', sections: active ? Object.fromEntries(Object.entries(sections).map(([key, value]) => [key, { ...value, ...(Object.hasOwn(value, 'next') ? { next: !!value.next } : {}) }])) : {},
        preference: active && statusFresh ? preference !== null : null, statusFresh: active && statusFresh,
        mutationBusy, pending: !!pending, pendingCollection: !!pending && pending.family === 'collection', retryable: !!pending && !pending.expired && !mutationBusy,
        libraryView, collections: collectionView(),
        warning: warning ? { acknowledged: warning.acknowledged, message: warning.message } : null, canMutate: active && connected && statusFresh && !pending && !mutationBusy && (!warning || warning.acknowledged) };
    }
    function emit() { onChange(view()); }
    function stamp(scope) { return { epoch, scope, generation: generations[scope] }; }
    function current(s, visible = true) { return s.epoch === epoch && s.generation === generations[s.scope] && !!credential && (!visible || active); }
    function invalidate(scope) { generations[scope]++; for (let i = waiters.length - 1; i >= 0; i--) if (waiters[i].scope === scope) { waiters.splice(i, 1)[0].resolve(false); } for (const r of requests) if (r.scope === scope) r.controller.abort(); }
    function abortReads() { for (const key of Object.keys(generations)) invalidate(key); }
    function clearCollections() { collectionCurrent = null; collectionWindows = {}; collectionError = ''; collectionBusy = false; memberSelection.clear(); pickerSelection.clear(); picker = false; }
    function clearScoped() { contexts = []; items = []; next = null; count = 0; sections = {}; preference = null; statusFresh = false; searchBusy = false; restoring = false; clearCollections(); }
    function reset(note) {
      epoch++; clearExpiry(); abortReads(); for (const key of Object.keys(generations)) generations[key] = 0; for (const r of requests) r.controller.abort();
      credential = ''; library = ''; connected = false; contextIDs = []; query = ''; selected = ''; collectionID = ''; pending = null; warning = null; mutationBusy = false;
      directoryState = 'active'; memberState = 'active'; collectionSection = 'members';
      clearScoped(); notice = note || ''; message = note || ''; emit();
    }
    function authFailure(s) {
      if (s.epoch !== epoch) return;
      const uncertain = pending || mutationBusy;
      reset(uncertain ? 'Access ended. An action may have been accepted before disconnection.' : messages.unauthenticated);
    }
    function errorFor(s, error) { if (!current(s)) return; if (error.code === 'unauthenticated') authFailure(s); else { message = error.message; emit(); } }
    async function acquire(scope) { if (readCount < READ_LIMIT) { readCount++; return true; } if (waiters.length >= 16) throw new LibraryError('busy'); return new Promise(resolve => waiters.push({ scope, resolve })); }
    function release() { const nextWaiter = waiters.shift(); if (nextWaiter) nextWaiter.resolve(true); else readCount--; }
    async function read(scope, route, value) {
      const ranked = route === 'v2/search' || route === 'v2/page';
      if (ranked && mutationBusy) throw new LibraryError('busy');
      const s = stamp(scope); if (!await acquire(scope)) return null;
      let r;
      try {
        if (!current(s)) return null;
        if (ranked && mutationBusy) throw new LibraryError('busy');
        r = { scope, controller: new AbortController() }; requests.add(r);
        const response = await send(route, JSON.stringify(value), credential, r.controller.signal);
        if (!current(s)) return null;
        return response;
      } catch (error) { if (error.code === 'unauthenticated' && current(s)) authFailure(s); throw error; }
      finally { if (r) requests.delete(r); release(); }
    }
    function body(version, extra) { return { version, library_id: library, ...extra }; }
    function dropSearch() { invalidate('search'); items = []; next = null; count = 0; searchError = false; searchBusy = false; }
    async function search(textValue = query, more = false) {
      if (!credential || !active || restoring || mutationBusy || searchBusy && more) return;
      if (!text(textValue, 256)) { message = 'Search text must fit within 256 UTF-8 bytes.'; emit(); return; }
      if (!more) { query = textValue; dropSearch(); }
      if (more && !next || items.length >= 1000) return;
      const s = stamp('search'); const cursor = next; searchBusy = true; message = ''; emit();
      try {
        const value = more ? await read('search', 'v2/page', body(2, { token: cursor })) : await read('search', 'v2/search', body(2, { text: query, context_asset_ids: contextIDs.slice(), page_size: 50 }));
        if (!value || !current(s)) return;
        requireValue(value.kind === 'search');
        items = (more ? items : []).concat(value.items).slice(0, 1000); next = items.length < 1000 ? value.next_page_token : null;
        count = value.matched_count; complete = value.window_complete; connected = true;
      } catch (error) { if (current(s)) { next = null; searchError = true; errorFor(s, error); } }
      finally { if (current(s)) { searchBusy = false; emit(); } }
    }
    async function resume() {
      if (!credential || !active) return;
      abortReads(); connected = false; clearScoped(); const s = stamp('restore'); restoring = true; message = 'Checking library access and temporary context…'; emit();
      if (libraryView === 'collections') {
        try { await collectionDirectory(); if (current(s) && collectionID) await openCollection(collectionID); }
        finally { if (current(s)) { restoring = false; emit(); } }
        return;
      }
      try {
        const saved = contextIDs.slice(), wanted = new Set(saved), recovered = new Map(); let cursor = null, proved = false;
        for (let page = 0; page < 5; page++) {
          const response = await read('restore', cursor ? 'v2/page' : 'v2/search', cursor ? body(2, { token: cursor }) : body(2, { text: '', context_asset_ids: saved, page_size: 100 }));
          if (!response || !current(s)) return;
          requireValue(response.kind === 'search');
          for (const a of response.items) {
            if (!wanted.has(a.asset_id)) { proved = true; break; }
            requireValue(a.reasons.includes('context')); recovered.set(a.asset_id, a);
          }
          if (recovered.size === wanted.size || proved || response.next_page_token === null) { proved = true; break; }
          cursor = response.next_page_token;
        }
        if (!proved) throw new LibraryError('context');
        if (!current(s)) return;
        contextIDs = saved.filter(id => recovered.has(id)); contexts = contextIDs.map(id => recovered.get(id)); connected = true; restoring = false; message = ''; emit();
        await search(query);
        if (current(s) && selected) await inspect(selected);
      } catch (error) { if (current(s)) { clearScoped(); errorFor(s, error); } }
      finally { if (current(s)) { restoring = false; emit(); } }
    }
    async function connect(idValue, bearer) {
      const lost = !!pending || mutationBusy; reset(lost ? 'An earlier action may have been accepted; this connection forgets it.' : '');
      if (!id(idValue, 'library') || !/^jmc1_[0-9a-f]{32}\.[0-9a-f]{64}$/.test(bearer)) { message = 'Enter a library ID and a valid access credential from the owner CLI.'; emit(); return; }
      library = idValue; credential = bearer; await resume();
    }
    function setActive(value) {
      if (active === value) return Promise.resolve(); active = value;
      if (!active) { connected = false; abortReads(); clearScoped(); message = ''; emit(); return Promise.resolve(); }
      emit(); return resume();
    }
    async function context(a, remove = false) {
      if (!active || !connected || !a || !id(a.asset_id, 'asset')) return;
      if (remove) { contextIDs = contextIDs.filter(x => x !== a.asset_id); contexts = contexts.filter(x => x.asset_id !== a.asset_id); }
      else if (!contextIDs.includes(a.asset_id)) { if (contextIDs.length >= 64) { message = 'Temporary context is limited to 64 assets.'; emit(); return; } contextIDs.push(a.asset_id); contexts.push(a); }
      invalidate('detail'); sections = {}; dropSearch(); emit(); await search(query);
    }
    const listKinds = ['revisions', 'relationships', 'locations', 'history'];
    async function loadSection(kind, more = false) {
      if (!active || !selected) return; const s = stamp('detail'), assetID = selected;
      const old = sections[kind]; if (more && (!old || old.busy || !old.next)) return;
      sections = { ...sections, [kind]: { ...(more ? old : {}), busy: true, error: '' } }; emit();
      try {
        const response = more ? await read('detail', 'v1/page', body(1, { token: old.next })) : await read('detail', 'v1/' + kind, body(1, { asset_id: assetID, ...(listKinds.includes(kind) ? { page_size: 50 } : {}) }));
        if (!response || !current(s) || selected !== assetID) return;
        if (kind === 'asset' || kind === 'counts') { requireValue((kind === 'asset' ? response.asset.asset_id : response.asset_id) === assetID); sections[kind] = { value: response, busy: false }; }
        else { requireValue(response.kind === kind); const values = (more ? old.items : []).concat(response.items).slice(0, 1000); sections[kind] = { items: values, next: values.length < 1000 ? response.next_page_token : null, count: response.matched_count, complete: response.window_complete, busy: false }; }
      } catch (error) { if (current(s)) { sections[kind] = { error: error.message, busy: false }; errorFor(s, error); } }
      finally { if (current(s)) emit(); }
    }
    async function status() {
      expire();
      if (!active || !selected || !credential) return false;
      invalidate('status'); statusFresh = false; preference = null; const s = stamp('status'), assetID = selected; emit();
      try {
        const response = await read('status', 'v2/preference', body(2, { asset_id: assetID }));
        if (!response || !current(s) || selected !== assetID) return false;
        requireValue(response.asset_id === assetID); preference = response.selection_token; statusFresh = true; emit(); return true;
      } catch (error) { errorFor(s, error); return false; }
    }
    async function inspect(assetID) {
      if (!active || !connected || !id(assetID, 'asset')) return;
      invalidate('detail'); invalidate('status'); invalidate('search'); next = null; searchBusy = false;
      selected = assetID; sections = {}; preference = null; statusFresh = false; emit();
      await Promise.all(['asset', 'counts', ...listKinds].map(kind => loadSection(kind)).concat(status()));
    }
    async function dispatch() {
      if (pending && pending.family === 'collection') return dispatchCollection();
      expire(); if (!pending || pending.expired || mutationBusy || !credential || !active) { emit(); return; }
      const operation = pending, s = { epoch }; let refreshRanked = false;
      mutationBusy = true; invalidate('restore'); restoring = false; dropSearch(); operation.dispatched = true; message = 'Sending personal action…'; emit();
      const r = { scope: 'mutation', controller: new AbortController() }; requests.add(r);
      try {
        const receipt = await send('v2/' + operation.kind, operation.serialized, credential, r.controller.signal);
        if (s.epoch !== epoch || pending !== operation) return;
        requireValue(receipt.operation_id === operation.id && receipt.kind === operation.kind);
        dropSearch(); clearExpiry(); pending = null; message = 'Action recorded. Checking current state…'; statusFresh = false;
        if (active) { if (selected !== operation.assetID) await inspect(operation.assetID); else await status(); refreshRanked = true; }
      } catch (error) {
        if (s.epoch !== epoch || pending !== operation) return;
        if (error.code === 'unauthenticated') { authFailure(s); return; }
        // Even a valid server error can follow COMMIT. Neither a later denial
        // nor a current null status proves the historical action rolled back.
        dropSearch(); operation.uncertain = true;
        if (error.code === 'expired_request') operation.expired = true;
        message = error.message + ' The action may have been accepted. Check current state.';
      } finally {
        requests.delete(r); if (s.epoch === epoch) {
          dropSearch(); mutationBusy = false; expire(); emit();
          if (refreshRanked && active && credential) { if (connected) await search(query); else await resume(); }
        }
      }
    }
    async function mutate(kind) {
      expire(); if (!active || !connected || !selected || pending || mutationBusy || warning && !warning.acknowledged) { emit(); return; }
      const actionEpoch = epoch, actionAsset = selected;
      if (!await status()) return;
      if (actionEpoch !== epoch || actionAsset !== selected || !active || !connected || pending || mutationBusy || warning && !warning.acknowledged) return;
      if (kind === 'clear' && preference === null) { message = 'There is no current personal selection to clear.'; emit(); return; }
      const bytes = new Uint8Array(16); random.getRandomValues(bytes);
      const operationID = 'ui_' + Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
      const expires = clock() + 600;
      const value = body(2, { operation_id: operationID, asset_id: selected, expected_token: kind === 'clear' ? preference : null, expires_unix: expires });
      pending = { id: operationID, assetID: selected, kind, expires, serialized: JSON.stringify(value), dispatched: false, uncertain: false, expired: false };
      expiryTimer = setTimeout(() => { if (pending && pending.id === operationID) { pending.expired = true; emit(); } }, 600000);
      if (expiryTimer && typeof expiryTimer.unref === 'function') expiryTimer.unref();
      await dispatch();
    }
    async function checkActionState() { if (pending && pending.family === 'collection') { if (libraryView !== 'collections') { libraryView = 'collections'; abortReads(); clearScoped(); connected = false; emit(); } if (pending.collectionID) await openCollection(pending.collectionID); else await collectionDirectory(); return; } const target = pending ? pending.assetID : warning ? warning.assetID : selected; if (target && target !== selected) await inspect(target); else await status(); }
    function acknowledge() { expire(); if (warning && statusFresh && selected === warning.assetID) { warning.acknowledged = true; message = 'Earlier outcome acknowledged. Current state is shown separately.'; emit(); } }
    function publicSummary(value) { if (!value) return null; const { collection_id, name, archived, creator, permissions } = value; return { collection_id, name, archived, creator, permissions: { ...permissions } }; }
    function collectionView() {
      const windows = {};
      if (active) for (const [kind, w] of Object.entries(collectionWindows)) {
        windows[kind] = { busy: !!w.busy, error: w.error || '', next: !!w.next, count: w.count || 0, complete: w.complete !== false, page: w.page || 0, canCreate: !!w.canCreate,
          items: (w.items || []).map(x => kind === 'directory' ? publicSummary(x) : kind === 'access' ? { grant_id: x.grant_id, subject_id: x.expected.value.subject_id, actions: x.expected.value.actions.slice(), removed: x.expected.removed } : JSON.parse(JSON.stringify(x))) };
      }
      return { current: active ? publicSummary(collectionCurrent) : null, windows, directoryState, section: collectionSection, memberState, error: active ? collectionError : '', busy: collectionBusy,
        canChange: active && connected && !collectionBusy && !!collectionCurrent && !!collectionWindows[collectionSection] && !collectionWindows[collectionSection].busy && !collectionWindows[collectionSection].error && collectionCurrent.permissions.read && collectionCurrent.permissions.edit && !pending && !mutationBusy && (!warning || warning.acknowledged),
        picker: active && picker, pickedAssets: active ? Array.from(pickerSelection.keys()) : [], selectedMembers: active ? Array.from(memberSelection.keys()) : [] };
    }
    function invalidateCollections() { for (const scope of collectionScopes) invalidate(scope); clearCollections(); }
    function settlementCurrent(owner) { return owner.epoch === epoch && owner.intent === intentGeneration && active && !!credential && libraryView === 'collections' && collectionID === owner.collectionID; }
    function userIntent(action) {
      return (...args) => {
        intentGeneration++;
        // Cancel only the older settlement's collection reads. A settled
        // receipt stays settled; the next user action owns displayed state.
        if (settlementOwner) {
          settlementOwner = null;
          for (const scope of collectionScopes) invalidate(scope);
          collectionBusy = false;
          for (const [kind, window] of Object.entries(collectionWindows)) if (window.busy) delete collectionWindows[kind];
        }
        return action(...args);
      };
    }
    async function switchView(value) {
      if (!['assets', 'collections'].includes(value)) return;
      libraryView = value; abortReads(); clearScoped(); connected = false; emit(); await resume();
    }
    function windowBinding(v) { return JSON.stringify([v.kind, v.collection, v.state, v.can_create, v.matched_count, v.count_scope, v.window_complete, v.sort_version, v.snapshot_scope]); }
    async function collectionList(kind, more = false) {
      if (!credential || !active || mutationBusy || !collectionScopes.includes(kind) || kind === 'collection') return;
      if (kind !== 'directory' && (!collectionCurrent || !collectionCurrent.permissions.read || kind === 'access' && !(collectionCurrent.permissions.edit && collectionCurrent.permissions.admin))) return;
      const previous = collectionWindows[kind]; if (more && (!previous || previous.busy || !previous.next)) return;
      if (!more) invalidate(kind);
      const s = stamp(kind), target = collectionID, state = kind === 'directory' ? directoryState : memberState;
      const old = more ? previous : { items: [], tokens: new Set(), identities: new Set(), page: 0, rows: 0 };
      collectionWindows[kind] = { ...old, items: more ? old.items : [], busy: true, error: '' }; emit();
      try {
        const extra = kind === 'directory' ? { state, page_size: 100 } : { collection_id: target, ...(kind === 'members' ? { state } : {}), page_size: 100 };
        const response = await read(kind, 'v1/collections/' + (more ? 'page' : kind), body(1, more ? { token: old.next } : extra));
        if (!response || !current(s) || kind !== 'directory' && target !== collectionID) return;
        requireValue(response.kind === kind && (kind !== 'members' || response.state === state));
        if (kind !== 'directory') requireValue(response.collection.collection_id === target && collectionCurrent && JSON.stringify(response.collection) === JSON.stringify(collectionCurrent));
        const binding = windowBinding(response); requireValue(!more || binding === old.binding);
        const identities = new Set(old.identities), tokens = new Set(old.tokens), cap = collectionCaps[kind];
        if (more) { requireValue(!tokens.has(old.next)); tokens.add(old.next); }
        requireValue(old.page < 8192 && old.rows + response.items.length <= response.matched_count && (!response.next_page_token || response.items.length > 0));
        requireValue(response.next_page_token !== null || old.rows + response.items.length === response.matched_count);
        if (response.next_page_token) requireValue(!tokens.has(response.next_page_token));
        if (kind !== 'history') for (const item of response.items) { const key = item.entry_id || item.grant_id || item.collection_id; requireValue(!identities.has(key) && identities.size < cap); identities.add(key); }
        collectionWindows[kind] = { items: response.items, next: response.next_page_token, count: response.matched_count, complete: response.window_complete, canCreate: response.can_create === true, busy: false, error: '', binding, identities, tokens, rows: old.rows + response.items.length, page: old.page + 1 };
        connected = true; collectionError = ''; message = '';
      } catch (error) {
        if (current(s)) { collectionWindows[kind] = { error: error.message, items: [], busy: false }; if (kind !== 'directory') { collectionCurrent = null; memberSelection.clear(); pickerSelection.clear(); for (const key of ['members', 'history', 'access']) if (key !== kind) { invalidate(key); delete collectionWindows[key]; } } collectionError = error.message; errorFor(s, error); }
      } finally { if (current(s)) { if (collectionWindows[kind]) collectionWindows[kind].busy = false; emit(); } }
    }
    async function collectionDirectory(state = directoryState, more = false) {
      if (!['active', 'archived', 'all'].includes(state)) return;
      if (state !== directoryState) { directoryState = state; more = false; }
      await collectionList('directory', more);
    }
    async function openCollection(target) {
      if (!credential || !active || mutationBusy || !cid(target, 'collection')) return;
      invalidateCollections(); collectionID = target; collectionBusy = true; const s = stamp('collection'); emit();
      try {
        const response = await read('collection', 'v1/collections/open', body(1, { collection_id: target }));
        if (!response || !current(s) || collectionID !== target) return;
        requireValue(response.collection.collection_id === target && response.collection.permissions.read);
        collectionCurrent = response.collection; connected = true;
        if (collectionSection === 'access' && !(collectionCurrent.permissions.edit && collectionCurrent.permissions.admin)) collectionSection = 'members';
        emit(); await collectionList(collectionSection);
      } catch (error) { if (current(s)) { collectionError = error.message; collectionCurrent = null; errorFor(s, error); } }
      finally { if (current(s)) { collectionBusy = false; emit(); } }
    }
    async function collectionTab(kind, state = memberState) {
      if (!['members', 'history', 'access'].includes(kind) || !['active', 'removed'].includes(state)) return;
      if (!collectionCurrent || kind === 'access' && !(collectionCurrent.permissions.read && collectionCurrent.permissions.edit && collectionCurrent.permissions.admin)) return;
      for (const k of ['members', 'history', 'access']) { invalidate(k); delete collectionWindows[k]; }
      memberSelection.clear(); pickerSelection.clear(); picker = false; collectionSection = kind; memberState = state; emit(); await collectionList(kind);
    }
    async function backToCollections() { collectionID = ''; invalidateCollections(); emit(); await collectionDirectory(); }
    function collectionNotice(value) { message = value; emit(); }
    function selectionToggle(assetID, entryID = '') {
      if (!active || !connected || pending || mutationBusy) return;
      const values = picker ? pickerSelection : memberSelection;
      if (values.has(assetID)) { values.delete(assetID); emit(); return; }
      if (values.size >= 64) { collectionNotice('Select up to 64 assets for one action. You can add more in another batch.'); return; }
      const found = picker ? items.find(x => x.asset_id === assetID) : (collectionWindows.members && collectionWindows.members.items || []).find(x => x.asset_id === assetID && x.entry_id === entryID && !x.removed);
      if (!found) return;
      values.set(assetID, { asset_id: assetID, ...(picker ? {} : { entry_id: entryID }) }); emit();
    }
    async function startPicker() {
      if (!collectionView().canChange || collectionCurrent.archived) return;
      picker = true; pickerSelection.clear(); memberSelection.clear(); invalidate('search'); items = []; query = ''; next = null; emit(); await search('');
    }
    function cancelPicker() { picker = false; pickerSelection.clear(); dropSearch(); emit(); }
    async function useCollectionContext() {
      if (!memberSelection.size || pending || mutationBusy) return;
      contextIDs = Array.from(memberSelection.keys()); contexts = []; selected = ''; libraryView = 'assets'; abortReads(); clearScoped(); connected = false; emit(); await resume();
    }
    async function inspectCollectionAsset(assetID) {
      const member = (collectionWindows.members && collectionWindows.members.items || []).find(x => x.asset_id === assetID);
      if (!member) return;
      selected = assetID; libraryView = 'assets'; abortReads(); clearScoped(); connected = false; emit(); await resume();
    }
    function newID(kind) { return randomCollectionID(random, kind); }
    async function collectionMutation(kind, value) {
      if (!active || !credential || pending || mutationBusy || warning && !warning.acknowledged) return;
      const create = kind === 'create', currentValue = collectionCurrent;
      if (create ? !(collectionWindows.directory && !collectionWindows.directory.busy && collectionWindows.directory.canCreate) : !collectionView().canChange || ['grants', 'archive'].includes(kind) && !currentValue.permissions.admin) return;
      try {
        const targetID = create ? newID('collection') : currentValue.collection_id;
        const operationID = newID('operation');
        const serialized = collectionCanonical({ version: 1, library_id: library, operation_id: operationID, collection_id: targetID, expected_version: create ? '' : currentValue.definition_version, kind, value: create ? { name: value.name, creator_grant_id: newID('grant'), entries: [] } : value });
        if (kind === 'rename' && value.name === currentValue.name || kind === 'archive' && value.archived === currentValue.archived) { collectionNotice('There is no change to save.'); return; }
        if (create) { collectionSection = 'members'; memberState = 'active'; }
        pending = { family: 'collection', id: operationID, collectionID: targetID, kind, serialized, uncertain: false };
        await dispatchCollection();
      } catch (error) { collectionNotice(error instanceof LibraryError ? error.message : messages.invalid); }
    }
    async function dispatchCollection() {
      if (!pending || pending.family !== 'collection' || mutationBusy || !active || !credential) return;
      const operation = pending, s = { epoch, intent: intentGeneration, collectionID: pending.collectionID }; let refresh = false;
      mutationBusy = true; invalidate('restore'); restoring = false; invalidateCollections(); dropSearch(); collectionID = operation.collectionID; message = 'Saving collection change…'; emit();
      const r = { scope: 'mutation', controller: new AbortController() }; requests.add(r);
      try {
        const receipt = await send('v1/collections/mutate', operation.serialized, credential, r.controller.signal);
        if (s.epoch !== epoch || pending !== operation) return;
        requireValue(receipt.operation_id === operation.id && receipt.collection_id === operation.collectionID);
        pending = null; refresh = true;
        if (s.intent === intentGeneration) message = libraryView === 'collections' ? 'Change recorded. Reading current collection…' : 'Collection change recorded. Check current collection state separately.';
        // The receipt is historical. Only a fresh open may install current CV.
      } catch (error) {
        if (s.epoch !== epoch || pending !== operation) return;
        if (error.code === 'unauthenticated') { authFailure(s); return; }
        if (error.code === 'conflict' && !operation.uncertain) { pending = null; refresh = true; if (s.intent === intentGeneration) message = 'This change was rejected because current state conflicts. Read the collection and enter the change again.'; }
        else { operation.uncertain = true; if (s.intent === intentGeneration) message = error.message + ' This change may have been accepted. Retry the same action or check current state separately.'; }
      } finally {
        requests.delete(r);
        if (s.epoch === epoch) {
          mutationBusy = false; emit();
          if (refresh && settlementCurrent(s)) {
            const resultMessage = message; settlementOwner = s;
            try {
              await collectionDirectory();
              if (!settlementCurrent(s)) return;
              await openCollection(operation.collectionID);
              if (settlementCurrent(s) && collectionCurrent) { message = resultMessage; emit(); }
            } finally { if (settlementOwner === s) settlementOwner = null; }
          }
        }
      }
    }
    async function addPicked() {
      if (!picker || !pickerSelection.size || !collectionView().canChange) return;
      try { await collectionMutation('entries', { changes: Array.from(pickerSelection.values(), a => ({ kind: 'add', entry_id: newID('entry'), asset_id: a.asset_id })) }); }
      catch (_) { collectionNotice(messages.unavailable); }
    }
    async function changeMember(entryID, kind) {
      const member = (collectionWindows.members && collectionWindows.members.items || []).find(x => x.entry_id === entryID);
      if (!member || !['remove', 'reactivate'].includes(kind) || member.removed !== (kind === 'reactivate')) return;
      await collectionMutation('entries', { changes: [{ kind, entry_id: member.entry_id, asset_id: member.asset_id }] });
    }
    async function changeAccess(subjectID, actions, grantID = '', remove = false) {
      if (!collectionView().canChange || !collectionCurrent.permissions.admin) return;
      try {
        const existing = grantID ? (collectionWindows.access && collectionWindows.access.items || []).find(x => x.grant_id === grantID) : null;
        if (grantID && !existing) return;
        const value = { subject_id: existing ? existing.expected.value.subject_id : subjectID, scope: { kind: 'collection', collection_id: collectionCurrent.collection_id }, actions: actions.slice().sort() };
        await collectionMutation('grants', { changes: [{ kind: remove ? 'remove' : 'put', grant_id: existing ? existing.grant_id : newID('grant'), expected: existing ? JSON.parse(JSON.stringify(existing.expected)) : null, value: remove ? null : value }] });
      } catch (_) { collectionNotice(messages.invalid); }
    }
    const client = { connect, setActive, search, inspect, status, loadSection, context, remember: () => mutate('select'), clear: () => mutate('clear'), retry: dispatch, acknowledge,
      switchView, collectionDirectory, openCollection, collectionTab, collectionNext: kind => collectionList(kind, true), collectionStart: kind => collectionList(kind), backToCollections,
      createCollection: name => collectionMutation('create', { name }), renameCollection: name => collectionMutation('rename', { name }), archiveCollection: archived => collectionMutation('archive', { archived }),
      startPicker, cancelPicker, selectionToggle, addPicked, useCollectionContext, inspectCollectionAsset, changeMember, changeAccess,
      checkActionState, refresh: resume, disconnect: () => reset(pending || mutationBusy ? 'Disconnected. An action may have been accepted; its retry details were forgotten.' : 'Disconnected.'), pagehide: () => reset(pending || mutationBusy ? 'An action may have been accepted before leaving.' : ''), snapshot: view };
    for (const name of Object.keys(client)) if (name !== 'snapshot') client[name] = userIntent(client[name]);
    return client;
  }

  // Metadata always enters the DOM through textContent. Locators are inert text,
  // and long declarations expand inside bounded, scrollable details elements.
  function node(document, tag, value, className) {
    const element = document.createElement(tag);
    if (value !== undefined) element.textContent = String(value);
    if (className) element.className = className;
    return element;
  }
  function exactNumber(value) { return Number.isSafeInteger(value) ? String(value) : 'Outside the browser’s exact numeric range'; }
  function field(document, parent, label, value) {
    const row = node(document, 'div', undefined, 'library-fact'); row.append(node(document, 'dt', label), node(document, 'dd', value)); parent.append(row);
  }
  function declaration(document, kind, value) {
    const dl = node(document, 'dl', undefined, 'library-facts');
    if (!value) { field(document, dl, 'State', 'Not present in this declaration'); return dl; }
    if (kind === 'revision') {
      field(document, dl, 'Revision', value.revision_id); field(document, dl, 'Asset', value.asset_id);
      field(document, dl, 'Declared SHA-256', value.content.sha256); field(document, dl, 'Declared bytes', exactNumber(value.content.size));
      field(document, dl, 'Parent revisions', value.parents.join('\n') || 'No declared parents');
    } else if (kind === 'relationship') {
      field(document, dl, 'Relationship', value.relationship_id); field(document, dl, 'Predicate', value.predicate);
      field(document, dl, 'Source asset', value.source_asset_id); field(document, dl, 'Source revision', value.source_revision_id === null ? 'Undisclosed or not anchored' : value.source_revision_id);
      field(document, dl, 'Target asset', value.target_asset_id); field(document, dl, 'Target revision', value.target_revision_id === null ? 'Undisclosed or not anchored' : value.target_revision_id);
      field(document, dl, 'Provenance kind', value.provenance); field(document, dl, 'Declared source', value.source);
    } else if (kind === 'location') {
      field(document, dl, 'Location claim', value.location_id); field(document, dl, 'Asset', value.asset_id);
      field(document, dl, 'Revision', value.revision_id === null ? 'Undisclosed or not anchored' : value.revision_id);
      field(document, dl, 'Declared source', value.source); field(document, dl, 'Recorded locator (not opened)', value.locator);
    } else { field(document, dl, 'Asset', value.asset_id); if ('label' in value) field(document, dl, 'Label', value.label); if ('revision_id' in value) field(document, dl, 'Revision', value.revision_id === null ? 'Undisclosed or not anchored' : value.revision_id); }
    if ('removed' in value) field(document, dl, 'Declaration state', value.removed ? 'Removed' : 'Active');
    return dl;
  }
  function renderRecord(document, kind, item) {
    const details = node(document, 'details', undefined, 'library-record');
    const title = kind === 'history' ? item.kind.replaceAll('_', ' ') : kind === 'revisions' ? item.revision_id : kind === 'relationships' ? item.predicate : item.source || 'Location claim';
    details.append(node(document, 'summary', title));
    if (kind === 'history') {
      const facts = node(document, 'dl', undefined, 'library-facts');
      field(document, facts, 'Claimed actor', item.actor_claim || 'No actor claim'); field(document, facts, 'Recorded nanoseconds', exactNumber(item.recorded_unix_nano)); details.append(facts);
      const type = item.kind.includes('relationship') ? 'relationship' : item.kind.includes('location') ? 'location' : item.kind === 'record_revision' ? 'revision' : 'asset';
      details.append(node(document, 'h5', 'Before'), declaration(document, type, item.before), node(document, 'h5', 'After'), declaration(document, type, item.after));
    } else details.append(declaration(document, kind === 'revisions' ? 'revision' : kind === 'relationships' ? 'relationship' : 'location', item));
    return details;
  }
  // The inspector's nonsensitive loading/failure gate is separate from the
  // verified asset-value gate: a denied read must not look like no selection.
  function renderInspectorNotice(document, parent, state, retryAsset) {
    if (state.inspector === 'ready') return false;
    if (state.inspector === 'error') {
      parent.append(node(document, 'h4', 'Asset unavailable'), node(document, 'p', state.inspectorError, 'library-error'));
      const retry = node(document, 'button', 'Retry asset read'); retry.type = 'button'; retry.addEventListener('click', retryAsset); parent.append(retry);
    } else parent.append(node(document, 'p', state.inspector === 'loading' ? 'Reading permitted asset metadata…' : 'Choose an asset to read its permitted declarations.', 'library-empty'));
    return true;
  }
  let mounted = null, tabActive = false;
  function init(base) {
    if (mounted) return;
    const document = globalThis.document; let displayedQuery = ''; const pane = document.getElementById('library-pane');
    if (!pane) return;
    const get = name => document.getElementById('library-' + name);
    let client;
    const action = (label, invoke, disabled = false, className = '') => {
      const button = node(document, 'button', label, className); button.type = 'button'; button.disabled = disabled; button.addEventListener('click', invoke); return button;
    };
    function formInput(form, label, inputID, value = '') {
      const fieldLabel = node(document, 'label', undefined, 'field'); fieldLabel.append(node(document, 'span', label));
      const input = node(document, 'input'); input.id = inputID; input.type = 'text'; input.autocomplete = 'off'; input.spellcheck = false; input.value = value; input.required = true; fieldLabel.append(input); form.append(fieldLabel); return input;
    }
    function submitButton(form, label, disabled) { const button = node(document, 'button', label); button.type = 'submit'; button.disabled = disabled; form.append(button); return button; }
    function collectionPager(parent, kind, window) {
      const bar = node(document, 'div', undefined, 'library-collection-actions');
      bar.append(action('Start over', () => client.collectionStart(kind), window.busy), action('Next page', () => client.collectionNext(kind), window.busy || !window.next)); parent.append(bar);
    }
    function collectionIdentity(parent, label, values) {
      const detail = node(document, 'details', undefined, 'library-identities'); detail.append(node(document, 'summary', label)); values.forEach(value => detail.append(node(document, 'code', value))); parent.append(detail);
    }
    function renderCollections(state) {
      const c = state.collections, root = get('collections-body'); root.replaceChildren();
      const heading = value => { const h = node(document, 'h3', value); h.id = 'library-collections-heading'; h.tabIndex = -1; return h; };
      const disabled = state.pending || state.mutationBusy || !state.connected || c.busy || !!(c.windows[c.section] && c.windows[c.section].busy);
      const current = c.current;
      if (!current) {
        root.append(heading('Collections'), node(document, 'p', 'Keep shared working sets without moving or deleting files.', 'hint'));
        const filters = node(document, 'div', undefined, 'library-collection-actions');
        for (const [value, label] of [['active', 'Active'], ['archived', 'Archived'], ['all', 'All']]) { const b = action(label, () => client.collectionDirectory(value), state.mutationBusy); b.setAttribute('aria-pressed', String(c.directoryState === value)); filters.append(b); }
        filters.append(action('Refresh collections', () => client.collectionDirectory(), state.mutationBusy)); root.append(filters);
        if (c.busy) root.append(node(document, 'p', 'Reading current collection…', 'library-empty'));
        if (c.error) root.append(node(document, 'p', c.error, 'library-error'));
        const w = c.windows.directory;
        if (w && w.canCreate) {
          const form = node(document, 'form', undefined, 'library-collection-form'); const input = formInput(form, 'New collection name', 'library-new-collection-name');
          submitButton(form, 'Create collection', disabled || w.busy); form.addEventListener('submit', event => { event.preventDefault(); client.createCollection(input.value); });
          root.append(form, node(document, 'p', 'Only your access entry is created initially. Share with a known person when you are ready.', 'hint'));
        }
        if (w) {
          if (w.busy) root.append(node(document, 'p', 'Reading permitted collections…'));
          if (w.error && w.error !== c.error) root.append(node(document, 'p', w.error, 'library-error'));
          const list = node(document, 'ul', undefined, 'library-results');
          for (const item of w.items) { const li = node(document, 'li', undefined, 'library-result'); li.append(action(item.name, () => client.openCollection(item.collection_id), state.mutationBusy, 'library-result-title'), node(document, 'p', item.archived ? 'Archived' : 'Active', 'hint')); list.append(li); }
          root.append(list);
          if (!w.busy && !w.error) { root.append(node(document, 'p', w.count + ' permitted collections in this window' + (w.complete ? '.' : ' · Incomplete window.'), 'hint')); if (!w.items.length) root.append(node(document, 'p', 'No collections in this permitted view.', 'library-empty')); collectionPager(root, 'directory', w); }
        } else if (!c.busy && !c.error) root.append(node(document, 'p', 'Open Collections to check this credential’s access.', 'library-empty'));
        return;
      }
      const top = node(document, 'div', undefined, 'library-collection-heading');
      top.append(action('All collections', () => client.backToCollections(), state.mutationBusy), heading(current.name), node(document, 'p', (current.archived ? 'Archived' : 'Active') + ' · Created by ' + (current.creator === 'you' ? 'you' : 'another authorized person') + '. Creator attribution does not determine current access.', 'hint'));
      top.append(node(document, 'p', 'Your access: ' + [['read', 'Read'], ['edit', 'Edit'], ['admin', 'Manage access']].filter(([key]) => current.permissions[key]).map(([, label]) => label).join(' · '), 'hint'));
      collectionIdentity(top, 'Collection identity', [current.collection_id]);
      top.append(action('Refresh current collection', () => client.openCollection(current.collection_id), state.mutationBusy)); root.append(top);
      if (c.canChange) {
        const rename = node(document, 'form', undefined, 'library-collection-form'); const name = formInput(rename, 'Collection name', 'library-rename-collection', current.name); submitButton(rename, 'Rename collection', disabled);
        rename.addEventListener('submit', event => { event.preventDefault(); client.renameCollection(name.value); }); root.append(rename);
        if (current.permissions.admin) root.append(action(current.archived ? 'Restore archived collection' : 'Archive collection', () => client.archiveCollection(!current.archived), disabled));
      }
      const tabs = node(document, 'nav', undefined, 'library-collection-actions'); tabs.setAttribute('aria-label', 'Collection sections');
      for (const [kind, label] of [['members', 'Members'], ['history', 'History'], ['access', 'Access']]) {
        if (kind === 'access' && !(current.permissions.read && current.permissions.edit && current.permissions.admin)) continue;
        const b = action(label, () => client.collectionTab(kind), state.mutationBusy); b.setAttribute('aria-pressed', String(c.section === kind)); tabs.append(b);
      }
      root.append(tabs);
      if (c.section === 'members') {
        const controls = node(document, 'div', undefined, 'library-collection-actions');
        for (const [value, label] of [['active', 'Active members'], ['removed', 'Removed members']]) { const b = action(label, () => client.collectionTab('members', value), state.mutationBusy); b.setAttribute('aria-pressed', String(c.memberState === value)); controls.append(b); }
        if (c.canChange && !current.archived) controls.append(action('Add assets', () => client.startPicker(), disabled));
        controls.append(action('Use selection as temporary context', () => client.useCollectionContext(), disabled || !c.selectedMembers.length)); root.append(controls);
        root.append(node(document, 'p', c.selectedMembers.length + ' selected · Select up to 64 for temporary context. Membership can grow in separate batches. Removal changes this collection only; files are not deleted.', 'hint'));
      }
      if (c.section === 'access') {
        root.append(node(document, 'p', 'Access entries are independent. Removing one entry may leave another entry for the same person. Asset access is managed separately.', 'hint'));
        if (c.canChange && c.windows.access && !c.windows.access.busy && !c.windows.access.error) root.append(accessForm(null, disabled));
      }
      const w = c.windows[c.section];
      if (!w || w.busy) root.append(node(document, 'p', 'Reading current permitted ' + c.section + '…'));
      if (w && w.error) root.append(node(document, 'p', w.error, 'library-error'));
      if (w && !w.error) {
        const list = node(document, 'ul', undefined, 'library-results');
        for (const item of w.items) {
          const li = node(document, 'li', undefined, 'library-result');
          if (c.section === 'members') {
            li.append(node(document, 'h4', item.label || 'Unlabelled asset', 'library-member-label'));
            collectionIdentity(li, 'Asset and entry identity', [item.asset_id, item.entry_id]);
            li.append(node(document, 'p', item.head_revision_id === null ? 'Head revision undisclosed or not set.' : 'Disclosed head: ' + item.head_revision_id, 'library-identities'));
            const actions = node(document, 'div', undefined, 'library-collection-actions'); actions.append(action('Inspect asset', () => client.inspectCollectionAsset(item.asset_id), state.mutationBusy));
            if (!item.removed) { const b = action(c.selectedMembers.includes(item.asset_id) ? 'Deselect for context' : 'Select for context', () => client.selectionToggle(item.asset_id, item.entry_id), disabled); b.setAttribute('aria-pressed', String(c.selectedMembers.includes(item.asset_id))); actions.append(b); }
            if (c.canChange && !current.archived) actions.append(action(item.removed ? 'Restore to collection' : 'Remove from collection', () => client.changeMember(item.entry_id, item.removed ? 'reactivate' : 'remove'), disabled)); li.append(actions);
          } else if (c.section === 'access') li.append(accessForm(item, disabled));
          else {
            li.append(node(document, 'h4', ({ create: 'Collection created', rename: 'Collection renamed', entries: 'Membership changed', archive: item.value.archived ? 'Collection archived' : 'Collection restored' })[item.kind]));
            li.append(node(document, 'p', (item.actor === 'you' ? 'You' : 'Another authorized person') + ' · Recorded nanoseconds: ' + item.recorded_unix_nano, 'hint'));
            if (item.value.name !== undefined) li.append(node(document, 'p', item.value.name, 'library-member-label'));
            for (const entry of item.value.entries || item.value.changes || []) { const detail = node(document, 'details', undefined, 'library-identities'); detail.append(node(document, 'summary', entry.kind ? ({ add: 'Added member', remove: 'Removed member', reactivate: 'Restored member' })[entry.kind] : 'Initial member'), node(document, 'code', entry.entry_id), node(document, 'code', entry.asset_id)); li.append(detail); }
          }
          list.append(li);
        }
        root.append(list);
        if (!w.busy) { root.append(node(document, 'p', (c.section === 'members' ? 'Visible members in this view: ' : c.section === 'history' ? 'Events in this history window: ' : 'Access entries in this window: ') + w.count + (w.complete ? '.' : ' · Incomplete window; this is not the full history.'), 'hint')); if (!w.items.length) root.append(node(document, 'p', 'No records in this permitted response.', 'library-empty')); collectionPager(root, c.section, w); }
      }
      function accessForm(item, isDisabled) {
        const form = node(document, 'form', undefined, 'library-access-form'); let subject;
        if (!item) subject = formInput(form, 'Known person ID', 'library-share-person');
        else { form.append(node(document, 'h4', item.subject_id), node(document, 'p', item.removed ? 'Removed access entry' : 'Active access entry', 'hint')); collectionIdentity(form, 'Access entry identity', [item.grant_id]); }
        const choices = node(document, 'fieldset'); choices.append(node(document, 'legend', 'Direct collection rights')); const boxes = [];
        for (const [key, label] of [['collections.read', 'Read'], ['collections.edit', 'Edit'], ['collections.admin', 'Manage access']]) { const line = node(document, 'label', undefined, 'library-right'); const input = node(document, 'input'); input.type = 'checkbox'; input.checked = item ? item.actions.includes(key) : key === 'collections.read'; input.disabled = isDisabled; input.value = key; line.append(input, node(document, 'span', label)); choices.append(line); boxes.push(input); }
        form.append(choices); submitButton(form, item ? item.removed ? 'Restore access entry' : 'Save access changes' : 'Share collection', isDisabled);
        form.addEventListener('submit', event => { event.preventDefault(); client.changeAccess(item ? item.subject_id : subject.value.trim(), boxes.filter(x => x.checked).map(x => x.value), item ? item.grant_id : ''); });
        if (item && !item.removed) form.append(action('Remove access entry', () => client.changeAccess(item.subject_id, item.actions, item.grant_id, true), isDisabled));
        return form;
      }
    }
    function render(state) {
      const focusOwner = document.activeElement, focused = focusOwner && focusOwner.id;
      const focusLabel = focusOwner && focusOwner.tagName === 'BUTTON' ? focusOwner.textContent : '';
      const collectionFocus = focusOwner && get('collections-panel').contains(focusOwner);
      get('connection-state').textContent = state.connected ? 'Connected to library' : state.hasConnection ? 'Connection needs revalidation' : 'Connect to an existing catalog';
      get('disconnect').hidden = !state.hasConnection; get('connection-form').hidden = state.hasConnection;
      get('status').textContent = [...new Set([state.notice, state.message].filter(Boolean))].join(' '); get('workspace').hidden = !state.active || !state.hasConnection;
      for (const view of ['assets', 'collections']) get('view-' + view).setAttribute('aria-pressed', String(state.libraryView === view));
      get('collections-panel').hidden = state.libraryView !== 'collections'; get('assets-panel').hidden = state.libraryView !== 'assets' && !state.collections.picker;
      renderCollections(state);
      const pickerBar = get('picker-bar'); pickerBar.replaceChildren(); pickerBar.hidden = !state.collections.picker;
      if (state.collections.picker) pickerBar.append(node(document, 'h3', 'Add permitted assets to this collection'), node(document, 'p', state.collections.pickedAssets.length + ' selected · up to 64 in this batch. Only selected assets are added.'), action('Add selected assets', () => client.addPicked(), state.pending || state.mutationBusy || !state.collections.pickedAssets.length), action('Cancel adding assets', () => client.cancelPicker(), state.mutationBusy));
      get('retry-access').hidden = !state.active || !state.hasConnection || state.connected || state.restoring;
      get('search-button').disabled = state.searchBusy || state.restoring || state.mutationBusy;
      if (!state.hasConnection || state.query !== displayedQuery) { get('query').value = state.query; displayedQuery = state.query; }
      const contextBox = get('contexts'); contextBox.replaceChildren();
      state.contexts.forEach(a => contextBox.append(action(a.label + ' ×', () => client.context(a, true), false, 'library-context-chip')));
      const resultBox = get('results'); resultBox.replaceChildren();
      if (state.connected && !state.items.length) resultBox.append(node(document, 'li', state.mutationBusy ? 'Updating personal ranking…' : state.searchBusy ? 'Searching permitted assets…' : state.searchError ? 'Results are unavailable. Run a fresh search.' : 'No assets in this response. Try another query or context.', 'library-empty'));
      state.items.forEach(a => {
        const row = node(document, 'li', undefined, 'library-result');
        row.append(action(a.label || 'Unlabelled asset', () => client.inspect(a.asset_id), false, 'library-result-title'));
        const reasonNames = { context: 'Temporary context', declared_selection: 'Remembered by you', label_exact: 'Exact label', label_prefix: 'Label starts with query', label_contains: 'Label contains query', stable_id: 'Stable order' };
        row.append(node(document, 'p', a.reasons.map(r => reasonNames[r]).join(' · '), 'library-reasons'));
        const facts = node(document, 'details', undefined, 'library-identities'); facts.append(node(document, 'summary', 'Asset identity'), node(document, 'code', a.asset_id), node(document, 'p', a.head_revision_id === null ? 'Head revision undisclosed or not set.' : 'Disclosed head: ' + a.head_revision_id)); row.append(facts);
        if (state.collections.picker) { const b = action(state.collections.pickedAssets.includes(a.asset_id) ? 'Deselect asset' : 'Select asset to add', () => client.selectionToggle(a.asset_id), state.pending || state.mutationBusy, 'library-text-button'); b.setAttribute('aria-pressed', String(state.collections.pickedAssets.includes(a.asset_id))); row.append(b); }
        else row.append(action('Use as context', () => client.context(a), state.contexts.some(c => c.asset_id === a.asset_id), 'library-text-button')); resultBox.append(row);
      });
      get('window').textContent = state.searchError ? 'Results could not be completed. Run a fresh search.' : state.connected ? state.count + ' in this result window' + (state.complete ? '.' : ' · Incomplete window. Other permitted matches may exist; narrow the query or add context.') : '';
      get('more').hidden = !state.next; get('more').disabled = state.searchBusy || state.mutationBusy;
      const inspector = get('inspector-body'); inspector.replaceChildren();
      if (!renderInspectorNotice(document, inspector, state, () => client.loadSection('asset'))) {
        const assetSection = state.sections.asset;
        inspector.append(node(document, 'h4', assetSection && assetSection.value ? assetSection.value.asset.label || 'Unlabelled asset' : 'Asset inspector'));
        const identity = node(document, 'details', undefined, 'library-identities'); identity.append(node(document, 'summary', 'Asset identity'), node(document, 'code', state.selected)); inspector.append(identity);
        const preferenceBox = node(document, 'div', undefined, 'library-preference');
        preferenceBox.append(node(document, 'p', state.statusFresh ? state.preference ? 'Remembered for your personal ranking.' : 'No current personal selection.' : 'Personal selection state has not been verified.'));
        preferenceBox.append(action('Remember for me', () => client.remember(), !state.canMutate), action('Clear remembered selection', () => client.clear(), !state.canMutate || !state.preference), action('Check current state', () => client.status(), state.mutationBusy));
        inspector.append(preferenceBox);
        for (const kind of ['asset', 'counts', 'revisions', 'relationships', 'locations', 'history']) {
          const section = state.sections[kind]; const block = node(document, 'section', undefined, 'library-inspector-section'); block.append(node(document, 'h4', ({ asset: 'Asset', counts: 'Disclosed counts', revisions: 'Revisions', relationships: 'Relationships', locations: 'Location claims', history: 'Mutation history' })[kind]));
          if (!section || section.busy) block.append(node(document, 'p', 'Reading permitted metadata…'));
          if (section && section.error) { block.append(node(document, 'p', section.error, 'library-error'), action('Retry section', () => client.loadSection(kind))); }
          if (section && section.value) {
            const dl = node(document, 'dl', undefined, 'library-facts');
            if (kind === 'asset') { field(document, dl, 'Label', section.value.asset.label); field(document, dl, 'Head revision', section.value.asset.head_revision_id === null ? 'Undisclosed or not set' : section.value.asset.head_revision_id); }
            else for (const [key, label] of [['revision_count', 'Revisions'], ['relationship_count', 'Relationships'], ['location_count', 'Location claims']]) field(document, dl, label, section.value[key] === null ? 'Not disclosed with this access' : exactNumber(section.value[key]));
            block.append(dl);
          }
          if (section && section.items) {
            if (!section.items.length) block.append(node(document, 'p', 'No records in this permitted response.'));
            section.items.forEach(item => block.append(renderRecord(document, kind, item)));
            block.append(node(document, 'p', section.count + ' in this result window' + (section.complete ? '.' : ' · Incomplete window.')));
            if (section.next) block.append(action('More ' + kind, () => client.loadSection(kind, true), section.busy));
          }
          inspector.append(block);
        }
      }
      const outcome = get('outcome'); outcome.replaceChildren();
      if (state.pending) { outcome.append(node(document, 'p', 'An action has no matching confirmed receipt. Check current state separately; it cannot prove the earlier outcome.'), action('Retry same action', () => client.retry(), !state.retryable), action('Check current state', () => client.checkActionState(), state.mutationBusy)); }
      if (state.warning) outcome.append(node(document, 'p', state.warning.message), action('Check current state', () => client.checkActionState(), state.mutationBusy), action(state.warning.acknowledged ? 'Earlier outcome acknowledged' : 'Acknowledge unresolved outcome', () => client.acknowledge(), state.warning.acknowledged || !state.statusFresh));
      outcome.hidden = !state.pending && !state.warning;
      if (focused) { const replacement = document.getElementById(focused); if (replacement && replacement !== document.activeElement && !replacement.disabled && !replacement.closest('[hidden]')) replacement.focus({ preventScroll: true }); }
      if (collectionFocus && focusOwner && !focusOwner.isConnected) {
        const candidates = focusLabel ? Array.from(get('collections-panel').querySelectorAll('button')).filter(b => b.textContent === focusLabel && !b.disabled && !b.closest('[hidden]')) : [];
        const replacement = focused && document.getElementById(focused) || (candidates.length === 1 ? candidates[0] : get('collections-heading'));
        if (replacement && !replacement.disabled && !replacement.closest('[hidden]')) replacement.focus({ preventScroll: true });
      }
    }
    client = createLibraryClient({ base, onChange: render }); mounted = client;
    get('connection-form').addEventListener('submit', event => { event.preventDefault(); const bearer = get('credential').value.trim(); get('credential').value = ''; const library = get('id').value.trim(); get('id').value = ''; client.connect(library, bearer); });
    get('disconnect').addEventListener('click', () => client.disconnect());
    get('retry-access').addEventListener('click', () => client.refresh());
    get('search-form').addEventListener('submit', event => { event.preventDefault(); client.search(get('query').value); });
    get('more').addEventListener('click', () => client.search(undefined, true));
    get('view-assets').addEventListener('click', () => client.switchView('assets'));
    get('view-collections').addEventListener('click', () => client.switchView('collections'));
    document.addEventListener('visibilitychange', () => client.setActive(tabActive && !document.hidden));
    globalThis.addEventListener('pagehide', () => { get('credential').value = ''; get('id').value = ''; client.pagehide(); });
    client.setActive(tabActive && !document.hidden); render(client.snapshot());
  }
  function setActive(value) { tabActive = !!value; if (mounted) return mounted.setActive(tabActive && !globalThis.document.hidden); }
  return { init, setActive, createLibraryClient, createTransport, validate, validateCollection, collectionCanonical, randomCollectionID, renderRecord, renderInspectorNotice, node, LibraryError };
});

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
  const ids = { asset: /^asset_[0-9a-f]{32}$/, library: /^lib_[0-9a-f]{32}$/, revision: /^rev1_[0-9a-f]{64}$/, selection: /^sel1_[0-9a-f]{64}$/, page: /^page1_[0-9a-f]{64}$/ };
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
  function createTransport(base, options = {}) {
    if (typeof base !== 'string' || !/^(?:\/[a-zA-Z0-9_-]+)*$/.test(base)) throw new LibraryError('invalid');
    const fetcher = options.fetch || globalThis.fetch;
    const later = options.setTimeout || setTimeout, cancelTimer = options.clearTimeout || clearTimeout;
    return async function transport(route, body, credential, signal) {
      if (!/^v[12]\/(search|page|asset|counts|revisions|relationships|locations|history|select|clear|preference)$/.test(route)) throw new LibraryError('invalid');
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
          if (size > MAX_BODY) throw new LibraryError('invalid_response');
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
    const generations = { search: 0, detail: 0, status: 0, restore: 0 };
    const requests = new Set(); let readCount = 0; const waiters = [];
    function clearExpiry() { if (expiryTimer !== null) { clearTimeout(expiryTimer); expiryTimer = null; } }
    function expire() {
      if (pending && (pending.expired || clock() >= pending.expires)) {
        clearExpiry(); invalidate('status');
        warning = { assetID: pending.assetID, acknowledged: false, message: 'An earlier action may have been accepted. Its retry window ended. Check current state, then acknowledge before a new action.' };
        pending = null; statusFresh = false;
      }
    }
    function view() {
      expire();
      return { active, hasConnection: !!credential, connected: active && connected, library: active && connected ? library : '', message, notice, searchError,
        query, contexts: active ? contexts.slice() : [], items: active ? items.slice() : [], next: active && !!next,
        count, complete, searchBusy, restoring, inspector: active && connected && selected ? sections.asset && sections.asset.value ? 'ready' : sections.asset && sections.asset.error ? 'error' : 'loading' : 'idle', inspectorError: active && connected && sections.asset && sections.asset.error || '', selected: active && connected && sections.asset && sections.asset.value ? selected : '', sections: active ? sections : {},
        preference: active && statusFresh ? preference !== null : null, statusFresh: active && statusFresh,
        mutationBusy, pending: !!pending, retryable: !!pending && !pending.expired && !mutationBusy,
        warning: warning ? { acknowledged: warning.acknowledged, message: warning.message } : null, canMutate: active && connected && statusFresh && !pending && !mutationBusy && (!warning || warning.acknowledged) };
    }
    function emit() { onChange(view()); }
    function stamp(scope) { return { epoch, scope, generation: generations[scope] }; }
    function current(s, visible = true) { return s.epoch === epoch && s.generation === generations[s.scope] && !!credential && (!visible || active); }
    function invalidate(scope) { generations[scope]++; for (let i = waiters.length - 1; i >= 0; i--) if (waiters[i].scope === scope) { waiters.splice(i, 1)[0].resolve(false); } for (const r of requests) if (r.scope === scope) r.controller.abort(); }
    function abortReads() { for (const key of Object.keys(generations)) invalidate(key); }
    function clearScoped() { contexts = []; items = []; next = null; count = 0; sections = {}; preference = null; statusFresh = false; searchBusy = false; restoring = false; }
    function reset(note) {
      epoch++; clearExpiry(); abortReads(); for (const key of Object.keys(generations)) generations[key] = 0; for (const r of requests) r.controller.abort();
      credential = ''; library = ''; connected = false; contextIDs = []; query = ''; selected = ''; pending = null; warning = null; mutationBusy = false;
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
      if (!await status()) return;
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
    async function checkActionState() { const target = pending ? pending.assetID : warning ? warning.assetID : selected; if (target && target !== selected) await inspect(target); else await status(); }
    function acknowledge() { expire(); if (warning && statusFresh && selected === warning.assetID) { warning.acknowledged = true; message = 'Earlier outcome acknowledged. Current state is shown separately.'; emit(); } }
    return { connect, setActive, search, inspect, status, loadSection, context, remember: () => mutate('select'), clear: () => mutate('clear'), retry: dispatch, acknowledge,
      checkActionState, refresh: resume, disconnect: () => reset(pending || mutationBusy ? 'Disconnected. An action may have been accepted; its retry details were forgotten.' : 'Disconnected.'), pagehide: () => reset(pending || mutationBusy ? 'An action may have been accepted before leaving.' : ''), snapshot: view };
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
    function render(state) {
      get('connection-state').textContent = state.connected ? 'Connected to library' : state.hasConnection ? 'Connection needs revalidation' : 'Connect to an existing catalog';
      get('disconnect').hidden = !state.hasConnection; get('connection-form').hidden = state.hasConnection;
      get('status').textContent = [...new Set([state.notice, state.message].filter(Boolean))].join(' '); get('workspace').hidden = !state.active || !state.connected;
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
        row.append(action('Use as context', () => client.context(a), state.contexts.some(c => c.asset_id === a.asset_id), 'library-text-button')); resultBox.append(row);
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
    }
    client = createLibraryClient({ base, onChange: render }); mounted = client;
    get('connection-form').addEventListener('submit', event => { event.preventDefault(); const bearer = get('credential').value.trim(); get('credential').value = ''; const library = get('id').value.trim(); get('id').value = ''; client.connect(library, bearer); });
    get('disconnect').addEventListener('click', () => client.disconnect());
    get('retry-access').addEventListener('click', () => client.refresh());
    get('search-form').addEventListener('submit', event => { event.preventDefault(); client.search(get('query').value); });
    get('more').addEventListener('click', () => client.search(undefined, true));
    document.addEventListener('visibilitychange', () => client.setActive(tabActive && !document.hidden));
    globalThis.addEventListener('pagehide', () => { get('credential').value = ''; get('id').value = ''; client.pagehide(); });
    client.setActive(tabActive && !document.hidden); render(client.snapshot());
  }
  function setActive(value) { tabActive = !!value; if (mounted) return mounted.setActive(tabActive && !globalThis.document.hidden); }
  return { init, setActive, createLibraryClient, createTransport, validate, renderRecord, renderInspectorNotice, node, LibraryError };
});

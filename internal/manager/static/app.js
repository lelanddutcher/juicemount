// JuiceMount Manager — vanilla JS UI. No frameworks, no build step.
// Embedded mode: served from /manager/ inside juicemount-server, so
// all API paths must be relative to the page's base path.
//
// SLICE 0 layout: a sidebar selects one of several <section
// data-tab="..."> panes via hash-routes (#/overview, #/migrations,
// #/trash, ...). The Migrations pane is the only functional one in
// this slice; every other entry shows a "Coming soon" placeholder.

(() => {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  // Base path = the dir part of the current URL (e.g. /manager/ → /manager).
  // All /api/... calls are prefixed with this so the same JS works under
  // any deployment prefix.
  const BASE = (() => {
    const p = location.pathname;
    const trimmed = p.endsWith('/') ? p.slice(0, -1) : p.replace(/\/[^/]*$/, '');
    return trimmed || '';
  })();
  const apiURL = (p) => BASE + '/api' + p;

  // ---------------------------------------------------------------
  // Hash routing
  // ---------------------------------------------------------------
  // The sidebar entries are plain anchors with href="#/<name>"; the
  // browser updates location.hash on click and fires the hashchange
  // event we listen for. Default route is #/migrations because it's
  // the only functional tab in SLICE 0 — landing the user anywhere
  // else would be confusing.
  //
  // Trade-off (per §3.3 of the manager roadmap): hash-routes vs.
  // History API. Hash-routes win because the static handler in Go
  // doesn't need to know about every route — every URL still resolves
  // to the same index.html and the JS picks the section.
  const TABS = [
    'overview',
    'migrations',
    'trash',
    'destinations',
    'backups',
    'maintenance',
    'settings',
  ];
  const DEFAULT_TAB = 'migrations';

  // route reads location.hash, normalizes it, and shows the matching
  // tab. Unknown / empty / malformed hashes fall back to DEFAULT_TAB.
  // Also rewrites location.hash so the URL bar reflects the resolved
  // route (so a bookmark of #/ becomes #/migrations).
  function route() {
    const raw = (location.hash || '').replace(/^#\/?/, '').split('?')[0];
    const name = TABS.includes(raw) ? raw : DEFAULT_TAB;
    if (location.hash !== '#/' + name) {
      // Use replaceState so this normalization doesn't pollute the
      // back-button history with an extra entry.
      history.replaceState(null, '', '#/' + name);
    }
    showTab(name);
  }

  // showTab swaps which <section data-tab="X"> is visible and marks
  // the corresponding sidebar link as active. Also runs the lazy-init
  // hook for Migrations on first activation — subsequent activations
  // are cheap (just unhiding the same DOM).
  function showTab(name) {
    $$('.tab-pane').forEach((el) => {
      el.hidden = el.dataset.tab !== name;
    });
    $$('.sidebar a[data-tab-link]').forEach((a) => {
      a.classList.toggle('active', a.dataset.tabLink === name);
    });
    if (name === 'migrations') {
      initMigrationsOnce();
    }
  }

  window.addEventListener('hashchange', route);

  // -------- State --------
  const state = {
    sourceRoots: [],
    destMount: '/jfs',
    activeRoot: null,
    cwd: null,
    selectedPath: null,
    jobs: new Map(),
    adminKey: localStorage.getItem('jm-admin-key') || '',
    previewAbort: null,
    destPreviewAbort: null,
    destPreviewTimer: null,
    // SLICE 1: which Direction the user has picked. Drives which
    // browse endpoint the source picker hits, what the dest input
    // placeholder shows, and which Direction value goes on the
    // POST /api/migrate body. Default "in" matches the pre-SLICE-1
    // behavior so the page boots into the familiar workflow.
    direction: 'in',
    // Last computed source-preview totals for the current selection.
    // Passed into the job on submit so the progress bar can show real %
    // instead of an indeterminate placeholder.
    previewTotals: { bytes: 0, files: 0, truncated: false },
  };

  // -------- Fetch helpers --------
  function authHeaders() {
    const h = { 'Content-Type': 'application/json' };
    if (state.adminKey) h['X-JuiceMount-Admin-Key'] = state.adminKey;
    return h;
  }

  async function api(method, path, body) {
    const opts = { method, headers: authHeaders() };
    if (body) opts.body = JSON.stringify(body);
    // path is "/api/..." with no base; rewrite to BASE-relative.
    const url = path.startsWith('/api') ? BASE + path : path;
    const r = await fetch(url, opts);
    if (r.status === 401) {
      const key = prompt('X-JuiceMount-Admin-Key (will be saved locally):');
      if (key) {
        state.adminKey = key;
        localStorage.setItem('jm-admin-key', key);
        return api(method, path, body); // retry once
      }
      throw new Error('authentication required');
    }
    if (!r.ok) throw new Error(`${r.status} ${r.statusText} — ${await r.text()}`);
    if (r.status === 204) return null;
    return r.json();
  }

  // -------- Source roots --------
  async function loadSources() {
    const data = await api('GET', '/api/sources');
    state.sourceRoots = data.sources || [];
    state.destMount = data.destination || '/jfs';
    renderSourceRoots();
  }

  // sourceRootsForDirection returns the list of root paths the
  // source-picker should surface for the active Direction. For
  // DirectionIn that's the configured /sources/... mounts; for
  // DirectionOut / DirectionBetween it's a single entry — the
  // JuiceFS volume root (/jfs). Keeps the UI flow identical across
  // directions: pick a root, then browse into it.
  function sourceRootsForDirection() {
    if (state.direction === 'in') return state.sourceRoots;
    // Out / Between: the only valid source root is the JuiceFS volume.
    return [state.destMount];
  }

  // browseEndpointForPath returns the right /api/browse... endpoint
  // for a path. /jfs/... paths hit the SLICE-1 /api/browse-jfs handler;
  // everything else hits the original /api/browse. The split keeps the
  // backend's pathAllowed checks tight (each handler validates against
  // exactly one root).
  function browseEndpointForPath(path) {
    if (path === state.destMount || path.startsWith(state.destMount + '/')) {
      return '/api/browse-jfs';
    }
    return '/api/browse';
  }

  function renderSourceRoots() {
    const wrap = $('#source-roots');
    wrap.innerHTML = '';
    const roots = sourceRootsForDirection();
    if (roots.length === 0) {
      wrap.textContent = '(no source roots configured)';
      return;
    }
    for (const root of roots) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'source-root-btn';
      btn.textContent = root;
      btn.addEventListener('click', () => browseInto(root, btn));
      wrap.appendChild(btn);
    }
  }

  // -------- Browser --------
  async function browseInto(path, btn) {
    $$('.source-root-btn').forEach((b) => b.classList.toggle('active', b === btn));
    const roots = sourceRootsForDirection();
    state.activeRoot = roots.find((r) => path === r || path.startsWith(r + '/')) || path;
    await listDir(path);
  }

  async function listDir(path) {
    const endpoint = browseEndpointForPath(path);
    const data = await api('GET', `${endpoint}?path=${encodeURIComponent(path)}`);
    state.cwd = data.path;
    state.selectedPath = path;
    updateSelectedDisplay();
    suggestDefaultDestination(path);

    $('#browser').hidden = false;
    $('#cwd').textContent = state.cwd;
    $('#up-btn').disabled = state.cwd === state.activeRoot;

    const ul = $('#entries');
    ul.innerHTML = '';

    for (const e of (data.entries || [])) {
      const li = document.createElement('li');
      li.dataset.name = e.name;
      li.dataset.isDir = String(e.is_dir);

      const icon = document.createElement('span');
      icon.className = 'entry-icon';
      icon.textContent = e.is_dir ? '📁' : '📄';
      li.appendChild(icon);

      const name = document.createElement('span');
      name.className = 'entry-name';
      name.textContent = e.name;
      li.appendChild(name);

      if (!e.is_dir) {
        const sz = document.createElement('span');
        sz.className = 'entry-size';
        sz.textContent = formatBytes(e.size);
        li.appendChild(sz);
      }

      li.addEventListener('click', () => onEntryClick(e, li));
      li.addEventListener('dblclick', () => {
        if (e.is_dir) listDir(state.cwd + '/' + e.name);
      });
      ul.appendChild(li);
    }
  }

  function onEntryClick(entry, li) {
    $$('#entries li').forEach((x) => x.classList.toggle('selected', x === li));
    state.selectedPath = state.cwd + '/' + entry.name;
    updateSelectedDisplay();
    suggestDefaultDestination(state.selectedPath);
  }

  function updateSelectedDisplay() {
    $('#selected-source').textContent = state.selectedPath || '(nothing selected yet)';
    $('#start-btn').disabled = !state.selectedPath;
    if (state.selectedPath) {
      fetchPreview(state.selectedPath);
    } else {
      $('#preview').hidden = true;
    }
  }

  function suggestDefaultDestination(source) {
    const input = $('#dest-input');
    if (input.value && input.dataset.userEdited === 'true') {
      updateDestPreview();
      return;
    }
    // SLICE 1: destination shape depends on Direction:
    //   - In: strip source-root prefix, prepend destMount (/jfs/...)
    //   - Out: strip destMount prefix from source, prepend the first
    //     configured source-root (so /jfs/Film → /sources/.../Film).
    //     The user can still edit.
    //   - Between: stub — leave blank; the validation flow surfaces
    //     the Destinations-tab message when they hit Start.
    if (state.direction === 'in') {
      let rel = source;
      let match = '';
      for (const root of state.sourceRoots) {
        if ((source === root || source.startsWith(root + '/')) && root.length > match.length) {
          match = root;
        }
      }
      if (match) {
        rel = source.slice(match.length);
        if (source === match) {
          rel = '/' + (match.split('/').filter(Boolean).pop() || 'imported');
        }
      }
      if (!rel.startsWith('/')) rel = '/' + rel;
      input.value = state.destMount + rel;
    } else if (state.direction === 'out') {
      let rel = source;
      if (source === state.destMount) {
        rel = '/exported';
      } else if (source.startsWith(state.destMount + '/')) {
        rel = source.slice(state.destMount.length);
      }
      if (!rel.startsWith('/')) rel = '/' + rel;
      const firstHostRoot = state.sourceRoots[0] || '/external';
      input.value = firstHostRoot + rel;
    } else {
      // direction === 'between' — placeholder only; submission will
      // surface the SLICE-4 message.
      input.value = '';
    }
    updateDestPreview();
  }

  $('#dest-input').addEventListener('input', (e) => {
    e.target.dataset.userEdited = 'true';
    updateDestPreview();
  });

  $('#up-btn').addEventListener('click', () => {
    const idx = state.cwd.lastIndexOf('/');
    if (idx <= 0) return;
    const parent = state.cwd.slice(0, idx) || '/';
    listDir(parent);
  });

  // -------- Direction --------
  // SLICE 1: hash-route stays on Migrations; the Direction picker
  // is a sub-control of the Migrations tab. Changing it resets the
  // active selection (source roots differ between In/Out) and
  // rerenders the picker. Submit-time the Direction value goes on
  // the POST body so handleMigrate can apply the right shape rules.
  function onDirectionChange() {
    const sel = document.querySelector('input[name="direction"]:checked');
    if (!sel) return;
    state.direction = sel.value;
    // Adjust the source-pane hint so users know which root the
    // picker is browsing.
    const hint = $('#source-hint');
    if (state.direction === 'in') {
      hint.textContent = 'Browse a source root and pick a directory to import into JuiceFS.';
    } else if (state.direction === 'out') {
      hint.textContent = 'Pick a directory inside /jfs to export to a host path.';
    } else {
      hint.textContent = 'Pick a directory inside /jfs to copy to a second JuiceFS volume (Destinations tab, slice-4).';
    }
    // Clear the current selection; the previous source root may not
    // even be visible under the new Direction.
    state.activeRoot = null;
    state.cwd = null;
    state.selectedPath = null;
    $('#browser').hidden = true;
    $('#preview').hidden = true;
    $('#dest-preview').hidden = true;
    $('#dest-input').value = '';
    $('#dest-input').dataset.userEdited = 'false';
    $('#start-btn').disabled = true;
    updateSelectedDisplay();
    renderSourceRoots();
  }
  $$('input[name="direction"]').forEach((r) => r.addEventListener('change', onDirectionChange));

  // -------- Migrate --------
  $('#start-btn').addEventListener('click', async () => {
    const dest = $('#dest-input').value.trim();
    if (!state.selectedPath || !dest) return;
    $('#error').hidden = true;
    try {
      const job = await api('POST', '/api/migrate', {
        source: state.selectedPath,
        destination: dest,
        direction: state.direction,
        options: collectOptions(),
        // Pass the preview's scanned bytes total. If the scan was
        // truncated we still send it — the bar will overshoot 100%
        // and clamp visually, which is better UX than no bar at all.
        total_bytes: state.previewTotals.bytes,
      });
      $('#dest-input').dataset.userEdited = 'false';
      addJob(job);
    } catch (err) {
      $('#error').textContent = err.message;
      $('#error').hidden = false;
    }
  });

  // -------- Options form → JSON --------
  function collectOptions() {
    const linesOf = (id) => $(id).value.split('\n').map(s => s.trim()).filter(Boolean);
    return {
      preserve_structure: $('#opt-preserve-structure').checked,
      preserve_times:     $('#opt-preserve-times').checked,
      dry_run:            $('#opt-dry-run').checked,
      skip_junk:          $('#opt-skip-junk').checked,
      verify:             $('#opt-verify').checked,
      bw_limit:           Math.max(0, parseInt($('#opt-bwlimit').value, 10) || 0),
      threads:            Math.max(1, parseInt($('#opt-threads').value, 10) || 10),
      excludes:           linesOf('#opt-excludes'),
      includes:           linesOf('#opt-includes'),
    };
  }

  // -------- Destination preview --------
  // Calls /api/resolve-destination with the current source + dest +
  // preserve toggle, displays the resolved URLs and 3 example file
  // mappings so users can sanity-check where files will land BEFORE
  // hitting Start. Debounced 150ms to absorb keystrokes.
  function updateDestPreview() {
    if (state.destPreviewTimer) clearTimeout(state.destPreviewTimer);
    state.destPreviewTimer = setTimeout(updateDestPreviewNow, 150);
  }

  async function updateDestPreviewNow() {
    const dest = $('#dest-input').value.trim();
    const previewEl = $('#dest-preview');
    if (!state.selectedPath || !dest) {
      previewEl.hidden = true;
      return;
    }
    previewEl.hidden = false;
    const errEl = $('#dest-preview-error');
    errEl.hidden = true;

    if (state.destPreviewAbort) state.destPreviewAbort.abort();
    state.destPreviewAbort = new AbortController();

    try {
      const r = await fetch(apiURL('/resolve-destination'), {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({
          source: state.selectedPath,
          destination: dest,
          direction: state.direction,
          preserve_structure: $('#opt-preserve-structure').checked,
        }),
        signal: state.destPreviewAbort.signal,
      });
      if (!r.ok) {
        const msg = await r.text();
        errEl.textContent = msg.trim() || `${r.status} ${r.statusText}`;
        errEl.hidden = false;
        $('#dest-preview-source-url').textContent = '—';
        $('#dest-preview-dest-url').textContent = '—';
        $('#dest-preview-examples').innerHTML = '';
        $('#dest-preview-info').textContent = '';
        return;
      }
      const data = await r.json();
      $('#dest-preview-info').textContent = data.info || '';
      $('#dest-preview-source-url').textContent = data.source_url || '—';
      $('#dest-preview-dest-url').textContent = data.destination_url || '—';
      const ul = $('#dest-preview-examples');
      ul.innerHTML = '';
      const mappings = (data.example_mappings || []).slice(0, 3);
      if (mappings.length === 0) {
        const li = document.createElement('li');
        li.className = 'src';
        li.textContent = '(no sample files found in selection)';
        ul.appendChild(li);
      }
      for (const m of mappings) {
        const li = document.createElement('li');
        li.innerHTML = `<span class="src">${escapeHtml(m.source)}</span>` +
                       `<span class="arrow">→</span>` +
                       `<span class="dst">${escapeHtml(m.destination)}</span>`;
        ul.appendChild(li);
      }
    } catch (err) {
      if (err.name === 'AbortError') return;
      errEl.textContent = 'Preview failed: ' + err.message;
      errEl.hidden = false;
    }
  }

  // Preserve-structure toggle directly affects destination semantics,
  // so re-run the preview when it flips.
  $('#opt-preserve-structure').addEventListener('change', updateDestPreview);

  // -------- Source preview --------
  async function fetchPreview(path) {
    if (state.previewAbort) state.previewAbort.abort();
    state.previewAbort = new AbortController();
    const previewEl = $('#preview');
    previewEl.hidden = false;
    $('#preview-files').textContent = '…';
    $('#preview-size').textContent = '…';
    $('#preview-dirs').textContent = '…';
    $('#preview-types').textContent = 'scanning…';
    try {
      const url = `${BASE}/api/preview?path=${encodeURIComponent(path)}`;
      const r = await fetch(url, { headers: authHeaders(), signal: state.previewAbort.signal });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      const data = await r.json();
      // When the walker hit the entry cap, every number is a lower
      // bound, not a final value. Prefix with "≥" and add a "still
      // scanning" note so users don't read partial totals as truth.
      state.previewTotals = {
        bytes: data.bytes,
        files: data.files,
        truncated: !!data.truncated,
      };
      const prefix = data.truncated ? '≥' : '';
      $('#preview-files').textContent = prefix + data.files.toLocaleString();
      $('#preview-size').textContent = prefix + formatBytes(data.bytes);
      $('#preview-dirs').textContent = prefix + data.directories.toLocaleString();
      const exts = Object.entries(data.ext_counts || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, 6)
        .map(([k, v]) => `<span class="ext-pill"><strong>${escapeHtml(k)}</strong>: ${v.toLocaleString()}</span>`)
        .join(' ');
      $('#preview-types').innerHTML = exts +
        (data.truncated ? ' <span class="hint">(scan capped — totals are lower bounds)</span>' : '');
    } catch (err) {
      if (err.name === 'AbortError') return;
      $('#preview-types').textContent = 'preview failed: ' + err.message;
    }
  }

  // -------- Jobs --------
  async function loadJobs() {
    const jobs = await api('GET', '/api/jobs');
    for (const j of (jobs || [])) addJob(j);
  }

  function addJob(job) {
    if (state.jobs.has(job.id)) {
      // Update existing card
      renderJob(job);
      return;
    }
    state.jobs.set(job.id, { job, sse: null });
    renderJob(job);
    if (job.state === 'pending' || job.state === 'running') {
      subscribeJob(job.id);
    }
  }

  function subscribeJob(id) {
    const entry = state.jobs.get(id);
    if (!entry || entry.sse) return;
    const url = state.adminKey
      ? `${BASE}/api/jobs/${id}/stream?key=${encodeURIComponent(state.adminKey)}`
      : `${BASE}/api/jobs/${id}/stream`;
    const es = new EventSource(url);
    entry.sse = es;
    es.addEventListener('message', async (e) => {
      try {
        const ev = JSON.parse(e.data);
        entry.job.last = ev;
        // Refresh job state from the JSON endpoint occasionally;
        // SSE only carries ProgressEvent so the state field may
        // be stale.
        const fresh = await api('GET', `/api/jobs/${id}`);
        Object.assign(entry.job, fresh);
        renderJob(entry.job);
        if (['done', 'error', 'canceled'].includes(entry.job.state)) {
          es.close();
          entry.sse = null;
        }
      } catch (err) {
        console.error('progress parse', err);
      }
    });
    es.addEventListener('error', () => {
      es.close();
      entry.sse = null;
      // poll once more in 2s to get final state
      setTimeout(async () => {
        try {
          const fresh = await api('GET', `/api/jobs/${id}`);
          Object.assign(entry.job, fresh);
          renderJob(entry.job);
        } catch (_) {}
      }, 2000);
    });
  }

  function renderJob(job) {
    let el = document.getElementById('job-' + job.id);
    if (!el) {
      el = document.createElement('li');
      el.id = 'job-' + job.id;
      el.className = 'job';
      $('#job-list').prepend(el);
    }

    const last = job.last || {};
    const bytes = last.bytes || 0;
    const files = last.files || 0;
    const errors = last.errors || 0;
    const etaSec = last.eta_sec || 0;

    // Real progress when we have a total from the preview pane;
    // indeterminate placeholder otherwise.
    let progressClass = '';
    let progressWidth = '0%';
    const total = job.total_bytes || 0;
    if (job.state === 'done') { progressClass = 'done'; progressWidth = '100%'; }
    else if (job.state === 'error') { progressClass = 'error'; progressWidth = '100%'; }
    else if (job.state === 'canceled') { progressWidth = '0%'; }
    else if (job.state === 'running') {
      if (total > 0) {
        const pct = Math.min(99, Math.max(1, Math.round((bytes / total) * 100)));
        progressWidth = pct + '%';
      } else {
        progressClass = 'indeterminate';
        progressWidth = '100%';
      }
    }

    el.innerHTML = `
      <div class="job-head">
        <span class="job-id">${job.id}</span>
        <span class="job-state ${job.state}">${job.state}</span>
      </div>
      <div class="job-paths">${escapeHtml(job.source)} → ${escapeHtml(job.destination)}</div>
      <div class="progress-bar"><div class="progress-fill ${progressClass}" style="width:${progressWidth}"></div></div>
      <div class="job-stats">
        <span><strong>${files}</strong> files</span>
        <span><strong>${formatBytes(bytes)}</strong>${total > 0 ? ' / ' + formatBytes(total) : ''} copied</span>
        ${total > 0 && job.state === 'running' ? `<span><strong>${Math.min(99, Math.round((bytes / total) * 100))}%</strong></span>` : ''}
        <span><strong>${errors}</strong> errors</span>
        ${etaSec > 0 ? `<span>ETA <strong>${etaSec}s</strong></span>` : ''}
        ${(job.state === 'pending' || job.state === 'running')
          ? `<button class="job-cancel" data-id="${job.id}">Cancel</button>` : ''}
        ${(job.state === 'canceled' || job.state === 'error')
          ? `<button class="job-resume" data-id="${job.id}">Resume</button>` : ''}
      </div>
      ${job.error ? `<p class="error">${escapeHtml(job.error)}</p>` : ''}
    `;
    el.querySelectorAll('.job-cancel').forEach((b) => {
      b.addEventListener('click', () => cancelJob(b.dataset.id));
    });
    el.querySelectorAll('.job-resume').forEach((b) => {
      b.addEventListener('click', () => resumeJob(b.dataset.id));
    });
  }

  // resumeJob re-submits a canceled/errored job's source+dest+options
  // as a new job. juicefs sync --update --check-change is in args by
  // default so files already at the destination are skipped, making
  // this a true resume rather than a re-copy. The original job stays
  // in the list (terminal state) and a fresh card appears at the top.
  async function resumeJob(id) {
    const entry = state.jobs.get(id);
    if (!entry) return;
    const orig = entry.job;
    // SLICE 1: prefer the persisted direction (now stored on Job since
    // the slice-1 reviewer fix). Falls back to path inference only for
    // pre-SLICE-1 records that lack the field. Backend also defaults
    // empty → "in", so this is belt-and-suspenders for the UI.
    let dir = orig.direction || '';
    if (!dir) {
      dir = 'in';
      if (orig.source && (orig.source === state.destMount || orig.source.startsWith(state.destMount + '/'))) {
        dir = 'out';
      }
    }
    // Sync the UI radio to the resumed job's direction so the picker
    // doesn't lie about the new job's direction immediately after submit.
    state.direction = dir;
    const radio = document.querySelector(`input[name="direction"][value="${dir}"]`);
    if (radio) radio.checked = true;
    try {
      const job = await api('POST', '/api/migrate', {
        source: orig.source,
        destination: orig.destination,
        direction: dir,
        options: orig.options,
        total_bytes: orig.total_bytes || 0,
      });
      addJob(job);
    } catch (err) {
      $('#error').textContent = 'Resume failed: ' + err.message;
      $('#error').hidden = false;
    }
  }

  async function cancelJob(id) {
    try {
      await api('DELETE', `/api/jobs/${id}`);
    } catch (e) {
      console.error('cancel', e);
    }
  }

  // -------- Helpers --------
  function formatBytes(b) {
    if (!b) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    let i = 0;
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return `${b.toFixed(i ? 2 : 0)} ${u[i]}`;
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  // -------- Migrations tab lazy init --------
  // The migrator API endpoints (sources, jobs, etc.) are network
  // calls; we don't want them firing for users who navigate straight
  // to a placeholder tab. initMigrationsOnce guards against repeat
  // initialization when the user toggles between tabs.
  let migrationsInited = false;
  function initMigrationsOnce() {
    if (migrationsInited) return;
    migrationsInited = true;
    (async () => {
      try {
        await loadSources();
        await loadJobs();
        if (state.adminKey) {
          $('#auth-state').textContent = 'Admin key configured';
        }
      } catch (err) {
        $('#error').textContent = 'Failed to initialize: ' + err.message;
        $('#error').hidden = false;
      }
    })();
    // Re-poll jobs list periodically to catch new entries created
    // out-of-band (e.g. from another browser tab or jmctl). Interval
    // starts only after first activation so placeholder tabs don't
    // fire useless requests.
    setInterval(loadJobs, 10000);
  }

  // -------- Boot --------
  // route() reads location.hash, falls back to DEFAULT_TAB
  // (#/migrations), and shows the matching <section data-tab>.
  // showTab → initMigrationsOnce, so visiting the page with the
  // default route immediately fires the migrator boot path.
  route();
})();

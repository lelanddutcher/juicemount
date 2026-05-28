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
    // SLICE 2: lifecycle the overview poller so it only runs while
    // the tab is actually visible to the user. startOverviewPolling
    // is idempotent — repeated start calls just refresh the cached
    // tab-active flag. The visibilitychange listener (set up in
    // initOverviewOnce) handles tab-hidden pauses orthogonally.
    if (name === 'overview') {
      initOverviewOnce();
      startOverviewPolling();
    } else {
      stopOverviewPolling();
    }
    // SLICE 3: lazy-init Trash on first activation. Subsequent
    // activations call refreshTrash() so the list reflects any
    // out-of-band deletions/restores since the user last viewed it.
    if (name === 'trash') {
      initTrashOnce();
      refreshTrash();
    }
    // SLICE 6: lazy-init Maintenance. Wires the per-card Run buttons
    // and resumes any SSE stream that was open when the user
    // navigated away — handled by re-reading active op state via
    // GET /api/maintenance/{kind}.
    if (name === 'maintenance') {
      initMaintenanceOnce();
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

  // -------- Overview (SLICE 2) --------
  // Read-only dashboard. Polls /api/overview every OVERVIEW_INTERVAL_MS
  // while the tab is visible AND the document is visible. Polling
  // pauses on document.hidden (background tab / screen lock) and on
  // tab-switch away from #/overview; it resumes on the next visible
  // window. Each card re-renders from one section of OverviewSnapshot
  // — per-section .error strings render in a small error pill so a
  // failing backend degrades only that card.
  const OVERVIEW_INTERVAL_MS = 10000;
  const overviewState = {
    inited: false,
    timer: null,
    inFlight: false,
  };

  // initOverviewOnce wires the visibilitychange listener exactly once.
  // The listener pauses/resumes the poller based on document.visibilityState
  // so background tabs don't burn CPU or fire backend probes.
  function initOverviewOnce() {
    if (overviewState.inited) return;
    overviewState.inited = true;
    document.addEventListener('visibilitychange', () => {
      // visibilityState transitions: 'visible' ↔ 'hidden'. We only
      // (re)start polling when the dashboard tab is the active one;
      // a hidden window while sitting on Overview should not be
      // polling. Conversely a foreground window on a different tab
      // should not be polling Overview either.
      const onOverview = location.hash === '#/overview';
      if (document.visibilityState === 'visible' && onOverview) {
        startOverviewPolling();
      } else {
        stopOverviewPolling();
      }
    });
  }

  function startOverviewPolling() {
    if (overviewState.timer) return; // already running
    if (document.visibilityState !== 'visible') return; // window hidden
    // Fire one immediately so the UI doesn't show stale "—" placeholders
    // for the first OVERVIEW_INTERVAL_MS while waiting for the tick.
    pollOverview();
    overviewState.timer = setInterval(pollOverview, OVERVIEW_INTERVAL_MS);
  }

  function stopOverviewPolling() {
    if (!overviewState.timer) return;
    clearInterval(overviewState.timer);
    overviewState.timer = null;
  }

  async function pollOverview() {
    if (overviewState.inFlight) return; // skip overlap; next tick re-tries
    overviewState.inFlight = true;
    try {
      const snap = await api('GET', '/api/overview');
      renderOverview(snap);
    } catch (err) {
      // The endpoint is supposed to never 5xx, so anything reaching
      // here is a transport-layer issue (network blip, auth prompt
      // cancellation, page navigated mid-flight). Show a non-blocking
      // hint in the header without nuking the previously-rendered cards.
      const upd = document.getElementById('overview-updated');
      if (upd) upd.textContent = 'last poll failed: ' + (err.message || err);
    } finally {
      overviewState.inFlight = false;
    }
  }

  function renderOverview(snap) {
    const upd = document.getElementById('overview-updated');
    if (upd && snap && snap.collected_at) {
      const d = new Date(snap.collected_at);
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      const ss = String(d.getSeconds()).padStart(2, '0');
      upd.textContent = `last updated ${hh}:${mm}:${ss}`;
    }
    renderOverviewCard('volume', snap.volume, (card, v) => {
      setField(card, 'name', v.name || '(unset)');
      setField(card, 'used', formatBytes(v.used_bytes || 0));
      setField(card, 'files', (v.files || 0).toLocaleString());
    });
    renderOverviewCard('redis', snap.redis, (card, v) => {
      setField(card, 'latency', v.latency_ms != null ? v.latency_ms + ' ms' : '—');
      setField(card, 'version', v.version || '—');
      setField(card, 'memory', v.used_memory_mb ? v.used_memory_mb + ' MB' : '—');
      setField(card, 'uptime', v.uptime_sec ? formatDuration(v.uptime_sec * 1000) : '—');
    });
    renderOverviewCard('minio', snap.minio, (card, v) => {
      setField(card, 'endpoint', v.endpoint || '—');
      setField(card, 'latency', v.latency_ms != null ? v.latency_ms + ' ms' : '—');
    });
    renderOverviewCard('cache', snap.cache, (card, v) => {
      setField(card, 'hit', v.available ? (v.hit_rate_pct || 0).toFixed(1) + '%' : 'unavailable');
      setField(card, 'reads', v.available ? (v.read_ops_per_s || 0).toFixed(1) : 'unavailable');
      setField(card, 'writes', v.available ? (v.write_ops_per_s || 0).toFixed(1) : 'unavailable');
    });
    renderOverviewJobs(snap.jobs);
  }

  // renderOverviewCard updates one stat-card. Each card has a section
  // header pill (ok / warn / error) and an .overview-error <p>
  // populated from the section's .error string. The body-render
  // closure handles the per-card field wiring.
  function renderOverviewCard(name, section, fillBody) {
    const card = document.querySelector(`.overview-card[data-card="${name}"]`);
    if (!card || !section) return;
    const errEl = card.querySelector('.overview-error');
    const stateEl = card.querySelector('.overview-card-state');
    if (section.error) {
      errEl.textContent = section.error;
      errEl.hidden = false;
      stateEl.textContent = 'error';
      stateEl.className = 'overview-card-state error';
    } else {
      errEl.hidden = true;
      errEl.textContent = '';
      // OK pill state: most cards default to "ok" when no error; the
      // Redis/MinIO cards key off .reachable, and Cache keys off
      // .available, so the dashboard's pill stays informative even
      // when the backend returns successfully-but-unreachable.
      let ok = true;
      if (name === 'redis' || name === 'minio') ok = !!section.reachable;
      if (name === 'cache') ok = !!section.available;
      stateEl.textContent = ok ? 'ok' : 'down';
      stateEl.className = ok ? 'overview-card-state ok' : 'overview-card-state warn';
    }
    fillBody(card, section);
  }

  function setField(card, key, value) {
    const el = card.querySelector(`[data-field="${key}"]`);
    if (el) el.textContent = value;
  }

  function renderOverviewJobs(section) {
    const card = document.querySelector('.overview-card[data-card="jobs"]');
    if (!card) return;
    const errEl = card.querySelector('.overview-error');
    const stateEl = card.querySelector('.overview-card-state');
    const list = card.querySelector('.overview-jobs');
    if (!section) return;
    if (section.error) {
      errEl.textContent = section.error;
      errEl.hidden = false;
      stateEl.textContent = 'error';
      stateEl.className = 'overview-card-state error';
      list.innerHTML = '';
      return;
    }
    errEl.hidden = true;
    const items = section.items || [];
    stateEl.textContent = items.length + ' recent';
    stateEl.className = 'overview-card-state ok';
    list.innerHTML = '';
    if (items.length === 0) {
      const li = document.createElement('li');
      li.innerHTML = '<span class="ov-paths">(no jobs yet)</span>';
      list.appendChild(li);
      return;
    }
    for (const j of items) {
      const li = document.createElement('li');
      const state = document.createElement('span');
      state.className = 'ov-state ' + j.state;
      state.textContent = j.state;
      const paths = document.createElement('span');
      paths.className = 'ov-paths';
      paths.textContent = j.source + ' → ' + j.destination;
      const bytes = document.createElement('span');
      bytes.className = 'ov-bytes';
      bytes.textContent = formatBytes(j.bytes || 0);
      const dur = document.createElement('span');
      dur.className = 'ov-duration';
      dur.textContent = formatDuration(j.duration_ms || 0);
      li.appendChild(state);
      li.appendChild(paths);
      li.appendChild(bytes);
      li.appendChild(dur);
      list.appendChild(li);
    }
  }

  // formatDuration takes milliseconds and renders a compact label
  // ("3.2s", "1m12s", "2h05m"). Used by the recent-jobs row + the
  // Redis uptime field. Bounded growth — never returns days-level
  // labels because the Overview slice doesn't surface long-running
  // job history at that resolution.
  function formatDuration(ms) {
    if (!ms || ms < 0) return '—';
    const totalSec = Math.round(ms / 1000);
    if (totalSec < 60) return totalSec + 's';
    const m = Math.floor(totalSec / 60);
    const s = totalSec % 60;
    if (m < 60) return m + 'm' + String(s).padStart(2, '0') + 's';
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return h + 'h' + String(mm).padStart(2, '0') + 'm';
  }

  // -------- Trash (SLICE 3) --------
  // JuiceFS .trash/ subtree browser + restore/delete UI.
  // Retention knob calls `juicefs config --trash-days N` via the
  // server. Empty Trash is gated by a typed-confirmation modal AND
  // the server-side X-Confirm-Empty: yes header — belt + suspenders
  // so a hijacked client UI can't slip an empty-trash past the
  // server without the explicit operator gesture.
  //
  // Pagination: ?offset=&limit=, default limit 100, server caps at
  // 1000. The "Load more" button bumps offset by the page size.
  // We keep the in-memory list cumulative so multi-page restores
  // don't have to re-scan from page 1.
  const TRASH_PAGE_SIZE = 100;
  const trashState = {
    inited: false,
    entries: [],          // cumulative across "load more" pages
    total: 0,
    offset: 0,
    truncated: false,
    selected: new Set(),  // entry.path → selected
    lastSelectedIndex: -1, // for shift-click range select
  };

  function initTrashOnce() {
    if (trashState.inited) return;
    trashState.inited = true;
    $('#trash-refresh').addEventListener('click', () => {
      trashState.entries = [];
      trashState.offset = 0;
      trashState.selected.clear();
      refreshTrash();
    });
    $('#trash-load-more').addEventListener('click', () => loadMoreTrash());
    $('#trash-bulk-restore').addEventListener('click', () => bulkRestoreTrash());
    $('#trash-bulk-delete').addEventListener('click', () => bulkDeleteTrash());
    $('#trash-empty').addEventListener('click', () => showTrashEmptyModal());
    $('#trash-modal-cancel').addEventListener('click', () => hideTrashEmptyModal());
    $('#trash-modal-confirm').addEventListener('input', (e) => {
      $('#trash-modal-go').disabled = e.target.value !== 'DELETE';
    });
    $('#trash-modal-go').addEventListener('click', () => emptyTrashConfirmed());
    $('#trash-retention-select').addEventListener('change', (e) => {
      setTrashRetention(parseInt(e.target.value, 10));
    });
    // Load the current retention setting once on first activation.
    loadTrashConfig();
  }

  async function refreshTrash() {
    // Reset cumulative state on full refresh.
    trashState.entries = [];
    trashState.offset = 0;
    trashState.selected.clear();
    await loadTrashPage();
  }

  async function loadMoreTrash() {
    trashState.offset += TRASH_PAGE_SIZE;
    await loadTrashPage();
  }

  async function loadTrashPage() {
    const err = $('#trash-error');
    err.hidden = true;
    const list = $('#trash-list');
    if (trashState.entries.length === 0) {
      list.innerHTML = '<li class="trash-empty-hint">Loading…</li>';
    }
    try {
      const url = `/api/trash/list?offset=${trashState.offset}&limit=${TRASH_PAGE_SIZE}`;
      const data = await api('GET', url);
      const page = data.entries || [];
      trashState.entries = trashState.entries.concat(page);
      trashState.total = data.total || 0;
      trashState.truncated = !!data.truncated;
      renderTrashList();
    } catch (e) {
      // 501 — standalone mode (no FUSE mount). Show the user a
      // helpful message instead of a raw error pill.
      const msg = (e && e.message) || String(e);
      err.textContent = msg;
      err.hidden = false;
      list.innerHTML = '<li class="trash-empty-hint">Trash unavailable in this deployment mode.</li>';
    }
  }

  // renderTrashList groups entries by deleted-at date and renders
  // one <li class="trash-row"> per entry. Group headers (date) are
  // <li class="trash-group-head"> so the entire list stays a single
  // flat UL — keeps the shift-click range-select simple (each row
  // has a stable index in the displayed order).
  function renderTrashList() {
    const list = $('#trash-list');
    list.innerHTML = '';
    if (trashState.entries.length === 0) {
      list.innerHTML = '<li class="trash-empty-hint">Trash is empty.</li>';
      $('#trash-count').textContent = '0';
      $('#trash-bytes').textContent = '0 B';
      $('#trash-truncated').hidden = true;
      $('#trash-load-more').hidden = true;
      updateTrashBulkButtons();
      return;
    }
    // Group by yyyy-mm-dd of DeletedAt.
    const byDate = new Map();
    let totalBytes = 0;
    for (const e of trashState.entries) {
      totalBytes += (e.size || 0);
      const d = new Date(e.deleted_at || 0);
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, '0');
      const dd = String(d.getDate()).padStart(2, '0');
      const key = `${yyyy}-${mm}-${dd}`;
      if (!byDate.has(key)) byDate.set(key, []);
      byDate.get(key).push(e);
    }
    const sortedKeys = Array.from(byDate.keys()).sort().reverse();
    let rowIndex = 0;
    for (const key of sortedKeys) {
      const head = document.createElement('li');
      head.className = 'trash-group-head';
      head.textContent = `Deleted ${key}`;
      list.appendChild(head);
      for (const e of byDate.get(key)) {
        const li = document.createElement('li');
        li.className = 'trash-row';
        li.dataset.path = e.path;
        li.dataset.rowIndex = String(rowIndex++);
        if (trashState.selected.has(e.path)) li.classList.add('selected');

        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.checked = trashState.selected.has(e.path);
        cb.addEventListener('click', (evt) => onTrashRowSelect(e, li, evt));
        li.appendChild(cb);

        const path = document.createElement('span');
        path.className = 'trash-path';
        path.textContent = e.original_path || e.path;
        path.title = e.path;
        li.appendChild(path);

        const size = document.createElement('span');
        size.className = 'trash-size';
        size.textContent = formatBytes(e.size || 0);
        li.appendChild(size);

        const actions = document.createElement('span');
        actions.className = 'trash-row-actions';
        const restore = document.createElement('button');
        restore.type = 'button';
        restore.textContent = 'Restore';
        restore.addEventListener('click', () => restoreOne(e));
        const del = document.createElement('button');
        del.type = 'button';
        del.textContent = 'Delete';
        del.className = 'danger';
        del.addEventListener('click', () => deleteOne(e));
        actions.appendChild(restore);
        actions.appendChild(del);
        li.appendChild(actions);

        list.appendChild(li);
      }
    }
    const prefix = trashState.truncated ? '≥' : '';
    $('#trash-count').textContent = prefix + trashState.total.toLocaleString();
    $('#trash-bytes').textContent = prefix + formatBytes(totalBytes);
    $('#trash-truncated').hidden = !trashState.truncated;
    $('#trash-load-more').hidden = trashState.entries.length >= trashState.total;
    updateTrashBulkButtons();
  }

  function onTrashRowSelect(entry, li, evt) {
    const rows = $$('#trash-list .trash-row');
    const idx = rows.indexOf(li);
    // Shift-click range select. Stops at the prior anchor so a
    // user can extend a selection without losing the anchor.
    if (evt.shiftKey && trashState.lastSelectedIndex >= 0) {
      const start = Math.min(idx, trashState.lastSelectedIndex);
      const end = Math.max(idx, trashState.lastSelectedIndex);
      for (let i = start; i <= end; i++) {
        const r = rows[i];
        if (!r) continue;
        const p = r.dataset.path;
        trashState.selected.add(p);
        r.classList.add('selected');
        const c = r.querySelector('input[type=checkbox]');
        if (c) c.checked = true;
      }
    } else {
      const cb = li.querySelector('input[type=checkbox]');
      if (cb && cb.checked) {
        trashState.selected.add(entry.path);
        li.classList.add('selected');
      } else {
        trashState.selected.delete(entry.path);
        li.classList.remove('selected');
      }
      trashState.lastSelectedIndex = idx;
    }
    updateTrashBulkButtons();
  }

  function updateTrashBulkButtons() {
    const has = trashState.selected.size > 0;
    $('#trash-bulk-restore').disabled = !has;
    $('#trash-bulk-delete').disabled = !has;
  }

  async function restoreOne(entry) {
    try {
      const r = await api('POST', '/api/trash/restore', { path: entry.path });
      removeEntryLocally(entry.path);
      renderTrashList();
      // Surface the final restored-at path so the user knows where
      // it landed (especially if collision-rename triggered).
      const msg = r && r.restored_at ? `Restored → ${r.restored_at}` : 'Restored.';
      showTrashFlash(msg);
    } catch (e) {
      showTrashError('Restore failed: ' + (e.message || e));
    }
  }

  async function deleteOne(entry) {
    if (!confirm(`Permanently delete ${entry.original_path || entry.path}?\n\nThis cannot be undone.`)) {
      return;
    }
    try {
      await api('POST', '/api/trash/delete', { path: entry.path });
      removeEntryLocally(entry.path);
      renderTrashList();
    } catch (e) {
      showTrashError('Delete failed: ' + (e.message || e));
    }
  }

  async function bulkRestoreTrash() {
    const targets = Array.from(trashState.selected);
    if (targets.length === 0) return;
    let okCount = 0;
    let failCount = 0;
    for (const p of targets) {
      try {
        await api('POST', '/api/trash/restore', { path: p });
        removeEntryLocally(p);
        okCount++;
      } catch (e) {
        console.error('bulk restore failed for', p, e);
        failCount++;
      }
    }
    renderTrashList();
    showTrashFlash(`Restored ${okCount} item(s)${failCount ? `, ${failCount} failed` : ''}.`);
  }

  async function bulkDeleteTrash() {
    const targets = Array.from(trashState.selected);
    if (targets.length === 0) return;
    if (!confirm(`Permanently delete ${targets.length} selected item(s)?\n\nThis cannot be undone.`)) {
      return;
    }
    let okCount = 0;
    let failCount = 0;
    for (const p of targets) {
      try {
        await api('POST', '/api/trash/delete', { path: p });
        removeEntryLocally(p);
        okCount++;
      } catch (e) {
        console.error('bulk delete failed for', p, e);
        failCount++;
      }
    }
    renderTrashList();
    showTrashFlash(`Deleted ${okCount} item(s)${failCount ? `, ${failCount} failed` : ''}.`);
  }

  function removeEntryLocally(path) {
    trashState.entries = trashState.entries.filter((e) => e.path !== path);
    trashState.selected.delete(path);
    if (trashState.total > 0) trashState.total--;
  }

  function showTrashEmptyModal() {
    $('#trash-modal-count').textContent = trashState.total.toLocaleString();
    let totalBytes = 0;
    for (const e of trashState.entries) totalBytes += (e.size || 0);
    $('#trash-modal-bytes').textContent = formatBytes(totalBytes);
    $('#trash-modal-confirm').value = '';
    $('#trash-modal-go').disabled = true;
    $('#trash-modal-error').hidden = true;
    $('#trash-empty-modal').hidden = false;
    setTimeout(() => $('#trash-modal-confirm').focus(), 50);
  }

  function hideTrashEmptyModal() {
    $('#trash-empty-modal').hidden = true;
  }

  async function emptyTrashConfirmed() {
    const err = $('#trash-modal-error');
    err.hidden = true;
    try {
      // The X-Confirm-Empty: yes header is the server-side gate. We
      // attach it here ALONGSIDE the typed-confirm in the modal so
      // the operator can't accidentally fire this from a curl one-
      // liner without the explicit header.
      const headers = authHeaders();
      headers['X-Confirm-Empty'] = 'yes';
      const r = await fetch(BASE + '/api/trash/empty', { method: 'POST', headers });
      if (!r.ok) {
        const msg = await r.text();
        throw new Error(msg.trim() || `${r.status} ${r.statusText}`);
      }
      const data = await r.json();
      hideTrashEmptyModal();
      showTrashFlash(`Emptied trash: ${(data.count || 0).toLocaleString()} item(s), ${formatBytes(data.bytes || 0)} freed.`);
      await refreshTrash();
    } catch (e) {
      err.textContent = e.message || String(e);
      err.hidden = false;
    }
  }

  async function loadTrashConfig() {
    try {
      const cfg = await api('GET', '/api/trash/config');
      const cur = $('#trash-retention-current');
      if (cfg.error) {
        cur.textContent = `current: (${cfg.error})`;
      } else if (cfg.days < 0) {
        cur.textContent = 'current: unknown';
      } else {
        cur.textContent = `current: ${cfg.days} day(s)`;
        // Sync the drop-down. If the current value isn't in our
        // choices list, the select shows blank — fine, the user
        // can still pick a new one.
        const sel = $('#trash-retention-select');
        const match = Array.from(sel.options).find((o) => parseInt(o.value, 10) === cfg.days);
        if (match) sel.value = match.value;
      }
    } catch (e) {
      const err = $('#trash-retention-error');
      err.textContent = 'Failed to load retention: ' + (e.message || e);
      err.hidden = false;
    }
  }

  async function setTrashRetention(days) {
    const err = $('#trash-retention-error');
    err.hidden = true;
    try {
      await api('PUT', '/api/trash/config', { days: days });
      await loadTrashConfig();
      showTrashFlash(`Retention set to ${days} day(s).`);
    } catch (e) {
      err.textContent = 'Failed to set retention: ' + (e.message || e);
      err.hidden = false;
    }
  }

  function showTrashError(msg) {
    const el = $('#trash-error');
    el.textContent = msg;
    el.hidden = false;
  }

  // showTrashFlash uses the same #trash-error element for a brief
  // success pill — color set inline via a transient .ok class so
  // we don't have to introduce a second toast element.
  function showTrashFlash(msg) {
    const el = $('#trash-error');
    el.textContent = msg;
    el.hidden = false;
    el.classList.add('ok');
    setTimeout(() => {
      el.hidden = true;
      el.classList.remove('ok');
    }, 4000);
  }

  // -------- Maintenance (SLICE 6) --------
  // Five operational levers wrapping juicefs CLI subprocesses. Each
  // card has a Run button; clicking it POSTs to /api/maintenance/{kind}
  // (with kind-specific query params: ?dry_run=true for gc, ?path=…
  // for warmup), then opens an EventSource on /api/maintenance/{kind}/stream
  // for live output. The stream closes itself when the op finishes; we
  // fetch the final snapshot via GET /api/maintenance/{kind} so the
  // state pill reflects done/error.
  const MAINTENANCE_KINDS = ['gc', 'fsck', 'warmup', 'cache-flush', 'compact-meta'];
  const maintenanceState = {
    inited: false,
    streams: new Map(), // kind → EventSource
  };

  function initMaintenanceOnce() {
    if (maintenanceState.inited) return;
    maintenanceState.inited = true;
    MAINTENANCE_KINDS.forEach((kind) => {
      const card = document.querySelector(`.maintenance-card[data-kind="${kind}"]`);
      if (!card) return;
      const runBtn = card.querySelector('[data-action="run"]');
      runBtn.addEventListener('click', () => runMaintenance(kind));
      // Restore the last-known state on first paint so navigating away
      // and back doesn't lose context. 404 is the normal "never ran"
      // case; we swallow it and leave the idle state.
      refreshMaintenanceState(kind).catch(() => {});
    });
  }

  async function runMaintenance(kind) {
    const card = document.querySelector(`.maintenance-card[data-kind="${kind}"]`);
    if (!card) return;
    const errEl = card.querySelector('[data-field="error"]');
    errEl.hidden = true;
    errEl.textContent = '';
    const outEl = card.querySelector('[data-field="output"]');
    outEl.textContent = '';
    // Build the kind-specific query string. GC honors the dry-run
    // checkbox; warmup forwards the optional path field. Other kinds
    // take no params.
    const params = new URLSearchParams();
    if (kind === 'gc') {
      const dry = card.querySelector('[data-field="dry-run"]');
      if (dry && dry.checked) params.set('dry_run', 'true');
    } else if (kind === 'warmup') {
      const pathInput = card.querySelector('[data-field="path"]');
      const p = (pathInput && pathInput.value || '').trim();
      if (p) params.set('path', p);
    }
    const qs = params.toString();
    const url = `/api/maintenance/${kind}${qs ? '?' + qs : ''}`;
    try {
      const op = await api('POST', url);
      setMaintenanceState(card, op);
      openMaintenanceStream(kind);
    } catch (e) {
      // 409 = same-kind already running. Surface it inline so the
      // user knows to wait rather than mash the button.
      const msg = (e && e.message) ? e.message : String(e);
      errEl.textContent = msg;
      errEl.hidden = false;
    }
  }

  function openMaintenanceStream(kind) {
    // Close any pre-existing stream for this kind (e.g. the user
    // reran while a previous stream was still open).
    const prior = maintenanceState.streams.get(kind);
    if (prior) {
      try { prior.close(); } catch (_) { /* ignore */ }
    }
    // EventSource can't set custom HTTP headers from JS, so we pass
    // the admin key via ?key=... — same shim the /api/jobs/{id}/stream
    // endpoint uses.
    const streamPath = `/api/maintenance/${kind}/stream`;
    const url = state.adminKey
      ? `${BASE}${streamPath}?key=${encodeURIComponent(state.adminKey)}`
      : `${BASE}${streamPath}`;
    const es = new EventSource(url);
    maintenanceState.streams.set(kind, es);
    const card = document.querySelector(`.maintenance-card[data-kind="${kind}"]`);
    const outEl = card.querySelector('[data-field="output"]');
    es.onmessage = (ev) => {
      try {
        const line = JSON.parse(ev.data);
        appendMaintenanceLine(outEl, line);
      } catch (_) {
        // Tolerate non-JSON payloads (shouldn't happen — the server
        // always JSON-encodes — but never let a parse error crash
        // the listener).
      }
    };
    es.onerror = () => {
      // The server closes the stream when the op finishes; the
      // EventSource emits an error in that case. Pull the final
      // snapshot so the state pill flips to done/error.
      es.close();
      maintenanceState.streams.delete(kind);
      refreshMaintenanceState(kind).catch(() => {});
    };
  }

  function appendMaintenanceLine(outEl, line) {
    // Append a line + newline. Trim the head if the on-screen log
    // exceeds the server-side cap so the DOM doesn't grow without
    // bound for a noisy op.
    const cap = 1000;
    const cur = outEl.textContent.split('\n');
    cur.push(line);
    if (cur.length > cap) {
      cur.splice(0, cur.length - cap, '[truncated]');
    }
    outEl.textContent = cur.join('\n');
    // Auto-scroll to bottom so the latest output is visible.
    outEl.scrollTop = outEl.scrollHeight;
  }

  async function refreshMaintenanceState(kind) {
    const card = document.querySelector(`.maintenance-card[data-kind="${kind}"]`);
    if (!card) return;
    try {
      const op = await api('GET', `/api/maintenance/${kind}`);
      setMaintenanceState(card, op);
      // Repaint the output panel with the captured snapshot so the
      // user sees what happened on the previous run.
      const outEl = card.querySelector('[data-field="output"]');
      outEl.textContent = (op.output || []).join('\n');
      outEl.scrollTop = outEl.scrollHeight;
    } catch (_) {
      // 404 = never ran; leave the idle defaults.
    }
  }

  function setMaintenanceState(card, op) {
    const stateEl = card.querySelector('[data-field="state"]');
    stateEl.textContent = op.state || 'idle';
    stateEl.dataset.state = op.state || 'idle';
    const errEl = card.querySelector('[data-field="error"]');
    if (op.error) {
      errEl.textContent = op.error;
      errEl.hidden = false;
    }
  }

  // -------- Boot --------
  // route() reads location.hash, falls back to DEFAULT_TAB
  // (#/migrations), and shows the matching <section data-tab>.
  // showTab → initMigrationsOnce, so visiting the page with the
  // default route immediately fires the migrator boot path.
  route();
})();

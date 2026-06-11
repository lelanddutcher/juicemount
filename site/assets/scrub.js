/* ==========================================================================
   JuiceMount scrub-streaming explainer — BRAND_SPRINT item H, the secondary
   concept from docs/web/INTERACTIVE_TOOL.md. Plain script, no frameworks,
   no build step, no fetch — works double-clicked from a folder (file://).

   The model is ILLUSTRATIVE and labeled as such in the UI: one hypothetical
   100 GB clip, ~4 MB blocks (the JuiceFS chunk story), 80 visual cells
   standing in for the real ~25,000 blocks, and link math at line rate.
   The only real measurements quoted are the README's author-measured
   226–571 MB/s cached reads, attributed where they appear.

   Motion: the sync-mode fill is the one JS animation (rAF, linear, time
   compressed 100x and labeled). prefers-reduced-motion skips it entirely
   and jumps to the end state; cell-fill transitions are CSS and are zeroed
   by the global reduced-motion rule in site.css.
   ========================================================================== */
(function () {
  "use strict";

  /* ---- the illustrative model (constants quoted in the UI) -------------- */
  var FILE_GB = 100;
  var FILE_MB = FILE_GB * 1000;          /* decimal MB throughout */
  var BLOCK_MB = 4;                      /* one JuiceFS-style block */
  var CELLS = 80;                        /* visual cells — legibility, not the real count */
  var REAL_BLOCKS = FILE_MB / BLOCK_MB;  /* 25,000 — quoted in the truth line */
  var COMPRESS = 100;                    /* sync fill: 1 s on screen = 100 s on the wire */
  var CACHED_MBPS_LO = 226;              /* author-measured cached-read range (README) */
  var CACHED_MBPS_HI = 571;

  var LINKS = {
    cellular: { label: "100 Mbit cellular", mbps: 12.5 },
    gbe1: { label: "1 GbE", mbps: 125 },
    gbe10: { label: "10 GbE", mbps: 1250 }
  };

  /* ======================================================================
     Pure math + formatting — exported below for node verification.
     ====================================================================== */

  /* JuiceMount first read at a fresh spot: ~one block over the link. */
  function jmFirstWaitS(linkMBps) { return BLOCK_MB / linkMBps; }

  /* JuiceMount read of an already-streamed block: NVMe, not network. */
  function jmCachedWaitS() {
    return { lo: BLOCK_MB / CACHED_MBPS_HI, hi: BLOCK_MB / CACHED_MBPS_LO };
  }

  /* Sync model: the frame at fraction f waits for the file up to it. */
  function syncWaitS(frac, linkMBps) { return (frac * FILE_MB) / linkMBps; }
  function syncTotalS(linkMBps) { return FILE_MB / linkMBps; }

  function jmMovedMB(blocksTouched) { return blocksTouched * BLOCK_MB; }

  function fmtTime(s) {
    if (s <= 0) return "0 s";
    if (s < 1) {
      var ms = s * 1000;
      return (ms < 10 ? Math.round(ms * 10) / 10 : Math.round(ms)) + " ms";
    }
    if (s < 60) return (s < 10 ? Math.round(s * 10) / 10 : Math.round(s)) + " s";
    var m = Math.floor(s / 60);
    var sec = Math.round(s - m * 60);
    if (sec === 60) { m += 1; sec = 0; }
    if (m < 60) return sec ? m + " m " + sec + " s" : m + " m";
    var h = Math.floor(m / 60);
    m = m - h * 60;
    return m ? h + " h " + m + " m" : h + " h";
  }

  function fmtMsRange(loS, hiS) {
    return "~" + Math.round(loS * 1000) + "–" + Math.round(hiS * 1000) + " ms";
  }

  function fmtData(mb) {
    if (mb < 1000) return Math.round(mb) + " MB";
    var gb = mb / 1000;
    var v = gb >= 100 ? Math.round(gb) : Math.round(gb * 10) / 10;
    return v.toLocaleString("en-US") + " GB";
  }

  function esc(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function num(v) { return '<span class="num">' + esc(v) + "</span>"; }

  /* ======================================================================
     DOM — everything below runs only in the browser.
     ====================================================================== */

  function initDom() {
    var root = document.getElementById("scrub-widget");
    if (!root) return;
    var $ = function (id) { return document.getElementById(id); };

    var strip = $("scrub-strip");
    var cellsBox = $("scrub-cells");
    var playhead = $("scrub-playhead");
    var modeJmBtn = $("scrub-mode-jm");
    var modeSyncBtn = $("scrub-mode-sync");
    var movedEl = $("scrub-moved");
    var movedNote = $("scrub-moved-note");
    var waitEl = $("scrub-wait");
    var waitNote = $("scrub-wait-note");
    var statusEl = $("scrub-status");
    var resetBtn = $("scrub-reset");

    var reducedMq = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
    function reduced() { return !!(reducedMq && reducedMq.matches); }

    /* ---- state: one shared playhead, per-mode progress ------------------ */
    var state = {
      mode: "jm",            /* "jm" | "sync" */
      link: "gbe1",
      frac: 0,               /* playhead position 0..1 */
      jmTouched: {},         /* cell index -> true; persists across mode flips (cache does) */
      jmCount: 0,
      lastFresh: false,      /* did the latest touch stream a new block (vs land on cache)? */
      syncStarted: false,
      syncMB: 0
    };

    var cells = [];
    function buildCells() {
      var frag = document.createDocumentFragment();
      for (var i = 0; i < CELLS; i++) {
        var d = document.createElement("div");
        d.className = "scrub-cell";
        frag.appendChild(d);
        cells.push(d);
      }
      cellsBox.appendChild(frag);
    }

    function linkMBps() { return LINKS[state.link].mbps; }
    function linkLabel() { return LINKS[state.link].label; }
    function cellAt(frac) { return Math.max(0, Math.min(CELLS - 1, Math.floor(frac * CELLS))); }
    function syncCellsDone() { return Math.min(CELLS, Math.floor(state.syncMB / (FILE_MB / CELLS))); }
    function syncDone() { return state.syncMB >= FILE_MB; }

    /* ---- rendering ------------------------------------------------------- */
    var paintedSync = 0;

    function repaintAllCells() {
      var i;
      if (state.mode === "jm") {
        for (i = 0; i < CELLS; i++) cells[i].className = state.jmTouched[i] ? "scrub-cell is-on" : "scrub-cell";
      } else {
        var done = syncCellsDone();
        for (i = 0; i < CELLS; i++) cells[i].className = i < done ? "scrub-cell is-on" : "scrub-cell";
        paintedSync = done;
      }
    }

    /* incremental repaint for the sync fill loop — touch only changed cells */
    function renderSyncCells() {
      var done = syncCellsDone();
      var i;
      if (done === paintedSync) return;
      if (done > paintedSync) { for (i = paintedSync; i < done; i++) cells[i].className = "scrub-cell is-on"; }
      else { for (i = done; i < paintedSync; i++) cells[i].className = "scrub-cell"; }
      paintedSync = done;
    }

    function renderPlayhead() {
      playhead.style.left = (state.frac * 100).toFixed(3) + "%";
      var gb = Math.round(state.frac * FILE_GB * 10) / 10;
      playhead.setAttribute("aria-valuenow", String(gb));
      var vt;
      if (state.mode === "jm") {
        vt = gb + " GB into the clip — " + state.jmCount + " of " + CELLS + " blocks streamed, " +
          fmtData(jmMovedMB(state.jmCount)) + " moved";
      } else if (syncDone()) {
        vt = gb + " GB into the clip — the whole 100 GB has moved; every frame is local";
      } else {
        vt = gb + " GB into the clip — needs " + fmtTime(syncWaitS(state.frac, linkMBps())) +
          " at " + linkLabel() + " from a cold start";
      }
      playhead.setAttribute("aria-valuetext", vt);
    }

    function renderReadouts() {
      var label = linkLabel();
      if (state.mode === "jm") {
        movedEl.textContent = fmtData(jmMovedMB(state.jmCount));
        movedNote.textContent = "of " + FILE_GB + " GB — " + state.jmCount + " of " + CELLS + " cells touched";
        if (state.jmCount > 0 && !state.lastFresh && state.jmTouched[cellAt(state.frac)]) {
          /* the playhead sits on a block that was cached before this touch */
          var c = jmCachedWaitS();
          waitEl.textContent = fmtMsRange(c.lo, c.hi);
          waitNote.textContent = "this block was already cached — NVMe reads at 226–571 MB/s, author-measured";
        } else {
          waitEl.textContent = "~" + fmtTime(jmFirstWaitS(linkMBps()));
          waitNote.textContent = "≈ one " + BLOCK_MB + " MB block at " + label + " line rate" +
            (state.jmCount > 0 ? " — cached now for the next pass" : "");
        }
      } else {
        movedEl.textContent = fmtData(state.syncMB);
        movedNote.textContent = "of " + FILE_GB + " GB — whole file, front to back";
        if (syncDone()) {
          waitEl.textContent = "0 s";
          waitNote.textContent = "the whole file moved first — every frame is local now";
        } else {
          waitEl.textContent = fmtTime(syncWaitS(state.frac, linkMBps()));
          if (state.frac === 0) {
            waitNote.textContent = "nothing sits before frame 1 — every later frame waits on the file up to it";
          } else {
            var arrived = state.syncStarted && state.syncMB >= state.frac * FILE_MB;
            waitNote.textContent = "from a cold start: the first " + fmtData(state.frac * FILE_MB) +
              " of the file must arrive at " + label + " line rate" +
              (arrived ? " — arrived; the far end still waits" : "");
          }
        }
      }
    }

    var lastStatus = "";
    function renderStatus() {
      var html;
      if (state.mode === "jm") {
        if (state.jmCount === 0) {
          html = "Move the playhead — only the blocks under it stream. Each cell you touch counts " +
            num("~" + BLOCK_MB + " MB") + ": the block under that frame.";
        } else if (state.jmCount >= CELLS) {
          html = "Every cell touched, end to end: " + num(fmtData(jmMovedMB(CELLS))) + " moved of " +
            num(FILE_GB + " GB") + " — that is the whole point.";
        } else {
          var rest = Math.round((FILE_GB - jmMovedMB(CELLS) / 1000) * 10) / 10;
          html = num(String(state.jmCount)) + " block" + (state.jmCount === 1 ? "" : "s") +
            " streamed. Scrub the entire strip and the counter still tops out at " +
            num(fmtData(jmMovedMB(CELLS))) + " — the other " + num("~" + rest + " GB") + " never moves.";
        }
      } else {
        if (!state.syncStarted) {
          html = "Any touch starts the whole " + num(FILE_GB + " GB") +
            " moving, front to back — a frame can’t play until the file has arrived up to it.";
        } else if (syncDone()) {
          html = "All " + num(FILE_GB + " GB") + " moved — " + num(fmtTime(syncTotalS(linkMBps()))) +
            " at " + num(linkLabel()) + " line rate before the far end is seekable.";
        } else {
          html = "Moving the whole file at " + num(linkLabel()) + " line rate, time compressed " +
            num(COMPRESS + "×") + " — one second here is " + num(fmtTime(COMPRESS)) + " on the wire.";
        }
      }
      if (html !== lastStatus) {
        lastStatus = html;
        statusEl.innerHTML = html;
      }
    }

    function renderAll() {
      repaintAllCells();
      renderPlayhead();
      renderReadouts();
      renderStatus();
    }

    /* ---- the sync fill loop (the one JS animation) ----------------------- */
    var rafId = null;
    var lastTs = 0;

    function startLoop() {
      if (rafId !== null) return;
      lastTs = 0;
      rafId = window.requestAnimationFrame(tick);
    }

    function stopLoop() {
      if (rafId !== null) {
        window.cancelAnimationFrame(rafId);
        rafId = null;
      }
    }

    function tick(ts) {
      rafId = null;
      var dt = lastTs ? Math.min(0.1, (ts - lastTs) / 1000) : 0.016; /* clamp tab-restore jumps */
      lastTs = ts;
      state.syncMB = Math.min(FILE_MB, state.syncMB + dt * COMPRESS * linkMBps());
      renderSyncCells();
      renderReadouts();
      if (syncDone()) {
        renderStatus();
        renderPlayhead();
      } else {
        rafId = window.requestAnimationFrame(tick);
      }
    }

    function startSync() {
      if (state.syncStarted) return;
      state.syncStarted = true;
      if (reduced()) {
        /* reduced motion: no animated filling — instant end state */
        state.syncMB = FILE_MB;
        repaintAllCells();
      } else {
        startLoop();
      }
    }

    /* ---- scrubbing -------------------------------------------------------- */
    function touchJm(idx) {
      if (!state.jmTouched[idx]) {
        state.jmTouched[idx] = true;
        state.jmCount++;
        cells[idx].className = "scrub-cell is-on";
        return true;
      }
      return false;
    }

    function setFrac(f) {
      state.frac = Math.max(0, Math.min(1, f));
      if (state.mode === "jm") state.lastFresh = touchJm(cellAt(state.frac));
      else startSync();
      renderPlayhead();
      renderReadouts();
      renderStatus();
    }

    function fracFromX(clientX) {
      var r = cellsBox.getBoundingClientRect();
      if (r.width <= 0) return state.frac;
      return (clientX - r.left) / r.width;
    }

    var dragging = false;
    strip.addEventListener("pointerdown", function (e) {
      if (e.pointerType === "mouse" && e.button !== 0) return;
      dragging = true;
      if (strip.setPointerCapture) {
        try { strip.setPointerCapture(e.pointerId); } catch (err) { /* capture is best-effort */ }
      }
      setFrac(fracFromX(e.clientX));
      try { playhead.focus({ preventScroll: true }); } catch (err) { playhead.focus(); }
      e.preventDefault();
    });
    strip.addEventListener("pointermove", function (e) {
      if (dragging) setFrac(fracFromX(e.clientX));
    });
    strip.addEventListener("pointerup", function () { dragging = false; });
    strip.addEventListener("pointercancel", function () { dragging = false; });

    /* keyboard: ARIA slider — arrows step one cell, PageUp/Down ten, Home/End jump */
    playhead.addEventListener("keydown", function (e) {
      var cell = cellAt(state.frac);
      var next;
      switch (e.key) {
        case "ArrowRight": case "ArrowUp": next = Math.min(CELLS - 1, cell + 1); break;
        case "ArrowLeft": case "ArrowDown": next = Math.max(0, cell - 1); break;
        case "PageUp": next = Math.min(CELLS - 1, cell + 10); break;
        case "PageDown": next = Math.max(0, cell - 10); break;
        case "Home": e.preventDefault(); setFrac(0); return;  /* exact ends so */
        case "End": e.preventDefault(); setFrac(1); return;   /* valuenow hits 0/100 */
        default: return;
      }
      e.preventDefault();
      setFrac((next + 0.5) / CELLS); /* snap to cell centers so each step streams one block */
    });

    /* ---- controls ---------------------------------------------------------- */
    function setMode(m) {
      if (state.mode === m) return;
      state.mode = m;
      modeJmBtn.setAttribute("aria-pressed", m === "jm" ? "true" : "false");
      modeSyncBtn.setAttribute("aria-pressed", m === "sync" ? "true" : "false");
      if (m === "jm") {
        stopLoop(); /* the sync transfer pauses while you look at the other model */
      } else if (state.syncStarted && !syncDone() && !reduced()) {
        startLoop();
      }
      renderAll();
    }
    modeJmBtn.addEventListener("click", function () { setMode("jm"); });
    modeSyncBtn.addEventListener("click", function () { setMode("sync"); });

    var radios = root.querySelectorAll('input[name="scrub-link"]');
    for (var ri = 0; ri < radios.length; ri++) {
      radios[ri].addEventListener("change", function (e) {
        if (e.target.checked && LINKS[e.target.value]) {
          state.link = e.target.value;
          renderPlayhead();
          renderReadouts();
          renderStatus();
        }
      });
    }

    resetBtn.addEventListener("click", function () {
      stopLoop();
      state.jmTouched = {};
      state.jmCount = 0;
      state.lastFresh = false;
      state.syncStarted = false;
      state.syncMB = 0;
      state.frac = 0;
      renderAll();
    });

    /* if the user turns reduced motion on mid-fill, finish instantly */
    if (reducedMq && reducedMq.addEventListener) {
      reducedMq.addEventListener("change", function () {
        if (reduced() && state.syncStarted && !syncDone()) {
          stopLoop();
          state.syncMB = FILE_MB;
          if (state.mode === "sync") renderAll();
        }
      });
    }

    /* ---- boot ---------------------------------------------------------------- */
    buildCells();
    renderAll();
  }

  /* ---- entry points ---------------------------------------------------------- */
  if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", initDom);
    } else {
      initDom();
    }
  }

  /* Pure functions exported for verification (node require, no DOM). */
  if (typeof module !== "undefined" && module.exports) {
    module.exports = {
      FILE_GB: FILE_GB,
      FILE_MB: FILE_MB,
      BLOCK_MB: BLOCK_MB,
      CELLS: CELLS,
      REAL_BLOCKS: REAL_BLOCKS,
      COMPRESS: COMPRESS,
      LINKS: LINKS,
      jmFirstWaitS: jmFirstWaitS,
      jmCachedWaitS: jmCachedWaitS,
      syncWaitS: syncWaitS,
      syncTotalS: syncTotalS,
      jmMovedMB: jmMovedMB,
      fmtTime: fmtTime,
      fmtData: fmtData,
      fmtMsRange: fmtMsRange
    };
  }
})();

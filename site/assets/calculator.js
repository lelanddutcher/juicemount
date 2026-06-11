/* ==========================================================================
   JuiceMount rent-vs-own calculator — docs/web/INTERACTIVE_TOOL.md Concept 1.
   Plain script, no frameworks, no build step. Not an ES module on purpose:
   Chromium refuses module scripts and fetch() over file://, and this page
   must work double-clicked from a folder.
   Pricing comes from assets/pricing.json when the page is served over http;
   on file:// the fetch fails and FALLBACK_PRICING below is used instead.
   KEEP FALLBACK_PRICING IN SYNC WITH assets/pricing.json.
   ========================================================================== */
(function () {
  "use strict";

  /* ---- pricing (mirror of assets/pricing.json — keep in sync) ----------- */
  var FALLBACK_PRICING = {
    fetched: "2026-06-11",
    checked: "2026-06",
    sources: {
      lucidlink: "https://www.lucidlink.com/pricing",
      suite: "https://www.suitestudios.io/pricing",
      suite_byo_terms: "https://support.suitestudios.io/en/articles/8302694-what-is-bring-your-own-storage-byo",
      shade: "https://shade.inc/pricing",
      iconik: "https://www.iconik.io/pricing",
      strada: "https://strada.tech/pricing",
      dropbox: "https://www.dropbox.com/business/plans-comparison",
      gworkspace: "https://workspace.google.com/pricing",
      nextcloud: "https://nextcloud.com/pricing/",
      b2: "https://www.backblaze.com/cloud-storage/pricing",
      aws_egress: "https://aws.amazon.com/s3/pricing/"
    },
    lucidlink_business: { seat: 27, seat_regular: 32, included_gb_per_seat: 400, extra_per_100gb: 8, soft_cap_tb: 10, max_seats: 25 },
    suite_managed: { per_tb: 75, included_seats: 5, extra_seat: 10 },
    suite_byo: { per_tb: 40, included_seats: 5, extra_seat: 10, min_tb: 20, csp_fees_extra: true },
    shade_growth: { seat: 29.75, seat_monthly: 35, active_gb_per_seat: 500, max_seats: 15, extra_storage: "unpublished" },
    iconik: { browse_seat: 9, standard_seat: 65, power_seat: 120, collaborator_seat: 0, storage: "credits-quote-only", egress_billed: true },
    strada: { category: "p2p-transfer-not-storage", basic_seat: 8, unlimited_seat: 24, basic_transfer_gb_mo: 250 },
    dropbox_advanced: { seat: 24, min_seats: 3, pooled_start_tb: 15, streaming: "whole-file" },
    gworkspace_business_plus: { seat: 22, pooled_tb_per_user: 5, streaming: "whole-file" },
    nextcloud: { license: 0, enterprise_per_user_year_eur: 68, enterprise_min_users: 100, streaming: "sync-and-share" },
    object_storage: { b2_per_tb: 6, wasabi_per_tb: 6.99, r2_per_tb: 15 },
    egress_per_gb: { aws_s3_first_10tb: 0.09, aws_s3_next_40tb: 0.085, b2: 0, b2_beyond_3x_storage: 0.01, r2: 0, wasabi: 0 },
    exit_20tb_usd: { aws_s3: 1741, b2: 0, r2: 0, wasabi: 0 },
    defaults: {
      drive_per_usable_tb: 25, diy_server: 1200, new_nas: 2500,
      tengig_addon: 300, watts: 120, kwh: 0.15, backup_per_tb: 6
    }
  };

  var DEFAULTS = {
    vendor: "suite_managed",
    seats: 5, tb: 10, growth: 1,
    hardware: 1200, drivePerTb: 25,
    gbe: true, gbeCost: 300,
    backup: true, backupPerTb: 6,
    kwh: 0.15, watts: 120, otherOpex: 0,
    provOverride: 0,
    bill: 750, billPerTb: 0, s3PerTb: 6
  };

  var VENDOR_LABELS = {
    suite_managed: "Suite managed",
    suite_byo: "Suite BYO",
    lucidlink: "LucidLink Business",
    shade: "Shade Growth",
    s3: "a generic S3 bucket",
    mybill: "your current bill"
  };

  var HOURS_PER_MONTH = 730;
  var CHART_MONTHS = 36;
  var SEARCH_MONTHS = 120;

  /* ======================================================================
     Pure math — formulas verbatim from INTERACTIVE_TOOL.md.
     ====================================================================== */

  function libraryAt(inp, t) { return inp.tb + inp.growth * t; }

  /* SaaS $/month at month t (library priced at its size entering month t).
     Returns null when the vendor's published tier can't honestly model the
     size (Shade Growth beyond its active-storage cap). */
  function saasRate(inp, P, t) {
    var T = libraryAt(inp, t);
    var S = inp.seats;
    switch (inp.vendor) {
      case "lucidlink": {
        var L = P.lucidlink_business;
        var extraGb = Math.max(0, T * 1000 - L.included_gb_per_seat * S);
        return L.seat * S + L.extra_per_100gb * (extraGb / 100);
      }
      case "suite_managed": {
        var M = P.suite_managed;
        return M.per_tb * T + M.extra_seat * Math.max(0, S - M.included_seats);
      }
      case "suite_byo": {
        var B = P.suite_byo;
        return B.per_tb * T + B.extra_seat * Math.max(0, S - B.included_seats) +
          P.object_storage.b2_per_tb * T;
      }
      case "shade": {
        var H = P.shade_growth;
        if (T > (H.active_gb_per_seat * S) / 1000) return null;
        return H.seat * S;
      }
      case "s3":
        return inp.s3PerTb * T;
      case "mybill":
        return inp.bill + inp.billPerTb * (inp.growth * t);
      default:
        return 0;
    }
  }

  /* Self-host $/month at month t: power + optional offsite mirror + other. */
  function selfRate(inp, P, t) {
    var power = (inp.watts / 1000) * HOURS_PER_MONTH * inp.kwh;
    var backup = inp.backup ? inp.backupPerTb * libraryAt(inp, t) : 0;
    return power + backup + inp.otherOpex;
  }

  /* One-time spend: hardware + ceil(provisioned TB) × drive cost + 10 GbE.
     provisioned = (today + a year of growth) × 1.25 headroom, overridable. */
  function capexOf(inp) {
    var provRaw = inp.provOverride > 0 ? inp.provOverride : (inp.tb + 12 * inp.growth) * 1.25;
    var provTb = Math.ceil(provRaw);
    var drives = provTb * inp.drivePerTb;
    var gbe = inp.gbe ? inp.gbeCost : 0;
    return {
      hardware: inp.hardware,
      provTb: provTb,
      provAuto: !(inp.provOverride > 0),
      drives: drives,
      gbe: gbe,
      total: inp.hardware + drives + gbe
    };
  }

  /* 36-month simulation (searching to 120 for late paybacks). Cumulative
     convention: cumX[m] = cost of months 0..m-1, so cum arrays have one more
     entry than rate arrays and cum[0] = 0. */
  function simulate(inp, P) {
    var capex = capexOf(inp);
    var saas = [], self_ = [], cumSaas = [0], cumSelf = [0];
    var valid = SEARCH_MONTHS;
    for (var t = 0; t < SEARCH_MONTHS; t++) {
      var r = saasRate(inp, P, t);
      if (r === null) { valid = t; break; }
      saas.push(r);
      self_.push(selfRate(inp, P, t));
      cumSaas.push(cumSaas[t] + r);
      cumSelf.push(cumSelf[t] + self_[t]);
    }

    /* payback: first month m where capex + cumSelf[m] <= cumSaas[m],
       interpolated within the month for the fractional value. */
    var payback = null;
    for (var m = 1; m <= valid; m++) {
      if (capex.total + cumSelf[m] <= cumSaas[m]) {
        var remaining = (capex.total + cumSelf[m - 1]) - cumSaas[m - 1];
        var netGain = saas[m - 1] - self_[m - 1];
        var frac = netGain > 0 ? (m - 1) + remaining / netGain : m;
        payback = { frac: frac, month: Math.max(1, Math.ceil(frac)) };
        break;
      }
    }

    var status;
    if (valid === 0) status = "not_comparable";
    else if (payback && payback.month <= Math.min(CHART_MONTHS, valid)) status = "payback";
    else if (payback) status = "payback_beyond";
    else if (valid < CHART_MONTHS) status = "capped_no_payback";
    else status = "saas_cheaper";

    var flags = {};
    if (inp.vendor === "lucidlink") {
      var cap = P.lucidlink_business.soft_cap_tb;
      flags.lucidCapMonth = -1;
      for (var k = 0; k <= CHART_MONTHS; k++) {
        if (libraryAt(inp, k) > cap) { flags.lucidCapMonth = k; break; }
      }
    }
    if (inp.vendor === "shade") {
      flags.shadeCapTb = (P.shade_growth.active_gb_per_seat * inp.seats) / 1000;
      flags.shadeCapMonth = valid < SEARCH_MONTHS ? valid : -1;
      flags.shadeSeatCap = inp.seats > P.shade_growth.max_seats;
    }

    return {
      capex: capex,
      saas: saas, self: self_,
      cumSaas: cumSaas, cumSelf: cumSelf,
      validMonths: valid,
      chartMonths: Math.min(CHART_MONTHS, valid),
      payback: payback,
      status: status,
      flags: flags
    };
  }

  /* ======================================================================
     Formatting — everything user-visible goes through these.
     ====================================================================== */

  function fmtUSD(n) { return "$" + Math.round(n).toLocaleString("en-US"); }

  function fmtUSDmo(n) {
    var rounded = Math.round(n * 100) / 100;
    if (Math.abs(rounded - Math.round(rounded)) < 0.005) return fmtUSD(rounded) + "/mo";
    return "$" + rounded.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + "/mo";
  }

  function fmtTB(n) {
    return n.toLocaleString("en-US", { maximumFractionDigits: 1 }) + " TB";
  }

  function fmtAxis(v) {
    if (v >= 1000) {
      var k = v / 1000;
      return "$" + (k >= 100 ? Math.round(k) : Math.round(k * 10) / 10).toLocaleString("en-US") + "k";
    }
    return "$" + Math.round(v).toLocaleString("en-US");
  }

  function esc(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  /* ======================================================================
     Chart — hand-rolled inline SVG, no libraries.
     ====================================================================== */

  function niceCeil(v) {
    if (v <= 0) return 1;
    var e = Math.pow(10, Math.floor(Math.log10(v)));
    var mults = [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10];
    for (var i = 0; i < mults.length; i++) {
      if (mults[i] * e >= v) return mults[i] * e;
    }
    return 10 * e;
  }

  function chartSVG(sim) {
    var W = 640, H = 360, padL = 64, padR = 14, padT = 18, padB = 44;
    var n = sim.chartMonths;
    if (n < 1) return "";
    var maxVal = Math.max(sim.cumSaas[n], sim.capex.total + sim.cumSelf[n]);
    var yMax = niceCeil(maxVal);
    var x = function (m) { return padL + (W - padL - padR) * (m / CHART_MONTHS); };
    var y = function (v) { return padT + (H - padT - padB) * (1 - v / yMax); };

    var s = [];
    s.push('<svg viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="' +
      esc(chartSummary(sim)) + '" xmlns="http://www.w3.org/2000/svg">');

    /* grid + y labels */
    var ticks = 4;
    for (var i = 0; i <= ticks; i++) {
      var v = (yMax / ticks) * i;
      var yy = y(v);
      s.push('<line class="grid" x1="' + padL + '" y1="' + yy + '" x2="' + (W - padR) + '" y2="' + yy + '"></line>');
      s.push('<text x="' + (padL - 8) + '" y="' + (yy + 3.5) + '" text-anchor="end">' + fmtAxis(v) + "</text>");
    }
    /* x labels */
    for (var mlab = 0; mlab <= CHART_MONTHS; mlab += 6) {
      s.push('<text x="' + x(mlab) + '" y="' + (H - padB + 18) + '" text-anchor="middle">' + mlab + "</text>");
    }
    s.push('<text x="' + (padL + (W - padL - padR) / 2) + '" y="' + (H - 8) + '" text-anchor="middle">months</text>');

    /* SaaS cumulative: area + line */
    var saasPts = [], m;
    for (m = 0; m <= n; m++) saasPts.push(x(m).toFixed(1) + "," + y(sim.cumSaas[m]).toFixed(1));
    s.push('<polygon class="saas-area" points="' + saasPts.join(" ") + " " +
      x(n).toFixed(1) + "," + y(0).toFixed(1) + " " + x(0).toFixed(1) + "," + y(0).toFixed(1) + '"></polygon>');
    s.push('<polyline class="saas-line" points="' + saasPts.join(" ") + '"></polyline>');

    /* Self-host: capex step at month 0, then opex slope */
    var selfPts = [x(0).toFixed(1) + "," + y(0).toFixed(1), x(0).toFixed(1) + "," + y(sim.capex.total).toFixed(1)];
    for (m = 0; m <= n; m++) selfPts.push(x(m).toFixed(1) + "," + y(sim.capex.total + sim.cumSelf[m]).toFixed(1));
    s.push('<polyline class="self-line" points="' + selfPts.join(" ") + '"></polyline>');

    /* Shade cap cutoff */
    if (n < CHART_MONTHS) {
      s.push('<line class="cap-line" x1="' + x(n) + '" y1="' + padT + '" x2="' + x(n) + '" y2="' + y(0) + '"></line>');
      s.push('<text x="' + (x(n) + 6) + '" y="' + (padT + 12) + '">tier cap</text>');
    }

    /* payback marker */
    if (sim.payback && sim.payback.frac <= n) {
      var f = sim.payback.frac, lo = Math.floor(f), hi = Math.min(lo + 1, n), t01 = f - lo;
      var vCross = sim.cumSaas[lo] + (sim.cumSaas[hi] - sim.cumSaas[lo]) * t01;
      var cx = x(f), cy = y(vCross);
      s.push('<circle class="cross-dot" cx="' + cx.toFixed(1) + '" cy="' + cy.toFixed(1) + '" r="5"></circle>');
      var anchor = f < CHART_MONTHS / 2 ? "start" : "end";
      var dx = f < CHART_MONTHS / 2 ? 10 : -10;
      s.push('<text class="cross-label" x="' + (cx + dx).toFixed(1) + '" y="' + (cy - 10).toFixed(1) +
        '" text-anchor="' + anchor + '">payback · month ' + sim.payback.month + "</text>");
    }

    s.push("</svg>");
    return s.join("");
  }

  function chartSummary(sim) {
    var n = sim.chartMonths;
    var out = "Cumulative cost over " + n + " months: SaaS reaches " + fmtUSD(sim.cumSaas[n]) +
      "; self-host including hardware reaches " + fmtUSD(sim.capex.total + sim.cumSelf[n]) + ".";
    if (sim.payback && sim.payback.frac <= n) out += " Lines cross at month " + sim.payback.month + ".";
    return out;
  }

  /* ======================================================================
     DOM — everything below runs only in the browser.
     ====================================================================== */

  function initDom() {
    var PRICING = FALLBACK_PRICING;
    var $ = function (id) { return document.getElementById(id); };
    var form = $("calc-form");
    /* Pages without the calculator form (compare's cost meter loads this
       file for the math exports below) skip all DOM wiring. */
    if (!form) return;

    /* ---- form <-> inputs ------------------------------------------------ */
    function numVal(id, min, max, fallback) {
      var v = parseFloat($(id).value);
      if (!isFinite(v)) return fallback;
      return Math.min(max, Math.max(min, v));
    }

    function readForm() {
      return {
        vendor: $("vendor").value,
        seats: Math.round(numVal("seats", 1, 100, DEFAULTS.seats)),
        tb: numVal("tb", 0.5, 1000, DEFAULTS.tb),
        growth: numVal("g", 0, 50, DEFAULTS.growth),
        hardware: numVal("hw", 0, 100000, DEFAULTS.hardware),
        drivePerTb: numVal("drv", 0, 1000, DEFAULTS.drivePerTb),
        gbe: $("gbe").checked,
        gbeCost: numVal("gbep", 0, 10000, DEFAULTS.gbeCost),
        backup: $("bk").checked,
        backupPerTb: numVal("bkr", 0, 100, DEFAULTS.backupPerTb),
        kwh: numVal("kwh", 0, 2, DEFAULTS.kwh),
        watts: numVal("w", 0, 2000, DEFAULTS.watts),
        otherOpex: numVal("op", 0, 100000, DEFAULTS.otherOpex),
        provOverride: numVal("prov", 0, 5000, 0),
        bill: numVal("bill", 0, 1000000, DEFAULTS.bill),
        billPerTb: numVal("billtb", 0, 1000, DEFAULTS.billPerTb),
        s3PerTb: numVal("s3", 0, 1000, DEFAULTS.s3PerTb)
      };
    }

    function setVal(id, v) { $(id).value = String(v); }

    /* aria-valuetext keeps screen-reader slider announcements in units
       (display attribute only; values come straight from the inputs). */
    function syncSliderText() {
      var tbRange = $("tb-range"), gRange = $("g-range");
      tbRange.setAttribute("aria-valuetext", tbRange.value + " TB");
      gRange.setAttribute("aria-valuetext", gRange.value + " TB per month");
    }

    function writeForm(inp) {
      setVal("vendor", inp.vendor);
      setVal("seats", inp.seats);
      setVal("tb", inp.tb); setVal("tb-range", Math.min(100, inp.tb));
      setVal("g", inp.growth); setVal("g-range", Math.min(10, inp.growth));
      setVal("hw", inp.hardware);
      setVal("drv", inp.drivePerTb);
      $("gbe").checked = inp.gbe; setVal("gbep", inp.gbeCost);
      $("bk").checked = inp.backup; setVal("bkr", inp.backupPerTb);
      setVal("kwh", inp.kwh); setVal("w", inp.watts); setVal("op", inp.otherOpex);
      setVal("prov", inp.provOverride || "");
      setVal("bill", inp.bill); setVal("billtb", inp.billPerTb); setVal("s3", inp.s3PerTb);
      syncHwPreset(inp.hardware);
      syncVendorFields(inp.vendor);
      syncSliderText();
    }

    function syncHwPreset(hwValue) {
      var preset = $("hw-preset");
      var match = { "0": "0", "1200": "1200", "2500": "2500" }[String(hwValue)];
      preset.value = match || "custom";
    }

    function syncVendorFields(vendor) {
      var rows = document.querySelectorAll("[data-vendor-only]");
      for (var i = 0; i < rows.length; i++) {
        var show = rows[i].getAttribute("data-vendor-only").split(" ").indexOf(vendor) !== -1;
        rows[i].hidden = !show;
      }
    }

    /* ---- URL state (the Reddit-thread mechanic) -------------------------- */
    var URL_KEYS = [
      ["vs", "vendor"], ["s", "seats"], ["tb", "tb"], ["g", "growth"],
      ["hw", "hardware"], ["drv", "drivePerTb"], ["gbe", "gbe"], ["gbep", "gbeCost"],
      ["bk", "backup"], ["bkr", "backupPerTb"], ["kwh", "kwh"], ["w", "watts"],
      ["op", "otherOpex"], ["prov", "provOverride"], ["bill", "bill"],
      ["billtb", "billPerTb"], ["s3", "s3PerTb"]
    ];

    function applyUrl() {
      var qs = new URLSearchParams(window.location.search);
      var inp = {};
      for (var k in DEFAULTS) inp[k] = DEFAULTS[k];
      URL_KEYS.forEach(function (pair) {
        var raw = qs.get(pair[0]);
        if (raw === null) return;
        var field = pair[1];
        if (field === "vendor") {
          if (VENDOR_LABELS[raw]) inp.vendor = raw;
        } else if (field === "gbe" || field === "backup") {
          inp[field] = raw !== "0" && raw !== "false";
        } else {
          var v = parseFloat(raw);
          if (isFinite(v)) inp[field] = v;
        }
      });
      writeForm(inp);
    }

    function shareUrl(inp) {
      var qs = new URLSearchParams();
      URL_KEYS.forEach(function (pair) {
        var field = pair[1], v = inp[field], d = DEFAULTS[field];
        if (v === d) return;
        if (field === "gbe" || field === "backup") qs.set(pair[0], v ? "1" : "0");
        else qs.set(pair[0], String(v));
      });
      var base = window.location.href.split("?")[0].split("#")[0];
      var q = qs.toString();
      return q ? base + "?" + q : base;
    }

    function writeUrl(inp) {
      var url = shareUrl(inp);
      $("share-url").value = url;
      try {
        var q = url.indexOf("?") === -1 ? "" : url.slice(url.indexOf("?"));
        window.history.replaceState(null, "", window.location.pathname + q);
      } catch (e) { /* file:// in some browsers refuses; the share field still works */ }
    }

    /* ---- rendering -------------------------------------------------------- */
    function calloutHtml(kind, html) {
      return '<div class="callout callout-' + kind + '"><p>' + html + "</p></div>";
    }

    function renderHeadline(sim, inp) {
      var el = $("headline");
      var label = VENDOR_LABELS[inp.vendor];
      var month0 = sim.saas.length ? sim.saas[0] : 0;
      var html = "";

      if (sim.status === "not_comparable") {
        html = '<p class="payback-headline">No honest Growth-tier math to show.</p>' +
          '<p class="small muted">Shade Growth includes ' +
          '<span class="num">500 GB</span> active storage per seat — <span class="num">' + esc(fmtTB(sim.flags.shadeCapTb)) +
          "</span> for your team, and your library is <span class=\"num\">" + esc(fmtTB(inp.tb)) +
          "</span> today. Shade quotes Enterprise (custom pricing) here; if you have that quote, compare it under “my bill / my quote”.</p>";
      } else if (sim.status === "payback") {
        html = '<p class="payback-headline">Hardware pays for itself in <span class="num">month ' +
          sim.payback.month + "</span>.</p>" +
          '<p class="small muted">Month 0: <span class="num">' + esc(fmtUSDmo(month0)) + "</span> at " + esc(label) +
          ' vs <span class="num">' + esc(fmtUSDmo(sim.self[0])) + '</span> self-host opex, after <span class="num">' +
          esc(fmtUSD(sim.capex.total)) + "</span> one-time hardware.</p>";
      } else if (sim.status === "payback_beyond") {
        html = '<p class="payback-headline">Payback lands in <span class="num">month ' + sim.payback.month +
          "</span> — outside the 36-month window charted below.</p>" +
          '<p class="small muted">The gap between ' + esc(label) + " and self-host opex is real but small at this size; the box still wins eventually.</p>";
      } else if (sim.status === "capped_no_payback") {
        html = '<p class="payback-headline">No payback before the comparison stops.</p>' +
          '<p class="small muted">Your library passes Shade Growth’s active-storage cap at month <span class="num">' +
          sim.validMonths + "</span>, and the hardware hasn’t paid back by then. Beyond the cap Shade quotes Enterprise (custom pricing).</p>";
      } else {
        var delta = (sim.capex.total + sim.cumSelf[CHART_MONTHS]) - sim.cumSaas[CHART_MONTHS];
        html = calloutHtml("info",
          "<strong>At this size, SaaS is cheaper for you.</strong> Self-hosting wins on ownership here, not dollars. " +
          "Over 36 months: " + esc(label) + " costs <span class=\"num\">" + esc(fmtUSD(sim.cumSaas[CHART_MONTHS])) +
          "</span> vs <span class=\"num\">" + esc(fmtUSD(sim.capex.total + sim.cumSelf[CHART_MONTHS])) +
          "</span> self-hosted (hardware included) — staying put keeps <span class=\"num\">" + esc(fmtUSD(delta)) + "</span>.");
      }

      /* 36-month totals line for the payback cases */
      if ((sim.status === "payback" || sim.status === "payback_beyond") && sim.validMonths >= CHART_MONTHS) {
        var save = sim.cumSaas[CHART_MONTHS] - (sim.capex.total + sim.cumSelf[CHART_MONTHS]);
        if (save > 0) {
          html += '<p class="small">Over 36 months: <span class="num">' + esc(fmtUSD(sim.cumSaas[CHART_MONTHS])) +
            "</span> at " + esc(label) + ' vs <span class="num">' + esc(fmtUSD(sim.capex.total + sim.cumSelf[CHART_MONTHS])) +
            '</span> self-hosted, hardware included — owning keeps <span class="num">' + esc(fmtUSD(save)) + "</span>.</p>";
        }
      }

      /* the drives one-liner */
      if (month0 > 0 && inp.drivePerTb > 0 && sim.status !== "not_comparable") {
        var shelfTb = Math.round(month0 / inp.drivePerTb);
        if (shelfTb >= 1) {
          html += '<p class="small muted"><span class="num">' + esc(fmtUSDmo(month0)) + "</span> at " + esc(label) +
            ' buys a <span class="num">' + shelfTb.toLocaleString("en-US") + " TB</span> drive shelf — every month.</p>";
        }
      }

      el.innerHTML = html;
    }

    function renderWarnings(sim, inp) {
      var parts = [];

      if (inp.vendor === "lucidlink" && sim.flags.lucidCapMonth >= 0) {
        var when = sim.flags.lucidCapMonth === 0 ? "from day one" : "at month " + sim.flags.lucidCapMonth;
        parts.push(calloutHtml("warn",
          "LucidLink labels Business “best for up to 10 TB” — your library passes that " + when +
          ". Beyond it they push Enterprise (custom pricing); the math here keeps using published Business rates."));
      }
      if (inp.vendor === "suite_byo") {
        parts.push(calloutHtml("info",
          "<span class=\"num\">$40/TB/mo</span> is Suite’s mount software alone, on storage you already bought — " +
          "the bucket itself is modeled separately at B2’s <span class=\"num\">$6/TB/mo</span>."));
      }
      if (inp.vendor === "s3") {
        parts.push(calloutHtml("info",
          "Raw object storage is a bucket, not a mounted editing workflow — it’s here as a storage-cost reference. " +
          "Per TB/mo, June 2026: B2 <span class=\"num\">$6</span> · Wasabi <span class=\"num\">$6.99</span> · R2 <span class=\"num\">$15</span>."));
      }
      if (inp.vendor === "shade" && sim.flags.shadeSeatCap) {
        parts.push(calloutHtml("warn", "Shade Growth caps at 15 seats — above that it’s Enterprise (custom pricing)."));
      }
      if (inp.vendor === "shade" && sim.status !== "not_comparable" && sim.flags.shadeCapMonth >= 0) {
        parts.push(calloutHtml("warn",
          "Your library passes Shade Growth’s active-storage cap (<span class=\"num\">" + esc(fmtTB(sim.flags.shadeCapTb)) +
          "</span>) at month <span class=\"num\">" + sim.flags.shadeCapMonth +
          "</span> — the chart and totals stop there instead of inventing Enterprise prices."));
      }
      if (inp.seats > 15) {
        parts.push(calloutHtml("info",
          "Past 15 seats you’re in enterprise territory — the math only gets better with scale, but compare against a real quote from the vendor’s sales team."));
      }

      $("warnings").innerHTML = parts.join("");
    }

    function describeSaasRows(sim, inp, P) {
      var rows = [];
      var T0 = inp.tb, S = inp.seats;
      var t36 = libraryAt(inp, CHART_MONTHS);
      function row(label, value, note) {
        return '<tr role="row"><td role="cell">' + label + (note ? '<span class="note">' + note + "</span>" : "") +
          '</td><td role="cell" class="num">' + value + "</td></tr>";
      }
      switch (inp.vendor) {
        case "suite_managed": {
          var M = P.suite_managed;
          rows.push(row("Storage — <span class=\"num\">$" + M.per_tb + "/TB/mo</span> × " + esc(fmtTB(T0)),
            esc(fmtUSDmo(M.per_tb * T0)),
            "at month 36: " + esc(fmtTB(t36)) + " → " + esc(fmtUSDmo(M.per_tb * t36))));
          rows.push(row("Seats — " + M.included_seats + " included, then $" + M.extra_seat + "/seat",
            esc(fmtUSDmo(M.extra_seat * Math.max(0, S - M.included_seats)))));
          break;
        }
        case "suite_byo": {
          var B = P.suite_byo;
          rows.push(row("Mount software — <span class=\"num\">$" + B.per_tb + "/TB/mo</span> × " + esc(fmtTB(T0)),
            esc(fmtUSDmo(B.per_tb * T0)),
            "at month 36: " + esc(fmtTB(t36)) + " → " + esc(fmtUSDmo(B.per_tb * t36))));
          rows.push(row("Seats — " + B.included_seats + " included, then $" + B.extra_seat + "/seat",
            esc(fmtUSDmo(B.extra_seat * Math.max(0, S - B.included_seats)))));
          rows.push(row("Your own bucket — B2-class <span class=\"num\">$" + P.object_storage.b2_per_tb + "/TB/mo</span>",
            esc(fmtUSDmo(P.object_storage.b2_per_tb * T0)),
            "grows with the library"));
          break;
        }
        case "lucidlink": {
          var L = P.lucidlink_business;
          var inclGb = L.included_gb_per_seat * S;
          var over0 = Math.max(0, T0 * 1000 - inclGb);
          var over36 = Math.max(0, t36 * 1000 - inclGb);
          rows.push(row("Seats — <span class=\"num\">$" + L.seat + "</span> × " + S + " (annual)", esc(fmtUSDmo(L.seat * S))));
          rows.push(row("Storage beyond " + (inclGb / 1000).toLocaleString("en-US") + " TB included — $" + L.extra_per_100gb + " per 100 GB",
            esc(fmtUSDmo(L.extra_per_100gb * over0 / 100)),
            "at month 36: " + esc(fmtUSDmo(L.extra_per_100gb * over36 / 100))));
          break;
        }
        case "shade": {
          rows.push(row("Seats — <span class=\"num\">$" + P.shade_growth.seat + "</span> × " + S +
            " (annual billing" + (P.shade_growth.seat_monthly ? "; $" + P.shade_growth.seat_monthly + " monthly" : "") + ")",
            esc(fmtUSDmo(P.shade_growth.seat * S)),
            "includes " + esc(fmtTB(sim.flags.shadeCapTb)) + " active storage"));
          break;
        }
        case "s3": {
          rows.push(row("Object storage — <span class=\"num\">$" + inp.s3PerTb + "/TB/mo</span> × " + esc(fmtTB(T0)),
            esc(fmtUSDmo(inp.s3PerTb * T0)),
            "at month 36: " + esc(fmtTB(t36)) + " → " + esc(fmtUSDmo(inp.s3PerTb * t36))));
          break;
        }
        case "mybill": {
          rows.push(row("Your bill", esc(fmtUSDmo(inp.bill))));
          if (inp.billPerTb > 0 && inp.growth > 0) {
            rows.push(row("Growth — $" + inp.billPerTb + "/TB on +" + esc(fmtTB(inp.growth)) + "/mo",
              "+" + esc(fmtUSDmo(inp.billPerTb * inp.growth)) + " each month",
              "at month 36: " + esc(fmtUSDmo(inp.bill + inp.billPerTb * inp.growth * CHART_MONTHS))));
          }
          break;
        }
      }
      return rows;
    }

    function renderReceipt(sim, inp, P) {
      var el = $("receipt");
      var capex = sim.capex;
      var n = sim.chartMonths;
      var power = (inp.watts / 1000) * HOURS_PER_MONTH * inp.kwh;

      var selfRows = [];
      function srow(label, value, note, cls) {
        selfRows.push('<tr role="row" class="' + (cls || "") + '"><td role="cell">' + label +
          (note ? '<span class="note">' + note + "</span>" : "") + '</td><td role="cell" class="num">' + value + "</td></tr>");
      }
      srow("Server hardware — one-time", esc(fmtUSD(capex.hardware)));
      srow("Drives — <span class=\"num\">" + capex.provTb + " TB</span> usable × $" + inp.drivePerTb + "/TB, one-time",
        esc(fmtUSD(capex.drives)),
        capex.provAuto ? (inp.growth > 0
          ? "(today’s " + esc(fmtTB(inp.tb)) + " + a year of growth) × 1.25 headroom — override under advanced"
          : "today’s " + esc(fmtTB(inp.tb)) + " × 1.25 headroom, growth set to 0 — override under advanced") : "provisioned size set by hand");
      if (inp.gbe) srow("10 GbE NIC + switch port — one-time", esc(fmtUSD(capex.gbe)));
      srow("JuiceMount software", esc(fmtUSD(0)), "open source, Apache-2.0 — this row is the point");
      srow("One-time total", esc(fmtUSD(capex.total)), "", "total");
      srow("Power — <span class=\"num\">" + inp.watts + " W</span> × $" + inp.kwh + "/kWh", esc(fmtUSDmo(power)));
      if (inp.backup) {
        srow("Offsite mirror — $" + inp.backupPerTb + "/TB/mo, B2-class", esc(fmtUSDmo(inp.backupPerTb * inp.tb)),
          "3-2-1 honesty: at month 36 it’s " + esc(fmtUSDmo(inp.backupPerTb * libraryAt(inp, CHART_MONTHS))));
      }
      if (inp.otherOpex > 0) srow("Other self-host opex", esc(fmtUSDmo(inp.otherOpex)));
      srow((n < CHART_MONTHS ? n : 36) + "-month total, hardware included", esc(fmtUSD(capex.total + sim.cumSelf[n])), "", "total");

      var saasRows = describeSaasRows(sim, inp, P);
      if (sim.status !== "not_comparable") {
        saasRows.push('<tr role="row" class="total"><td role="cell">' + (n < CHART_MONTHS ? n + "-month total (to the tier cap)" : "36-month total") +
          '</td><td role="cell" class="num">' + esc(fmtUSD(sim.cumSaas[n])) + "</td></tr>");
      }

      /* role attributes restore the table semantics that the receipt CSS's
         display:block strips in Chrome/Safari accessibility trees. */
      el.innerHTML =
        '<table role="table"><caption role="caption">Self-host receipt</caption><tbody role="rowgroup">' + selfRows.join("") + "</tbody></table>" +
        '<table role="table"><caption role="caption">' + esc(VENDOR_LABELS[inp.vendor].charAt(0).toUpperCase() + VENDOR_LABELS[inp.vendor].slice(1)) +
        ' receipt</caption><tbody role="rowgroup">' + (saasRows.join("") || '<tr role="row"><td role="cell" class="muted">no published math at this size</td><td role="cell"></td></tr>') + "</tbody></table>";
    }

    function renderChart(sim) {
      var wrap = $("chart-wrap");
      if (sim.status === "not_comparable" || sim.chartMonths < 1) {
        $("chart").innerHTML = "";
        wrap.hidden = true;
        return;
      }
      wrap.hidden = false;
      $("chart").innerHTML = chartSVG(sim);
    }

    function update() {
      var inp = readForm();
      var sim = simulate(inp, PRICING);
      renderHeadline(sim, inp);
      renderWarnings(sim, inp);
      renderChart(sim);
      renderReceipt(sim, inp, PRICING);
      writeUrl(inp);
      /* rendering hook for page scripts (the payback explainer line and
         chart decorations listen for this); no math happens out there. */
      try {
        document.dispatchEvent(new CustomEvent("jm:sim", { detail: { sim: sim, inp: inp } }));
      } catch (e) { /* no CustomEvent constructor: the static copy stands */ }
    }

    /* ---- wiring ------------------------------------------------------------ */
    function bind() {
      form.addEventListener("input", function (ev) {
        var id = ev.target.id;
        if (id === "tb-range") setVal("tb", ev.target.value);
        if (id === "tb") setVal("tb-range", Math.min(100, parseFloat(ev.target.value) || 0));
        if (id === "g-range") setVal("g", ev.target.value);
        if (id === "g") setVal("g-range", Math.min(10, parseFloat(ev.target.value) || 0));
        if (id === "hw") syncHwPreset(ev.target.value);
        if (id === "vendor") syncVendorFields(ev.target.value);
        syncSliderText();
        update();
      });

      $("hw-preset").addEventListener("change", function (ev) {
        if (ev.target.value !== "custom") {
          setVal("hw", ev.target.value);
          update();
        }
      });

      $("reset").addEventListener("click", function () {
        writeForm(DEFAULTS);
        update();
      });

      $("copy").addEventListener("click", function () {
        var field = $("share-url");
        var done = function () {
          var btn = $("copy");
          btn.textContent = "Copied";
          window.setTimeout(function () { btn.textContent = "Copy link"; }, 1600);
        };
        if (navigator.clipboard && window.isSecureContext) {
          navigator.clipboard.writeText(field.value).then(done, function () { field.select(); });
        } else {
          field.select();
          try { document.execCommand("copy"); done(); } catch (e) { /* selection is enough */ }
        }
      });
    }

    /* ---- pricing load + boot ------------------------------------------------ */
    function boot(pricing) {
      PRICING = pricing;
      var meta = $("pricing-meta");
      if (meta) {
        meta.textContent = "Competitor prices fetched " + pricing.fetched +
          " (re-checked " + pricing.checked + ") from the public pricing pages linked below.";
      }
      applyUrl();
      bind();
      update();
    }

    if (window.fetch && window.location.protocol !== "file:") {
      /* no-cache = always revalidate: stale cached pricing once served a
         row "$undefined monthly" after a data update added a key the old
         cached JSON lacked. 304s keep it cheap; data files must be fresh. */
      window.fetch("assets/pricing.json", { cache: "no-cache" })
        .then(function (r) { if (!r.ok) throw new Error("http " + r.status); return r.json(); })
        .then(boot)
        .catch(function () { boot(FALLBACK_PRICING); });
    } else {
      boot(FALLBACK_PRICING);
    }
  }

  /* ---- entry points --------------------------------------------------------- */
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
      DEFAULTS: DEFAULTS,
      FALLBACK_PRICING: FALLBACK_PRICING,
      libraryAt: libraryAt,
      saasRate: saasRate,
      selfRate: selfRate,
      capexOf: capexOf,
      simulate: simulate
    };
  }

  /* Same pure functions for other pages (compare's annual-cost meter loads
     this file and derives every figure from these — one set of formulas,
     no forks). DOM wiring above no-ops on pages without #calc-form. */
  if (typeof window !== "undefined") {
    window.JMCalc = {
      DEFAULTS: DEFAULTS,
      FALLBACK_PRICING: FALLBACK_PRICING,
      VENDOR_LABELS: VENDOR_LABELS,
      libraryAt: libraryAt,
      saasRate: saasRate,
      selfRate: selfRate,
      capexOf: capexOf,
      simulate: simulate
    };
  }
})();

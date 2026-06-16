/* ════════════════════════════════════════════════════
   admin/routes.js — Express router for /admin/*
   Mounted from server.js with: app.use('/admin', adminRouter)
   ════════════════════════════════════════════════════ */

const express = require('express');
const path    = require('path');
const crypto  = require('crypto');
const db      = require('./db');
const auth    = require('./auth');
const sync    = require('./sync');

const router = express.Router();

const TOKEN_SECRET   = process.env.TOKEN_SECRET || 'tgp-portal-secret-2026';
const ADMIN_BASE_URL = process.env.BASE_URL || 'https://help.thegreatproject.com';
const CC_API_BASE    = 'https://api.checkoutchamp.com';
const RC_API_BASE    = 'https://api.rechargeapps.com';
const SHOPIFY_STORE  = process.env.SHOPIFY_STORE  || 'bigferverv.myshopify.com';
const SHOPIFY_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN || '';

/* ── Europe/Amsterdam timezone helpers ──────────────────────────────────
   All "today" boundaries in this dashboard use Amsterdam local midnight,
   not UTC midnight. In CEST (summer, UTC+2) Amsterdam midnight = UTC 22:00
   the previous day; in CET (winter, UTC+1) it = UTC 23:00 the previous day.
   Using UTC midnight would misclassify the 00:00–02:00 CEST window as the
   previous day, causing early-morning subs/orders to show under yesterday. */

function amsDateStr(d) {
  /* 'YYYY-MM-DD' in Amsterdam timezone */
  return d.toLocaleDateString('en-CA', { timeZone: 'Europe/Amsterdam' });
}

function amsMidnightUTC(d) {
  /* UTC timestamp that corresponds to Amsterdam midnight on the day of `d` */
  var date    = amsDateStr(d);
  var noonUTC = new Date(date + 'T12:00:00Z');
  /* Determine Amsterdam UTC offset (+2 CEST, +1 CET) from noon that day */
  var offset  = parseInt(new Intl.DateTimeFormat('en-US', {
    timeZone: 'Europe/Amsterdam', hour: 'numeric', hour12: false
  }).format(noonUTC)) - 12;
  return new Date(new Date(date + 'T00:00:00Z').getTime() - offset * 3600000);
}

/* Parse a CC dateCreated string (e.g. "2026-06-08 09:07:12") as a proper UTC Date.
   CC runs on America/New_York (EDT = UTC-4 in summer, EST = UTC-5 in winter).
   Using a hardcoded -04:00 offset breaks in winter — this computes it dynamically. */
function ccParseDate(s) {
  if (!s) return null;
  var iso     = s.replace(' ', 'T');
  var noonUTC = new Date(iso.split('T')[0] + 'T12:00:00Z');
  var nyHour  = parseInt(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', hour: 'numeric', hour12: false
  }).format(noonUTC));
  var off     = nyHour - 12;                                   /* -4 EDT, -5 EST */
  var sign    = off < 0 ? '-' : '+';
  var pad     = String(Math.abs(off)).padStart(2, '0');
  return new Date(iso + sign + pad + ':00');
}

function adminCreateToken(payload) {
  payload.expires = Date.now() + 24 * 60 * 60 * 1000;
  var data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  var sig  = crypto.createHmac('sha256', TOKEN_SECRET).update(data).digest('base64url');
  return data + '.' + sig;
}

/* ════════════════════════════════════════════════════
   PUBLIC ENDPOINTS (no auth)
   ════════════════════════════════════════════════════ */

/* Login tracking — called by magic-login.html.
   Public on purpose; rate-limit-worthy in v2.            */
router.post('/track-login', async function(req, res) {
  try {
    const email   = (req.body.email || '').trim().toLowerCase();
    const source  = req.body.source || null;
    const foundIn = Array.isArray(req.body.foundIn) ? req.body.foundIn : null;
    const ua      = req.body.ua || req.headers['user-agent'] || null;
    const ip      = (req.headers['x-forwarded-for'] || req.connection.remoteAddress || '').split(',')[0].trim();

    if (!email) return res.json({ ok: true }); /* swallow silently */

    /* Look up customer if exists, but don't fail if not */
    const cust = await db.one('SELECT id FROM customers WHERE email = $1', [email]);

    await db.query(
      'INSERT INTO logins (customer_id, email, source, found_in, user_agent, ip) VALUES ($1, $2, $3, $4, $5, $6)',
      [cust ? cust.id : null, email, source, foundIn, ua, ip]
    );

    await db.recordEvent('login', source, email, { foundIn: foundIn });

    res.json({ ok: true });
  } catch (err) {
    console.error('[admin/track-login]', err.message);
    res.json({ ok: true }); /* never block the customer's login flow */
  }
});

/* Auth — request magic link */
router.post('/auth/request', async function(req, res) {
  try {
    const r = await auth.requestMagicLink(req.body.email);
    res.json(r);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});
/* Auth — password login */
router.post('/auth/login', function(req, res) {
  try {
    const r = auth.verifyPassword(req.body.email, req.body.password);
    if (!r) return res.status(401).json({ error: 'Invalid email or password' });
    res.json(r);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
/* Auth — verify magic link, set session token, redirect to dashboard */
router.get('/auth/verify', async function(req, res) {
  try {
    const r = await auth.verifyMagicLink(req.query.t);
    /* Redirect to dashboard with session token in URL fragment so JS picks it up
       (fragment isn't sent to the server — it stays browser-side). */
    res.redirect('/admin/#session=' + r.session);
  } catch (err) {
    res.status(401).send(
      '<html><body style="font-family:Inter,sans-serif;background:#0d0d0d;color:#eee;padding:40px;text-align:center">' +
      '<h1 style="color:#f04e00">Login link invalid</h1>' +
      '<p>' + err.message + '</p>' +
      '<p><a href="/admin/login" style="color:#f04e00">Request a new link</a></p>' +
      '</body></html>'
    );
  }
});

/* ════════════════════════════════════════════════════
   STATIC PAGES
   ════════════════════════════════════════════════════ */

router.get('/login', function(req, res) {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, '..', 'public', 'admin', 'login.html'));
});

router.get('/', function(req, res) {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, '..', 'public', 'admin', 'index.html'));
});

router.get('/integrations', function(req, res) {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, '..', 'public', 'admin', 'integrations.html'));
});

/* ════════════════════════════════════════════════════
   AUTHED API ENDPOINTS
   All endpoints below require a valid session token.
   ════════════════════════════════════════════════════ */

router.use('/api', auth.requireAdmin);
router.use('/sync', auth.requireAdmin);

/* ── /admin/api/overview — single-call dashboard payload ── */

router.get('/api/overview', async function(req, res) {
  try {
    const now    = new Date();
    /* Use Amsterdam midnight — not UTC midnight — so today's counts correctly
       reflect the Amsterdam day (off by 1–2 h otherwise in CEST/CET). */
    const todayStart = amsMidnightUTC(now);
    const todayEnd   = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    const periodDays = Math.min(Math.max(parseInt(req.query.days || '30', 10), 1), 365);
    const periodAgo  = new Date(todayStart); periodAgo.setUTCDate(periodAgo.getUTCDate() - periodDays);
    const tomorrowEnd   = new Date(todayStart); tomorrowEnd.setUTCDate(tomorrowEnd.getUTCDate() + 2);
    const tomorrowStart = new Date(todayStart); tomorrowStart.setUTCDate(tomorrowStart.getUTCDate() + 1);
    const weekStart    = new Date(todayStart); weekStart.setUTCDate(weekStart.getUTCDate() - 7);
    const twoWeeksAgo  = new Date(todayStart); twoWeeksAgo.setUTCDate(twoWeeksAgo.getUTCDate() - 14);

    /* Exclude PayPal Commerce and Unknown/Not Set gateway CC subscriptions */
    const NO_PAYPAL = `AND NOT (source = 'cc' AND raw->>'merchant' ILIKE '%paypal%')
      AND NOT (source = 'cc' AND COALESCE(NULLIF(TRIM(raw->>'merchant'), ''), '') = '')`;

    const [
      activeCount,
      mrrRow,
      todayRevRow,
      todayNewRow,
      todayCancelRow,
      yesterdayNewRow,
      yesterdayCancelRow,
      thisWeekNewRow,
      lastWeekNewRow,
      thisWeekCancelRow,
      lastWeekCancelRow,
      tomorrowForecastRow,
      sourceBreakdown,
      revTrend,
      forecast7d,
      newSubsTrend,
      cancelsTrend,
      recentCancels,
      recentEvents,
      productBreakdown,
      tomorrowRebillsList,
      gatewayBreakdown
    ] = await Promise.all([
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() ${NO_PAYPAL}`),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() ${NO_PAYPAL}`),
      db.one(`SELECT COALESCE(SUM(amount_cents),0)::bigint AS cents, COUNT(*)::int AS n FROM orders WHERE created_at >= $1`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 ${NO_PAYPAL}`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 ${NO_PAYPAL}`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 AND started_at < $2 ${NO_PAYPAL}`,
             [new Date(todayStart.getTime() - 24*60*60*1000), todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 AND cancelled_at < $2 ${NO_PAYPAL}`,
             [new Date(todayStart.getTime() - 24*60*60*1000), todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 ${NO_PAYPAL}`, [weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 AND started_at < $2 ${NO_PAYPAL}`, [twoWeeksAgo, weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 ${NO_PAYPAL}`, [weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 AND cancelled_at < $2 ${NO_PAYPAL}`, [twoWeeksAgo, weekStart]),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents, COUNT(*)::int AS n
              FROM subscriptions
              WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2 ${NO_PAYPAL}`,
             [tomorrowStart, tomorrowEnd]),
      db.many(`SELECT source, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() ${NO_PAYPAL} GROUP BY source ORDER BY n DESC`),
      db.many(`SELECT TO_CHAR(DATE(created_at), 'YYYY-MM-DD') AS day, COALESCE(SUM(amount_cents),0)::bigint AS cents
               FROM orders WHERE created_at >= $1
               GROUP BY TO_CHAR(DATE(created_at), 'YYYY-MM-DD') ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT TO_CHAR(DATE(next_bill_at), 'YYYY-MM-DD') AS day, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS cents
               FROM subscriptions
               WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2 ${NO_PAYPAL}
               GROUP BY TO_CHAR(DATE(next_bill_at), 'YYYY-MM-DD') ORDER BY day ASC`,
              [todayStart, new Date(todayStart.getTime() + 8 * 24 * 60 * 60 * 1000)]),
      db.many(`SELECT TO_CHAR(DATE(started_at), 'YYYY-MM-DD') AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE started_at >= $1 ${NO_PAYPAL}
               GROUP BY TO_CHAR(DATE(started_at), 'YYYY-MM-DD'), source ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT TO_CHAR(DATE(cancelled_at), 'YYYY-MM-DD') AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE cancelled_at >= $1 ${NO_PAYPAL}
               GROUP BY TO_CHAR(DATE(cancelled_at), 'YYYY-MM-DD'), source ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT id, customer_email, product, source, price_cents, cancelled_at,
               COALESCE(cancel_reason, CASE WHEN status = 'RECYCLE_FAILED' THEN 'Payment failed' END) AS cancel_reason
               FROM subscriptions WHERE cancelled_at >= $1 ${NO_PAYPAL}
               ORDER BY cancelled_at DESC LIMIT 2000`, [periodAgo]),
      Promise.resolve([]),  /* recentEvents removed — activity feed has its own live endpoint */
      db.many(`SELECT product, source, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() AND product IS NOT NULL ${NO_PAYPAL}
               GROUP BY product, source ORDER BY n DESC LIMIT 15`),
      db.many(`SELECT customer_email, product, source, price_cents, next_bill_at
               FROM subscriptions
               WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2 ${NO_PAYPAL}
               ORDER BY source, price_cents DESC`,
              [tomorrowStart, tomorrowEnd]),
      db.many(`SELECT COALESCE(NULLIF(TRIM(raw->>'merchant'), ''), 'Unknown / Not Set') AS gateway,
               COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions
               WHERE source = 'cc' AND status = 'ACTIVE' AND next_bill_at >= NOW()
               AND raw->>'merchant' NOT ILIKE '%paypal%'
               AND COALESCE(NULLIF(TRIM(raw->>'merchant'), ''), '') != ''
               GROUP BY NULLIF(TRIM(raw->>'merchant'), '') ORDER BY n DESC`)
    ]);

    res.json({
      generatedAt: now.toISOString(),
      periodDays:  periodDays,
      kpis: {
        activeSubs:        activeCount.n,
        mrrCents:          Number(mrrRow.cents),
        todayRevenueCents: Number(todayRevRow.cents),
        todayOrders:       todayRevRow.n,
        newToday:          todayNewRow.n,
        cancelsToday:      todayCancelRow.n,
        netToday:          todayNewRow.n - todayCancelRow.n,
        newYesterday:      yesterdayNewRow.n,
        cancelsYesterday:  yesterdayCancelRow.n,
        newThisWeek:       thisWeekNewRow.n,
        newLastWeek:       lastWeekNewRow.n,
        cancelsThisWeek:   thisWeekCancelRow.n,
        cancelsLastWeek:   lastWeekCancelRow.n,
        tomorrowForecastCents: Number(tomorrowForecastRow.cents),
        tomorrowRebillCount:   tomorrowForecastRow.n
      },
      sourceBreakdown:   sourceBreakdown,
      productBreakdown:  productBreakdown,
      revenueTrend:      revTrend,
      forecast7d:        forecast7d,
      newSubsTrend:      newSubsTrend,
      cancelsTrend:      cancelsTrend,
      recentCancels:       recentCancels,
      cancelTotal:         cancelsTrend.reduce(function(s, r) { return s + r.n; }, 0),
      recentEvents:        recentEvents,
      tomorrowRebillsList: tomorrowRebillsList,
      gatewayBreakdown:    gatewayBreakdown
    });
  } catch (err) {
    console.error('[admin/api/overview]', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/cc/missing-gateway — CC active subs with no merchant set ── */

router.get('/api/cc/missing-gateway', async function(req, res) {
  try {
    const rows = await db.many(`
      SELECT native_id, customer_email, product, price_cents, next_bill_at, started_at,
             raw->>'merchantId'                                          AS merchant_id,
             raw->>'merchant'                                            AS merchant,
             raw->>'descriptor'                                          AS descriptor,
             raw->>'campaignId'                                          AS campaign_id,
             raw->>'sourceTitle'                                         AS source_title,
             raw->'transactions'->0->>'paySource'                       AS pay_source,
             raw->'transactions'->0->>'merchant'                        AS txn_merchant,
             raw->'transactions'->0->>'descriptor'                      AS txn_descriptor,
             raw->'transactions'->0->>'responseType'                    AS txn_response
      FROM subscriptions
      WHERE source = 'cc'
        AND status = 'ACTIVE'
        AND next_bill_at >= NOW()
        AND COALESCE(NULLIF(TRIM(raw->>'merchant'), ''), '') = ''
      ORDER BY price_cents DESC
    `);

    /* group by paySource so user can see the breakdown */
    const byPaySource = {};
    rows.forEach(function(r) {
      const k = r.pay_source || 'Unknown';
      byPaySource[k] = (byPaySource[k] || 0) + 1;
    });

    res.json({ total: rows.length, byPaySource: byPaySource, subscriptions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/subscriptions — list with filters ── */

router.get('/api/subscriptions', async function(req, res) {
  try {
    const status = req.query.status || null;
    const source = req.query.source || null;
    const limit  = Math.min(parseInt(req.query.limit || '100', 10), 500);
    const where  = [];
    const vals   = [];
    if (status) { vals.push(status.toUpperCase()); where.push('status = $' + vals.length); }
    if (source) { vals.push(source);               where.push('source = $' + vals.length); }
    const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';
    vals.push(limit);
    const rows = await db.many(
      `SELECT id, source, customer_email, product, status, price_cents, frequency,
              next_bill_at, started_at, cancelled_at
       FROM subscriptions ` + whereSql + `
       ORDER BY (status='ACTIVE') DESC, next_bill_at ASC NULLS LAST
       LIMIT $` + vals.length,
      vals
    );
    res.json({ subscriptions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/logins — recent logins ── */

router.get('/api/logins', async function(req, res) {
  try {
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
    const rows = await db.many(
      `SELECT email, source, found_in, ts, ip
       FROM logins ORDER BY ts DESC LIMIT $1`, [limit]
    );
    res.json({ logins: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/sync-state — when did we last sync ── */

router.get('/api/sync-state', async function(req, res) {
  try {
    const [rows, whopTotal, whopActive, whopCred] = await Promise.all([
      db.many('SELECT * FROM sync_state ORDER BY source'),
      db.one("SELECT COUNT(*)::int AS n FROM subscriptions WHERE source='whop'"),
      db.one("SELECT COUNT(*)::int AS n FROM subscriptions WHERE source='whop' AND status='ACTIVE' AND next_bill_at >= NOW()"),
      db.one("SELECT creds FROM integration_credentials WHERE name='whop'").catch(function(){ return null; })
    ]);
    res.json({
      sync: rows,
      whop: {
        total_rows:    whopTotal  ? whopTotal.n  : 0,
        active_rows:   whopActive ? whopActive.n : 0,
        api_key_in_db: !!(whopCred && whopCred.creds && whopCred.creds['WHOP_API_KEY']),
        api_key_in_env: !!(process.env.WHOP_API_KEY)
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/sync/whop — Whop-only sync trigger ── */

router.post('/sync/whop', async function(req, res) {
  try {
    const full = req.query.full === '1' || req.body.full === true;
    sync.syncWhop({ full: full })
      .then(function(r) {
        console.log('[admin] whop-only sync complete', r);
        return db.query(
          full
            ? "UPDATE sync_state SET last_full_sync_at=NOW(), last_delta_sync_at=NOW(), last_error=NULL WHERE source='whop'"
            : "UPDATE sync_state SET last_delta_sync_at=NOW(), last_error=NULL WHERE source='whop'"
        );
      })
      .catch(function(e) {
        console.error('[admin] whop-only sync error', e.message);
        db.query("UPDATE sync_state SET last_error=$1, last_error_at=NOW() WHERE source='whop'", [e.message]).catch(function(){});
      });
    res.json({ started: true, full: full });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/sync/run — manual sync trigger ── */

router.post('/sync/run', async function(req, res) {
  try {
    const full = req.query.full === '1' || req.body.full === true;
    /* Don't await — kick off in background and return immediately */
    sync.runSyncCycle({ full: full })
      .then(function(r) { console.log('[admin] manual sync complete', r); })
      .catch(function(e){ console.error('[admin] manual sync error', e.message); });
    res.json({ started: true, full: full });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/sync/today — force-stamp today's CC billings immediately ── */

router.post('/sync/today', async function(req, res) {
  try {
    const result = await sync.syncTodayBillings();
    res.json({ ok: true, result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ════════════════════════════════════════════════════
   CC API TEST ENDPOINTS
   All require admin auth. Use these to inspect raw CC
   API responses and diagnose data accuracy issues.
   ════════════════════════════════════════════════════ */

const fetch = require('node-fetch');

function ccTestParams(extra) {
  const p = new URLSearchParams({
    loginId:  process.env.CC_LOGIN_ID  || '',
    password: process.env.CC_API_PASSWORD || ''
  });
  if (extra) for (const k in extra) p.append(k, extra[k]);
  return p.toString();
}

function ccFmtDate(d) {
  return (d.getMonth()+1).toString().padStart(2,'0') + '/' +
          d.getDate().toString().padStart(2,'0') + '/' +
          d.getFullYear();
}

/* ── GET /admin/api/cc/purchases
   Returns raw purchase/query results.
   Query params:
     startDate  MM/DD/YYYY  (default: 30 days ago)
     endDate    MM/DD/YYYY  (default: today)
     page       number      (default: 1)
     limit      number      (default: 25, max 200)
     status     e.g. ACTIVE (optional filter)
   ────────────────────────────────────────────────── */
router.get('/api/cc/purchases', async function(req, res) {
  try {
    const tomorrow = new Date(); tomorrow.setDate(tomorrow.getDate() + 1);

    const startDate = req.query.startDate || '01/01/2015';
    const endDate   = req.query.endDate   || ccFmtDate(tomorrow);
    const page      = parseInt(req.query.page  || '1',  10);
    const limit     = Math.min(parseInt(req.query.limit || '25', 10), 200);

    const extra = { startDate, endDate, resultsPerPage: limit, page };
    if (req.query.status) extra.status = req.query.status;

    const url = 'https://api.checkoutchamp.com/purchase/query/?' + ccTestParams(extra);
    const r   = await fetch(url, { method: 'POST' });
    const raw = await r.text();

    let d;
    try { d = JSON.parse(raw); } catch(e) {
      return res.status(502).json({ error: 'CC returned non-JSON', preview: raw.substring(0, 300) });
    }

    const rows = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];

    /* Summarise distinct values to help diagnose field names */
    const statusValues  = [...new Set(rows.map(function(p){ return p.status; }))];
    const priceFields   = rows.length ? Object.keys(rows[0]).filter(function(k){ return /price|amount|cost|billing/i.test(k); }) : [];
    const sampleRecord  = rows[0] || null;

    res.json({
      _meta: {
        requestedUrl: 'POST https://api.checkoutchamp.com/purchase/query/',
        params: { startDate, endDate, page, limit, status: req.query.status || '(all)' },
        ccResult:   d.result,
        totalCount: d.message && d.message.totalCount,
        returnedCount: rows.length,
        distinctStatusValues: statusValues,
        priceRelatedFields:   priceFields
      },
      sampleRecord: sampleRecord,
      allRecords:   rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/cc/purchases/summary
   Counts active vs cancelled vs other across a date window.
   Same query params as /purchases.
   ────────────────────────────────────────────────── */
router.get('/api/cc/purchases/summary', async function(req, res) {
  try {
    const tomorrow = new Date(); tomorrow.setDate(tomorrow.getDate() + 1);

    const startDate = req.query.startDate || '01/01/2015';
    const endDate   = req.query.endDate   || ccFmtDate(tomorrow);

    /* Page through ALL results — capture totalCount from first response */
    const limit    = 200;
    let page       = 1;
    let allRows    = [];
    let ccTotalCount = null;

    while (page <= 100) {
      const url = 'https://api.checkoutchamp.com/purchase/query/?' + ccTestParams({ startDate, endDate, resultsPerPage: limit, page });
      const r   = await fetch(url, { method: 'POST' });
      const d   = await r.json();
      if (page === 1) ccTotalCount = (d.message && d.message.totalCount) || null;
      const rows = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
      allRows = allRows.concat(rows);
      if (rows.length < limit) break;
      page++;
    }

    /* Count by status */
    const byStatus = {};
    let activeMRRCents = 0;
    allRows.forEach(function(p) {
      const s = String(p.status || 'UNKNOWN');
      byStatus[s] = (byStatus[s] || 0) + 1;
      if (s === 'ACTIVE') {
        const price = parseFloat(p.price || p.recurringPrice || p.billingAmount || 0);
        if (!isNaN(price)) activeMRRCents += Math.round(price * 100);
      }
    });

    res.json({
      _meta: {
        params: { startDate, endDate },
        ccReportedTotal:   ccTotalCount,
        totalRecordsFetched: allRows.length,
        note: ccTotalCount && allRows.length < ccTotalCount
          ? 'WARNING: fetched ' + allRows.length + ' but CC reports ' + ccTotalCount + ' total — some records may be missing'
          : 'All records fetched'
      },
      countByStatus:   byStatus,
      activeMRRCents:  activeMRRCents,
      activeMRRDollars: (activeMRRCents / 100).toFixed(2),
      priceFieldSample: allRows.filter(function(p){ return p.status === 'ACTIVE'; }).slice(0,3).map(function(p){ return {
        purchaseId:  p.purchaseId,
        status:      p.status,
        price:       p.price,
        productName: p.productName
      }; })
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/cc/members
   Calls /members/query/ — may give a more accurate active count
   than purchase/query since it queries membership status directly.
   Query params:
     page   (default: 1)
     limit  (default: 25)
   ────────────────────────────────────────────────── */
router.get('/api/cc/members', async function(req, res) {
  try {
    const page  = parseInt(req.query.page  || '1',  10);
    const limit = Math.min(parseInt(req.query.limit || '25', 10), 200);

    const today    = new Date();
    const ago2yr   = new Date(); ago2yr.setFullYear(ago2yr.getFullYear() - 2);

    const url = 'https://api.checkoutchamp.com/members/query/?' + ccTestParams({
      startDate:      ccFmtDate(ago2yr),
      endDate:        ccFmtDate(today),
      resultsPerPage: limit,
      page:           page
    });

    const r   = await fetch(url, { method: 'POST' });
    const raw = await r.text();
    let d;
    try { d = JSON.parse(raw); } catch(e) {
      return res.status(502).json({ error: 'CC returned non-JSON', preview: raw.substring(0, 300) });
    }

    const rows = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
    const statusValues = [...new Set(rows.map(function(m){ return m.status || m.memberStatus; }))];

    res.json({
      _meta: {
        endpoint: 'POST https://api.checkoutchamp.com/members/query/',
        totalCount:    d.message && d.message.totalCount,
        returnedCount: rows.length,
        distinctStatusValues: statusValues
      },
      sampleRecord: rows[0] || null,
      allRecords:   rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/cc/db-vs-cc
   Compares our local DB counts against a fresh CC API pull
   so you can see the gap at a glance.
   ────────────────────────────────────────────────── */
router.get('/api/cc/db-vs-cc', async function(req, res) {
  try {
    /* DB counts */
    const [dbActive, dbTotal, dbMrr] = await Promise.all([
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE source='cc' AND status='ACTIVE'`),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE source='cc'`),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents FROM subscriptions WHERE source='cc' AND status='ACTIVE'`)
    ]);

    /* CC API — last 30 days */
    const today    = new Date();
    const ago30    = new Date(); ago30.setDate(ago30.getDate() - 30);
    const tomorrow = new Date(); tomorrow.setDate(tomorrow.getDate() + 1);

    const url30 = 'https://api.checkoutchamp.com/purchase/query/?' + ccTestParams({
      startDate: ccFmtDate(ago30), endDate: ccFmtDate(tomorrow), resultsPerPage: 1, page: 1
    });
    const r30  = await fetch(url30, { method: 'POST' });
    const d30  = await r30.json();

    /* CC API — all time (from 2015 epoch) */
    const url2y  = 'https://api.checkoutchamp.com/purchase/query/?' + ccTestParams({
      startDate: '01/01/2015', endDate: ccFmtDate(tomorrow), resultsPerPage: 1, page: 1
    });
    const r2y  = await fetch(url2y, { method: 'POST' });
    const d2y  = await r2y.json();

    res.json({
      database: {
        activeSubscriptions: dbActive.n,
        totalSubscriptions:  dbTotal.n,
        mrrDollars: (Number(dbMrr.cents) / 100).toFixed(2),
        note: 'Only subscriptions that have been synced into local DB'
      },
      ccApi: {
        totalInLast30Days: d30.message && d30.message.totalCount,
        totalInLast2Years: d2y.message && d2y.message.totalCount,
        note: 'Raw count from CC API — includes all statuses (active + cancelled)'
      },
      diagnosis: {
        dbHasLessRecordsThanCC: dbTotal.n < (d2y.message && d2y.message.totalCount),
        recommendation: dbTotal.n < (d2y.message && d2y.message.totalCount)
          ? 'Run a FULL sync (POST /admin/sync/run?full=1) to import all historical CC subscriptions'
          : 'Record counts look close — check status field mapping'
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/cc-orders?email=xxx — raw CC order/query response ── */

router.get('/api/debug/cc-orders', async function(req, res) {
  try {
    const email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email query param required' });

    const today   = new Date();
    const endDate = (today.getMonth()+1).toString().padStart(2,'0') + '/' +
                    today.getDate().toString().padStart(2,'0') + '/' + today.getFullYear();

    const params = new URLSearchParams({
      loginId:        process.env.CC_LOGIN_ID      || '',
      password:       process.env.CC_API_PASSWORD  || '',
      emailAddress:   email,
      startDate:      '01/01/2016',
      endDate:        endDate,
      resultsPerPage: 10,
      sortDir:        -1
    });

    const r    = await fetch('https://api.checkoutchamp.com/order/query/?' + params.toString(), { method: 'POST' });
    const raw  = await r.text();
    let parsed;
    try { parsed = JSON.parse(raw); } catch(e) { parsed = null; }

    const orders = parsed && parsed.result === 'SUCCESS' && parsed.message && parsed.message.data
      ? parsed.message.data : [];

    res.json({
      _meta: {
        email,
        ccResult:      parsed && parsed.result,
        totalCount:    parsed && parsed.message && parsed.message.totalCount,
        returnedCount: orders.length,
        note: 'First 10 orders sorted newest-first. Check dateCreated, status, orderType, recurringFlag, responseType, shippingStatus fields.'
      },
      firstOrder:  orders[0] || null,
      allOrders:   orders
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/rc-charges?email=xxx — raw Recharge charge + sub data ── */

router.get('/api/debug/rc-charges', async function(req, res) {
  try {
    const email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email query param required' });

    const RC_KEY = process.env.RECHARGE_API_KEY;
    if (!RC_KEY) return res.status(500).json({ error: 'RECHARGE_API_KEY not set' });

    const headers = { 'X-Recharge-Access-Token': RC_KEY, 'Content-Type': 'application/json' };

    /* 1. Find customer by email */
    const custR = await fetch('https://api.rechargeapps.com/customers?email=' + encodeURIComponent(email), { headers });
    const custD = await custR.json();
    const customer = custD.customers && custD.customers[0];

    if (!customer) return res.json({ found: false, email, note: 'No Recharge customer found for this email' });

    /* 2. Fetch subscriptions for this customer */
    const subR = await fetch('https://api.rechargeapps.com/subscriptions?customer_id=' + customer.id, { headers });
    const subD = await subR.json();

    /* 3. Fetch recent charges (last 5) */
    const chgR = await fetch('https://api.rechargeapps.com/charges?customer_id=' + customer.id + '&limit=5&sort_by=scheduled_at-desc', { headers });
    const chgD = await chgR.json();

    /* 4. Also check our local DB */
    const dbSub = await db.many(
      `SELECT id, status, next_bill_at, last_billed_at, last_synced_at FROM subscriptions
       WHERE LOWER(customer_email) = $1 AND source = 'recharge'`, [email]
    );

    res.json({
      email,
      recharge_customer_id: customer.id,
      customer_status:      customer.status,
      subscriptions_live:   subD.subscriptions || [],
      charges_recent:       chgD.charges || [],
      db_subscriptions:     dbSub,
      _note: 'subscriptions_live shows next_charge_scheduled_at from Recharge. charges_recent shows actual charge history with status and scheduled_at.'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/rc-today-charges — raw Recharge charges for today ──
   Diagnostic: shows exactly what the charges API returns, email field location,
   and whether the date filter is working.                                       */
router.get('/api/debug/rc-today-charges', async function(req, res) {
  try {
    const now        = new Date();
    const todayAms   = amsDateStr(now);
    const todayStart = amsMidnightUTC(now);
    const yesterday  = new Date(todayStart.getTime() - 24 * 3600 * 1000);
    const RC_KEY     = process.env.RECHARGE_API_KEY;
    if (!RC_KEY) return res.status(500).json({ error: 'RECHARGE_API_KEY not set' });
    const headers    = { 'X-Recharge-Access-Token': RC_KEY };

    /* Fetch last 2 days of charges — same window used by today-orders */
    const url = RC_API_BASE + '/charges?status=success&limit=250&page=1' +
      '&created_at_min=' + yesterday.toISOString();
    const r = await fetch(url, { headers });
    const d = await r.json();
    const all = d.charges || [];

    /* Apply the same scheduled_at filter used by today-orders */
    const todayCharges = all.filter(function(c) {
      return (c.scheduled_at || '').slice(0, 10) === todayAms;
    });

    res.json({
      todayAms:            todayAms,
      window_from:         yesterday.toISOString(),
      total_in_window:     all.length,
      scheduled_for_today: todayCharges.length,
      first_3_raw:         all.slice(0, 3),
      first_3_today:       todayCharges.slice(0, 3)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/shopify/tracking?email=xxx — Shopify fulfillment tracking ── */

router.get('/api/shopify/tracking', async function(req, res) {
  try {
    const email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });
    if (!SHOPIFY_TOKEN) return res.status(503).json({ error: 'SHOPIFY_ACCESS_TOKEN not configured' });

    /* Fetch recent Shopify orders for this email (last 10, any status) */
    const url = 'https://' + SHOPIFY_STORE + '/admin/api/2024-01/orders.json' +
      '?email=' + encodeURIComponent(email) +
      '&status=any&limit=10&fields=id,name,created_at,fulfillment_status,fulfillments';

    const r = await fetch(url, {
      headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' }
    });
    if (!r.ok) {
      const t = await r.text();
      return res.status(r.status).json({ error: 'Shopify API error: ' + t.substring(0, 200) });
    }
    const data = await r.json();
    const orders = data.orders || [];

    /* Pull tracking from the most recent fulfilled order */
    let tracking = null;
    for (const order of orders) {
      if (!order.fulfillments || !order.fulfillments.length) continue;
      for (const f of order.fulfillments) {
        if (f.tracking_number) {
          tracking = {
            order_id:          order.id,
            order_name:        order.name,
            order_created_at:  order.created_at,
            tracking_number:   f.tracking_number,
            tracking_company:  f.tracking_company || null,
            tracking_url:      (f.tracking_urls && f.tracking_urls[0]) || f.tracking_url || null,
            status:            f.shipment_status || f.status || null
          };
          break;
        }
      }
      if (tracking) break;
    }

    res.json({ email, tracking, orders_checked: orders.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/today-orders — subscriptions billed today ──
   CC and Recharge are queried live. DB is used only as a fallback
   for Recharge rows not yet surfaced by the live API.              */

const TEST_EMAILS = new Set([
  'nobel@eydigitalmedia.com',
  'md.nobelislamjoy@gmail.com',
  'wardun@eydigitalmedia.com'
]);

router.get('/api/today-orders', async function(req, res) {
  try {
    const now        = new Date();
    /* All day boundaries in Amsterdam local time */
    const todayAms   = amsDateStr(now);                /* 'YYYY-MM-DD' Amsterdam */
    const todayStart = amsMidnightUTC(now);            /* Amsterdam midnight → UTC */
    const todayEnd   = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    /* todayUTC = Amsterdam date string (YYYY-MM-DD) — used to match RC next_charge_scheduled_at */
    const todayUTC   = todayAms;
    /* RC charge window uses Amsterdam boundaries in ISO form */
    const tomorrowUTC = amsDateStr(todayEnd);
    /* CC order/query uses MM/DD/YYYY — derived from Amsterdam date */
    const [ty, tm, td] = todayAms.split('-');
    const ccTodayStr = tm + '/' + td + '/' + ty;

    /* Run all queries in parallel for speed */
    const [ccRows, ccPendingRows, rcRows, whopRows, shopifyResult] = await Promise.all([

      /* ── CC: transactions/query for today's CHARGED RECURRING billings ──
         CC stores timestamps in EDT (UTC-4). User is in Amsterdam (CEST = UTC+2).
         Amsterdam today starts at CEST 00:00 = UTC-2h = EDT previous day 18:00.
         Query yesterday + today (UTC) and filter client-side to the CEST 24h window
         so early-morning CEST charges (which fall on yesterday in EDT) are included. */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return [];
        /* todayStart is already Amsterdam midnight in UTC — use directly as window */
        var cestStart = todayStart;
        var cestEnd   = todayEnd;
        /* CC API dates: yesterday + today in Amsterdam (covers the full UTC window) */
        var prevAms   = amsDateStr(new Date(todayStart.getTime() - 1)); /* 1ms before Amsterdam midnight */
        var [py, pm, pd] = prevAms.split('-');
        var ccStart   = pm + '/' + pd + '/' + py;
        var ccEnd     = ccTodayStr;
        var rows = [], seenEmails = [], page = 1;
        try {
          while (page <= 10) {
            const p = new URLSearchParams({
              loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
              startDate: ccStart, endDate: ccEnd,
              billType: 'RECURRING', txnType: 'SALE', responseType: 'SUCCESS',
              resultsPerPage: 200, page: page
            });
            const r = await fetch(CC_API_BASE + '/transactions/query/?' + p.toString(), { method: 'POST' });
            const d = await r.json();
            const txns = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
            txns.forEach(function(t) {
              /* Keep only transactions within the CEST today window */
              var ds = (t.dateCreated || '');
              if (ds) {
                var dt = ccParseDate(ds);
                if (!dt || isNaN(dt) || dt < cestStart || dt >= cestEnd) return;
              }
              var email = (t.emailAddress || '').trim().toLowerCase();
              if (!email || seenEmails.includes(email) || TEST_EMAILS.has(email)) return;
              seenEmails.push(email);
              /* transactions/query uses campaignName for the product; productName is also
                 present on some accounts — try both. Convert dateCreated (CC EDT) to UTC
                 so the frontend can display it in Amsterdam time without ambiguity. */
              var chargedAt = ds ? (ccParseDate(ds) || now) : now;
              rows.push({
                source: 'cc', native_id: t.orderId || null,
                customer_email: email,
                product: t.productName || t.campaignName || null,
                price_cents: Math.round(parseFloat(t.amount || t.totalAmount || 0) * 100),
                next_bill_at: null, cc_charge_status: 'CHARGED', status: 'ACTIVE',
                first_name: t.firstName || null, last_name: t.lastName || null,
                frequency: null, last_billed_at: chargedAt.toISOString()
              });
            });
            if (txns.length < 200) break;
            page++;
          }
        } catch (e) { console.error('[today-orders] CC transactions query failed:', e.message); }
        console.log('[today-orders] CC charged today (CEST):', rows.length);
        return rows;
      })(),

      /* ── CC: purchase/query for today's PENDING scheduled subscriptions (live) ──
         Delta sync only covers subs created in the last 30 days, so cycle 2+ subs
         started earlier are missing from the DB. Query CC live and filter by
         nextBillDate = today so we never depend on sync freshness.
         Query last 90 days to cover subs up to billing cycle ~3.            */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return [];
        var ago90 = new Date(todayStart.getTime() - 90 * 24 * 3600 * 1000);
        var ccStart90 = (ago90.getUTCMonth()+1).toString().padStart(2,'0') + '/' +
                        ago90.getUTCDate().toString().padStart(2,'0') + '/' + ago90.getUTCFullYear();
        var rows = [], seenEmails = [], page = 1;
        try {
          while (page <= 20) {
            const p = new URLSearchParams({
              loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
              startDate: ccStart90, endDate: ccTodayStr,
              status: 'ACTIVE', resultsPerPage: 200, page: page
            });
            const r = await fetch(CC_API_BASE + '/purchase/query/?' + p.toString(), { method: 'POST' });
            const d = await r.json();
            const subs = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
            subs.forEach(function(s) {
              if (s.nextBillDate !== todayUTC) return;
              var merchant = (s.merchant || '').trim();
              if (!merchant || /paypal/i.test(merchant)) return;
              var email = (s.emailAddress || '').trim().toLowerCase();
              if (!email || seenEmails.includes(email) || TEST_EMAILS.has(email)) return;
              seenEmails.push(email);
              rows.push({
                source: 'cc', native_id: String(s.purchaseId || ''),
                customer_email: email,
                product: s.productName || null,
                price_cents: Math.round(parseFloat(s.price || s.recurringPrice || 0) * 100),
                next_bill_at: todayUTC + 'T00:00:00Z',
                cc_charge_status: 'SCHEDULED', status: 'ACTIVE',
                first_name: s.firstName || null, last_name: s.lastName || null,
                frequency: null, last_billed_at: null
              });
            });
            if (subs.length < 200) break;
            page++;
          }
        } catch (e) { console.error('[today-orders] CC pending query failed:', e.message); }
        console.log('[today-orders] CC pending today (live):', rows.length);
        return rows;
      })(),

      /* ── Recharge: query DB directly — no live API calls ──
         The sync maintains last_billed_at and next_bill_at on every subscription.
         Billed today:    last_billed_at in today's Amsterdam window.
         Scheduled today: next_bill_at in today's Amsterdam window, not yet billed.  */
      (async function() {
        var rows = [], seenEmails = [];
        try {
          var billed = await db.many(`
            SELECT s.customer_email, s.product, s.price_cents,
                   s.last_billed_at, s.next_bill_at,
                   c.first_name, c.last_name
            FROM   subscriptions s
            LEFT JOIN customers c ON LOWER(c.email) = LOWER(s.customer_email)
            WHERE  s.source = 'recharge'
              AND  s.status = 'ACTIVE'
              AND  s.last_billed_at >= $1 AND s.last_billed_at < $2
            ORDER  BY s.last_billed_at DESC
          `, [todayStart, todayEnd]);

          billed.forEach(function(s) {
            var email = (s.customer_email || '').toLowerCase();
            if (!email || TEST_EMAILS.has(email) || seenEmails.includes(email)) return;
            seenEmails.push(email);
            rows.push({
              source: 'recharge', native_id: email,
              customer_email: email,
              product: s.product || null,
              price_cents: s.price_cents || 0,
              next_bill_at: null, status: 'ACTIVE',
              first_name: s.first_name || null, last_name: s.last_name || null,
              frequency: null,
              last_billed_at: s.last_billed_at ? new Date(s.last_billed_at).toISOString() : null
            });
          });
          console.log('[today-orders] RC billed today (DB):', rows.length);

          var scheduled = await db.many(`
            SELECT s.customer_email, s.product, s.price_cents,
                   s.last_billed_at, s.next_bill_at,
                   c.first_name, c.last_name
            FROM   subscriptions s
            LEFT JOIN customers c ON LOWER(c.email) = LOWER(s.customer_email)
            WHERE  s.source = 'recharge'
              AND  s.status = 'ACTIVE'
              AND  s.next_bill_at >= $1 AND s.next_bill_at < $2
              AND  (s.last_billed_at IS NULL OR s.last_billed_at < $1)
            ORDER  BY s.next_bill_at
          `, [todayStart, todayEnd]);

          scheduled.forEach(function(s) {
            var email = (s.customer_email || '').toLowerCase();
            if (!email || TEST_EMAILS.has(email) || seenEmails.includes(email)) return;
            seenEmails.push(email);
            rows.push({
              source: 'recharge', native_id: email,
              customer_email: email,
              product: s.product || null,
              price_cents: s.price_cents || 0,
              next_bill_at: s.next_bill_at ? new Date(s.next_bill_at).toISOString() : null,
              status: 'ACTIVE',
              first_name: s.first_name || null, last_name: s.last_name || null,
              frequency: null, last_billed_at: null
            });
          });
          console.log('[today-orders] RC scheduled today (DB):', scheduled.length);
        } catch (e) { console.error('[today-orders] RC DB query failed:', e.message); }
        console.log('[today-orders] RC today total:', rows.length);
        return rows;
      })(),

      /* ── Whop: subscriptions renewing today (DB) ── */
      (async function() {
        var rows = [], seenEmails = [];
        try {
          var whop = await db.many(`
            SELECT s.customer_email, s.product, s.price_cents,
                   s.next_bill_at, c.first_name, c.last_name
            FROM   subscriptions s
            LEFT JOIN customers c ON LOWER(c.email) = LOWER(s.customer_email)
            WHERE  s.source = 'whop'
              AND  s.status = 'ACTIVE'
              AND  s.next_bill_at >= $1 AND s.next_bill_at < $2
            ORDER  BY s.price_cents DESC
          `, [todayStart, todayEnd]);
          whop.forEach(function(s) {
            var email = (s.customer_email || '').toLowerCase();
            if (!email || TEST_EMAILS.has(email) || seenEmails.includes(email)) return;
            seenEmails.push(email);
            rows.push({
              source: 'whop', native_id: email,
              customer_email: email,
              product: s.product || null,
              price_cents: s.price_cents || 0,
              next_bill_at: s.next_bill_at ? new Date(s.next_bill_at).toISOString() : null,
              status: 'ACTIVE',
              first_name: s.first_name || null, last_name: s.last_name || null,
              frequency: null, last_billed_at: null
            });
          });
        } catch (e) { console.error('[today-orders] Whop DB query failed:', e.message); }
        return rows;
      })(),

      /* ── Shopify: emails of customers with orders today (confirms Recharge billing) ── */
      (async function() {
        var emails = new Set();
        var nativeSubEmails = new Set(); /* subscription_contract_checkout_one = definitely recurring */
        if (!SHOPIFY_TOKEN) return { emails: emails, nativeSubEmails: nativeSubEmails };
        try {
          var url = 'https://' + SHOPIFY_STORE + '/admin/api/2024-01/orders.json' +
            '?status=any&limit=250&fields=email,created_at,source_name' +
            '&created_at_min=' + encodeURIComponent(todayStart.toISOString());
          while (url) {
            var r = await fetch(url, { headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN } });
            var link = r.headers.get('link') || '';
            var d = await r.json();
            (d.orders || []).forEach(function(o) {
              if (!o.email) return;
              var e = o.email.toLowerCase().trim();
              emails.add(e);
              if (o.source_name === 'subscription_contract_checkout_one') nativeSubEmails.add(e);
            });
            var next = link.match(/<([^>]+)>;\s*rel="next"/);
            url = next ? next[1] : null;
          }
          console.log('[today-orders] Shopify billed emails today:', emails.size, '| native-sub:', nativeSubEmails.size);
        } catch (e) { console.error('[today-orders] Shopify orders fetch failed:', e.message); }
        return { emails: emails, nativeSubEmails: nativeSubEmails };
      })()
    ]);

    var shopifyEmailsToday    = (shopifyResult && shopifyResult.emails)         || new Set();
    var shopifyNativeSubEmails = (shopifyResult && shopifyResult.nativeSubEmails) || new Set();

    /* Cross-reference Recharge pending rows against Shopify orders.
       If a customer's email has a Shopify order today → confirmed billed.
       Update DB in background so subsequent calls reflect the confirmed billing. */
    var shopifyConfirmed = [];
    rcRows.forEach(function(r) {
      if (!r.last_billed_at && shopifyEmailsToday.has(r.customer_email)) {
        r.last_billed_at = now.toISOString();
        shopifyConfirmed.push(r.customer_email);
      }
    });
    if (shopifyConfirmed.length > 0) {
      console.log('[today-orders] Shopify confirmed', shopifyConfirmed.length, 'Recharge subs');
      db.query(
        `UPDATE subscriptions
         SET last_billed_at = NOW()
         WHERE source = 'recharge' AND status = 'ACTIVE'
           AND LOWER(customer_email) = ANY($1)
           AND next_bill_at >= $2 AND next_bill_at < $3
           AND (last_billed_at IS NULL OR last_billed_at < $2)`,
        [shopifyConfirmed, todayStart, todayEnd]
      ).then(function(r) {
        console.log('[today-orders] stamped last_billed_at on', r.rowCount, 'Recharge subs via Shopify');
      }).catch(function(e) {
        console.error('[today-orders] Shopify confirm DB update failed:', e.message);
      });
    }

    /* Catch subscription_contract_checkout_one recurring orders not already in rcRows.
       These are Shopify-native subscription renewals whose DB record may have a stale
       next_bill_at (e.g. already updated to next month) so they fall outside today's window. */
    var rcRowEmailSet = new Set(rcRows.map(function(r) { return r.customer_email; }));
    var missingNativeSubs = [...shopifyNativeSubEmails].filter(function(e) { return !rcRowEmailSet.has(e); });
    if (missingNativeSubs.length > 0) {
      try {
        var nativeRows = await db.many(`
          SELECT DISTINCT ON (s.customer_email)
                 s.customer_email, s.product, s.price_cents,
                 s.last_billed_at, s.next_bill_at,
                 c.first_name, c.last_name
          FROM   subscriptions s
          LEFT JOIN customers c ON LOWER(c.email) = LOWER(s.customer_email)
          WHERE  s.source = 'recharge' AND s.status = 'ACTIVE'
            AND  LOWER(s.customer_email) = ANY($1)
          ORDER  BY s.customer_email, s.next_bill_at DESC NULLS LAST
        `, [missingNativeSubs]);
        var foundEmails = new Set(nativeRows.map(function(r) { return r.customer_email; }));
        nativeRows.forEach(function(s) {
          rcRows.push({
            source: 'recharge', native_id: s.customer_email,
            customer_email: s.customer_email,
            product: s.product || null,
            price_cents: s.price_cents || 0,
            next_bill_at: s.next_bill_at ? new Date(s.next_bill_at).toISOString() : null,
            last_billed_at: now.toISOString(),
            status: 'ACTIVE',
            first_name: s.first_name || null,
            last_name: s.last_name || null,
            frequency: null
          });
        });
        /* Customers billed via Shopify native contracts but absent from DB entirely */
        missingNativeSubs.filter(function(e) { return !foundEmails.has(e); }).forEach(function(e) {
          rcRows.push({
            source: 'recharge', native_id: e,
            customer_email: e,
            product: null, price_cents: 0,
            next_bill_at: null, last_billed_at: now.toISOString(),
            status: 'ACTIVE', first_name: null, last_name: null, frequency: null
          });
        });
        /* Stamp last_billed_at in DB so today-revenue picks them up on next poll */
        if (nativeRows.length > 0) {
          db.query(
            `UPDATE subscriptions SET last_billed_at = NOW()
             WHERE source = 'recharge' AND status = 'ACTIVE'
               AND LOWER(customer_email) = ANY($1)
               AND (last_billed_at IS NULL OR last_billed_at < $2)`,
            [nativeRows.map(function(r) { return r.customer_email; }), todayStart]
          ).then(function(r) {
            console.log('[today-orders] stamped last_billed_at on', r.rowCount, 'native-sub Recharge subs');
          }).catch(function(e) {
            console.error('[today-orders] native-sub DB stamp failed:', e.message);
          });
        }
        console.log('[today-orders] native-sub fallback: added', missingNativeSubs.length,
          '(', nativeRows.length, 'from DB,', (missingNativeSubs.length - nativeRows.length), 'synthetic)');
      } catch (e) { console.error('[today-orders] native-sub lookup failed:', e.message); }
    }

    /* Merge: CC charged + CC pending + Recharge + Whop */
    const ccLiveEmails = new Set(ccRows.map(function(r) { return r.customer_email; }));

    const filteredCcPending = ccPendingRows.filter(function(r) {
      return !ccLiveEmails.has(r.customer_email);
    });

    const allOrders = ccRows.concat(filteredCcPending).concat(rcRows).concat(whopRows);
    console.log('[today-orders] total:', allOrders.length);
    res.json({ orders: allOrders, count: allOrders.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/activity-feed
   Live 48-hour activity stream from CC + Recharge.
   Queries both platforms directly every call — no DB cache.
   Auto-polled by the dashboard every 30 seconds.            */

router.get('/api/activity-feed', auth.requireAdmin, async function(req, res) {
  try {
    const now   = new Date();
    const hours = Math.min(parseInt(req.query.hours || '24', 10), 72);
    const since = new Date(now.getTime() - hours * 3600000);

    /* CC API takes MM/DD/YYYY. Add 1-day margin on start side so timezone
       edges don't drop the earliest events; we filter client-side with ccParseDate anyway. */
    var ccSinceDate = new Date(since.getTime() - 24 * 3600000);
    var ccStart = String(ccSinceDate.getUTCMonth() + 1).padStart(2, '0') + '/' +
                  String(ccSinceDate.getUTCDate()).padStart(2, '0') + '/' +
                  String(ccSinceDate.getUTCFullYear());
    var [ty, tm, td] = amsDateStr(now).split('-');
    var ccEnd = tm + '/' + td + '/' + ty;

    var rcHeaders = process.env.RECHARGE_API_KEY
      ? { 'X-Recharge-Access-Token': process.env.RECHARGE_API_KEY }
      : null;

    var events = [];

    await Promise.all([

      /* ── CC: successful transactions (rebills + new subs) ── */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return;
        var page = 1;
        while (page <= 20) {
          const p = new URLSearchParams({
            loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
            startDate: ccStart, endDate: ccEnd,
            txnType: 'SALE', responseType: 'SUCCESS',
            resultsPerPage: 200, page: page
          });
          const r = await fetch(CC_API_BASE + '/transactions/query/?' + p.toString(), { method: 'POST' });
          const d = await r.json();
          const txns = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
          txns.forEach(function(t) {
            var ts = ccParseDate(t.dateCreated || '');
            if (!ts || ts < since) return;
            var email = (t.emailAddress || '').trim().toLowerCase();
            if (!email || TEST_EMAILS.has(email)) return;
            events.push({
              kind:    t.orderType === 'RECURRING' ? 'rebill' : 'new_order',
              source:  'cc',
              email:   email,
              ts:      ts.toISOString(),
              payload: {
                amount:  t.totalAmount || t.amount || null,
                product: t.productName || t.campaignName || null
              }
            });
          });
          if (txns.length < 200) break;
          page++;
        }
      })(),

      /* ── CC: declined transactions (failed charges) ── */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return;
        var page = 1;
        while (page <= 10) {
          const p = new URLSearchParams({
            loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
            startDate: ccStart, endDate: ccEnd,
            txnType: 'SALE', responseType: 'DECLINE',
            resultsPerPage: 200, page: page
          });
          const r = await fetch(CC_API_BASE + '/transactions/query/?' + p.toString(), { method: 'POST' });
          const d = await r.json();
          const txns = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
          txns.forEach(function(t) {
            var ts = ccParseDate(t.dateCreated || '');
            if (!ts || ts < since) return;
            var email = (t.emailAddress || '').trim().toLowerCase();
            if (!email || TEST_EMAILS.has(email)) return;
            events.push({
              kind:    'failed_charge',
              source:  'cc',
              email:   email,
              ts:      ts.toISOString(),
              payload: {
                amount:  t.amount || t.totalAmount || null,
                product: t.productName || t.campaignName || null,
                reason:  t.declineReason || t.responseReason || null
              }
            });
          });
          if (txns.length < 200) break;
          page++;
        }
      })(),

      /* ── CC: recently cancelled subscriptions ──
         purchase/query startDate/endDate filter on dateCreated. We query the full
         since window and keep only rows whose dateUpdated falls in the 48h window. */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return;
        var page = 1;
        while (page <= 5) {
          const p = new URLSearchParams({
            loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
            startDate: ccStart, endDate: ccEnd,
            status: 'CANCELLED', resultsPerPage: 200, page: page
          });
          const r = await fetch(CC_API_BASE + '/purchase/query/?' + p.toString(), { method: 'POST' });
          const d = await r.json();
          const subs = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
          subs.forEach(function(s) {
            /* Only use dateUpdated/cancelDate — never dateCreated.
               A sub created recently but cancelled months ago must not appear. */
            var ts = ccParseDate(s.dateUpdated || s.cancelDate || '');
            if (!ts || ts < since) return;
            var email = (s.emailAddress || '').trim().toLowerCase();
            if (!email || TEST_EMAILS.has(email)) return;
            events.push({
              kind:    'sub_cancelled',
              source:  'cc',
              email:   email,
              ts:      ts.toISOString(),
              payload: { product: s.productName || null, reason: s.cancelReason || null }
            });
          });
          if (subs.length < 200) break;
          page++;
        }
      })(),

      /* ── Recharge: successful charges (rebills + initial) ── */
      (async function() {
        if (!rcHeaders) return;
        var page = 1;
        while (page <= 20) {
          const url = RC_API_BASE + '/charges?limit=250&page=' + page +
            '&created_at_min=' + encodeURIComponent(since.toISOString()) +
            '&status=success';
          const r = await fetch(url, { headers: rcHeaders });
          const d = await r.json();
          const charges = d.charges || [];
          charges.forEach(function(c) {
            var email = ((c.customer && c.customer.email) || c.email || '').trim().toLowerCase();
            if (!email) return;
            events.push({
              kind:    c.type === 'recurring' ? 'rebill' : 'sub_created',
              source:  'recharge',
              email:   email,
              ts:      c.created_at,
              payload: {
                amount:  c.total_price || null,
                product: (c.line_items && c.line_items[0]) ? c.line_items[0].title : null
              }
            });
          });
          if (charges.length < 250) break;
          page++;
        }
      })(),

      /* ── Recharge: failed charges ── */
      (async function() {
        if (!rcHeaders) return;
        var page = 1;
        while (page <= 5) {
          const url = RC_API_BASE + '/charges?limit=250&page=' + page +
            '&created_at_min=' + encodeURIComponent(since.toISOString()) +
            '&status=error';
          const r = await fetch(url, { headers: rcHeaders });
          const d = await r.json();
          const charges = d.charges || [];
          charges.forEach(function(c) {
            var email = ((c.customer && c.customer.email) || c.email || '').trim().toLowerCase();
            if (!email) return;
            events.push({
              kind:    'failed_charge',
              source:  'recharge',
              email:   email,
              ts:      c.created_at,
              payload: {
                amount:  c.total_price || null,
                product: (c.line_items && c.line_items[0]) ? c.line_items[0].title : null,
                reason:  c.error || c.error_type || null
              }
            });
          });
          if (charges.length < 250) break;
          page++;
        }
      })(),

      /* ── Recharge: new subscriptions (no email on sub — DB lookup by customer_id) ── */
      (async function() {
        if (!rcHeaders) return;
        var subs = [], page = 1;
        while (page <= 5) {
          const url = RC_API_BASE + '/subscriptions?limit=250&page=' + page +
            '&created_at_min=' + encodeURIComponent(since.toISOString());
          const r = await fetch(url, { headers: rcHeaders });
          const d = await r.json();
          const batch = d.subscriptions || [];
          batch.forEach(function(s) { if (s.customer_id) subs.push(s); });
          if (batch.length < 250) break;
          page++;
        }
        if (!subs.length) return;
        var ids = [...new Set(subs.map(function(s) { return String(s.customer_id); }))];
        var rows = await db.many('SELECT recharge_id, email FROM customers WHERE recharge_id = ANY($1)', [ids])
          .catch(function() { return []; });
        var emailMap = {};
        rows.forEach(function(r) { if (r.recharge_id) emailMap[String(r.recharge_id)] = r.email; });
        subs.forEach(function(s) {
          var email = (emailMap[String(s.customer_id)] || '').trim().toLowerCase();
          if (!email) return;
          events.push({
            kind:    'sub_created',
            source:  'recharge',
            email:   email,
            ts:      s.created_at,
            payload: { product: s.product_title || null, price: s.price || null }
          });
        });
      })(),

      /* ── Recharge: cancellations ── */
      (async function() {
        if (!rcHeaders) return;
        var subs = [], page = 1;
        while (page <= 5) {
          const url = RC_API_BASE + '/subscriptions?limit=250&page=' + page +
            '&updated_at_min=' + encodeURIComponent(since.toISOString()) +
            '&status=cancelled';
          const r = await fetch(url, { headers: rcHeaders });
          const d = await r.json();
          const batch = d.subscriptions || [];
          batch.forEach(function(s) {
            if (!s.customer_id) return;
            /* Require actual cancelled_at in window — updated_at can be recent
               even for subs cancelled months ago, which would cause false entries. */
            if (!s.cancelled_at || new Date(s.cancelled_at) < since) return;
            subs.push(s);
          });
          if (batch.length < 250) break;
          page++;
        }
        if (!subs.length) return;
        var ids = [...new Set(subs.map(function(s) { return String(s.customer_id); }))];
        var rows = await db.many('SELECT recharge_id, email FROM customers WHERE recharge_id = ANY($1)', [ids])
          .catch(function() { return []; });
        var emailMap = {};
        rows.forEach(function(r) { if (r.recharge_id) emailMap[String(r.recharge_id)] = r.email; });
        subs.forEach(function(s) {
          var email = (emailMap[String(s.customer_id)] || '').trim().toLowerCase();
          if (!email) return;
          events.push({
            kind:    'sub_cancelled',
            source:  'recharge',
            email:   email,
            ts:      s.cancelled_at,
            payload: { product: s.product_title || null, reason: s.cancellation_reason || null }
          });
        });
      })()

    ]);

    /* Sort newest-first; deduplicate (same email+kind within 5-minute bucket = same event) */
    events.sort(function(a, b) { return new Date(b.ts) - new Date(a.ts); });
    var seen = new Set();
    var unique = events.filter(function(e) {
      var bucket = Math.floor(new Date(e.ts).getTime() / 300000);
      var key    = (e.email || '') + '|' + e.kind + '|' + bucket;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    res.json({ events: unique.slice(0, 500), since: since.toISOString(), count: unique.length });
  } catch (err) {
    console.error('[admin/api/activity-feed]', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── POST /admin/api/magic-link — generate portal link for a customer (no email sent) ── */

router.post('/api/magic-link', async function(req, res) {
  try {
    const email = (req.body.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    /* Use local DB as the authoritative source — avoids slow external API round-trips */
    const dbRows = await db.many(`
      SELECT DISTINCT source FROM subscriptions
      WHERE LOWER(customer_email) = $1 AND status = 'ACTIVE'
    `, [email]);

    if (!dbRows || dbRows.length === 0) {
      return res.status(404).json({ error: 'No active subscription found for this customer' });
    }

    const sources    = dbRows.map(function(r) { return r.source; });
    const foundIn    = [...new Set(sources.map(function(s) { return s === 'cc' ? 'checkoutchamp' : s; }))];
    const primaryType = sources.includes('cc') ? 'checkoutchamp' : 'recharge';

    let ccMember = null;
    if (primaryType === 'checkoutchamp') {
      try {
        const CC_LOGIN_ID = process.env.CC_LOGIN_ID    || '';
        const CC_API_PASS = process.env.CC_API_PASSWORD || '';
        const CC_CLUB_ID  = process.env.CC_CLUB_ID      || '12';
        const today   = new Date();
        const endDate = (today.getMonth()+1).toString().padStart(2,'0') + '/' +
                        today.getDate().toString().padStart(2,'0') + '/' + today.getFullYear();
        const params  = new URLSearchParams({
          clubId: CC_CLUB_ID, loginId: CC_LOGIN_ID, password: CC_API_PASS,
          emailAddress: email, startDate: '01/01/2016', endDate, resultsPerPage: 200
        });
        const r = await fetch(CC_API_BASE + '/members/query/?' + params.toString(), { method: 'POST' });
        const d = await r.json();
        if (d.result === 'SUCCESS' && d.message && d.message.data && d.message.data.length > 0) {
          const records = d.message.data;
          records.sort(function(a, b) { return new Date(b.dateCreated) - new Date(a.dateCreated); });
          ccMember = records[0];
        }
      } catch(e) { console.error('[admin/magic-link] CC error:', e.message); }
    }

    let token;
    if (primaryType === 'checkoutchamp') {
      token = adminCreateToken({
        email, type: 'checkoutchamp', loginType: 'club',
        memberId:     ccMember ? (ccMember.memberId     || null) : null,
        clubUsername: ccMember ? (ccMember.clubUsername || null) : null,
        password:     ccMember ? (ccMember.clubPassword || null) : null,
        foundIn
      });
    } else {
      token = adminCreateToken({ email, type: primaryType, foundIn });
    }

    const link = ADMIN_BASE_URL + '/dashboard?token=' + token;
    res.json({ link, email, primaryType, foundIn });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/cc-billed-today
   Returns CC recurring orders already processed today —
   i.e. subscriptions whose nextBillDate was today but CC
   has billed them and moved the date forward.
   ────────────────────────────────────────────────── */
router.get('/api/debug/cc-billed-today', async function(req, res) {
  try {
    const now = new Date();
    const todayStr = (now.getMonth()+1).toString().padStart(2,'0') + '/' +
                     now.getDate().toString().padStart(2,'0') + '/' + now.getFullYear();

    let allOrders = [], page = 1;
    while (page <= 20) {
      const p = new URLSearchParams({
        loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
        startDate: todayStr, endDate: todayStr, resultsPerPage: 200, page, sortDir: -1
      });
      const r = await fetch(CC_API_BASE + '/order/query/?' + p.toString(), { method: 'POST' });
      const d = await r.json();
      const batch = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
      allOrders = allOrders.concat(batch);
      if (batch.length < 200) break;
      page++;
    }

    const recurring = allOrders.filter(function(o) {
      return o.orderType === 'RECURRING' || o.recurringFlag === '1' || o.parentOrderId;
    });

    const orders = recurring.map(function(o) {
      return {
        order_id:    o.orderId,
        email:       o.emailAddress,
        name:        (o.firstName || '') + ' ' + (o.lastName || ''),
        product:     o.productName,
        amount:      o.totalAmount,
        status:      o.orderStatus || o.status,
        billed_at:   o.dateCreated
      };
    });

    res.json({
      as_of:           now.toISOString(),
      date_queried:    todayStr,
      total_today:     allOrders.length,
      already_billed:  orders.length,
      still_pending:   28 - orders.length > 0 ? (28 - orders.length) + ' (estimated)' : 0,
      orders:          orders
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/billed-and-exported
   Finds CC recurring orders that were billed today (or within ?days=N)
   where next_bill_at has already moved to the future, and checks
   Shopify to see if the order was exported and fulfilled/shipped.
   ────────────────────────────────────────────────── */
router.get('/api/debug/billed-and-exported', async function(req, res) {
  try {
    const days    = Math.min(parseInt(req.query.days || '3', 10), 14);
    const now     = new Date();
    const endDate = (now.getMonth()+1).toString().padStart(2,'0') + '/' +
                    now.getDate().toString().padStart(2,'0') + '/' + now.getFullYear();
    const startD  = new Date(now); startD.setDate(startD.getDate() - days);
    const startDate = (startD.getMonth()+1).toString().padStart(2,'0') + '/' +
                      startD.getDate().toString().padStart(2,'0') + '/' + startD.getFullYear();

    /* ── 1. Pull CC recurring orders for window ── */
    let ccOrders = [], page = 1;
    while (page <= 20) {
      const p = new URLSearchParams({
        loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
        startDate, endDate, resultsPerPage: 200, page, sortDir: -1
      });
      const r = await fetch(CC_API_BASE + '/order/query/?' + p.toString(), { method: 'POST' });
      const d = await r.json();
      const batch = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
      const recurring = batch.filter(function(o) {
        return o.orderType === 'RECURRING' || o.recurringFlag === '1' || o.parentOrderId;
      });
      ccOrders = ccOrders.concat(recurring);
      if (batch.length < 200) break;
      page++;
    }

    /* ── 2. For each recurring order, get current nextBillDate from CC purchase/query ── */
    /* batch by purchaseId — CC purchase/query can filter by purchaseId */
    const purchaseIds = ccOrders.map(function(o) { return o.purchaseId; }).filter(Boolean);
    const purchaseMap = {};

    /* fetch in chunks of 10 to avoid overloading CC */
    for (var i = 0; i < purchaseIds.length; i++) {
      const pid = purchaseIds[i];
      try {
        const p = new URLSearchParams({
          loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
          purchaseId: pid, resultsPerPage: 1, page: 1
        });
        const r = await fetch(CC_API_BASE + '/purchase/query/?' + p.toString(), { method: 'POST' });
        const d = await r.json();
        const row = (d.result === 'SUCCESS' && d.message && d.message.data && d.message.data[0]);
        if (row) purchaseMap[pid] = { nextBillDate: row.nextBillDate, status: row.status, merchant: row.merchant };
      } catch(e) { /* skip */ }
    }

    /* ── 3. Check Shopify for each unique email ── */
    const uniqueEmails = Array.from(new Set(ccOrders.map(function(o) {
      return (o.emailAddress || '').trim().toLowerCase();
    }).filter(Boolean)));

    const shopifyMap = {};
    if (SHOPIFY_TOKEN) {
      for (var j = 0; j < uniqueEmails.length; j++) {
        const email = uniqueEmails[j];
        try {
          const url = 'https://' + SHOPIFY_STORE + '/admin/api/2024-01/orders.json' +
            '?email=' + encodeURIComponent(email) +
            '&status=any&limit=5&fields=id,name,created_at,fulfillment_status,fulfillments';
          const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN } });
          if (r.ok) {
            const d = await r.json();
            const orders = (d.orders || []);
            /* find most recent fulfilled order */
            let shipment = null;
            for (const ord of orders) {
              if (ord.fulfillments && ord.fulfillments.length) {
                const f = ord.fulfillments[0];
                shipment = {
                  order_name:       ord.name,
                  order_created_at: ord.created_at,
                  fulfillment_status: ord.fulfillment_status,
                  shipment_status:  f.shipment_status || f.status || null,
                  tracking_company: f.tracking_company || null,
                  tracking_url:     (f.tracking_urls && f.tracking_urls[0]) || null
                };
                break;
              }
            }
            shopifyMap[email] = shipment;
          }
        } catch(e) { /* skip */ }
      }
    }

    /* ── 4. Build result rows ── */
    const rows = ccOrders.map(function(o) {
      const email   = (o.emailAddress || '').trim().toLowerCase();
      const pid     = o.purchaseId;
      const sub     = purchaseMap[pid] || {};
      const shopify = shopifyMap[email] || null;
      return {
        cc_order_id:    o.orderId,
        purchase_id:    pid,
        email:          o.emailAddress,
        product:        o.productName,
        amount:         o.totalAmount,
        cc_order_status: o.orderStatus || o.status,
        cc_order_date:  o.dateCreated,
        subscription: {
          current_next_bill_date: sub.nextBillDate || null,
          status:   sub.status   || null,
          merchant: sub.merchant || null
        },
        shopify: shopify
          ? {
              exported:    true,
              order_name:  shopify.order_name,
              order_date:  shopify.order_created_at,
              fulfillment: shopify.fulfillment_status,
              shipment:    shopify.shipment_status,
              carrier:     shopify.tracking_company,
              tracking_url: shopify.tracking_url
            }
          : { exported: SHOPIFY_TOKEN ? false : 'SHOPIFY_TOKEN_NOT_SET' }
      };
    });

    const exported   = rows.filter(function(r) { return r.shopify && r.shopify.exported === true; });
    const notExported = rows.filter(function(r) { return r.shopify && r.shopify.exported === false; });

    res.json({
      as_of:    now.toISOString(),
      window:   startDate + ' → ' + endDate,
      summary: {
        cc_recurring_orders: ccOrders.length,
        exported_to_shopify: exported.length,
        not_in_shopify:      notExported.length,
        shopify_configured:  !!SHOPIFY_TOKEN
      },
      exported_and_shipped: exported.filter(function(r) { return r.shopify.shipment && r.shopify.shipment !== 'label_printed'; }),
      exported_pending:     exported.filter(function(r) { return !r.shopify.shipment || r.shopify.shipment === 'label_printed'; }),
      not_exported:         notExported,
      all:                  rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/today-revenue — intraday hourly breakdown (live CC + Recharge) ── */
router.get('/api/today-revenue', async function(req, res) {
  try {
    const now         = new Date();
    const todayStart  = amsMidnightUTC(now);
    const todayEnd    = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    const yesterStart = new Date(todayStart.getTime() - 24 * 3600 * 1000);

    /* CC date strings for the query window */
    const todayAms  = amsDateStr(now);
    const prevAms   = amsDateStr(new Date(todayStart.getTime() - 1));
    const [ty, tm, td] = todayAms.split('-');
    const [py, pm, pd] = prevAms.split('-');
    const ccStart   = pm + '/' + pd + '/' + py;
    const ccEnd     = tm + '/' + td + '/' + ty;

    /* ── Fetch CC recurring transactions + Recharge charges + yesterday DB + Whop DB in parallel ── */
    const [ccRows, rcRows, yesterday, whopRow, nativeSubEmails] = await Promise.all([

      /* CC live: today's RECURRING charged transactions */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return [];
        var rows = [], page = 1;
        while (page <= 10) {
          const p = new URLSearchParams({
            loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
            startDate: ccStart, endDate: ccEnd,
            billType: 'RECURRING', txnType: 'SALE', responseType: 'SUCCESS',
            resultsPerPage: 200, page: page
          });
          const r = await fetch(CC_API_BASE + '/transactions/query/?' + p.toString(), { method: 'POST' });
          const d = await r.json();
          const txns = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
          txns.forEach(function(t) {
            var ts = ccParseDate(t.dateCreated || '');
            if (!ts || ts < todayStart || ts >= todayEnd) return;
            rows.push({ ts: ts, price_cents: Math.round(parseFloat(t.amount || t.totalAmount || 0) * 100), source: 'cc' });
          });
          if (txns.length < 200) break;
          page++;
        }
        return rows;
      })(),

      /* Recharge DB: billed today + scheduled today (same logic as today-orders).
         Use last_billed_at as chart timestamp when available; fall back to next_bill_at
         (midnight UTC = 02:00 Amsterdam) for subscriptions not yet confirmed billed. */
      (async function() {
        var rows = [];
        try {
          var rc = await db.many(`
            SELECT LOWER(customer_email) AS email,
                   COALESCE(last_billed_at, next_bill_at) AS ts, price_cents,
                   (last_billed_at IS NOT NULL AND last_billed_at >= $1) AS is_billed
            FROM   subscriptions
            WHERE  source = 'recharge'
              AND  status = 'ACTIVE'
              AND  (
                (last_billed_at >= $1 AND last_billed_at < $2)
                OR (next_bill_at >= $1 AND next_bill_at < $2
                    AND (last_billed_at IS NULL OR last_billed_at < $1))
              )
          `, [todayStart, todayEnd]);
          rc.forEach(function(r) {
            rows.push({ ts: new Date(r.ts), price_cents: r.price_cents || 0, source: 'recharge',
                        confirmed: r.is_billed, _email: r.email });
          });
        } catch (e) { console.error('[today-revenue] RC DB error:', e.message); }
        return rows;
      })(),

      /* Yesterday totals from DB for delta */
      db.one(`SELECT COALESCE(SUM(amount_cents),0)::bigint AS revenue_cents, COUNT(*)::int AS orders
              FROM orders WHERE created_at >= $1 AND created_at < $2`, [yesterStart, todayStart]),

      /* Whop: subscriptions renewing today (from local DB) */
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS wh_cents,
                     COUNT(*)::int AS wh_orders
              FROM subscriptions
              WHERE source = 'whop'
                AND status IN ('ACTIVE','TRIALING')
                AND next_bill_at >= $1 AND next_bill_at < $2`,
        [todayStart, todayEnd])
        .catch(function() { return { wh_cents: '0', wh_orders: 0 }; }),

      /* Shopify: subscription_contract_checkout_one emails billed today.
         These are native Shopify recurring orders whose next_bill_at in DB may already
         point to next month (Recharge advanced it after billing), falling outside
         today's window and getting missed by the rcRows DB query above. */
      (async function() {
        var emails = new Set();
        if (!SHOPIFY_TOKEN) return emails;
        try {
          var url = 'https://' + SHOPIFY_STORE + '/admin/api/2024-01/orders.json' +
            '?status=any&limit=250&fields=email,source_name' +
            '&created_at_min=' + encodeURIComponent(todayStart.toISOString());
          while (url) {
            var r = await fetch(url, { headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN } });
            var link = r.headers.get('link') || '';
            var d = await r.json();
            (d.orders || []).forEach(function(o) {
              if (o.email && o.source_name === 'subscription_contract_checkout_one')
                emails.add(o.email.toLowerCase().trim());
            });
            var next = link.match(/<([^>]+)>;\s*rel="next"/);
            url = next ? next[1] : null;
          }
        } catch (e) { console.error('[today-revenue] Shopify native-sub fetch failed:', e.message); }
        return emails;
      })()
    ]);

    /* Add native-sub rows missing from the DB-window rcRows query */
    var rcEmailsForDedup = new Set(rcRows.map(function(r) { return r._email; }).filter(Boolean));
    var missingNative = [...nativeSubEmails].filter(function(e) { return !rcEmailsForDedup.has(e); });
    if (missingNative.length > 0) {
      try {
        var nativeDbRows = await db.many(`
          SELECT DISTINCT ON (s.customer_email) s.customer_email, s.price_cents
          FROM   subscriptions s
          WHERE  s.source = 'recharge' AND s.status = 'ACTIVE'
            AND  LOWER(s.customer_email) = ANY($1)
          ORDER  BY s.customer_email, s.next_bill_at DESC NULLS LAST
        `, [missingNative]);
        var foundNativeEmails = new Set(nativeDbRows.map(function(r) { return r.customer_email; }));
        nativeDbRows.forEach(function(s) {
          rcRows.push({ ts: now, price_cents: s.price_cents || 0, source: 'recharge', confirmed: true, _email: s.customer_email });
        });
        /* Synthetic for any completely absent from DB */
        missingNative.filter(function(e) { return !foundNativeEmails.has(e); }).forEach(function(e) {
          rcRows.push({ ts: now, price_cents: 0, source: 'recharge', confirmed: true, _email: e });
        });
        if (missingNative.length > 0)
          console.log('[today-revenue] native-sub added', missingNative.length, 'RC rows (', nativeDbRows.length, 'from DB )');
      } catch (e) { console.error('[today-revenue] native-sub DB lookup failed:', e.message); }
    }

    /* Aggregate by Amsterdam hour.
       Revenue: only confirmed CC + confirmed RC billed (not scheduled).
       Order count: all rows including scheduled. */
    const hourMap = {};
    var ccCents = 0, rcCents = 0;
    ccRows.concat(rcRows).forEach(function(o) {
      var h = parseInt(o.ts.toLocaleString('en-US', { timeZone: 'Europe/Amsterdam', hour: 'numeric', hour12: false }));
      if (!hourMap[h]) hourMap[h] = { revenue_cents: 0, orders: 0 };
      hourMap[h].orders++;
      if (o.source === 'cc') {
        hourMap[h].revenue_cents += o.price_cents;
        ccCents += o.price_cents;
      } else if (o.confirmed) {
        hourMap[h].revenue_cents += o.price_cents;
        rcCents += o.price_cents;
      }
    });

    const full24 = Array.from({ length: 24 }, function(_, h) {
      return { hour: h, revenue_cents: (hourMap[h] || {}).revenue_cents || 0, orders: (hourMap[h] || {}).orders || 0 };
    });
    const whCents     = Number(whopRow.wh_cents)  || 0;
    const whOrders    = Number(whopRow.wh_orders) || 0;
    const totalOrders = ccRows.length + rcRows.length + whOrders;
    const totalCents  = ccCents + rcCents + whCents;

    res.json({
      hourly: full24,
      totals: {
        revenue_cents: totalCents,
        orders:        totalOrders,
        cc_orders:     ccRows.length,
        cc_cents:      ccCents,
        rc_orders:     rcRows.length,
        rc_cents:      rcCents,
        wh_orders:     whOrders,
        wh_cents:      whCents,
        recurring:     totalOrders,
        new_orders:    0
      },
      yesterday: { revenue_cents: Number(yesterday.revenue_cents), orders: yesterday.orders }
    });
  } catch (err) {
    console.error('[admin/api/today-revenue]', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/today-breakdown
   Side-by-side comparison: DB subscriptions scheduled for today
   vs CC API recurring orders charged today.
   Shows what's in DB only, CC only, and both.
   ────────────────────────────────────────────────── */
router.get('/api/debug/today-breakdown', async function(req, res) {
  try {
    const now        = new Date();
    const todayAmsD  = amsDateStr(now);
    const todayStart = amsMidnightUTC(now);
    const todayEnd   = new Date(todayStart.getTime() + 24 * 3600 * 1000);
    const [dby, dbm, dbd] = todayAmsD.split('-');
    const todayStr   = dbm + '/' + dbd + '/' + dby;

    /* ── 1. DB: all sources scheduled/billed today ── */
    const dbRows = await db.many(`
      SELECT s.source, s.native_id, s.customer_email, s.product,
             s.price_cents, s.status, s.next_bill_at, s.last_billed_at, s.last_synced_at
      FROM subscriptions s
      WHERE s.status = 'ACTIVE'
      AND (
        (s.next_bill_at  >= $1 AND s.next_bill_at  < $2)
        OR (s.last_billed_at >= $1 AND s.last_billed_at < $2)
      )
      AND NOT (s.source = 'cc' AND s.raw->>'merchant' ILIKE '%paypal%')
      AND NOT (s.source = 'cc' AND COALESCE(NULLIF(TRIM(s.raw->>'merchant'), ''), '') = '')
      ORDER BY s.source, s.price_cents DESC
    `, [todayStart, todayEnd]);

    const dbByEmail = {};
    dbRows.forEach(function(r) {
      const key = (r.customer_email || '').toLowerCase();
      if (!dbByEmail[key]) dbByEmail[key] = [];
      dbByEmail[key].push(r);
    });

    /* ── 2. CC API: today's recurring orders ── */
    let ccOrders = [];
    let ccError  = null;
    if (process.env.CC_LOGIN_ID && process.env.CC_API_PASSWORD) {
      try {
        let page = 1;
        while (page <= 10) {
          const p = new URLSearchParams({
            loginId: process.env.CC_LOGIN_ID, password: process.env.CC_API_PASSWORD,
            startDate: todayStr, endDate: todayStr, resultsPerPage: 200, page: page, sortDir: -1
          });
          const r = await fetch(CC_API_BASE + '/order/query/?' + p.toString(), { method: 'POST' });
          const d = await r.json();
          const batch = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
          ccOrders = ccOrders.concat(batch);
          if (batch.length < 200) break;
          page++;
        }
      } catch (e) { ccError = e.message; }
    }

    const ccAllToday   = ccOrders.length;
    const ccRecurring  = ccOrders.filter(function(o) {
      return o.orderType === 'RECURRING' || o.recurringFlag === '1' || o.parentOrderId;
    });
    const ccNewSales   = ccOrders.filter(function(o) {
      return o.orderType !== 'RECURRING' && !o.recurringFlag && !o.parentOrderId;
    });

    const ccByEmail = {};
    ccRecurring.forEach(function(o) {
      const key = (o.emailAddress || '').trim().toLowerCase();
      if (!ccByEmail[key]) ccByEmail[key] = [];
      ccByEmail[key].push({
        orderId:     o.orderId,
        product:     o.productName,
        total:       o.totalAmount,
        status:      o.orderStatus || o.status,
        orderType:   o.orderType,
        dateCreated: o.dateCreated
      });
    });

    /* ── 3. Cross-reference CC-source DB rows vs CC API ── */
    const dbCcRows      = dbRows.filter(function(r){ return r.source === 'cc'; });
    const dbOtherRows   = dbRows.filter(function(r){ return r.source !== 'cc'; });

    const ccEmailsInDb  = new Set(dbCcRows.map(function(r){ return (r.customer_email||'').toLowerCase(); }));
    const ccEmailsInApi = new Set(Object.keys(ccByEmail));
    const allCcEmails   = new Set([...ccEmailsInDb, ...ccEmailsInApi]);

    const cc_billed_and_in_db   = [];
    const cc_scheduled_not_yet  = [];
    const cc_billed_missing_db  = [];

    allCcEmails.forEach(function(email) {
      const inDb  = ccEmailsInDb.has(email);
      const inApi = ccEmailsInApi.has(email);
      const entry = {
        email:     email,
        db:        dbByEmail[email] || null,
        cc_orders: ccByEmail[email] || null
      };
      if (inDb && inApi)  cc_billed_and_in_db.push(entry);
      else if (inDb)      cc_scheduled_not_yet.push(entry);   /* in DB, CC hasn't charged yet */
      else                cc_billed_missing_db.push(entry);   /* CC charged, not in DB */
    });

    /* Group non-CC (Recharge) separately — CC API doesn't cover them */
    const otherBySource = {};
    dbOtherRows.forEach(function(r) {
      if (!otherBySource[r.source]) otherBySource[r.source] = [];
      otherBySource[r.source].push(r);
    });

    res.json({
      as_of:     now.toISOString(),
      today_utc: todayStart.toISOString().slice(0, 10),
      note: 'CC cross-reference compares CC-source DB subscriptions vs CC order/query API. Recharge is listed separately — it is not in the CC API.',
      summary: {
        db_total:              dbRows.length,
        db_cc_source:          dbCcRows.length,
        db_recharge:           (otherBySource.recharge||[]).length,
        cc_api_all_today:      ccAllToday,
        cc_api_recurring:      ccRecurring.length,
        cc_api_new_sales:      ccNewSales.length,
        cc_billed_and_in_db:   cc_billed_and_in_db.length,
        cc_scheduled_not_billed_yet: cc_scheduled_not_yet.length,
        cc_billed_missing_from_db:   cc_billed_missing_db.length,
        cc_api_error:          ccError
      },
      cc_billed_and_in_db:          cc_billed_and_in_db,
      cc_scheduled_not_billed_yet:  cc_scheduled_not_yet,
      cc_billed_missing_from_db:    cc_billed_missing_db,
      recharge_scheduled_today:     otherBySource.recharge || []
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET /admin/api/debug/shopify-rc-diff
   Compares today's Shopify orders against Recharge subscriptions in DB.
   Shows: in Shopify but missing from DB, in DB but no Shopify order, and both.
   Helps explain count discrepancies between Shopify (72) and our DB (67).       */
router.get('/api/debug/shopify-rc-diff', async function(req, res) {
  try {
    const now        = new Date();
    const todayStart = amsMidnightUTC(now);
    const todayEnd   = new Date(todayStart.getTime() + 24 * 3600 * 1000);

    /* 1. All Shopify orders created today */
    var shopifyOrders = [];
    if (SHOPIFY_TOKEN) {
      var url = 'https://' + SHOPIFY_STORE + '/admin/api/2024-01/orders.json' +
        '?status=any&limit=250' +
        '&fields=id,name,email,created_at,source_name,tags,note_attributes,financial_status' +
        '&created_at_min=' + encodeURIComponent(todayStart.toISOString());
      while (url) {
        var r = await fetch(url, { headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN } });
        var link = r.headers.get('link') || '';
        var d = await r.json();
        shopifyOrders = shopifyOrders.concat(d.orders || []);
        var next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = next ? next[1] : null;
      }
    }

    /* 2. Our Recharge subscriptions for today (DB) */
    var dbSubs = await db.many(`
      SELECT customer_email, product, price_cents, next_bill_at, last_billed_at, last_synced_at
      FROM subscriptions
      WHERE source = 'recharge' AND status = 'ACTIVE'
        AND (
          (last_billed_at >= $1 AND last_billed_at < $2)
          OR (next_bill_at >= $1 AND next_bill_at < $2 AND (last_billed_at IS NULL OR last_billed_at < $1))
        )
      ORDER BY customer_email
    `, [todayStart, todayEnd]);

    /* 3. Build email sets */
    var shopifyEmailMap = {};
    shopifyOrders.forEach(function(o) {
      var e = (o.email || '').toLowerCase().trim();
      if (!e) return;
      if (!shopifyEmailMap[e]) shopifyEmailMap[e] = [];
      shopifyEmailMap[e].push({ id: o.id, name: o.name, source_name: o.source_name,
        tags: o.tags, financial_status: o.financial_status, created_at: o.created_at });
    });

    var dbEmailMap = {};
    dbSubs.forEach(function(s) {
      var e = (s.customer_email || '').toLowerCase();
      if (!e) return;
      if (!dbEmailMap[e]) dbEmailMap[e] = [];
      dbEmailMap[e].push(s);
    });

    var shopifyEmails = new Set(Object.keys(shopifyEmailMap));
    var dbEmails      = new Set(Object.keys(dbEmailMap));

    var inShopifyNotDb = [...shopifyEmails].filter(function(e) { return !dbEmails.has(e); });
    var inDbNotShopify = [...dbEmails].filter(function(e) { return !shopifyEmails.has(e); });
    var inBoth         = [...shopifyEmails].filter(function(e) { return dbEmails.has(e); });

    res.json({
      as_of: now.toISOString(),
      summary: {
        shopify_orders_today_total:    shopifyOrders.length,
        shopify_unique_emails:         shopifyEmails.size,
        db_recharge_rows:              dbSubs.length,
        db_unique_emails:              dbEmails.size,
        in_shopify_missing_from_db:    inShopifyNotDb.length,
        in_db_missing_from_shopify:    inDbNotShopify.length,
        confirmed_in_both:             inBoth.length
      },
      in_shopify_missing_from_db: inShopifyNotDb.map(function(e) {
        return { email: e, shopify_orders: shopifyEmailMap[e] };
      }),
      in_db_missing_from_shopify: inDbNotShopify.map(function(e) {
        return { email: e, db_subs: dbEmailMap[e] };
      })
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── /admin/api/debug/whop-status — whop sync diagnostics ── */

/* ════════════════════════════════════════════════════
   INTEGRATIONS
   ════════════════════════════════════════════════════ */

const INTEGRATIONS_META = [
  {
    name:        'checkoutchamp',
    label:       'CheckoutChamp',
    description: 'Subscription billing & order management platform',
    color:       '#2563eb',
    fields: [
      { key: 'CC_LOGIN_ID',     label: 'Login ID',    type: 'text',     placeholder: 'e.g. TGPAPI' },
      { key: 'CC_API_PASSWORD', label: 'API Password', type: 'password', placeholder: 'Your CC password' },
      { key: 'CC_CLUB_ID',      label: 'Club ID',      type: 'text',     placeholder: 'e.g. 12' },
    ],
  },
  {
    name:        'recharge',
    label:       'Recharge',
    description: 'Subscription management & recurring billing',
    color:       '#16a34a',
    fields: [
      { key: 'RECHARGE_API_KEY', label: 'API Key', type: 'password', placeholder: 'sk_1x_...' },
    ],
  },
  {
    name:        'shopify',
    label:       'Shopify',
    description: 'E-commerce storefront & product catalog',
    color:       '#5a8a3c',
    fields: [
      { key: 'SHOPIFY_STORE',        label: 'Store Domain',  type: 'text',     placeholder: 'yourstore.myshopify.com' },
      { key: 'SHOPIFY_ACCESS_TOKEN', label: 'Access Token',  type: 'password', placeholder: 'shpat_...' },
    ],
  },
  {
    name:        'klaviyo',
    label:       'Klaviyo',
    description: 'Email marketing & automation',
    color:       '#9333ea',
    fields: [
      { key: 'KLAVIYO_API_KEY', label: 'Private API Key', type: 'password', placeholder: 'pk_...' },
    ],
  },
  {
    name:        'whop',
    label:       'Whop',
    description: 'Membership, digital products & payments',
    color:       '#000000',
    fields: [
      { key: 'WHOP_API_KEY', label: 'API Key', type: 'password', placeholder: 'Your Whop API key' },
    ],
  },
];

/* ── Resolve credential: DB first, then env var ───── */
async function resolveCredentials(integrationName) {
  var meta = INTEGRATIONS_META.find(function(i) { return i.name === integrationName; });
  if (!meta) return {};
  var dbCreds = {};
  try {
    var row = await db.one('SELECT creds FROM integration_credentials WHERE name = $1', [integrationName]);
    if (row && row.creds) dbCreds = row.creds;
  } catch(e) {}
  var result = {};
  meta.fields.forEach(function(f) {
    result[f.key] = dbCreds[f.key] || process.env[f.key] || '';
  });
  return result;
}

/* ── GET /admin/api/integrations ─────────────────── */
router.get('/api/integrations', auth.requireAdmin, async function(req, res) {
  var stateMap = {}, credMap = {};
  try {
    var [stateRows, credRows] = await Promise.all([
      db.many('SELECT name, enabled, updated_at FROM integrations'),
      db.many('SELECT name, creds FROM integration_credentials'),
    ]);
    stateRows.forEach(function(r) { stateMap[r.name] = r; });
    credRows.forEach(function(r)  { credMap[r.name]  = r.creds || {}; });
  } catch(e) {}

  var result = INTEGRATIONS_META.map(function(int) {
    var dbCreds    = credMap[int.name] || {};
    var stateRow   = stateMap[int.name];
    var configured = int.fields.some(function(f) { return !!(dbCreds[f.key] || process.env[f.key]); });
    var enabled    = stateRow != null ? stateRow.enabled : configured;

    /* Build field list with masked current value */
    var fieldStatus = int.fields.map(function(f) {
      var val = dbCreds[f.key] || process.env[f.key] || '';
      var source = dbCreds[f.key] ? 'db' : (process.env[f.key] ? 'env' : 'none');
      var masked = val ? val.substring(0, 4) + '···' + val.slice(-4) : '';
      return { key: f.key, label: f.label, type: f.type, placeholder: f.placeholder, masked: masked, source: source };
    });

    return {
      name:        int.name,
      label:       int.label,
      description: int.description,
      color:       int.color,
      configured:  configured,
      enabled:     enabled,
      fields:      fieldStatus,
      lastUpdated: stateRow ? stateRow.updated_at : null,
    };
  });

  res.json({ integrations: result });
});

/* ── POST /admin/api/integrations/:name/toggle ───── */
router.post('/api/integrations/:name/toggle', auth.requireAdmin, async function(req, res) {
  var name    = req.params.name;
  var enabled = req.body.enabled !== false;
  try {
    await db.query(
      `INSERT INTO integrations (name, enabled, updated_at) VALUES ($1, $2, NOW())
       ON CONFLICT (name) DO UPDATE SET enabled = $2, updated_at = NOW()`,
      [name, enabled]
    );
    res.json({ ok: true, name: name, enabled: enabled });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ── POST /admin/api/integrations/:name/credentials ─ */
router.post('/api/integrations/:name/credentials', auth.requireAdmin, async function(req, res) {
  var name = req.params.name;
  var meta = INTEGRATIONS_META.find(function(i) { return i.name === name; });
  if (!meta) return res.status(400).json({ error: 'Unknown integration' });

  var incoming = req.body.creds || {};
  /* Fetch existing DB creds so we don't wipe fields the user didn't touch */
  var existing = {};
  try {
    var row = await db.one('SELECT creds FROM integration_credentials WHERE name = $1', [name]);
    if (row && row.creds) existing = row.creds;
  } catch(e) {}

  var merged = Object.assign({}, existing);
  meta.fields.forEach(function(f) {
    var v = (incoming[f.key] || '').trim();
    if (v) merged[f.key] = v;       /* only overwrite if user typed something */
  });

  try {
    await db.query(
      `INSERT INTO integration_credentials (name, creds, updated_at) VALUES ($1, $2, NOW())
       ON CONFLICT (name) DO UPDATE SET creds = $2, updated_at = NOW()`,
      [name, JSON.stringify(merged)]
    );
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/* ── POST /admin/api/integrations/:name/test ──────── */
router.post('/api/integrations/:name/test', auth.requireAdmin, async function(req, res) {
  var name = req.params.name;
  try {
    /* Resolve credentials: DB first, then env vars */
    var creds = await resolveCredentials(name);
    var ok = false, message = '';

    if (name === 'recharge') {
      var key = creds['RECHARGE_API_KEY'];
      if (!key) return res.json({ ok: false, message: 'RECHARGE_API_KEY not configured' });
      var r = await fetch('https://api.rechargeapps.com/shop', {
        headers: { 'X-Recharge-Access-Token': key, 'X-Recharge-Version': '2021-11' }
      });
      if (r.ok) {
        var d = await r.json();
        ok = true;
        message = 'Connected — ' + ((d.shop && d.shop.name) || 'Recharge API OK');
      } else {
        message = 'API returned HTTP ' + r.status;
      }

    } else if (name === 'checkoutchamp') {
      var loginId = creds['CC_LOGIN_ID'], pw = creds['CC_API_PASSWORD'];
      if (!loginId || !pw) return res.json({ ok: false, message: 'Login ID and API Password are required' });
      var r2 = await fetch(CC_API_BASE + '/purchase/query/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ loginId: loginId, password: pw, startDate: '01/01/2026', endDate: '01/01/2026', resultsPerPage: 1, page: 1 })
      });
      var d2 = await r2.json();
      if (d2.result === 'SUCCESS' || Array.isArray(d2.data)) {
        ok = true; message = 'Connected — CheckoutChamp API OK';
      } else {
        message = d2.message || ('API returned HTTP ' + r2.status);
      }

    } else if (name === 'shopify') {
      var store = creds['SHOPIFY_STORE'] || SHOPIFY_STORE;
      var token = creds['SHOPIFY_ACCESS_TOKEN'] || SHOPIFY_TOKEN;
      if (!token) return res.json({ ok: false, message: 'Access Token not configured' });
      var r3 = await fetch('https://' + store + '/admin/api/2024-01/shop.json', {
        headers: { 'X-Shopify-Access-Token': token }
      });
      if (r3.ok) {
        var d3 = await r3.json();
        ok = true; message = 'Connected — ' + ((d3.shop && d3.shop.name) || 'Shopify API OK');
      } else {
        message = 'API returned HTTP ' + r3.status;
      }

    } else if (name === 'klaviyo') {
      var kkey = creds['KLAVIYO_API_KEY'];
      if (!kkey) return res.json({ ok: false, message: 'KLAVIYO_API_KEY not configured' });
      var r4 = await fetch('https://a.klaviyo.com/api/accounts/', {
        headers: { 'Authorization': 'Klaviyo-API-Key ' + kkey, 'revision': '2024-10-15', 'accept': 'application/json' }
      });
      if (r4.ok) {
        ok = true; message = 'Connected — Klaviyo API OK';
      } else {
        message = 'API returned HTTP ' + r4.status;
      }

    } else if (name === 'whop') {
      var wkey = creds['WHOP_API_KEY'];
      if (!wkey) return res.json({ ok: false, message: 'WHOP_API_KEY not configured' });
      var whopH = { 'Authorization': 'Bearer ' + wkey, 'accept': 'application/json' };
      /* Test 1: products (basic key check) */
      var r5 = await fetch('https://api.whop.com/api/v2/products?per_page=1', { headers: whopH });
      if (!r5.ok) {
        message = r5.status === 401
          ? 'Invalid API key — check your Whop API key and try again'
          : 'API returned HTTP ' + r5.status;
      } else {
        /* Test 2: memberships (required for sync) */
        var r5b = await fetch('https://api.whop.com/api/v2/memberships?per_page=1&status=active', { headers: whopH });
        if (r5b.ok) {
          var d5b = await r5b.json();
          var mCount = (d5b.pagination && d5b.pagination.total_count) || 0;
          ok = true; message = 'Connected — ' + mCount + ' active membership' + (mCount !== 1 ? 's' : '') + ' readable';
        } else if (r5b.status === 401 || r5b.status === 403) {
          message = 'API key cannot read memberships (HTTP ' + r5b.status + ') — in your Whop dashboard, regenerate the API key and enable Memberships read permission';
        } else {
          message = 'Products OK but memberships returned HTTP ' + r5b.status;
        }
      }

    } else {
      return res.json({ ok: false, message: 'Unknown integration: ' + name });
    }

    res.json({ ok: ok, message: message });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

module.exports = router;

/* ════════════════════════════════════════════════════
   admin/routes.js — Express router for /admin/*
   Mounted from server.js with: app.use('/admin', adminRouter)
   ════════════════════════════════════════════════════ */

const express = require('express');
const path    = require('path');
const db      = require('./db');
const auth    = require('./auth');
const sync    = require('./sync');

const router = express.Router();

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
  res.sendFile(path.join(__dirname, '..', 'public', 'admin', 'login.html'));
});

router.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, '..', 'public', 'admin', 'index.html'));
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
    /* Use UTC midnight so counts match PostgreSQL UTC-stored timestamps */
    const todayStart = new Date(now); todayStart.setUTCHours(0, 0, 0, 0);
    const periodDays = Math.min(Math.max(parseInt(req.query.days || '30', 10), 1), 365);
    const periodAgo  = new Date(todayStart); periodAgo.setUTCDate(periodAgo.getUTCDate() - periodDays);
    const tomorrowEnd   = new Date(todayStart); tomorrowEnd.setUTCDate(tomorrowEnd.getUTCDate() + 2);
    const tomorrowStart = new Date(todayStart); tomorrowStart.setUTCDate(tomorrowStart.getUTCDate() + 1);
    const weekStart    = new Date(todayStart); weekStart.setUTCDate(weekStart.getUTCDate() - 7);
    const twoWeeksAgo  = new Date(todayStart); twoWeeksAgo.setUTCDate(twoWeeksAgo.getUTCDate() - 14);

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
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW()`),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW()`),
      db.one(`SELECT COALESCE(SUM(amount_cents),0)::bigint AS cents, COUNT(*)::int AS n FROM orders WHERE created_at >= $1`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1`, [todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 AND started_at < $2`,
             [new Date(todayStart.getTime() - 24*60*60*1000), todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 AND cancelled_at < $2`,
             [new Date(todayStart.getTime() - 24*60*60*1000), todayStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1`, [weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE started_at >= $1 AND started_at < $2`, [twoWeeksAgo, weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1`, [weekStart]),
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE cancelled_at >= $1 AND cancelled_at < $2`, [twoWeeksAgo, weekStart]),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents, COUNT(*)::int AS n
              FROM subscriptions
              WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2`,
             [tomorrowStart, tomorrowEnd]),
      db.many(`SELECT source, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() GROUP BY source ORDER BY n DESC`),
      db.many(`SELECT TO_CHAR(DATE(created_at), 'YYYY-MM-DD') AS day, COALESCE(SUM(amount_cents),0)::bigint AS cents
               FROM orders WHERE created_at >= $1
               GROUP BY TO_CHAR(DATE(created_at), 'YYYY-MM-DD') ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT TO_CHAR(DATE(next_bill_at), 'YYYY-MM-DD') AS day, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS cents
               FROM subscriptions
               WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2
               GROUP BY TO_CHAR(DATE(next_bill_at), 'YYYY-MM-DD') ORDER BY day ASC`,
              [todayStart, new Date(todayStart.getTime() + 8 * 24 * 60 * 60 * 1000)]),
      db.many(`SELECT TO_CHAR(DATE(started_at), 'YYYY-MM-DD') AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE started_at >= $1
               GROUP BY TO_CHAR(DATE(started_at), 'YYYY-MM-DD'), source ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT TO_CHAR(DATE(cancelled_at), 'YYYY-MM-DD') AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE cancelled_at >= $1
               GROUP BY TO_CHAR(DATE(cancelled_at), 'YYYY-MM-DD'), source ORDER BY day ASC`, [periodAgo]),
      db.many(`SELECT id, customer_email, product, source, price_cents, cancelled_at, cancel_reason
               FROM subscriptions WHERE cancelled_at IS NOT NULL
               ORDER BY cancelled_at DESC LIMIT 10`),
      db.many(`SELECT kind, source, email, payload, ts FROM events ORDER BY ts DESC LIMIT 50`),
      db.many(`SELECT product, source, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions WHERE status = 'ACTIVE' AND next_bill_at >= NOW() AND product IS NOT NULL
               GROUP BY product, source ORDER BY n DESC LIMIT 15`),
      db.many(`SELECT customer_email, product, source, price_cents, next_bill_at
               FROM subscriptions
               WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2
               ORDER BY source, price_cents DESC`,
              [tomorrowStart, tomorrowEnd]),
      db.many(`SELECT COALESCE(raw->>'merchant', 'Unknown') AS gateway,
               COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions
               WHERE source = 'cc' AND status = 'ACTIVE' AND next_bill_at >= NOW()
               GROUP BY raw->>'merchant' ORDER BY n DESC`)
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
      recentEvents:        recentEvents,
      tomorrowRebillsList: tomorrowRebillsList,
      gatewayBreakdown:    gatewayBreakdown
    });
  } catch (err) {
    console.error('[admin/api/overview]', err);
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
    const rows = await db.many('SELECT * FROM sync_state');
    res.json({ sync: rows });
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

module.exports = router;

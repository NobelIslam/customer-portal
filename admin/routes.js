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
const SUBI_API_BASE  = 'https://api.subi.co/public/v1.0';
const SHOPIFY_STORE  = process.env.SHOPIFY_STORE  || 'bigferverv.myshopify.com';
const SHOPIFY_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN || '';

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

    /* Exclude PayPal Commerce from all subscription metrics */
    const NO_PAYPAL = `AND NOT (source = 'cc' AND raw->>'merchant' ILIKE '%paypal%')`;

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
      db.many(`SELECT id, customer_email, product, source, price_cents, cancelled_at, cancel_reason
               FROM subscriptions WHERE cancelled_at IS NOT NULL ${NO_PAYPAL}
               ORDER BY cancelled_at DESC LIMIT 10`),
      db.many(`SELECT kind, source, email, payload, ts FROM events ORDER BY ts DESC LIMIT 50`),
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
               GROUP BY COALESCE(NULLIF(TRIM(raw->>'merchant'), ''), 'Unknown / Not Set') ORDER BY n DESC`)
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

/* ── GET /admin/api/today-orders — subscriptions due today ── */

router.get('/api/today-orders', async function(req, res) {
  try {
    const now        = new Date();
    const todayStart = new Date(now); todayStart.setUTCHours(0, 0, 0, 0);
    const todayEnd   = new Date(todayStart); todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);

    const rows = await db.many(`
      SELECT s.id, s.source, s.native_id, s.customer_email, s.product,
             s.status, s.price_cents, s.next_bill_at, s.frequency,
             s.last_billed_at,
             c.first_name, c.last_name
      FROM subscriptions s
      LEFT JOIN customers c ON s.customer_id = c.id
      WHERE s.status = 'ACTIVE'
      AND (
        (s.next_bill_at >= $1 AND s.next_bill_at < $2)
        OR (s.last_billed_at >= $1 AND s.last_billed_at < $2)
      )
      AND NOT (s.source = 'cc' AND s.raw->>'merchant' ILIKE '%paypal%')
      ORDER BY s.source, s.price_cents DESC
    `, [todayStart, todayEnd]);

    res.json({ orders: rows, count: rows.length });
  } catch (err) {
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
    const primaryType = sources.includes('cc')       ? 'checkoutchamp'
                      : sources.includes('recharge') ? 'recharge'
                      : 'subi';

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

module.exports = router;

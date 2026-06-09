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
   Always queries CC order/query API directly for today's recurring
   charges so orders billed >30 days into the subscription are never
   missed due to the delta-sync window. DB results for Recharge/Subi
   come from the last_billed_at / next_bill_at columns as before.   */

const TEST_EMAILS = new Set([
  'nobel@eydigitalmedia.com',
  'md.nobelislamjoy@gmail.com',
  'wardun@eydigitalmedia.com'
]);

router.get('/api/today-orders', async function(req, res) {
  try {
    const now         = new Date();
    const todayStart  = new Date(now); todayStart.setUTCHours(0, 0, 0, 0);
    const todayEnd    = new Date(todayStart); todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);
    const todayUTC    = now.toISOString().split('T')[0];       /* YYYY-MM-DD UTC */
    const tomorrowUTC = todayEnd.toISOString().split('T')[0];
    /* CC order/query uses MM/DD/YYYY — derived from UTC date */
    const ccTodayStr  = (now.getUTCMonth()+1).toString().padStart(2,'0') + '/' +
                        now.getUTCDate().toString().padStart(2,'0') + '/' + now.getUTCFullYear();

    /* Run all four source queries in parallel for speed */
    const [ccRows, ccPendingRows, rcRows, dbRows] = await Promise.all([

      /* ── CC: transactions/query for today's CHARGED RECURRING billings ──
         CC stores timestamps in EDT (UTC-4). User is in Amsterdam (CEST = UTC+2).
         Amsterdam today starts at CEST 00:00 = UTC-2h = EDT previous day 18:00.
         Query yesterday + today (UTC) and filter client-side to the CEST 24h window
         so early-morning CEST charges (which fall on yesterday in EDT) are included. */
      (async function() {
        if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return [];
        /* CEST today window in UTC: midnight−2h → midnight+22h */
        var cestStart = new Date(todayStart.getTime() - 2 * 3600 * 1000);
        var cestEnd   = new Date(cestStart.getTime() + 24 * 3600 * 1000);
        var yesterday = new Date(todayStart.getTime() - 24 * 3600 * 1000);
        var ccStart   = (yesterday.getUTCMonth()+1).toString().padStart(2,'0') + '/' +
                        yesterday.getUTCDate().toString().padStart(2,'0') + '/' + yesterday.getUTCFullYear();
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
                var dt = new Date(ds.replace(' ', 'T') + '-04:00');
                if (isNaN(dt) || dt < cestStart || dt >= cestEnd) return;
              }
              var email = (t.emailAddress || '').trim().toLowerCase();
              if (!email || seenEmails.includes(email) || TEST_EMAILS.has(email)) return;
              seenEmails.push(email);
              rows.push({
                source: 'cc', native_id: t.orderId || null,
                customer_email: email,
                product: t.productName || null,
                price_cents: Math.round(parseFloat(t.amount || t.totalAmount || 0) * 100),
                next_bill_at: null, cc_charge_status: 'CHARGED', status: 'ACTIVE',
                first_name: t.firstName || null, last_name: t.lastName || null,
                frequency: null, last_billed_at: ds || now.toISOString()
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

      /* ── Recharge: fetch all active subs + filter client-side by today UTC ──
         The RC API date filter (next_charge_scheduled_at_min/max) does not
         work reliably in v1 — it returns all active subs regardless.
         We also check today's successful charges for already-billed orders.  */
      (async function() {
        if (!process.env.RECHARGE_API_KEY) return [];
        var rows = [], seenEmails = [];
        const headers = { 'X-Recharge-Access-Token': process.env.RECHARGE_API_KEY };
        try {
          /* 2a. All active subscriptions — filter client-side for today */
          var rcPage = 1;
          while (true) {
            const url = RC_API_BASE + '/subscriptions?status=active&limit=250&page=' + rcPage;
            const r = await fetch(url, { headers: headers });
            const d = await r.json();
            const subs = d.subscriptions || [];
            subs.forEach(function(s) {
              const nextCharge = (s.next_charge_scheduled_at || '');
              /* Only include if next_charge_scheduled_at starts with today's UTC date */
              if (!nextCharge.startsWith(todayUTC)) return;
              const email = (s.email || '').toLowerCase();
              if (!email || seenEmails.includes(email)) return;
              seenEmails.push(email);
              rows.push({
                source: 'recharge', native_id: String(s.id),
                customer_email: email,
                product: s.product_title || s.title || null,
                price_cents: Math.round(parseFloat(s.price || 0) * 100),
                next_bill_at: nextCharge, status: 'ACTIVE',
                first_name: null, last_name: null,
                frequency: s.order_interval_frequency + ' ' + s.order_interval_unit,
                last_billed_at: null
              });
            });
            if (subs.length < 250) break;
            rcPage++;
          }

          /* 2b. Already billed today: successful charges created today */
          var chPage = 1;
          while (true) {
            const url = RC_API_BASE + '/charges?status=success' +
              '&created_at_min=' + todayUTC + 'T00:00:00Z' +
              '&created_at_max=' + tomorrowUTC + 'T00:00:00Z' +
              '&limit=250&page=' + chPage;
            const r = await fetch(url, { headers: headers });
            const d = await r.json();
            const charges = d.charges || [];
            charges.forEach(function(c) {
              const email = (c.email || '').toLowerCase();
              if (!email || seenEmails.includes(email)) return;
              seenEmails.push(email);
              rows.push({
                source: 'recharge', native_id: String(c.subscription_id || c.id),
                customer_email: email,
                product: (c.line_items && c.line_items[0]) ? c.line_items[0].title : null,
                price_cents: Math.round(parseFloat(c.total_price || 0) * 100),
                next_bill_at: null, status: 'ACTIVE',
                first_name: null, last_name: null,
                frequency: null, last_billed_at: c.created_at || now.toISOString()
              });
            });
            if (charges.length < 250) break;
            chPage++;
          }
        } catch (e) { console.error('[today-orders] RC query failed:', e.message); }
        console.log('[today-orders] RC today (sched+billed):', rows.length);
        return rows;
      })(),

      /* ── Subi: DB query only — CC is fully handled by live API queries above ──
         CC pending comes from live purchase/query (ccPendingRows) so the DB is
         not used for CC; stale DB CC rows (e.g. RECYCLE_FAILED subs not yet
         re-synced) would otherwise create ghost entries.                       */
      db.many(`
        SELECT s.id, s.source, s.native_id, s.customer_email, s.product,
               s.status, s.price_cents, s.next_bill_at, s.frequency,
               s.last_billed_at, c.first_name, c.last_name
        FROM subscriptions s
        LEFT JOIN customers c ON s.customer_id = c.id
        WHERE s.status = 'ACTIVE'
          AND s.source = 'subi'
          AND (
            (s.next_bill_at >= $1 AND s.next_bill_at < $2)
            OR (s.last_billed_at >= $1 AND s.last_billed_at < $2)
          )
        ORDER BY s.price_cents DESC
      `, [todayStart, todayEnd])
    ]);

    /* Merge: CC charged (live txns) + CC pending (live purchases) + RC (live) + DB (Subi + DB-only CC)
       Priority: ccRows (charged) > ccPendingRows (live pending) > dbRows (stale-sync fallback) */
    const ccLiveEmails    = new Set(ccRows.map(function(r) { return r.customer_email; }));
    const ccPendingEmails = new Set(ccPendingRows.map(function(r) { return r.customer_email; }));
    const rcLiveEmails    = new Set(rcRows.map(function(r) { return r.customer_email; }));

    /* Remove live-pending rows that were already charged */
    const filteredCcPending = ccPendingRows.filter(function(r) {
      return !ccLiveEmails.has(r.customer_email);
    });

    /* DB rows are Subi only — no CC or RC rows to filter out */
    const filteredDb = dbRows;

    const allOrders = ccRows.concat(filteredCcPending).concat(rcRows).concat(filteredDb);
    console.log('[today-orders] total:', allOrders.length);
    res.json({ orders: allOrders, count: allOrders.length });
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

/* ── GET /admin/api/debug/today-breakdown
   Side-by-side comparison: DB subscriptions scheduled for today
   vs CC API recurring orders charged today.
   Shows what's in DB only, CC only, and both.
   ────────────────────────────────────────────────── */
router.get('/api/debug/today-breakdown', async function(req, res) {
  try {
    const now        = new Date();
    const todayStart = new Date(now); todayStart.setUTCHours(0, 0, 0, 0);
    const todayEnd   = new Date(todayStart); todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);
    const todayStr   = (now.getMonth()+1).toString().padStart(2,'0') + '/' +
                       now.getDate().toString().padStart(2,'0') + '/' + now.getFullYear();

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

    /* Group non-CC (Recharge/Subi) separately — CC API doesn't cover them */
    const otherBySource = {};
    dbOtherRows.forEach(function(r) {
      if (!otherBySource[r.source]) otherBySource[r.source] = [];
      otherBySource[r.source].push(r);
    });

    res.json({
      as_of:     now.toISOString(),
      today_utc: todayStart.toISOString().slice(0, 10),
      note: 'CC cross-reference compares CC-source DB subscriptions vs CC order/query API. Recharge/Subi are listed separately — they are not in the CC API.',
      summary: {
        db_total:              dbRows.length,
        db_cc_source:          dbCcRows.length,
        db_recharge:           (otherBySource.recharge||[]).length,
        db_subi:               (otherBySource.subi||[]).length,
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
      recharge_scheduled_today:     otherBySource.recharge || [],
      subi_scheduled_today:         otherBySource.subi     || []
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

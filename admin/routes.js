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
    const todayStart = new Date(now); todayStart.setHours(0,0,0,0);
    const weekAgo    = new Date(now); weekAgo.setDate(weekAgo.getDate() - 7);
    const monthAgo   = new Date(now); monthAgo.setDate(monthAgo.getDate() - 30);
    const tomorrowEnd = new Date(todayStart); tomorrowEnd.setDate(tomorrowEnd.getDate() + 2);
    const tomorrowStart = new Date(todayStart); tomorrowStart.setDate(tomorrowStart.getDate() + 1);

    const weekStart     = new Date(todayStart); weekStart.setDate(weekStart.getDate() - 7);
    const twoWeeksAgo  = new Date(todayStart); twoWeeksAgo.setDate(twoWeeksAgo.getDate() - 14);

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
      productBreakdown
    ] = await Promise.all([
      db.one(`SELECT COUNT(*)::int AS n FROM subscriptions WHERE status = 'ACTIVE'`),
      db.one(`SELECT COALESCE(SUM(price_cents),0)::bigint AS cents FROM subscriptions WHERE status = 'ACTIVE'`),
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
               FROM subscriptions WHERE status = 'ACTIVE' GROUP BY source ORDER BY n DESC`),
      db.many(`SELECT DATE(created_at) AS day, COALESCE(SUM(amount_cents),0)::bigint AS cents
               FROM orders WHERE created_at >= $1
               GROUP BY DATE(created_at) ORDER BY day ASC`, [monthAgo]),
      db.many(`SELECT DATE(next_bill_at) AS day, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS cents
               FROM subscriptions
               WHERE status = 'ACTIVE' AND next_bill_at >= $1 AND next_bill_at < $2
               GROUP BY DATE(next_bill_at) ORDER BY day ASC`,
              [todayStart, new Date(todayStart.getTime() + 8 * 24 * 60 * 60 * 1000)]),
      db.many(`SELECT DATE(started_at) AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE started_at >= $1
               GROUP BY DATE(started_at), source ORDER BY day ASC`, [monthAgo]),
      db.many(`SELECT DATE(cancelled_at) AS day, source, COUNT(*)::int AS n
               FROM subscriptions WHERE cancelled_at >= $1
               GROUP BY DATE(cancelled_at), source ORDER BY day ASC`, [monthAgo]),
      db.many(`SELECT id, customer_email, product, source, price_cents, cancelled_at, cancel_reason
               FROM subscriptions WHERE cancelled_at IS NOT NULL
               ORDER BY cancelled_at DESC LIMIT 10`),
      db.many(`SELECT kind, source, email, payload, ts FROM events ORDER BY ts DESC LIMIT 30`),
      db.many(`SELECT product, source, COUNT(*)::int AS n, COALESCE(SUM(price_cents),0)::bigint AS mrr_cents
               FROM subscriptions WHERE status = 'ACTIVE' AND product IS NOT NULL
               GROUP BY product, source ORDER BY n DESC LIMIT 15`)
    ]);

    res.json({
      generatedAt: now.toISOString(),
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
      recentCancels:     recentCancels,
      recentEvents:      recentEvents
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

module.exports = router;

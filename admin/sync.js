/* ════════════════════════════════════════════════════
   admin/sync.js — pulls from CC + Recharge into Postgres.
   Run every 5 min by node-cron.
   ════════════════════════════════════════════════════ */

const fetch = require('node-fetch');
const db    = require('./db');

const CC_BASE       = 'https://api.checkoutchamp.com';
const RECHARGE_BASE = 'https://api.rechargeapps.com';
const WHOP_BASE     = 'https://api.whop.com/api/v2';

/* ────────────────────────────────────────────────
   Helpers
   ──────────────────────────────────────────────── */

function delay(ms) { return new Promise(function(r) { setTimeout(r, ms); }); }

/* Resilient fetch — same rationale as admin/routes.js. Node 19+ defaults the global
   agent to keepAlive:true; CheckoutChamp/Whop drop idle pooled sockets, so reused
   sockets throw "Premature close". Force a fresh socket + retry the full fetch+read. */
const _https = require('https');
const _http  = require('http');
const _agentHttps = new _https.Agent({ keepAlive: false });
const _agentHttp  = new _http.Agent({ keepAlive: false });
function _agentFor(parsedURL) {
  return (parsedURL && parsedURL.protocol === 'http:') ? _agentHttp : _agentHttps;
}
async function fetchR(url, opts, tries) {
  tries = tries || 4;
  const o = Object.assign({}, opts || {});
  if (!o.agent)   o.agent   = _agentFor;
  if (!o.timeout) o.timeout = 25000;
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const r    = await fetch(url, o);
      const body = await r.text();
      const ok = r.ok, status = r.status, headers = r.headers;
      return {
        ok: ok, status: status, headers: headers,
        text: function() { return Promise.resolve(body); },
        json: function() { return Promise.resolve(body ? JSON.parse(body) : null); }
      };
    } catch (e) {
      lastErr = e;
      if (i < tries - 1) await delay(350 * (i + 1));
    }
  }
  throw lastErr;
}

function ccParams(extra) {
  const p = new URLSearchParams({
    loginId:  process.env.CC_LOGIN_ID  || '',
    password: process.env.CC_API_PASSWORD || ''
  });
  if (extra) for (const k in extra) p.append(k, extra[k]);
  return p.toString();
}

function ccDate(d) {
  /* CC expects MM/DD/YYYY */
  return (d.getMonth()+1).toString().padStart(2,'0') + '/' +
          d.getDate().toString().padStart(2,'0') + '/' +
          d.getFullYear();
}

function toCents(price) {
  if (price == null || price === '') return 0;
  const n = parseFloat(price);
  return isNaN(n) ? 0 : Math.round(n * 100);
}

function parseDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

async function setSyncState(source, fields) {
  const sets = [];
  const vals = [source];
  let i = 2;
  for (const k in fields) {
    sets.push(k + ' = $' + i);
    vals.push(fields[k]);
    i++;
  }
  await db.query(
    'UPDATE sync_state SET ' + sets.join(', ') + ' WHERE source = $1',
    vals
  );
}

/* ════════════════════════════════════════════════════
   CHECKOUTCHAMP SYNC
   Pulls /purchase/query/ (subscriptions) and
   /order/query/ (orders/rebills) within a date window.
   ════════════════════════════════════════════════════ */

async function syncCC(opts) {
  opts = opts || {};
  const isFull = opts.full === true;

  if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) {
    throw new Error('CC credentials missing (CC_LOGIN_ID / CC_API_PASSWORD)');
  }

  const today    = new Date();
  /* Full sync: fixed epoch so ALL-TIME subscriptions are captured regardless of age.
     Delta sync: 90-day rolling window for recent changes (covers billing cycles 1-3). */
  const startDate = isFull ? '01/01/2015' : ccDate(new Date(today.getFullYear(), today.getMonth(), today.getDate() - 90));
  /* Use tomorrow as endDate so today's records are never excluded by an exclusive boundary */
  const tomorrow  = new Date(today);
  tomorrow.setDate(tomorrow.getDate() + 1);
  const endDate   = ccDate(tomorrow);

  console.log('[sync:cc] window:', startDate, '→', endDate, '| full:', isFull);

  /* ── 1a. Purchases (subscriptions) — date-windowed ── */
  /* perPage kept small (50): a 200-row response is ~508 KB and drops mid-transfer
     on Render → "Premature close". 50 rows ≈ 122 KB transfers reliably. */
  let subsTouched = 0;
  let page = 1;
  const perPage = 50;
  while (page <= 200) {
    let d;
    try {
      const r = await fetchR(CC_BASE + '/purchase/query/?' + ccParams({
        startDate:      startDate,
        endDate:        endDate,
        resultsPerPage: perPage,
        page:           page
      }), { method: 'POST' });
      d = JSON.parse(await r.text());
    } catch (e) {
      console.error('[sync:cc] purchases page', page, 'failed —', e.message);
      break;
    }
    const data = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
    for (const p of data) await upsertCCPurchase(p);
    subsTouched += data.length;
    console.log('[sync:cc] purchases page', page, '| got', data.length, '| total', subsTouched);
    if (data.length < perPage) break;
    page++;
  }

  /* ── 1b. All-time ACTIVE subscriptions refresh ──
     The date-windowed pass above only catches subs created in the last 90 days.
     Subscriptions older than that can still change status or nextBillDate (e.g.
     a cycle-4 sub going RECYCLE_FAILED won't appear in the 90-day window).
     This second pass fetches every currently-ACTIVE sub from CC to ensure their
     nextBillDate and status are always current in our DB.                      */
  if (!isFull) {
    let activePage = 1;
    let activeTouched = 0;
    while (activePage <= 100) {
      let d;
      try {
        const r = await fetchR(CC_BASE + '/purchase/query/?' + ccParams({
          startDate:      '01/01/2015',
          endDate:        endDate,
          status:         'ACTIVE',
          resultsPerPage: perPage,
          page:           activePage
        }), { method: 'POST' });
        d = JSON.parse(await r.text());
      } catch (e) {
        console.error('[sync:cc] active-refresh page', activePage, 'failed —', e.message);
        break;
      }
      const data = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
      for (const p of data) await upsertCCPurchase(p);
      activeTouched += data.length;
      subsTouched   += data.length;
      console.log('[sync:cc] active-refresh page', activePage, '| got', data.length, '| total', activeTouched);
      if (data.length < perPage) break;
      activePage++;
    }
  }

  /* ── 1c. All-time RECYCLE_FAILED subscriptions refresh ──
     Same rationale as the ACTIVE pass: subs older than 90 days that
     recently hit RECYCLE_FAILED won't appear in the date-windowed pass.
     Fetching them all-time ensures cancelled_at is always populated so
     they show up correctly in the Recent Cancellations panel.           */
  if (!isFull) {
    let rfPage = 1, rfTouched = 0;
    while (rfPage <= 100) {
      let d;
      try {
        const r = await fetchR(CC_BASE + '/purchase/query/?' + ccParams({
          startDate:      '01/01/2015',
          endDate:        endDate,
          status:         'RECYCLE_FAILED',
          resultsPerPage: perPage,
          page:           rfPage
        }), { method: 'POST' });
        d = JSON.parse(await r.text());
      } catch (e) {
        console.error('[sync:cc] recycle-failed page', rfPage, 'failed —', e.message);
        break;
      }
      const data = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
      for (const p of data) await upsertCCPurchase(p);
      rfTouched   += data.length;
      subsTouched += data.length;
      console.log('[sync:cc] recycle-failed page', rfPage, '| got', data.length, '| total', rfTouched);
      if (data.length < perPage) break;
      rfPage++;
    }
  }

  /* ── 2. Orders ── */
  let ordersTouched = 0;
  page = 1;
  while (page <= 300) {
    let d;
    try {
      const r = await fetchR(CC_BASE + '/order/query/?' + ccParams({
        startDate:      startDate,
        endDate:        endDate,
        resultsPerPage: perPage,
        page:           page
      }), { method: 'POST' });
      d = JSON.parse(await r.text());
    } catch (e) {
      console.error('[sync:cc] orders page', page, 'failed —', e.message);
      break;
    }
    const data = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
    for (const o of data) await upsertCCOrder(o);
    ordersTouched += data.length;
    console.log('[sync:cc] orders page', page, '| got', data.length, '| total', ordersTouched);
    if (data.length < perPage) break;
    page++;
  }

  console.log('[sync:cc] done | subs:', subsTouched, '| orders:', ordersTouched);
  return { subs: subsTouched, orders: ordersTouched };
}

async function upsertCCPurchase(p) {
  const email = (p.emailAddress || '').trim().toLowerCase();
  if (!email) return;

  const customerId = await db.upsertCustomer({
    email:        email,
    cc_member_id: p.memberId || null,
    first_name:   p.firstName || null,
    last_name:    p.lastName || null
  });

  const id          = 'cc:' + p.purchaseId;
  const status      = (p.status || 'ACTIVE').toUpperCase();
  const priceCents  = toCents(p.price);
  const startedAt   = parseDate(p.dateCreated);
  const nextBillAt  = parseDate(p.nextBillDate);
  const isCancelledStatus = status === 'CANCELLED' || status === 'INACTIVE' || status === 'RECYCLE_FAILED';
  const cancelledAt = isCancelledStatus ? parseDate(p.dateUpdated || p.cancelDate) : null;
  const cancelReason = status === 'RECYCLE_FAILED' ? 'Payment failed' : (p.cancelReason || null);

  /* detect if this is a NEW row → fire sub_created event */
  const existing = await db.one('SELECT id, status FROM subscriptions WHERE id = $1', [id]);
  const isNew    = !existing;
  const wasCancelled = existing && (existing.status === 'CANCELLED' || existing.status === 'RECYCLE_FAILED');
  const becameCancelled = existing && !wasCancelled && isCancelledStatus;

  await db.query(`
    INSERT INTO subscriptions
      (id, source, native_id, customer_id, customer_email, product, product_id,
       status, price_cents, frequency, next_bill_at, started_at, cancelled_at,
       cancel_reason, raw, last_synced_at)
    VALUES ($1,'cc',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
    ON CONFLICT (id) DO UPDATE SET
      customer_id    = EXCLUDED.customer_id,
      customer_email = EXCLUDED.customer_email,
      product        = EXCLUDED.product,
      product_id     = EXCLUDED.product_id,
      status         = EXCLUDED.status,
      price_cents    = EXCLUDED.price_cents,
      frequency      = EXCLUDED.frequency,
      next_bill_at   = EXCLUDED.next_bill_at,
      cancelled_at   = EXCLUDED.cancelled_at,
      cancel_reason  = EXCLUDED.cancel_reason,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW(),
      last_billed_at = CASE
        WHEN subscriptions.next_bill_at IS NOT NULL
             AND subscriptions.next_bill_at < NOW()
             AND EXCLUDED.next_bill_at > subscriptions.next_bill_at
        THEN NOW()
        ELSE subscriptions.last_billed_at
      END
  `, [
    id, String(p.purchaseId), customerId, email,
    p.productName || null, p.productId ? String(p.productId) : null,
    status, priceCents,
    p.billingIntervalDays ? String(p.billingIntervalDays) : null,
    nextBillAt, startedAt, cancelledAt,
    cancelReason, p
  ]);

  if (isNew) await db.recordEvent('sub_created', 'cc', email, { product: p.productName, price: p.price });
  if (becameCancelled) await db.recordEvent('sub_cancelled', 'cc', email, { product: p.productName });
}

async function upsertCCOrder(o) {
  const email = (o.emailAddress || '').trim().toLowerCase();
  if (!email) return;

  const customerId = await db.upsertCustomer({
    email:        email,
    cc_member_id: o.memberId || null,
    first_name:   o.firstName || null,
    last_name:    o.lastName || null
  });

  const id           = 'cc:' + o.orderId;
  const totalCents   = toCents(o.totalAmount || o.orderTotal || o.price);
  const createdAt    = parseDate(o.dateCreated) || new Date().toISOString();
  const isRebill     = o.parentOrderId || o.recurringFlag === '1' || o.orderType === 'RECURRING';
  const isUpsell     = !isRebill && (o.parentOrderId != null || o.upsellFlag === '1');
  const type         = isRebill ? 'rebill' : (isUpsell ? 'upsell' : 'initial');
  const subId        = o.purchaseId ? ('cc:' + o.purchaseId) : null;

  /* fire rebill event for new rebills only */
  const existing = await db.one('SELECT id FROM orders WHERE id = $1', [id]);
  const isNew = !existing;

  await db.query(`
    INSERT INTO orders
      (id, source, native_id, customer_id, customer_email, amount_cents, type,
       product, status, subscription_id, created_at, raw, last_synced_at)
    VALUES ($1,'cc',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    ON CONFLICT (id) DO UPDATE SET
      amount_cents   = EXCLUDED.amount_cents,
      status         = EXCLUDED.status,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW()
  `, [
    id, String(o.orderId), customerId, email,
    totalCents, type,
    o.productName || null,
    (o.orderStatus || o.status || 'COMPLETE').toUpperCase(),
    subId, createdAt, o
  ]);

  if (isNew && type === 'rebill') {
    await db.recordEvent('rebill', 'cc', email, { amount: o.totalAmount, product: o.productName });
  }
}

/* ════════════════════════════════════════════════════
   RECHARGE SYNC
   ════════════════════════════════════════════════════ */

async function getRechargeKey() {
  /* Env var wins when set (so updating it on the host takes effect immediately);
     fall back to the integration_credentials DB row otherwise. */
  if (process.env.RECHARGE_API_KEY) return process.env.RECHARGE_API_KEY;
  try {
    const row = await db.one(
      "SELECT creds FROM integration_credentials WHERE name = 'recharge'"
    );
    if (row && row.creds && row.creds['RECHARGE_API_KEY']) return row.creds['RECHARGE_API_KEY'];
  } catch (e) { /* table may not exist yet */ }
  return '';
}

async function syncRecharge(opts) {
  opts = opts || {};
  const isFull = opts.full === true;

  const rcKey = await getRechargeKey();
  if (!rcKey) {
    throw new Error('RECHARGE_API_KEY not set');
  }

  const headers = {
    'X-Recharge-Access-Token': rcKey,
    'Content-Type': 'application/json'
  };

  /* ── 1. Subscriptions ── */
  let subsTouched = 0;
  let page = 1;
  const limit = 250;
  while (page <= 50) {
    const url = RECHARGE_BASE + '/subscriptions?limit=' + limit + '&page=' + page;
    const r   = await fetchR(url, { headers });
    const d   = await r.json();
    const data = d.subscriptions || [];
    for (const s of data) await upsertRechargeSub(s);
    subsTouched += data.length;
    console.log('[sync:rc] subs page', page, '| got', data.length, '| total', subsTouched);
    if (data.length < limit) break;
    page++;
  }

  /* ── 2. Charges (orders) ── */
  let ordersTouched = 0;
  page = 1;
  const since = new Date();
  /* 45-day delta window: charges are created a few days before processing, so a
     charge processed early this month may have been created late last month. */
  since.setDate(since.getDate() - (isFull ? 365 * 2 : 45));
  while (page <= 50) {
    const url = RECHARGE_BASE + '/charges?limit=' + limit + '&page=' + page +
                '&created_at_min=' + since.toISOString() + '&status=success';
    const r = await fetchR(url, { headers });
    const d = await r.json();
    const data = d.charges || [];
    for (const c of data) await upsertRechargeCharge(c);
    ordersTouched += data.length;
    console.log('[sync:rc] charges page', page, '| got', data.length, '| total', ordersTouched);
    if (data.length < limit) break;
    page++;
  }

  console.log('[sync:rc] done | subs:', subsTouched, '| orders:', ordersTouched);
  return { subs: subsTouched, orders: ordersTouched };
}

async function upsertRechargeSub(s) {
  /* Recharge subscription objects carry `email` directly — use it. The old
     per-customer /customers lookup rate-limited and (when it failed) cached null
     permanently, silently dropping hundreds of active subs from the sync. */
  const email = (s.email || '').trim().toLowerCase() || await getRechargeEmail(s.customer_id);
  if (!email) return;

  const customerId = await db.upsertCustomer({
    email:       email,
    recharge_id: String(s.customer_id)
  });

  const id           = 'rc:' + s.id;
  const status       = (s.status || 'ACTIVE').toUpperCase();
  const priceCents   = toCents(s.price);
  const startedAt    = parseDate(s.created_at);
  const nextBillAt   = parseDate(s.next_charge_scheduled_at);
  const cancelledAt  = parseDate(s.cancelled_at);
  const frequency    = s.order_interval_frequency && s.order_interval_unit
    ? 'Every ' + s.order_interval_frequency + ' ' + s.order_interval_unit
    : null;

  const existing = await db.one('SELECT id, status FROM subscriptions WHERE id = $1', [id]);
  const isNew    = !existing;
  const becameCancelled = existing && existing.status !== 'CANCELLED' && status === 'CANCELLED';

  await db.query(`
    INSERT INTO subscriptions
      (id, source, native_id, customer_id, customer_email, product,
       status, price_cents, frequency, next_bill_at, started_at, cancelled_at, cancel_reason, raw, last_synced_at)
    VALUES ($1,'recharge',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
    ON CONFLICT (id) DO UPDATE SET
      customer_id    = EXCLUDED.customer_id,
      customer_email = EXCLUDED.customer_email,
      product        = EXCLUDED.product,
      status         = EXCLUDED.status,
      price_cents    = EXCLUDED.price_cents,
      frequency      = EXCLUDED.frequency,
      next_bill_at   = EXCLUDED.next_bill_at,
      cancelled_at   = EXCLUDED.cancelled_at,
      cancel_reason  = EXCLUDED.cancel_reason,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW(),
      last_billed_at = CASE
        WHEN subscriptions.next_bill_at IS NOT NULL
             AND subscriptions.next_bill_at < NOW()
             AND EXCLUDED.next_bill_at > subscriptions.next_bill_at
        THEN NOW()
        ELSE subscriptions.last_billed_at
      END
  `, [
    id, String(s.id), customerId, email,
    s.product_title || null, status, priceCents, frequency,
    nextBillAt, startedAt, cancelledAt, s.cancellation_reason || null, s
  ]);

  if (isNew)            await db.recordEvent('sub_created',   'recharge', email, { product: s.product_title, price: s.price });
  if (becameCancelled)  await db.recordEvent('sub_cancelled', 'recharge', email, { product: s.product_title });
}

const _rcEmailCache = new Map();
async function getRechargeEmail(customerId) {
  if (!customerId) return null;
  const key = String(customerId);
  if (_rcEmailCache.has(key)) return _rcEmailCache.get(key);

  const rcKey = await getRechargeKey();
  const r = await fetchR(RECHARGE_BASE + '/customers/' + customerId, {
    headers: { 'X-Recharge-Access-Token': rcKey }
  });
  const d = await r.json();
  const email = d.customer && d.customer.email
    ? d.customer.email.toLowerCase().trim()
    : null;
  /* Only cache successful lookups — caching null made transient failures permanent. */
  if (email) _rcEmailCache.set(key, email);
  return email;
}

async function upsertRechargeCharge(c) {
  const email = (c.email || '').trim().toLowerCase() || await getRechargeEmail(c.customer_id);
  if (!email) return;

  const customerId = await db.upsertCustomer({ email: email, recharge_id: String(c.customer_id) });

  const id        = 'rc:' + c.id;
  const total     = toCents(c.total_price);
  /* Recharge creates charge rows when a charge is SCHEDULED, so created_at is
     a prior-cycle date. Revenue is collected at processed_at — use that as the
     order date so "collected this month" buckets by when cash was captured. */
  const createdAt = parseDate(c.processed_at || c.created_at) || new Date().toISOString();
  /* Recharge marks subsequent charges with subscription_id; first checkout has type=checkout */
  const subId     = (c.line_items && c.line_items[0] && c.line_items[0].subscription_id)
    ? 'rc:' + c.line_items[0].subscription_id
    : null;
  const type      = (c.type || '').toLowerCase() === 'recurring' ? 'rebill' : 'initial';

  const existing = await db.one('SELECT id FROM orders WHERE id = $1', [id]);
  const isNew = !existing;

  await db.query(`
    INSERT INTO orders
      (id, source, native_id, customer_id, customer_email, amount_cents, type,
       product, status, subscription_id, created_at, raw, last_synced_at)
    VALUES ($1,'recharge',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    ON CONFLICT (id) DO UPDATE SET
      amount_cents   = EXCLUDED.amount_cents,
      status         = EXCLUDED.status,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW()
  `, [
    id, String(c.id), customerId, email, total, type,
    (c.line_items && c.line_items[0] && c.line_items[0].title) || null,
    (c.status || 'success').toUpperCase(),
    subId, createdAt, c
  ]);

  if (isNew && type === 'rebill') {
    await db.recordEvent('rebill', 'recharge', email, { amount: c.total_price });
  }
}

/* ════════════════════════════════════════════════════
   WHOP SYNC
   Pulls /memberships from Whop API v2 and upserts
   into the subscriptions table.
   ════════════════════════════════════════════════════ */

async function getWhopKey() {
  /* Env var wins when set (so updating it on the host takes effect immediately);
     fall back to the integration_credentials DB row otherwise. The DB row held a
     stale/expired token that was 401ing the Whop sync. */
  if (process.env.WHOP_API_KEY) return process.env.WHOP_API_KEY;
  try {
    const row = await db.one(
      "SELECT creds FROM integration_credentials WHERE name = 'whop'"
    );
    if (row && row.creds && row.creds['WHOP_API_KEY']) return row.creds['WHOP_API_KEY'];
  } catch (e) { /* table may not exist yet */ }
  return '';
}

async function syncWhop(opts) {
  opts = opts || {};
  const isFull = opts.full === true;

  const key = await getWhopKey();
  if (!key) throw new Error('WHOP_API_KEY not configured');

  const headers = { 'Authorization': 'Bearer ' + key, 'accept': 'application/json' };

  /* ── helper: fetch JSON and throw on HTTP error ── */
  async function whopFetch(url) {
    const r = await fetchR(url, { headers });
    if (!r.ok) {
      let body = '';
      try { body = JSON.stringify(await r.json()); } catch(e) { body = await r.text().catch(() => ''); }
      throw new Error('Whop API HTTP ' + r.status + ' at ' + url + ' — ' + body.substring(0, 200));
    }
    return r.json();
  }

  /* ── 1. Pre-fetch all products for name lookup ── */
  console.log('[sync:whop] loading product catalog...');
  const productMap = {};
  let pPage = 1, totalPPages = 1;
  do {
    let d;
    try { d = await whopFetch(WHOP_BASE + '/products?per_page=10&page=' + pPage); }
    catch (e) { console.warn('[sync:whop] product page', pPage, 'failed —', e.message); break; }
    (d.data || []).forEach(function(p) { productMap[p.id] = p.title || p.name || p.id; });
    totalPPages = (d.pagination && d.pagination.total_page) || 1;
    pPage++;
    if (pPage <= totalPPages) await delay(150);
  } while (pPage <= totalPPages);
  console.log('[sync:whop] loaded', Object.keys(productMap).length, 'products');

  /* ── 2. Plan cache — fetched on demand and memoised ── */
  const planCache = {};
  async function getPlan(planId) {
    if (!planId) return { price: '0', billingDays: 30 };
    if (planCache[planId]) return planCache[planId];
    try {
      const d = await whopFetch(WHOP_BASE + '/plans/' + planId);
      planCache[planId] = {
        price:       d.renewal_price || d.initial_price || '0',
        billingDays: d.billing_period || 30
      };
    } catch (e) {
      console.warn('[sync:whop] plan fetch failed for', planId, '—', e.message);
      planCache[planId] = { price: '0', billingDays: 30 };
    }
    await delay(120);
    return planCache[planId];
  }

  /* ── 3. Active memberships ── */
  let touched = 0;
  let page = 1, totalPages = 1;
  do {
    let d;
    try {
      d = await whopFetch(WHOP_BASE + '/memberships?per_page=10&page=' + page + '&status=active');
    } catch (e) {
      console.warn('[sync:whop] active membership page', page, 'failed —', e.message);
      break;   /* keep what we synced; next cycle catches the rest */
    }
    const items = d.data || [];
    totalPages = (d.pagination && d.pagination.total_page) || 1;

    for (const m of items) {
      const plan = await getPlan(m.plan);
      await upsertWhopMembership(m, productMap, plan);
      touched++;
    }

    console.log('[sync:whop] active page', page + '/' + totalPages, '| total', touched);
    page++;
    if (page <= totalPages) await delay(200);
  } while (page <= totalPages);

  /* ── 4. Cancelled/expired — full sync only (to seed historical data) ── */
  let cancelledTouched = 0;
  if (isFull) {
    const cancelStatuses = ['expired', 'canceled'];
    for (const cs of cancelStatuses) {
      let cPage = 1, cTotalPages = 1;
      do {
        let d;
        try {
          d = await whopFetch(WHOP_BASE + '/memberships?per_page=10&page=' + cPage + '&status=' + cs);
        } catch (e) {
          console.warn('[sync:whop]', cs, 'page', cPage, 'failed —', e.message);
          break;
        }
        const items = d.data || [];
        cTotalPages = (d.pagination && d.pagination.total_page) || 1;

        for (const m of items) {
          const plan = await getPlan(m.plan);
          await upsertWhopMembership(m, productMap, plan);
          cancelledTouched++;
        }

        console.log('[sync:whop]', cs, 'page', cPage + '/' + cTotalPages, '| total', cancelledTouched);
        cPage++;
        if (cPage <= cTotalPages) await delay(200);
      } while (cPage <= cTotalPages);
    }
  }

  /* ── 5. Payments → orders table ──
     Whop memberships don't capture actual charges, so without this Whop
     contributes $0 to any orders-based revenue/collection metric. Pull paid
     payments (newest-first) back to a cutoff and upsert them as orders.      */
  let payTouched = 0;
  const payCutoff = Math.floor((Date.now() - (isFull ? 730 : 45) * 24 * 3600 * 1000) / 1000);
  let payPage = 1, payTotal = 1, stopPay = false;
  do {
    let d;
    try {
      d = await whopFetch(WHOP_BASE + '/payments?per_page=50&page=' + payPage + '&status=paid');
    } catch (e) {
      console.warn('[sync:whop] payments page', payPage, 'failed —', e.message);
      break;
    }
    const items = d.data || [];
    payTotal = (d.pagination && d.pagination.total_page) || 1;
    for (const p of items) {
      const created = p.paid_at || p.created_at || 0;
      if (created && created < payCutoff) { stopPay = true; continue; }
      await upsertWhopPayment(p, productMap);
      payTouched++;
    }
    console.log('[sync:whop] payments page', payPage + '/' + payTotal, '| total', payTouched);
    if (stopPay) break;   /* newest-first: once past the cutoff we're done */
    payPage++;
    if (payPage <= payTotal) await delay(150);
  } while (payPage <= payTotal && payPage <= 400);

  console.log('[sync:whop] done | active:', touched, '| cancelled:', cancelledTouched, '| payments:', payTouched);
  return { memberships: touched, cancelled: cancelledTouched, payments: payTouched };
}

/* Upsert a Whop payment into the orders table (source='whop').
   billing_reason 'subscription_cycle'/'renewal' → rebill; everything else → initial. */
async function upsertWhopPayment(p, productMap) {
  if (!p || !p.id) return;
  const id        = 'whop:pay:' + p.id;
  const subId     = p.membership ? ('whop:' + p.membership) : null;
  const amount    = toCents(p.final_amount != null ? p.final_amount : (p.subtotal != null ? p.subtotal : p.total));
  const br        = (p.billing_reason || '').toLowerCase();
  const type      = (br === 'subscription_cycle' || br === 'renewal') ? 'rebill' : 'initial';
  const createdAt = (p.paid_at || p.created_at)
    ? new Date((p.paid_at || p.created_at) * 1000).toISOString()
    : new Date().toISOString();
  const product   = (p.product && productMap[p.product]) || null;

  /* Resolve customer from the membership's subscription (payments carry no email). */
  let customerId = null, email = null;
  if (subId) {
    const sub = await db.one('SELECT customer_id, customer_email FROM subscriptions WHERE id = $1', [subId]);
    if (sub) { customerId = sub.customer_id; email = sub.customer_email; }
  }
  if (!email) email = (p.user || p.id) + '@whop.local';
  if (!customerId) customerId = await db.upsertCustomer({ email: email });

  await db.query(`
    INSERT INTO orders
      (id, source, native_id, customer_id, customer_email, amount_cents, type,
       product, status, subscription_id, created_at, raw, last_synced_at)
    VALUES ($1,'whop',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    ON CONFLICT (id) DO UPDATE SET
      amount_cents   = EXCLUDED.amount_cents,
      status         = EXCLUDED.status,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW()
  `, [
    id, String(p.id), customerId, email, amount, type,
    product, (p.status || 'paid').toUpperCase(), subId, createdAt, p
  ]);
}

function whopFrequency(billingDays) {
  if (billingDays >= 355) return 'Yearly';
  if (billingDays >= 80)  return 'Every 3 months';
  if (billingDays >= 25)  return 'Monthly';
  if (billingDays >= 6)   return 'Weekly';
  return 'Every ' + billingDays + ' days';
}

async function upsertWhopMembership(m, productMap, plan) {
  const email = (m.email || '').trim().toLowerCase();
  if (!email) return;

  const customerId = await db.upsertCustomer({ email: email });

  const id = 'whop:' + m.id;

  /* Status mapping */
  const s = (m.status || '').toLowerCase();
  let status;
  if (s === 'active' || s === 'trialing' || s === 'past_due') {
    status = 'ACTIVE';
  } else if (s === 'completed' && m.valid) {
    status = 'ACTIVE';
  } else {
    status = 'CANCELLED';
  }

  const isCancelled = status === 'CANCELLED';
  const priceCents  = toCents(plan.price);

  const nextBillAt = m.renewal_period_end
    ? new Date(m.renewal_period_end * 1000).toISOString()
    : null;
  const startedAt  = m.created_at
    ? new Date(m.created_at * 1000).toISOString()
    : null;
  const cancelledAt = isCancelled
    ? (m.expires_at ? new Date(m.expires_at * 1000).toISOString() : startedAt)
    : null;

  const productName = productMap[m.product] || null;
  const frequency   = whopFrequency(plan.billingDays);

  const existing = await db.one('SELECT id, status FROM subscriptions WHERE id = $1', [id]);
  const isNew    = !existing;
  const becameCancelled = existing && existing.status !== 'CANCELLED' && isCancelled;

  await db.query(`
    INSERT INTO subscriptions
      (id, source, native_id, customer_id, customer_email, product, product_id,
       status, price_cents, frequency, next_bill_at, started_at, cancelled_at,
       cancel_reason, raw, last_synced_at)
    VALUES ($1,'whop',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
    ON CONFLICT (id) DO UPDATE SET
      customer_id    = EXCLUDED.customer_id,
      customer_email = EXCLUDED.customer_email,
      product        = EXCLUDED.product,
      product_id     = EXCLUDED.product_id,
      status         = EXCLUDED.status,
      price_cents    = EXCLUDED.price_cents,
      frequency      = EXCLUDED.frequency,
      next_bill_at   = EXCLUDED.next_bill_at,
      cancelled_at   = EXCLUDED.cancelled_at,
      cancel_reason  = EXCLUDED.cancel_reason,
      raw            = EXCLUDED.raw,
      last_synced_at = NOW()
  `, [
    id, m.id, customerId, email,
    productName, m.product || null,
    status, priceCents, frequency,
    nextBillAt, startedAt, cancelledAt,
    isCancelled ? (m.status || null) : null,
    m
  ]);

  if (isNew && !isCancelled) {
    await db.recordEvent('sub_created', 'whop', email, { product: productName, price: plan.price });
  }
  if (becameCancelled) {
    await db.recordEvent('sub_cancelled', 'whop', email, { product: productName });
  }
}

/* ════════════════════════════════════════════════════
   TODAY'S BILLINGS SYNC
   Queries CC order/query for today's date to catch
   any subscriptions billed today that the 30-day
   delta window would miss (created >30 days ago).
   Sets last_billed_at on matched subscriptions.
   ════════════════════════════════════════════════════ */

async function syncTodayBillings() {
  if (!process.env.CC_LOGIN_ID || !process.env.CC_API_PASSWORD) return;

  const today    = new Date();
  /* Use Amsterdam date for CC query — matches the user's "today" */
  const amsDate  = today.toLocaleDateString('en-CA', { timeZone: 'Europe/Amsterdam' });
  const [sy, sm, sd] = amsDate.split('-');
  const todayStr = sm + '/' + sd + '/' + sy;

  let allOrders = [];
  let page = 1;
  const perPage = 200;

  while (page <= 20) {
    const url = CC_BASE + '/order/query/?' + ccParams({
      startDate:      todayStr,
      endDate:        todayStr,
      resultsPerPage: perPage,
      page:           page,
      sortDir:        -1
    });
    const r = await fetchR(url, { method: 'POST' });
    const d = await r.json();
    const orders = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
    allOrders = allOrders.concat(orders);
    console.log('[sync:today] CC orders page', page, '| got', orders.length);
    if (orders.length < perPage) break;
    page++;
  }

  /* Keep only recurring orders */
  const recurring = allOrders.filter(function(o) {
    return o.recurringFlag === '1' || o.orderType === 'RECURRING' || o.parentOrderId;
  });

  if (!recurring.length) {
    console.log('[sync:today] no CC recurring orders found for today');
    return { billed: 0 };
  }

  /* Collect unique emails */
  const emails = Array.from(new Set(
    recurring.map(function(o) { return (o.emailAddress || '').trim().toLowerCase(); }).filter(Boolean)
  ));

  console.log('[sync:today] CC recurring today:', recurring.length, 'orders |', emails.length, 'unique emails');

  /* Stamp last_billed_at for any matching active CC subscription that hasn't been stamped yet today */
  const todayStart = new Date(today); todayStart.setUTCHours(0,0,0,0);
  const todayEnd   = new Date(todayStart); todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);

  const result = await db.query(`
    UPDATE subscriptions
    SET last_billed_at = NOW()
    WHERE source = 'cc'
      AND status  = 'ACTIVE'
      AND (last_billed_at IS NULL OR last_billed_at < $1)
      AND LOWER(customer_email) = ANY($2)
  `, [todayStart, emails]);

  const updated = result.rowCount || 0;
  console.log('[sync:today] stamped last_billed_at on', updated, 'CC subscriptions');
  return {
    billed:        emails.length,
    updated:       updated,
    total_orders:  allOrders.length,
    recurring_orders: recurring.length,
    orders:        recurring.map(function(o) {
      return {
        orderId:       o.orderId,
        emailAddress:  o.emailAddress,
        firstName:     o.firstName,
        lastName:      o.lastName,
        productName:   o.productName,
        totalAmount:   o.totalAmount,
        orderStatus:   o.orderStatus || o.status,
        orderType:     o.orderType,
        recurringFlag: o.recurringFlag,
        parentOrderId: o.parentOrderId,
        dateCreated:   o.dateCreated,
        shippingStatus: o.shippingStatus || null
      };
    })
  };
}

/* ════════════════════════════════════════════════════
   ORCHESTRATOR
   ════════════════════════════════════════════════════ */

async function runSyncCycle(opts) {
  opts = opts || {};
  console.log('[sync] starting cycle | full:', opts.full === true);

  const results = {};
  const sources = ['whop', 'cc', 'recharge'];

  /* Optimistically clear stored errors at the start of every cycle so a stale
     last_error (e.g. left over from a previous deploy's code) can never linger.
     If a source actually fails this run, its error is re-set in the catch below. */
  try { await db.query("UPDATE sync_state SET last_error = NULL, last_error_at = NULL"); }
  catch (e) { console.error('[sync] could not clear stale errors:', e.message); }

  for (const source of sources) {
    const start = Date.now();
    try {
      let r;
      if (source === 'cc')       r = await syncCC(opts);
      if (source === 'recharge') r = await syncRecharge(opts);
      if (source === 'whop')     r = await syncWhop(opts);
      results[source] = Object.assign({ ok: true, ms: Date.now() - start }, r);
      await setSyncState(source, opts.full
        ? { last_full_sync_at: new Date(), last_delta_sync_at: new Date(), last_error: null }
        : { last_delta_sync_at: new Date(), last_error: null });
    } catch (err) {
      console.error('[sync:' + source + '] failed:', err.message);
      results[source] = { ok: false, error: err.message, ms: Date.now() - start };
      await setSyncState(source, { last_error: err.message, last_error_at: new Date() });
    }
  }

  /* Always run today's billing sync to catch CC orders billed today
     regardless of subscription creation date (fixes 30-day delta gap) */
  try {
    const todayResult = await syncTodayBillings();
    results.todayBillings = Object.assign({ ok: true }, todayResult);
  } catch (err) {
    console.error('[sync:today] failed:', err.message);
    results.todayBillings = { ok: false, error: err.message };
  }

  console.log('[sync] cycle done', JSON.stringify(results));
  return results;
}

module.exports = {
  runSyncCycle:       runSyncCycle,
  syncCC:             syncCC,
  syncRecharge:       syncRecharge,
  syncWhop:           syncWhop,
  syncTodayBillings:  syncTodayBillings
};

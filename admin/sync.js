/* ════════════════════════════════════════════════════
   admin/sync.js — pulls from CC + Recharge into Postgres.
   Run every 5 min by node-cron.
   ════════════════════════════════════════════════════ */

const fetch = require('node-fetch');
const db    = require('./db');

const CC_BASE       = 'https://api.checkoutchamp.com';
const RECHARGE_BASE = 'https://api.rechargeapps.com';

/* ────────────────────────────────────────────────
   Helpers
   ──────────────────────────────────────────────── */

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
  let subsTouched = 0;
  let page = 1;
  const perPage = 200;
  while (page <= 50) {
    const r = await fetch(CC_BASE + '/purchase/query/?' + ccParams({
      startDate:      startDate,
      endDate:        endDate,
      resultsPerPage: perPage,
      page:           page
    }), { method: 'POST' });
    const text = await r.text();
    let d;
    try { d = JSON.parse(text); } catch (e) {
      console.error('[sync:cc] non-JSON response page', page, ':', text.substring(0, 200));
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
    while (activePage <= 50) {
      const r = await fetch(CC_BASE + '/purchase/query/?' + ccParams({
        startDate:      '01/01/2015',
        endDate:        endDate,
        status:         'ACTIVE',
        resultsPerPage: perPage,
        page:           activePage
      }), { method: 'POST' });
      const text = await r.text();
      let d;
      try { d = JSON.parse(text); } catch (e) {
        console.error('[sync:cc] non-JSON active-refresh page', activePage, ':', text.substring(0, 200));
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
    while (rfPage <= 50) {
      const r = await fetch(CC_BASE + '/purchase/query/?' + ccParams({
        startDate:      '01/01/2015',
        endDate:        endDate,
        status:         'RECYCLE_FAILED',
        resultsPerPage: perPage,
        page:           rfPage
      }), { method: 'POST' });
      const text = await r.text();
      let d;
      try { d = JSON.parse(text); } catch (e) {
        console.error('[sync:cc] non-JSON recycle-failed page', rfPage, ':', text.substring(0, 200));
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
  while (page <= 50) {
    const r = await fetch(CC_BASE + '/order/query/?' + ccParams({
      startDate:      startDate,
      endDate:        endDate,
      resultsPerPage: perPage,
      page:           page
    }), { method: 'POST' });
    const text = await r.text();
    let d;
    try { d = JSON.parse(text); } catch (e) {
      console.error('[sync:cc] non-JSON orders response page', page, ':', text.substring(0, 200));
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
    p.cancelReason || null, p
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

async function syncRecharge(opts) {
  opts = opts || {};
  const isFull = opts.full === true;

  if (!process.env.RECHARGE_API_KEY) {
    throw new Error('RECHARGE_API_KEY not set');
  }

  const headers = {
    'X-Recharge-Access-Token': process.env.RECHARGE_API_KEY,
    'Content-Type': 'application/json'
  };

  /* ── 1. Subscriptions ── */
  let subsTouched = 0;
  let page = 1;
  const limit = 250;
  while (page <= 50) {
    const url = RECHARGE_BASE + '/subscriptions?limit=' + limit + '&page=' + page;
    const r   = await fetch(url, { headers });
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
  since.setDate(since.getDate() - (isFull ? 365 * 2 : 30));
  while (page <= 50) {
    const url = RECHARGE_BASE + '/charges?limit=' + limit + '&page=' + page +
                '&created_at_min=' + since.toISOString() + '&status=success';
    const r = await fetch(url, { headers });
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
  /* Recharge sub doesn't carry email directly — need a customer lookup
     For sync efficiency we cache customer-id → email in a small map */
  const email = await getRechargeEmail(s.customer_id);
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

  const r = await fetch(RECHARGE_BASE + '/customers/' + customerId, {
    headers: { 'X-Recharge-Access-Token': process.env.RECHARGE_API_KEY }
  });
  const d = await r.json();
  const email = d.customer && d.customer.email
    ? d.customer.email.toLowerCase().trim()
    : null;
  _rcEmailCache.set(key, email);
  return email;
}

async function upsertRechargeCharge(c) {
  const email = await getRechargeEmail(c.customer_id);
  if (!email) return;

  const customerId = await db.upsertCustomer({ email: email, recharge_id: String(c.customer_id) });

  const id        = 'rc:' + c.id;
  const total     = toCents(c.total_price);
  const createdAt = parseDate(c.created_at) || new Date().toISOString();
  /* Recharge marks subsequent charges with subscription_id; first checkout has type=checkout */
  const subId     = (c.line_items && c.line_items[0] && c.line_items[0].subscription_id)
    ? 'rc:' + c.line_items[0].subscription_id
    : null;
  const type      = c.type === 'recurring' ? 'rebill' : 'initial';

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
    const r = await fetch(url, { method: 'POST' });
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
  const sources = ['cc', 'recharge'];

  for (const source of sources) {
    const start = Date.now();
    try {
      let r;
      if (source === 'cc')       r = await syncCC(opts);
      if (source === 'recharge') r = await syncRecharge(opts);
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
  syncTodayBillings:  syncTodayBillings
};

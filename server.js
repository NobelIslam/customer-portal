require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const crypto  = require('crypto');
const path    = require('path');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

const RECHARGE_API_KEY = process.env.RECHARGE_API_KEY;
const RECHARGE_BASE    = 'https://api.rechargeapps.com';
const KLAVIYO_API_KEY  = process.env.KLAVIYO_API_KEY;

/* ════════════════════════════════════════════════════
   PORTAL MODE CONFIG
   'unified' → everyone goes to /dashboard (magic link for all)
   'split'   → CC goes to /memberarea, Recharge goes to /recharge-portal
════════════════════════════════════════════════════ */
const PORTAL_MODE = process.env.PORTAL_MODE || 'unified';
const BASE_URL    = process.env.BASE_URL    || 'https://help.thegreatproject.com';

var PORTAL_URLS = {
  unified:  BASE_URL + '/dashboard',
  cc:       BASE_URL + '/memberarea',        /* split mode — CC customers */
  recharge: BASE_URL + '/recharge-portal',   /* split mode — Recharge-only */
  profile:  BASE_URL + '/unified-profile',
  login:    BASE_URL + '/login'
};

console.log('Portal mode:', PORTAL_MODE);

function rcHeaders() {
  return {
    'X-Recharge-Access-Token': RECHARGE_API_KEY,
    'Content-Type': 'application/json'
  };
}

/* ═══════════════════════════
   GET /recharge/subscriptions?email=xxx
═══════════════════════════ */
app.get('/recharge/subscriptions', async (req, res) => {
  try {
    const email = req.query.email;
    if (!email) return res.status(400).json({ error: 'email required' });

    const custRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
    const custData = await custRes.json();
    if (!custData.customers || !custData.customers.length) return res.json({ subscriptions: [] });

    const customerId = custData.customers[0].id;
    const subRes     = await fetch(RECHARGE_BASE + '/subscriptions?customer_id=' + customerId + '&limit=50', { headers: rcHeaders() });
    const subData    = await subRes.json();

    res.json({
      subscriptions: (subData.subscriptions || []).map(function(s) {
        return {
          id:                s.id,
          status:            s.status,
          productTitle:      s.product_title,
          variantTitle:      s.variant_title,
          price:             s.price,
          quantity:          s.quantity,
          nextChargeDate:    s.next_charge_scheduled_at,
          intervalFrequency: s.order_interval_frequency,
          intervalUnit:      s.order_interval_unit,
          isSkippable:       s.is_skippable,
          cancelledAt:       s.cancelled_at,
          createdAt:         s.created_at
        };
      })
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/cancel
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/cancel', async (req, res) => {
  try {
    const r    = await fetch(RECHARGE_BASE + '/subscriptions/' + req.params.id + '/cancel', {
      method: 'POST', headers: rcHeaders(),
      body: JSON.stringify({ cancellation_reason: req.body.reason || 'Customer requested' })
    });
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/pause
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/pause', async (req, res) => {
  try {
    const months = parseInt(req.body.months || 3);
    var d = new Date(); d.setMonth(d.getMonth() + months);
    var dateStr = d.toISOString().split('T')[0];
    const r    = await fetch(RECHARGE_BASE + '/subscriptions/' + req.params.id + '/set_next_charge_date', {
      method: 'POST', headers: rcHeaders(), body: JSON.stringify({ date: dateStr })
    });
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true, nextChargeDate: dateStr });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/activate
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/activate', async (req, res) => {
  try {
    const r    = await fetch(RECHARGE_BASE + '/subscriptions/' + req.params.id + '/activate', {
      method: 'POST', headers: rcHeaders(), body: JSON.stringify({})
    });
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/skip
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/skip', async (req, res) => {
  try {
    const chargeId = req.body.chargeId;
    if (!chargeId) return res.status(400).json({ error: 'chargeId required' });
    const r    = await fetch(RECHARGE_BASE + '/charges/' + chargeId + '/skip', {
      method: 'POST', headers: rcHeaders(), body: JSON.stringify({ subscription_id: req.params.id })
    });
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ═══════════════════════════════════════════════
   Klaviyo helper
═══════════════════════════════════════════════ */
async function sendKlaviyoEvent(email, eventName, properties) {
  const response = await fetch('https://a.klaviyo.com/api/events/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Klaviyo-API-Key ' + KLAVIYO_API_KEY, 'revision': '2024-10-15' },
    body: JSON.stringify({ data: { type: 'event', attributes: { metric: { data: { type: 'metric', attributes: { name: eventName } } }, profile: { data: { type: 'profile', attributes: { email } } }, properties: properties || {} } } })
  });
  const body = response.status !== 204 ? await response.json().catch(() => null) : null;
  return { ok: response.ok, status: response.status, body };
}

/* ═══════════════════════════
   POST /klaviyo/test-event
═══════════════════════════ */
app.post('/klaviyo/test-event', async (req, res) => {
  try {
    const { email, eventName, properties } = req.body;
    if (!email || !eventName) return res.status(400).json({ error: 'email and eventName are required' });
    const result = await sendKlaviyoEvent(email, eventName, properties);
    if (!result.ok) return res.status(result.status).json({ error: result.body });
    res.json({ success: true, status: result.status });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ═══════════════════════════════════════════════════════
   GET + POST /webhooks/checkoutchamp
═══════════════════════════════════════════════════════ */
async function handleCCWebhook(req, res) {
  try {
    var payload = Object.keys(req.query).length > 0 ? req.query : req.body;
    console.log('CheckoutChamp webhook received:', JSON.stringify(payload));

    var email   = payload.emailAddress || payload.email || payload.Email || '';
    var orderId = payload.orderId      || payload.OrderId || payload.order_id || '';

    var productIds = [
      payload.product1_id, payload.product2_id, payload.product3_id,
      payload.product4_id, payload.product5_id
    ].filter(Boolean).map(Number);

    var nextBillDateRaw = payload.nextBillDate || payload.next_bill_date
      || payload.nextRebillDate || payload.next_rebill_date || payload.rebillDate || '';

    if (!email) return res.status(400).json({ error: 'email not found in webhook payload' });

    var tempPassword = '';
    var nextBillDate = '';

    try {
      var CC_LOGIN_ID = process.env.CC_LOGIN_ID;
      var CC_API_PASS = process.env.CC_API_PASSWORD;
      var CC_CLUB_ID  = process.env.CC_CLUB_ID || '12';
      var today       = new Date();
      var endDate     = (today.getMonth()+1).toString().padStart(2,'0') + '/' + today.getDate().toString().padStart(2,'0') + '/' + today.getFullYear();

      var memberParams = new URLSearchParams({
        clubId: CC_CLUB_ID, loginId: CC_LOGIN_ID, password: CC_API_PASS,
        emailAddress: email, startDate: '01/01/2016', endDate: endDate, resultsPerPage: 200
      });
      var memberRes  = await fetch('https://api.checkoutchamp.com/members/query/?' + memberParams.toString(), { method: 'POST' });
      var memberData = await memberRes.json();

      if (memberData.result === 'SUCCESS' && memberData.message && memberData.message.data && memberData.message.data.length > 0) {
        var records = memberData.message.data;
        records.sort(function(a, b) { return new Date(b.dateCreated) - new Date(a.dateCreated); });
        var latest   = records[0];
        tempPassword = latest.clubPassword || '';
        var rawDate  = nextBillDateRaw || latest.nextBillDate || latest.next_bill_date || latest.nextRebillDate || '';
        if (rawDate) {
          var d = new Date(rawDate);
          if (!isNaN(d.getTime())) {
            nextBillDate = (d.getMonth()+1).toString().padStart(2,'0') + '/' + d.getDate().toString().padStart(2,'0') + '/' + d.getFullYear();
          } else { nextBillDate = rawDate; }
        }
      }
    } catch (e) { console.error('CC member lookup error in webhook:', e.message); }

    var result = await sendKlaviyoEvent(email, 'Active_Membership', {
      ProductIDs: productIds, OrderId: orderId,
      login_url: 'https://try.thegreatproject.com/login',
      temp_password: tempPassword, next_bill_date: nextBillDate,
      manage_subscription_url: 'https://try.thegreatproject.com/account'
    });

    if (!result.ok) return res.status(502).json({ error: 'Klaviyo push failed', details: result.body });
    console.log('Klaviyo Active_Membership event sent for', email);
    res.json({ success: true, event: 'Active_Membership', email, productIds, orderId, nextBillDate });
  } catch (err) { res.status(500).json({ error: err.message }); }
}

app.get('/webhooks/checkoutchamp',  handleCCWebhook);
app.post('/webhooks/checkoutchamp', handleCCWebhook);

/* ════════════════════════════════════════════════════
   TOKEN STORE — shared by magic-login + recharge-portal
════════════════════════════════════════════════════ */
/* ── Signed tokens — survives Render restarts, no in-memory store needed ── */
const TOKEN_SECRET = process.env.TOKEN_SECRET || 'tgp-portal-secret-2026';

function createToken(payload) {
  payload.expires = Date.now() + 24 * 60 * 60 * 1000;
  var data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  var sig  = crypto.createHmac('sha256', TOKEN_SECRET).update(data).digest('base64url');
  return data + '.' + sig;
}

function verifyToken(token) {
  if (!token) return null;
  var parts = token.split('.');
  if (parts.length !== 2) return null;
  var expectedSig = crypto.createHmac('sha256', TOKEN_SECRET).update(parts[0]).digest('base64url');
  if (expectedSig !== parts[1]) return null;
  try {
    var payload = JSON.parse(Buffer.from(parts[0], 'base64url').toString());
    if (payload.expires < Date.now()) return null;
    return payload;
  } catch(e) { return null; }
}

/* ════════════════════════════════════════════════════
   MAGIC LOGIN — CheckoutChamp customers
════════════════════════════════════════════════════ */

app.get('/portal-access', function(req, res) {
  res.sendFile(path.join(__dirname, 'portal-access.html'));
});

/* POST /magic-login/request
   Identifies customer → Recharge OR CC → sends correct magic link */
app.post('/magic-login/request', async function(req, res) {
  try {
    var email = (req.body.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email is required' });

    var foundIn  = [];
    var ccMember = null;

    /* ── 1. Check Recharge ── */
    try {
      var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
      var rcData = await rcRes.json();
      if (rcData.customers && rcData.customers.length > 0) foundIn.push('recharge');
    } catch (e) { console.error('Recharge lookup error:', e.message); }

    /* ── 2. Check CheckoutChamp ── */
    try {
      var CC_LOGIN_ID = process.env.CC_LOGIN_ID;
      var CC_API_PASS = process.env.CC_API_PASSWORD;
      var CC_CLUB_ID  = process.env.CC_CLUB_ID || '12';
      var CC_BASE     = 'https://api.checkoutchamp.com';
      var today       = new Date();
      var endDate     = (today.getMonth()+1).toString().padStart(2,'0') + '/' + today.getDate().toString().padStart(2,'0') + '/' + today.getFullYear();

      var params = new URLSearchParams({
        clubId: CC_CLUB_ID, loginId: CC_LOGIN_ID, password: CC_API_PASS,
        emailAddress: email, startDate: '01/01/2016', endDate: endDate, resultsPerPage: 200
      });
      var qRes  = await fetch(CC_BASE + '/members/query/?' + params.toString(), { method: 'POST' });
      var qData = await qRes.json();

      if (qData.result === 'SUCCESS' && qData.message && qData.message.data && qData.message.data.length > 0) {
        var records = qData.message.data;
        records.sort(function(a, b) { return new Date(b.dateCreated) - new Date(a.dateCreated); });
        ccMember = records[0];
        foundIn.push('checkoutchamp');
        console.log('CC member found for', email, '— memberId:', ccMember.memberId);
      }
    } catch (e) { console.error('CheckoutChamp lookup error:', e.message); }

    if (foundIn.length === 0) {
      return res.json({
        found: false,
        error: 'No subscription found for this email address.'
      });
    }

    var token;
    var magicLink;
    var isRechargeOnly = foundIn.includes('recharge') && !foundIn.includes('checkoutchamp');

    if (isRechargeOnly) {
      /* ── RECHARGE-ONLY customer ── */
      token = createToken({ email: email, type: 'recharge' });
      /* In split mode → /recharge-portal, in unified mode → /dashboard */
      magicLink = PORTAL_MODE === 'unified'
        ? PORTAL_URLS.unified + '?token=' + token
        : PORTAL_URLS.recharge + '?token=' + token;
      console.log('Recharge-only magic link for', email, '| mode:', PORTAL_MODE);

    } else {
      /* ── CC customer (with or without Recharge) ── */
      var tempPassword = ccMember ? (ccMember.clubPassword || null) : null;
      token = createToken({
        email:        email,
        type:         'checkoutchamp',
        loginType:    'club',
        memberId:     ccMember ? (ccMember.memberId     || null) : null,
        clubUsername: ccMember ? (ccMember.clubUsername || null) : null,
        password:     tempPassword
      });
      /* In split mode → /magic-login (CC login page), in unified mode → /dashboard */
      magicLink = PORTAL_MODE === 'unified'
        ? PORTAL_URLS.unified + '?token=' + token
        : PORTAL_URLS.login.replace('/login', '/magic-login') + '?token=' + token;
      console.log('CC magic link for', email, '| mode:', PORTAL_MODE);
    }

    /* ── Fire Klaviyo Magic_Link_Access event ── */
    try {
      await sendKlaviyoEvent(email, 'Magic_Link_Access', {
        magic_link:  magicLink,
        link_expiry: '24 hours',
        login_url:   'https://try.thegreatproject.com/login',
        portal_type: foundIn.includes('checkoutchamp') ? 'checkoutchamp' : 'recharge'
      });
    } catch (e) { console.error('Klaviyo event error:', e.message); }

    res.json({ found: true, foundIn, expiresIn: '24 hours' });

  } catch (err) {
    console.error('magic-login/request error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── Serve magic-login.html ── */
app.get('/magic-login', function(req, res) {
  res.sendFile(path.join(__dirname, 'magic-login.html'));
});

/* ── TEST: generate CC magic link ── */
app.get('/magic-login/test', async function(req, res) {
  var email    = (req.query.email    || '').trim().toLowerCase();
  var password = (req.query.password || '').trim();
  if (!email) return res.status(400).json({ error: 'email required' });

  var ccMember = null;
  try {
    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();
    var qRes  = await fetch(CC_BASE + '/members/query/?' + ccParams({
      emailAddress: email, startDate: '01/01/2016', endDate, resultsPerPage: 200
    }), { method: 'POST' });
    var qData = await qRes.json();
    if (qData.result === 'SUCCESS' && qData.message && qData.message.data && qData.message.data.length > 0) {
      var records = qData.message.data;
      records.sort(function(a,b){ return new Date(b.dateCreated)-new Date(a.dateCreated); });
      ccMember = records[0];
    }
  } catch(e) { console.error('Test endpoint CC lookup error:', e.message); }

  /* Use provided password or fall back to CC member password */
  var resolvedPassword = password || (ccMember ? ccMember.clubPassword : null);
  var token = createToken({
    email:        email,
    type:         'checkoutchamp',
    loginType:    'club',
    memberId:     ccMember ? (ccMember.memberId     || null) : null,
    clubUsername: ccMember ? (ccMember.clubUsername || null) : null,
    password:     resolvedPassword
  });
  var link = PORTAL_MODE === 'unified'
    ? PORTAL_URLS.unified + '?token=' + token
    : BASE_URL + '/magic-login?token=' + token;
  res.json({
    magicLink:   link,
    expiresIn:   '24 hours',
    mode:        PORTAL_MODE,
    memberFound: !!ccMember,
    memberId:    ccMember ? ccMember.memberId : null
  });
});

/* ── POST /magic-login/verify — CC portal login ── */
app.post('/magic-login/verify', function(req, res) {
  try {
    var token = req.body.token;
    var data  = verifyToken(token);
    if (!data) return res.status(401).json({ error: 'Invalid or expired token' });

    /* Token signature already proves authenticity — no CC login needed */
    console.log('Magic login verify OK for:', data.email, '| type:', data.type);
    res.json({
      success: true,
      email:   data.email,
      type:    data.type || 'checkoutchamp'
    });

  } catch (err) {
    console.error('Magic login verify error:', err);
    res.status(500).json({ error: err.message });
  }
});



/* ════════════════════════════════════════════════════
   UNIFIED PORTAL — CC API ENDPOINTS
════════════════════════════════════════════════════ */

function ccParams(extra) {
  var base = {
    loginId:  process.env.CC_LOGIN_ID,
    password: process.env.CC_API_PASSWORD
  };
  return new URLSearchParams(Object.assign(base, extra || {})).toString();
}

var CC_BASE = 'https://api.checkoutchamp.com';

/* GET /cc/customer?email=xxx — get customerId + profile */
app.get('/cc/customer', async function(req, res) {
  try {
    var email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();
    var r = await fetch(CC_BASE + '/customer/query/?' + ccParams({
      emailAddress: email, startDate: '01/01/2016', endDate, resultsPerPage: 1, sortDir: -1
    }), { method: 'POST' });
    var d = await r.json();
    if (d.result !== 'SUCCESS' || !d.message || !d.message.data || !d.message.data.length) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    var customer = d.message.data[0];
    res.json({ success: true, customer });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* GET /cc/orders?email=xxx */
app.get('/cc/orders', async function(req, res) {
  try {
    var email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();
    var r = await fetch(CC_BASE + '/order/query/?' + ccParams({
      emailAddress: email, startDate: '01/01/2016', endDate, resultsPerPage: 200, sortDir: -1
    }), { method: 'POST' });
    var d = await r.json();
    var orders = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
    res.json({ success: true, orders });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* GET /cc/order?orderId=xxx — order details */
app.get('/cc/order', async function(req, res) {
  try {
    var orderId = req.query.orderId;
    if (!orderId) return res.status(400).json({ error: 'orderId required' });
    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();
    var r = await fetch(CC_BASE + '/order/query/?' + ccParams({
      orderId, startDate: '01/01/2016', endDate, resultsPerPage: 1
    }), { method: 'POST' });
    var text = await r.text();
    var d;
    try { d = JSON.parse(text); } catch(e) {
      return res.status(500).json({ error: 'CC API error: ' + text.substring(0,100) });
    }
    if (d.result !== 'SUCCESS' || !d.message || !d.message.data || !d.message.data.length) {
      return res.status(404).json({ error: 'Order not found' });
    }
    res.json({ success: true, order: d.message.data[0] });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* GET /cc/subscriptions?email=xxx */
app.get('/cc/subscriptions', async function(req, res) {
  try {
    var email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    var CC_CLUB_ID = process.env.CC_CLUB_ID || '12';
    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();

    /* Simple email query — returns all subscriptions across all clubs */
    var subs = [];
    var page = 1, perPage = 200, keepGoing = true;
    while (keepGoing) {
      try {
        var r = await fetch(CC_BASE + '/members/query/?' + ccParams({
          emailAddress: email,
          startDate: '01/01/2016', endDate,
          resultsPerPage: perPage, page: page
        }), { method: 'POST' });
        var d = JSON.parse(await r.text());
        var pageData = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];
        subs = subs.concat(pageData);
        console.log('CC subscriptions page', page, '| got:', pageData.length, '| total:', subs.length);
        if (page === 1 && pageData.length > 0) {
          console.log('CC membership sample fields:', JSON.stringify(Object.keys(pageData[0])));
          console.log('CC membership sample record:', JSON.stringify(pageData[0]).substring(0, 500));
        }
        if (pageData.length < perPage) keepGoing = false; else page++;
        if (page > 10) keepGoing = false;
      } catch(e) { console.error('Subscriptions error:', e.message); break; }
    }

    /* Enrich with product name by fetching related order */
    if (subs.length > 0) {
      var orderIds = [...new Set(subs.map(function(s){ return s.orderId; }).filter(Boolean))];
      var orderMap = {};
      var today3  = new Date();
      var endDate3 = (today3.getMonth()+1).toString().padStart(2,'0')+'/'+today3.getDate().toString().padStart(2,'0')+'/'+today3.getFullYear();
      await Promise.all(orderIds.slice(0,50).map(async function(orderId) {
        try {
          var or   = await fetch(CC_BASE + '/order/query/?' + ccParams({
            orderId, startDate: '01/01/2016', endDate: endDate3, resultsPerPage: 1
          }), { method: 'POST' });
          var text = await or.text();
          var od   = JSON.parse(text);
          if (od.result === 'SUCCESS' && od.message && od.message.data && od.message.data.length) {
            var order = od.message.data[0];
            var items = order.items ? Object.values(order.items) : [];
            /* Keep all items that have a name — no type filtering */
            var namedItems = items.filter(function(i){ return i.name && i.name.trim(); });
            orderMap[orderId] = {
              name: namedItems.map(function(i){ return i.name; }).join(', ') || '--',
              frequency: order.billingFrequency || order.rebillFrequency || ''
            };
          }
        } catch(e) { console.error('Order fetch error for', orderId, e.message); }
      }));
      subs = subs.map(function(s) {
        if (s.orderId && orderMap[s.orderId]) {
          s.product   = orderMap[s.orderId].name;
          s.frequency = orderMap[s.orderId].frequency;
        }
        return s;
      });
    }

    res.json({ success: true, subscriptions: subs });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* GET /cc/shipments?email=xxx */
app.get('/cc/shipments', async function(req, res) {
  try {
    var email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    var today   = new Date();
    var endDate = (today.getMonth()+1).toString().padStart(2,'0')+'/'+today.getDate().toString().padStart(2,'0')+'/'+today.getFullYear();
    /* CC shipments — group orders by unique shipping address */
    var r = await fetch(CC_BASE + '/order/query/?' + ccParams({
      emailAddress: email, startDate: '01/01/2016', endDate, resultsPerPage: 200
    }), { method: 'POST' });
    var text = await r.text();
    var d;
    try { d = JSON.parse(text); } catch(e) {
      console.error('Shipments raw:', text.substring(0,200));
      return res.json({ success: true, shipments: [] });
    }
    var orders = (d.result === 'SUCCESS' && d.message && d.message.data) ? d.message.data : [];

    /* Group by unique shipping address — key = address1+city+country+zip */
    var shipMap = {};
    orders.forEach(function(o) {
      var addrKey = [
        (o.shipAddress1 || '').toLowerCase().trim(),
        (o.shipCity     || '').toLowerCase().trim(),
        (o.shipCountry  || '').toLowerCase().trim(),
        (o.shipPostalCode || o.shipZip || '').toLowerCase().trim()
      ].join('|');

      if (!addrKey.replace(/\|/g,'').trim()) return; /* skip if no address */

      if (!shipMap[addrKey]) {
        shipMap[addrKey] = {
          shipAddress1:  o.shipAddress1  || '',
          shipAddress2:  o.shipAddress2  || '',
          shipCity:      o.shipCity      || '',
          shipState:     o.shipState     || '',
          shipCountry:   o.shipCountry   || '',
          shipPostalCode: o.shipPostalCode || o.shipZip || '',
          shipFirstName: o.shipFirstName || '',
          shipLastName:  o.shipLastName  || '',
          orderIds:      [],
          lastOrderDate: ''
        };
      }
      shipMap[addrKey].orderIds.push(o.orderId);
      if (!shipMap[addrKey].lastOrderDate || o.dateCreated > shipMap[addrKey].lastOrderDate) {
        shipMap[addrKey].lastOrderDate = o.dateCreated;
      }
    });

    var shipments = Object.values(shipMap).sort(function(a,b){
      return b.lastOrderDate.localeCompare(a.lastOrderDate);
    });

    console.log('Unique shipping addresses:', shipments.length, 'from', orders.length, 'orders');
    res.json({ success: true, shipments });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* POST /cc/order/cancel  body: { orderId } */
app.post('/cc/order/cancel', async function(req, res) {
  try {
    var orderId = req.body.orderId;
    if (!orderId) return res.status(400).json({ error: 'orderId required' });
    var r = await fetch(CC_BASE + '/order/cancel/?' + ccParams({ orderId, cancelReason: req.body.reason || 'Customer requested cancellation' }), { method: 'POST' });
    var text = await r.text();
    var d;
    try { d = JSON.parse(text); } catch(e) {
      return res.status(500).json({ error: 'CC API error: ' + text.substring(0,100) });
    }
    if (d.result !== 'SUCCESS') return res.status(400).json({ error: d.message || 'Cancel failed' });
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* POST /cc/subscription/cancel  body: { purchaseId } */
app.post('/cc/subscription/cancel', async function(req, res) {
  try {
    var memberId   = req.body.memberId;
    var purchaseId = req.body.purchaseId;
    var clubId     = req.body.clubId || process.env.CC_CLUB_ID || '12';
    var reason     = req.body.reason || 'Customer requested';
    if (!memberId && !purchaseId) return res.status(400).json({ error: 'memberId or purchaseId required' });
    var params = { clubId, cancelReason: reason };
    if (memberId)   params.memberId   = memberId;
    if (purchaseId) params.purchaseId = purchaseId;
    var r    = await fetch(CC_BASE + '/members/cancel/?' + ccParams(params), { method: 'POST' });
    var text = await r.text();
    console.log('CC sub cancel HTTP:', r.status, '| raw:', text.substring(0, 300));
    var d;
    try { d = JSON.parse(text); } catch(e) {
      return res.status(500).json({ error: 'CC API error (HTTP ' + r.status + '): ' + text.substring(0, 200) });
    }
    if (d.result !== 'SUCCESS') return res.status(400).json({ error: typeof d.message === 'object' ? JSON.stringify(d.message) : (d.message || 'Cancel failed') });
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* POST /cc/subscription/pause  body: { purchaseId } */
app.post('/cc/subscription/pause', async function(req, res) {
  try {
    var memberId   = req.body.memberId;
    var purchaseId = req.body.purchaseId;
    var clubId     = req.body.clubId || process.env.CC_CLUB_ID || '12';
    if (!memberId && !purchaseId) return res.status(400).json({ error: 'memberId or purchaseId required' });
    var params = { clubId };
    if (memberId)   params.memberId   = memberId;
    if (purchaseId) params.purchaseId = purchaseId;
    var r    = await fetch(CC_BASE + '/members/pause/?' + ccParams(params), { method: 'POST' });
    var text = await r.text();
    console.log('CC sub pause HTTP:', r.status, '| raw:', text.substring(0, 300));
    var d;
    try { d = JSON.parse(text); } catch(e) {
      return res.status(500).json({ error: 'CC API error (HTTP ' + r.status + '): ' + text.substring(0, 200) });
    }
    if (d.result !== 'SUCCESS') return res.status(400).json({ error: typeof d.message === 'object' ? JSON.stringify(d.message) : (d.message || 'Pause failed') });
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* POST /cc/subscription/restart  body: { purchaseId } */
app.post('/cc/subscription/restart', async function(req, res) {
  try {
    var memberId   = req.body.memberId;
    var purchaseId = req.body.purchaseId;
    var clubId     = req.body.clubId || process.env.CC_CLUB_ID || '12';
    if (!memberId && !purchaseId) return res.status(400).json({ error: 'memberId or purchaseId required' });
    var params = { clubId };
    if (memberId)   params.memberId   = memberId;
    if (purchaseId) params.purchaseId = purchaseId;
    var r    = await fetch(CC_BASE + '/members/reactivate/?' + ccParams(params), { method: 'POST' });
    var text = await r.text();
    var fullUrl = CC_BASE + '/members/reactivate/?' + ccParams(params);
    console.log('CC sub restart URL:', fullUrl);
    console.log('CC sub restart HTTP:', r.status, '| raw:', text.substring(0, 300));
    var d;
    try { d = JSON.parse(text); } catch(e) {
      return res.status(500).json({ error: 'CC API error (HTTP ' + r.status + '): ' + text.substring(0, 200) });
    }
    if (d.result !== 'SUCCESS') return res.status(400).json({ error: typeof d.message === 'object' ? JSON.stringify(d.message) : (d.message || 'Restart failed') });
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* POST /cc/profile/update  body: { customerId, firstName, lastName, phone, address... } */
app.post('/cc/profile/update', async function(req, res) {
  try {
    var b = req.body;
    if (!b.customerId) return res.status(400).json({ error: 'customerId required' });
    var params = ccParams({
      customerId:   b.customerId,
      firstName:    b.firstName    || '',
      lastName:     b.lastName     || '',
      phoneNumber:  b.phone        || '',
      address1:     b.address1     || '',
      address2:     b.address2     || '',
      city:         b.city         || '',
      state:        b.state        || '',
      country:      b.country      || '',
      postalCode:   b.postalCode   || '',
      shipAddress1: b.shipAddress1 || '',
      shipAddress2: b.shipAddress2 || '',
      shipCity:     b.shipCity     || '',
      shipState:    b.shipState    || '',
      shipCountry:  b.shipCountry  || '',
      shipPostalCode: b.shipPostalCode || ''
    });
    var r = await fetch(CC_BASE + '/customer/update/?' + params, { method: 'POST' });
    var d = await r.json();
    if (d.result !== 'SUCCESS') return res.status(400).json({ error: d.message || 'Update failed' });
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

/* ════════════════════════════════════════════════════
   RECHARGE CUSTOMER PROFILE
════════════════════════════════════════════════════ */

/* GET /recharge/customer?email=xxx — fetch profile */
app.get('/recharge/customer', async function(req, res) {
  try {
    var email = (req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
    var rcData = await rcRes.json();

    if (!rcData.customers || !rcData.customers.length) {
      return res.status(404).json({ error: 'Customer not found' });
    }

    var c = rcData.customers[0];
    res.json({
      id:              c.id,
      email:           c.email,
      firstName:       c.first_name || '',
      lastName:        c.last_name  || '',
      phone:           c.phone      || '',
      billingAddress: {
        address1:   c.billing_address1   || '',
        address2:   c.billing_address2   || '',
        city:       c.billing_city       || '',
        province:   c.billing_province   || '',
        country:    c.billing_country    || '',
        zip:        c.billing_zip        || ''
      },
      shippingAddress: {
        address1:   c.shipping_address && c.shipping_address.address1 || '',
        address2:   c.shipping_address && c.shipping_address.address2 || '',
        city:       c.shipping_address && c.shipping_address.city     || '',
        province:   c.shipping_address && c.shipping_address.province || '',
        country:    c.shipping_address && c.shipping_address.country  || '',
        zip:        c.shipping_address && c.shipping_address.zip      || ''
      }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* POST /recharge/customer/update — update profile */
app.post('/recharge/customer/update', async function(req, res) {
  try {
    var email = (req.body.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email required' });

    /* Find customer ID first */
    var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
    var rcData = await rcRes.json();
    if (!rcData.customers || !rcData.customers.length) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    var customerId = rcData.customers[0].id;

    /* Build update payload — never update email */
    var payload = {};
    if (req.body.firstName)  payload.first_name = req.body.firstName;
    if (req.body.lastName)   payload.last_name  = req.body.lastName;
    if (req.body.phone)      payload.phone       = req.body.phone;

    if (req.body.billingAddress) {
      var b = req.body.billingAddress;
      if (b.address1) payload.billing_address1  = b.address1;
      if (b.address2) payload.billing_address2  = b.address2;
      if (b.city)     payload.billing_city      = b.city;
      if (b.province) payload.billing_province  = b.province;
      if (b.country)  payload.billing_country   = b.country;
      if (b.zip)      payload.billing_zip       = b.zip;
    }

    if (req.body.shippingAddress) {
      var s = req.body.shippingAddress;
      payload.shipping_address = {};
      if (s.address1) payload.shipping_address.address1 = s.address1;
      if (s.address2) payload.shipping_address.address2 = s.address2;
      if (s.city)     payload.shipping_address.city     = s.city;
      if (s.province) payload.shipping_address.province = s.province;
      if (s.country)  payload.shipping_address.country  = s.country;
      if (s.zip)      payload.shipping_address.zip      = s.zip;
    }

    var updateRes  = await fetch(RECHARGE_BASE + '/customers/' + customerId, {
      method:  'PUT',
      headers: rcHeaders(),
      body:    JSON.stringify(payload)
    });
    var updateData = await updateRes.json();

    if (!updateRes.ok) return res.status(updateRes.status).json({ error: updateData.error || 'Update failed' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ════════════════════════════════════════════════════
   RECHARGE PORTAL — Recharge-only customers
════════════════════════════════════════════════════ */

/* ── TEST: generate Recharge portal magic link ── */
app.get('/recharge-portal/test', async function(req, res) {
  var email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email required' });

  /* Check email exists in Recharge before generating link */
  try {
    var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
    var rcData = await rcRes.json();
    if (!rcData.customers || rcData.customers.length === 0) {
      return res.status(404).json({ error: 'No Recharge subscription found for this email.' });
    }
  } catch (e) {
    return res.status(500).json({ error: 'Could not verify Recharge subscription: ' + e.message });
  }

  var token = createToken({ email, type: 'recharge' });
  var link = PORTAL_MODE === 'unified'
    ? PORTAL_URLS.unified + '?token=' + token
    : PORTAL_URLS.recharge + '?token=' + token;
  res.json({ magicLink: link, expiresIn: '24 hours', mode: PORTAL_MODE });
});

/* ── POST /recharge-portal/verify ── */
app.post('/recharge-portal/verify', async function(req, res) {
  var token = req.body.token;
  var data   = verifyToken(token);
  if (!data) return res.status(401).json({ error: 'Invalid or expired token' });

  /* Verify email actually exists in Recharge */
  try {
    var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(data.email), { headers: rcHeaders() });
    var rcData = await rcRes.json();
    if (!rcData.customers || rcData.customers.length === 0) {
      console.log('Recharge portal verify failed — no Recharge customer for:', data.email);
      return res.status(403).json({ error: 'No Recharge subscription found for this email.' });
    }
  } catch (e) {
    console.error('Recharge verify check error:', e.message);
    return res.status(500).json({ error: 'Could not verify subscription. Please try again.' });
  }

  console.log('Recharge portal verify OK for:', data.email);
  res.json({ success: true, email: data.email });
});

/* ════════════════════════════════════════════════════ */
app.get('/health', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, function() {
  console.log('Server running on port ' + PORT);
});

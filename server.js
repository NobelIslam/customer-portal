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
const tokenStore = {};

setInterval(function() {
  var now = Date.now();
  Object.keys(tokenStore).forEach(function(t) {
    if (tokenStore[t].expires < now) delete tokenStore[t];
  });
}, 10 * 60 * 1000);

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

    var token    = crypto.randomBytes(32).toString('hex');
    var BASE_URL = process.env.BASE_URL || 'https://help.thegreatproject.com';
    var magicLink;

    /* ── RECHARGE-ONLY customer → recharge-portal ── */
    if (foundIn.includes('recharge') && !foundIn.includes('checkoutchamp')) {
      tokenStore[token] = {
        email:     email,
        type:      'recharge',
        expires:   Date.now() + 24 * 60 * 60 * 1000
      };
      magicLink = BASE_URL + '/recharge-portal?token=' + token;
      console.log('Recharge-only magic link for', email);

    /* ── CC customer (with or without Recharge) → CC portal ── */
    } else {
      var tempPassword = ccMember ? (ccMember.clubPassword || null) : null;
      tokenStore[token] = {
        email:        email,
        type:         'checkoutchamp',
        loginType:    'club',
        memberId:     ccMember ? (ccMember.memberId    || null) : null,
        clubUsername: ccMember ? (ccMember.clubUsername || null) : null,
        password:     tempPassword,
        expires:      Date.now() + 24 * 60 * 60 * 1000
      };
      magicLink = BASE_URL + '/magic-login?token=' + token;
      console.log('CC magic link for', email);
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
app.get('/magic-login/test', function(req, res) {
  var email    = (req.query.email    || '').trim().toLowerCase();
  var password = (req.query.password || '').trim();
  if (!email || !password) return res.status(400).json({ error: 'email and password required' });
  var token = crypto.randomBytes(32).toString('hex');
  tokenStore[token] = { email, password, type: 'checkoutchamp', expires: Date.now() + 15 * 60 * 1000 };
  res.json({
    magicLink: 'https://help.thegreatproject.com/magic-login?token=' + token,
    expiresIn: '15 minutes'
  });
});

/* ── POST /magic-login/verify — CC portal login ── */
app.post('/magic-login/verify', async function(req, res) {
  try {
    var token = req.body.token;
    if (!token || !tokenStore[token]) return res.status(401).json({ error: 'Invalid or expired token' });
    var data = tokenStore[token];
    if (data.expires < Date.now()) { delete tokenStore[token]; return res.status(401).json({ error: 'Token expired' }); }

    if (!data.password) {
      delete tokenStore[token];
      return res.json({ success: true, email: data.email });
    }

    var CC_CLUB_ID  = process.env.CC_CLUB_ID || '12';
    var CC_LOGIN_ID = process.env.CC_LOGIN_ID;
    var CC_API_PASS = process.env.CC_API_PASSWORD;
    var CC_BASE     = 'https://api.checkoutchamp.com';

    var loginRes  = await fetch(CC_BASE + '/members/login/?' + new URLSearchParams({
      clubId: CC_CLUB_ID, clubUsername: data.email, clubPassword: data.password,
      loginId: CC_LOGIN_ID, password: CC_API_PASS
    }).toString(), { method: 'POST' });
    var loginData = await loginRes.json();
    console.log('CC login for', data.email, ':', JSON.stringify(loginData).substring(0, 200));

    if (loginData.result === 'Error' || loginData.result === 'ERROR') {
      return res.status(401).json({ error: 'Login failed: ' + JSON.stringify(loginData.message) });
    }

    var loginMsg = {};
    try { loginMsg = typeof loginData.message === 'string' ? JSON.parse(loginData.message) : loginData.message; } catch(e) {}

    delete tokenStore[token];
    res.json({ success: true, email: data.email, password: data.password, status: loginMsg.status || 'ACTIVE' });

  } catch (err) {
    console.error('Magic login error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ════════════════════════════════════════════════════
   RECHARGE PORTAL — Recharge-only customers
════════════════════════════════════════════════════ */

/* ── TEST: generate Recharge portal magic link ── */
app.get('/recharge-portal/test', function(req, res) {
  var email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email required' });
  var token = crypto.randomBytes(32).toString('hex');
  tokenStore[token] = { email, type: 'recharge', expires: Date.now() + 24 * 60 * 60 * 1000 };
  res.json({
    magicLink: 'https://help.thegreatproject.com/recharge-portal?token=' + token,
    expiresIn: '24 hours'
  });
});

/* ── POST /recharge-portal/verify ── */
app.post('/recharge-portal/verify', function(req, res) {
  var token = req.body.token;
  if (!token || !tokenStore[token]) return res.status(401).json({ error: 'Invalid or expired token' });
  var data = tokenStore[token];
  if (data.expires < Date.now()) { delete tokenStore[token]; return res.status(401).json({ error: 'Token expired' }); }
  /* Keep token alive — don't delete, Recharge portal uses session */
  console.log('Recharge portal verify for:', data.email);
  res.json({ success: true, email: data.email });
});

/* ════════════════════════════════════════════════════ */
app.get('/health', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, function() {
  console.log('Server running on port ' + PORT);
});

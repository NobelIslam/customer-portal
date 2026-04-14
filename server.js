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

/* ═══════════════════════════
   POST /webhooks/checkoutchamp
═══════════════════════════ */
app.post('/webhooks/checkoutchamp', async (req, res) => {
  try {
    const { email, eventName, properties } = req.body;
    if (!email || !eventName) return res.status(400).json({ error: 'email and eventName are required' });
    console.log('CheckoutChamp webhook received:', { email, eventName });
    const result = await sendKlaviyoEvent(email, eventName, properties);
    if (!result.ok) return res.status(502).json({ error: 'Klaviyo push failed', details: result.body });
    console.log('Klaviyo event pushed:', eventName, 'for', email);
    res.json({ success: true, event: eventName, email });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ════════════════════════════════════════════════════
   MAGIC LOGIN
════════════════════════════════════════════════════ */
const tokenStore = {};

setInterval(function() {
  var now = Date.now();
  Object.keys(tokenStore).forEach(function(t) {
    if (tokenStore[t].expires < now) delete tokenStore[t];
  });
}, 10 * 60 * 1000);

/* ── Serve portal-access.html ── */
app.get('/portal-access', function(req, res) {
  res.sendFile(path.join(__dirname, 'portal-access.html'));
});

/* ══════════════════════════════════════════════════════════════
   POST /magic-login/request
   body: { email }

   1. Checks Recharge for a matching customer
   2. Checks CheckoutChamp for a matching customer
   3. If found in either → generates a magic-link token
   4. Returns the magic link directly (email sending skipped for now)
══════════════════════════════════════════════════════════════ */
app.post('/magic-login/request', async function(req, res) {
  try {
    var email = (req.body.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'email is required' });

    var foundIn = [];

    /* ── 1. Check Recharge ── */
    try {
      var rcRes  = await fetch(RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email), { headers: rcHeaders() });
      var rcData = await rcRes.json();
      if (rcData.customers && rcData.customers.length > 0) {
        foundIn.push('recharge');
      }
    } catch (e) {
      console.error('Recharge lookup error:', e.message);
    }

    /* ── 2. Check CheckoutChamp via members/query (direct email filter) ── */
    var ccMember = null;
    try {
      var CC_LOGIN_ID = process.env.CC_LOGIN_ID;
      var CC_API_PASS = process.env.CC_API_PASSWORD;
      var CC_CLUB_ID  = process.env.CC_CLUB_ID || '12';
      var CC_BASE     = 'https://api.checkoutchamp.com';

      var today     = new Date();
      var endDate   = (today.getMonth()+1).toString().padStart(2,'0') + '/' + today.getDate().toString().padStart(2,'0') + '/' + today.getFullYear();

      var params = new URLSearchParams({
        clubId:         CC_CLUB_ID,
        loginId:        CC_LOGIN_ID,
        password:       CC_API_PASS,
        emailAddress:   email,
        startDate:      '01/01/2016',
        endDate:        endDate,
        resultsPerPage: 1
      });

      var qRes  = await fetch(CC_BASE + '/members/query/?' + params.toString(), { method: 'POST' });
      var qData = await qRes.json();

      if (qData.result === 'SUCCESS' && qData.message && qData.message.data && qData.message.data.length > 0) {
        ccMember = qData.message.data[0];
        foundIn.push('checkoutchamp');
        console.log('CC member found for', email, '— status:', ccMember.status);
      } else {
        console.log('CC member not found for', email);
      }
    } catch (e) {
      console.error('CheckoutChamp lookup error:', e.message);
    }

    if (foundIn.length === 0) {
      return res.json({
        found: false,
        error: 'No subscription found for this email address. Please check you used the same email as your purchase.'
      });
    }

    /* ── 3. Generate magic token ── */
    var token    = crypto.randomBytes(32).toString('hex');
    var BASE_URL = process.env.BASE_URL || 'http://localhost:' + (process.env.PORT || 3000);
    tokenStore[token] = {
      email:         email,
      memberId:      ccMember ? (ccMember.memberId      || null) : null,
      clubUsername:  ccMember ? (ccMember.clubUsername  || null) : null,
      password:      ccMember ? (ccMember.clubPassword  || null) : null,
      expires:       Date.now() + 15 * 60 * 1000
    };

    var magicLink = BASE_URL + '/magic-login?token=' + token;
    console.log('Magic link generated for', email, '— found in:', foundIn.join(', '));

    /* ── 4. Return link (email sending skipped for now) ── */
    res.json({
      found:     true,
      foundIn:   foundIn,
      magicLink: magicLink,
      expiresIn: '15 minutes'
    });

  } catch (err) {
    console.error('magic-login/request error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── Serve magic-login.html ── */
app.get('/magic-login', function(req, res) {
  res.sendFile(path.join(__dirname, 'magic-login.html'));
});

/* ── TEST: generate magic link ──
   GET /magic-login/test?email=xxx&password=xxx */
app.get('/magic-login/test', function(req, res) {
  var email    = (req.query.email    || '').trim().toLowerCase();
  var password = (req.query.password || '').trim();
  if (!email || !password) return res.status(400).json({ error: 'email and password required' });
  var token = crypto.randomBytes(32).toString('hex');
  tokenStore[token] = { email, password, expires: Date.now() + 15 * 60 * 1000 };
  res.json({
    magicLink: 'https://try.thegreatproject.com/magic-login?token=' + token,
    expiresIn: '15 minutes'
  });
});

/* ── LOGIN: validate token + get real CC session + return to browser ──
   GET /magic-login/verify?token=xxx                                   */
app.post('/magic-login/verify', async function(req, res) {
  try {
    var token = req.body.token;

    if (!token || !tokenStore[token]) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    var data = tokenStore[token];
    if (data.expires < Date.now()) {
      delete tokenStore[token];
      return res.status(401).json({ error: 'Token expired' });
    }
    var CC_CLUB_ID       = process.env.CC_CLUB_ID || '12';
    var CC_COMPANY_TOKEN = 'ef04a8c0-b281-11ef-82be-b17d7998efda';
    var CC_FUNNEL_REF    = 'b0267726-11c5-491f-bdd5-62cfd0a19248';
    var CC_PAGES_API     = 'https://pages-live-api.checkoutchamp.com';

    /* ── Email-only token (from /magic-login/request — no password) ── */
    if (!data.password) {
      delete tokenStore[token];
      console.log('Email-only magic login for:', data.email);
      return res.json({ success: true, email: data.email });
    }

    /* ── Password token (from /magic-login/test) — full CC login ── */
    var loginRes  = await fetch('https://api.checkoutchamp.com/members/login/?' + new URLSearchParams({
      clubId:       CC_CLUB_ID,
      clubUsername: data.email,
      clubPassword: data.password,
      loginId:      process.env.CC_LOGIN_ID,
      password:     process.env.CC_API_PASSWORD
    }).toString(), { method: 'POST' });
    var loginData = await loginRes.json();
    console.log('CC login for', data.email, ':', JSON.stringify(loginData).substring(0, 200));

    if (loginData.result === 'Error' || loginData.result === 'ERROR') {
      return res.status(401).json({ error: 'Login failed: ' + JSON.stringify(loginData.message) });
    }

    /* Parse login message to get memberId */
    var loginMsg = {};
    try {
      loginMsg = typeof loginData.message === 'string'
        ? JSON.parse(loginData.message)
        : loginData.message;
    } catch(e) {}

    var memberId = loginMsg.memberId || '';

    /* Fetch customerOrders and customerPurchases from CC API */
    var CC_LOGIN_ID = process.env.CC_LOGIN_ID;
    var CC_API_PASS = process.env.CC_API_PASSWORD;
    var CC_BASE     = 'https://api.checkoutchamp.com';

    var customerOrders    = [];
    var customerPurchases = {};

    try {
      var memberUrl  = CC_BASE + '/members/?' + new URLSearchParams({
        memberId: memberId, loginId: CC_LOGIN_ID, password: CC_API_PASS
      });
      var memberRes  = await fetch(memberUrl);
      var memberData = JSON.parse(await memberRes.text());
      console.log('Member data result:', memberData.result);

      var member = memberData.message && memberData.message[0] ? memberData.message[0] : null;

      if (member && member.customerId) {
        var ordersRes  = await fetch(CC_BASE + '/order/?' + new URLSearchParams({
          customerId: member.customerId, loginId: CC_LOGIN_ID, password: CC_API_PASS
        }));
        var ordersData = await ordersRes.json();
        if (ordersData.message) customerOrders = ordersData.message;

        var purchasesRes  = await fetch(CC_BASE + '/membership/?' + new URLSearchParams({
          customerId: member.customerId, clubId: CC_CLUB_ID, loginId: CC_LOGIN_ID, password: CC_API_PASS
        }));
        var purchasesData = await purchasesRes.json();
        if (purchasesData.message) {
          var purchases = Array.isArray(purchasesData.message) ? purchasesData.message : [];
          purchases.forEach(function(p) {
            if (p.purchaseId) customerPurchases[p.purchaseId] = p;
          });
        }
      }
    } catch(e) {
      console.error('Error fetching customer data:', e.message);
    }

    delete tokenStore[token];

    res.json({
      success:  true,
      email:    data.email,
      password: data.password,
      status:   loginMsg.status || 'ACTIVE'
    });

  } catch (err) {
    console.error('Magic login error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ════════════════════════════════════════════════════ */
app.get('/health', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, function() {
  console.log('Server running on port ' + PORT);
});

const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');

const app = express();
app.use(express.json());

app.use(cors({
  origin: '*' // restrict to your CC domain in production
}));

const RECHARGE_API_KEY  = process.env.RECHARGE_API_KEY;
const RECHARGE_BASE     = 'https://api.rechargeapps.com';
const KLAVIYO_API_KEY   = process.env.KLAVIYO_API_KEY || 'pk_8adb059279aa6cc149c08cf14acdaa6cc9';

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

    /* Find customer */
    const custRes = await fetch(
      RECHARGE_BASE + '/customers?email=' + encodeURIComponent(email),
      { headers: rcHeaders() }
    );
    const custData = await custRes.json();
    if (!custData.customers || !custData.customers.length) {
      return res.json({ subscriptions: [] });
    }
    const customerId = custData.customers[0].id;

    /* Get subscriptions */
    const subRes  = await fetch(
      RECHARGE_BASE + '/subscriptions?customer_id=' + customerId + '&limit=50',
      { headers: rcHeaders() }
    );
    const subData = await subRes.json();

    res.json({
      subscriptions: (subData.subscriptions || []).map(function(s) {
        return {
          id:               s.id,
          status:           s.status,
          productTitle:     s.product_title,
          variantTitle:     s.variant_title,
          price:            s.price,
          quantity:         s.quantity,
          nextChargeDate:   s.next_charge_scheduled_at,
          intervalFrequency: s.order_interval_frequency,
          intervalUnit:     s.order_interval_unit,
          isSkippable:      s.is_skippable,
          cancelledAt:      s.cancelled_at,
          createdAt:        s.created_at
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
   body: { reason }
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/cancel', async (req, res) => {
  try {
    const r = await fetch(
      RECHARGE_BASE + '/subscriptions/' + req.params.id + '/cancel',
      {
        method: 'POST',
        headers: rcHeaders(),
        body: JSON.stringify({
          cancellation_reason: req.body.reason || 'Customer requested'
        })
      }
    );
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/pause
   Recharge Classic pause = set next_charge_scheduled_at to far future
   body: { months } — how many months to pause (default 3)
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/pause', async (req, res) => {
  try {
    const months = parseInt(req.body.months || 3);
    var d = new Date();
    d.setMonth(d.getMonth() + months);
    var dateStr = d.toISOString().split('T')[0]; // YYYY-MM-DD

    const r = await fetch(
      RECHARGE_BASE + '/subscriptions/' + req.params.id + '/set_next_charge_date',
      {
        method: 'POST',
        headers: rcHeaders(),
        body: JSON.stringify({ date: dateStr })
      }
    );
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true, nextChargeDate: dateStr });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/activate
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/activate', async (req, res) => {
  try {
    const r = await fetch(
      RECHARGE_BASE + '/subscriptions/' + req.params.id + '/activate',
      { method: 'POST', headers: rcHeaders(), body: JSON.stringify({}) }
    );
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════
   POST /recharge/subscriptions/:id/skip
   Skip next charge (only if is_skippable = true)
═══════════════════════════ */
app.post('/recharge/subscriptions/:id/skip', async (req, res) => {
  try {
    const chargeId = req.body.chargeId;
    if (!chargeId) return res.status(400).json({ error: 'chargeId required' });
    const r = await fetch(
      RECHARGE_BASE + '/charges/' + chargeId + '/skip',
      { method: 'POST', headers: rcHeaders(), body: JSON.stringify({ subscription_id: req.params.id }) }
    );
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json(data);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════════════════════════
   Klaviyo helper — sends a single event to Klaviyo
   Returns { ok, status, body }
═══════════════════════════════════════════════ */
async function sendKlaviyoEvent(email, eventName, properties) {
  const response = await fetch('https://a.klaviyo.com/api/events/', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Klaviyo-API-Key ' + KLAVIYO_API_KEY,
      'revision': '2024-10-15'
    },
    body: JSON.stringify({
      data: {
        type: 'event',
        attributes: {
          metric: {
            data: {
              type: 'metric',
              attributes: { name: eventName }
            }
          },
          profile: {
            data: {
              type: 'profile',
              attributes: { email: email }
            }
          },
          properties: properties || {}
        }
      }
    })
  });

  const body = response.status !== 204 ? await response.json().catch(() => null) : null;
  return { ok: response.ok, status: response.status, body };
}

/* ═══════════════════════════
   POST /klaviyo/test-event
   body: { email, eventName, properties }
═══════════════════════════ */
app.post('/klaviyo/test-event', async (req, res) => {
  try {
    const { email, eventName, properties } = req.body;
    if (!email || !eventName) {
      return res.status(400).json({ error: 'email and eventName are required' });
    }

    const result = await sendKlaviyoEvent(email, eventName, properties);
    if (!result.ok) {
      console.error('Klaviyo error:', result.body);
      return res.status(result.status).json({ error: result.body });
    }

    res.json({ success: true, status: result.status });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════════════════════════════════
   POST /webhooks/checkoutchamp
   Receives CheckoutChamp webhook events and forwards them
   to Klaviyo as profile events.

   Expected payload:
   {
     "email": "customer@example.com",
     "eventName": "Active_Membership",
     "properties": {
       "PurchaseID": "...",
       "OrderId": "...",
       "temp_password": "...",
       "login_url": "...",
       "manage_subscription_url": "..."
     }
   }
═══════════════════════════════════════════════════════ */
app.post('/webhooks/checkoutchamp', async (req, res) => {
  try {
    const { email, eventName, properties } = req.body;

    if (!email || !eventName) {
      return res.status(400).json({ error: 'email and eventName are required' });
    }

    console.log('CheckoutChamp webhook received:', { email, eventName });

    const result = await sendKlaviyoEvent(email, eventName, properties);

    if (!result.ok) {
      console.error('Klaviyo push failed:', result.status, result.body);
      return res.status(502).json({ error: 'Klaviyo push failed', details: result.body });
    }

    console.log('Klaviyo event pushed successfully:', eventName, 'for', email);
    res.json({ success: true, event: eventName, email });
  } catch (err) {
    console.error('CheckoutChamp webhook error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, function() {
  console.log('Recharge proxy running on port ' + PORT);
});

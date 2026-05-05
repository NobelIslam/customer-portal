/* ════════════════════════════════════════════════════
   admin/auth.js — magic-link auth for the dashboard
   Email allowlist via ADMIN_EMAILS env var.
   ════════════════════════════════════════════════════ */

const crypto = require('crypto');
const fetch  = require('node-fetch');
const db     = require('./db');

const SECRET     = process.env.ADMIN_TOKEN_SECRET || process.env.TOKEN_SECRET || 'tgp-admin-default-change-me';
const KLAVIYO_KEY = process.env.KLAVIYO_API_KEY;
const BASE_URL   = process.env.BASE_URL || 'https://customer-portal-vl02.onrender.com';

function getAllowlist() {
  return (process.env.ADMIN_EMAILS || '')
    .split(',')
    .map(function(e) { return e.trim().toLowerCase(); })
    .filter(Boolean);
}

function isAllowed(email) {
  const list = getAllowlist();
  return list.indexOf((email || '').trim().toLowerCase()) !== -1;
}

/* ── Signed session token ────────────────────────── */

function createSessionToken(email, days) {
  days = days || 7;
  const payload = {
    email:   email.toLowerCase(),
    expires: Date.now() + days * 24 * 60 * 60 * 1000,
    nonce:   crypto.randomBytes(8).toString('hex')
  };
  const data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig  = crypto.createHmac('sha256', SECRET).update(data).digest('base64url');
  return data + '.' + sig;
}

function verifySessionToken(token) {
  if (!token) return null;
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const expectedSig = crypto.createHmac('sha256', SECRET).update(parts[0]).digest('base64url');
  if (expectedSig !== parts[1]) return null;
  try {
    const payload = JSON.parse(Buffer.from(parts[0], 'base64url').toString());
    if (payload.expires < Date.now()) return null;
    if (!isAllowed(payload.email)) return null;
    return payload;
  } catch (e) { return null; }
}

/* ── Express middleware ──────────────────────────── */

function requireAdmin(req, res, next) {
  const auth = req.headers.authorization || '';
  const tok  = auth.startsWith('Bearer ') ? auth.slice(7) : (req.query.token || '');
  const data = verifySessionToken(tok);
  if (!data) return res.status(401).json({ error: 'Unauthorized' });
  req.admin = data;
  next();
}

/* ── Magic-link request ──────────────────────────── */

async function requestMagicLink(email) {
  email = (email || '').trim().toLowerCase();
  if (!email) throw new Error('email required');
  if (!isAllowed(email)) {
    /* Don't leak which emails are allowed — pretend success */
    console.log('[admin-auth] non-allowlisted attempt:', email);
    return { sent: true };
  }

  /* one-time token, separate from session — short TTL */
  const oneTime = crypto.randomBytes(24).toString('base64url');
  const expiresAt = new Date(Date.now() + 15 * 60 * 1000); /* 15 min */

  await db.query(
    'INSERT INTO admin_tokens (token, email, expires_at) VALUES ($1, $2, $3)',
    [oneTime, email, expiresAt]
  );

  const link = BASE_URL + '/admin/auth/verify?t=' + oneTime;

  /* Always log to console as a fallback so dev can grab the link */
  console.log('[admin-auth] magic link for', email, ':', link);

  /* Try Klaviyo if available — uses the same Admin_Magic_Link event
     pattern as the customer-side magic links. Requires a Klaviyo
     flow listening for that event with the email template. */
  if (KLAVIYO_KEY) {
    try {
      await sendKlaviyoEvent(email, 'Admin_Magic_Link', {
        magic_link:  link,
        link_expiry: '15 minutes'
      });
      console.log('[admin-auth] Klaviyo event sent to', email);
    } catch (e) {
      console.error('[admin-auth] Klaviyo error:', e.message);
    }
  }

  return { sent: true };
}

async function sendKlaviyoEvent(email, eventName, properties) {
  const r = await fetch('https://a.klaviyo.com/api/events/', {
    method: 'POST',
    headers: {
      'accept':        'application/json',
      'revision':      '2024-10-15',
      'content-type':  'application/json',
      'Authorization': 'Klaviyo-API-Key ' + KLAVIYO_KEY
    },
    body: JSON.stringify({
      data: {
        type: 'event',
        attributes: {
          properties:    properties,
          metric:        { data: { type: 'metric', attributes: { name: eventName } } },
          profile:       { data: { type: 'profile', attributes: { email: email } } }
        }
      }
    })
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error('Klaviyo ' + r.status + ': ' + t.substring(0, 200));
  }
}

/* ── Verify one-time token, swap for session token ─ */

async function verifyMagicLink(oneTime) {
  if (!oneTime) throw new Error('token required');

  const row = await db.one(
    'SELECT email, expires_at, used_at FROM admin_tokens WHERE token = $1',
    [oneTime]
  );
  if (!row) throw new Error('invalid token');
  if (row.used_at) throw new Error('token already used');
  if (new Date(row.expires_at).getTime() < Date.now()) throw new Error('token expired');
  if (!isAllowed(row.email)) throw new Error('not allowed');

  await db.query('UPDATE admin_tokens SET used_at = NOW() WHERE token = $1', [oneTime]);

  const session = createSessionToken(row.email);
  return { email: row.email, session: session };
}

module.exports = {
  requireAdmin:     requireAdmin,
  requestMagicLink: requestMagicLink,
  verifyMagicLink:  verifyMagicLink,
  isAllowed:        isAllowed,
  getAllowlist:     getAllowlist
};

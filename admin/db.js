/* ════════════════════════════════════════════════════
   admin/db.js — Postgres pool + migration runner
   Auto-applies SQL files in admin/migrations on boot.
   ════════════════════════════════════════════════════ */

const { Pool } = require('pg');
const fs       = require('fs');
const path     = require('path');

if (!process.env.DATABASE_URL) {
  console.error('[db] DATABASE_URL not set — admin dashboard will not work');
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('render.com')
    ? { rejectUnauthorized: false }
    : false,
  max: 10,
  idleTimeoutMillis: 30000
});

pool.on('error', function(err) {
  console.error('[db] unexpected pool error:', err.message);
});

/* ── Query helpers ───────────────────────────────── */

async function query(text, params) {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    const ms  = Date.now() - start;
    if (ms > 500) console.log('[db] slow query', ms + 'ms:', text.substring(0, 80));
    return res;
  } catch (err) {
    console.error('[db] query failed:', err.message, '| sql:', text.substring(0, 120));
    throw err;
  }
}

async function one(text, params) {
  const r = await query(text, params);
  return r.rows[0] || null;
}

async function many(text, params) {
  const r = await query(text, params);
  return r.rows;
}

/* ── Migration runner ────────────────────────────── */

async function runMigrations() {
  if (!process.env.DATABASE_URL) {
    console.warn('[db] skipping migrations — DATABASE_URL not set');
    return;
  }

  /* tracking table */
  await query(`
    CREATE TABLE IF NOT EXISTS _migrations (
      filename   TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  const dir   = path.join(__dirname, 'migrations');
  if (!fs.existsSync(dir)) {
    console.log('[db] no migrations directory at', dir);
    return;
  }

  const files = fs.readdirSync(dir).filter(function(f) { return f.endsWith('.sql'); }).sort();
  const applied = await many('SELECT filename FROM _migrations');
  const appliedSet = new Set(applied.map(function(r) { return r.filename; }));

  for (const file of files) {
    if (appliedSet.has(file)) continue;
    console.log('[db] applying migration:', file);
    const sql = fs.readFileSync(path.join(dir, file), 'utf8');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('INSERT INTO _migrations (filename) VALUES ($1)', [file]);
      await client.query('COMMIT');
      console.log('[db] applied:', file);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('[db] migration failed:', file, '|', err.message);
      throw err;
    } finally {
      client.release();
    }
  }
}

/* ── Customer upsert helper ─────────────────────── */
/* Used by every sync worker to ensure the customer row exists
   and get back the local id for FK references.                 */

async function upsertCustomer(opts) {
  const email = (opts.email || '').trim().toLowerCase();
  if (!email) return null;

  const sql = `
    INSERT INTO customers (email, cc_member_id, recharge_id, subi_id, first_name, last_name, last_seen_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW())
    ON CONFLICT (email) DO UPDATE SET
      cc_member_id = COALESCE(EXCLUDED.cc_member_id, customers.cc_member_id),
      recharge_id  = COALESCE(EXCLUDED.recharge_id,  customers.recharge_id),
      subi_id      = COALESCE(EXCLUDED.subi_id,      customers.subi_id),
      first_name   = COALESCE(EXCLUDED.first_name,   customers.first_name),
      last_name    = COALESCE(EXCLUDED.last_name,    customers.last_name),
      last_seen_at = NOW()
    RETURNING id
  `;
  const row = await one(sql, [
    email,
    opts.cc_member_id || null,
    opts.recharge_id  || null,
    opts.subi_id      || null,
    opts.first_name   || null,
    opts.last_name    || null
  ]);
  return row ? row.id : null;
}

/* ── Event recording helper ─────────────────────── */

async function recordEvent(kind, source, email, payload) {
  try {
    await query(
      'INSERT INTO events (kind, source, email, payload) VALUES ($1, $2, $3, $4)',
      [kind, source || null, email || null, payload || {}]
    );
  } catch (err) {
    console.error('[db] event insert failed:', err.message);
  }
}

module.exports = {
  pool: pool,
  query: query,
  one: one,
  many: many,
  runMigrations: runMigrations,
  upsertCustomer: upsertCustomer,
  recordEvent: recordEvent
};

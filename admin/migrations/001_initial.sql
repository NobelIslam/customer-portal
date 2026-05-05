-- ════════════════════════════════════════════════════
-- TGP Subscription Dashboard — initial schema
-- Run automatically on boot by admin/db.js
-- ════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS customers (
  id            SERIAL PRIMARY KEY,
  email         TEXT UNIQUE NOT NULL,
  cc_member_id  TEXT,
  recharge_id   TEXT,
  subi_id       TEXT,
  first_name    TEXT,
  last_name     TEXT,
  first_seen_at TIMESTAMPTZ DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS customers_cc_idx       ON customers (cc_member_id);
CREATE INDEX IF NOT EXISTS customers_recharge_idx ON customers (recharge_id);
CREATE INDEX IF NOT EXISTS customers_subi_idx     ON customers (subi_id);

-- ────────────────────────────────────────────────────
-- subscriptions: one row per active or historical sub
-- id format: "{source}:{native_id}" so we never collide
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS subscriptions (
  id              TEXT PRIMARY KEY,
  source          TEXT NOT NULL,             -- 'cc' | 'recharge' | 'subi'
  native_id       TEXT NOT NULL,             -- the platform's own id
  customer_id     INTEGER REFERENCES customers(id) ON DELETE SET NULL,
  customer_email  TEXT,                       -- denormalised for fast lookup
  product         TEXT,
  product_id      TEXT,
  status          TEXT,                       -- 'ACTIVE' | 'PAUSED' | 'CANCELLED' | etc.
  price_cents     INTEGER DEFAULT 0,
  currency        TEXT DEFAULT 'USD',
  frequency       TEXT,                       -- e.g. "Every 1 MONTH" or "30"
  next_bill_at    TIMESTAMPTZ,
  started_at      TIMESTAMPTZ,
  cancelled_at    TIMESTAMPTZ,
  cancel_reason   TEXT,
  raw             JSONB,                      -- original platform payload, for debugging
  last_synced_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS subs_status_next_idx ON subscriptions (status, next_bill_at);
CREATE INDEX IF NOT EXISTS subs_source_idx      ON subscriptions (source);
CREATE INDEX IF NOT EXISTS subs_customer_idx    ON subscriptions (customer_id);
CREATE INDEX IF NOT EXISTS subs_started_idx     ON subscriptions (started_at);
CREATE INDEX IF NOT EXISTS subs_cancelled_idx   ON subscriptions (cancelled_at);

-- ────────────────────────────────────────────────────
-- orders: one row per charge/order, used for revenue
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
  id              TEXT PRIMARY KEY,           -- "{source}:{native_id}"
  source          TEXT NOT NULL,
  native_id       TEXT NOT NULL,
  customer_id     INTEGER REFERENCES customers(id) ON DELETE SET NULL,
  customer_email  TEXT,
  amount_cents    INTEGER DEFAULT 0,
  currency        TEXT DEFAULT 'USD',
  type            TEXT,                       -- 'initial' | 'rebill' | 'upsell' | 'unknown'
  product         TEXT,
  status          TEXT,                       -- 'COMPLETE' | 'REFUNDED' | etc.
  subscription_id TEXT REFERENCES subscriptions(id) ON DELETE SET NULL,
  created_at      TIMESTAMPTZ NOT NULL,
  raw             JSONB,
  last_synced_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS orders_created_idx  ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS orders_source_idx   ON orders (source);
CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id);

-- ────────────────────────────────────────────────────
-- logins: one row per portal login, captured by
-- magic-login.html via POST /admin/track-login
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logins (
  id          SERIAL PRIMARY KEY,
  customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
  email       TEXT NOT NULL,
  source      TEXT,                           -- which platform verified them
  found_in    TEXT[],                         -- all platforms they exist in
  ts          TIMESTAMPTZ DEFAULT NOW(),
  user_agent  TEXT,
  ip          TEXT
);

CREATE INDEX IF NOT EXISTS logins_ts_idx    ON logins (ts DESC);
CREATE INDEX IF NOT EXISTS logins_email_idx ON logins (email);

-- ────────────────────────────────────────────────────
-- events: live activity feed (sub_created, sub_cancelled,
-- rebill, login, failed_charge, etc.)
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
  id      SERIAL PRIMARY KEY,
  kind    TEXT NOT NULL,
  source  TEXT,
  email   TEXT,
  payload JSONB,
  ts      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS events_ts_idx   ON events (ts DESC);
CREATE INDEX IF NOT EXISTS events_kind_idx ON events (kind);

-- ────────────────────────────────────────────────────
-- sync_state: tracks last-sync cursor per source so the
-- 15-min cron can pull deltas instead of full history
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sync_state (
  source            TEXT PRIMARY KEY,         -- 'cc' | 'recharge' | 'subi'
  last_full_sync_at TIMESTAMPTZ,
  last_delta_sync_at TIMESTAMPTZ,
  last_error        TEXT,
  last_error_at     TIMESTAMPTZ
);

INSERT INTO sync_state (source) VALUES ('cc'), ('recharge'), ('subi')
ON CONFLICT (source) DO NOTHING;

-- ────────────────────────────────────────────────────
-- admin_tokens: short-lived magic-link tokens for the
-- admin login flow. Stored so we can revoke if needed.
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS admin_tokens (
  token       TEXT PRIMARY KEY,
  email       TEXT NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT NOW(),
  expires_at  TIMESTAMPTZ NOT NULL,
  used_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS admin_tokens_email_idx ON admin_tokens (email);

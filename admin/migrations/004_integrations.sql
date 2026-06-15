-- ────────────────────────────────────────────────────
-- integrations: enable/disable state per integration
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS integrations (
  name       TEXT PRIMARY KEY,
  enabled    BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO integrations (name, enabled) VALUES
  ('checkoutchamp', TRUE),
  ('recharge',      TRUE),
  ('shopify',       TRUE),
  ('klaviyo',       TRUE),
  ('whop',          FALSE)
ON CONFLICT (name) DO NOTHING;

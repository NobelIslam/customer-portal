-- ────────────────────────────────────────────────────
-- integration_credentials: API keys entered via the UI
-- DB values take priority over env vars at runtime.
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS integration_credentials (
  name       TEXT PRIMARY KEY,
  creds      JSONB DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

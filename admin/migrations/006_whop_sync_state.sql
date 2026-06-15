-- Add whop as a sync source
INSERT INTO sync_state (source) VALUES ('whop')
ON CONFLICT (source) DO NOTHING;

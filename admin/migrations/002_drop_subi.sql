-- Remove Subi integration from existing databases.
-- Safe to run multiple times (IF EXISTS guards).

DROP INDEX  IF EXISTS customers_subi_idx;
ALTER TABLE customers DROP COLUMN IF EXISTS subi_id;

DELETE FROM sync_state   WHERE source = 'subi';
DELETE FROM events       WHERE source = 'subi';
DELETE FROM orders       WHERE source = 'subi';
DELETE FROM subscriptions WHERE source = 'subi';

-- Backfill last_billed_at for subscriptions already billed today:
-- next_bill_at has moved to the future, but there's an order in the DB for today.
UPDATE subscriptions
SET last_billed_at = date_trunc('day', NOW() AT TIME ZONE 'UTC')
WHERE status = 'ACTIVE'
  AND last_billed_at IS NULL
  AND next_bill_at > NOW()
  AND EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_email = subscriptions.customer_email
      AND o.created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
      AND o.created_at <  date_trunc('day', NOW() AT TIME ZONE 'UTC') + INTERVAL '1 day'
  );

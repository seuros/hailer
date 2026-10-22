-- A message intake flags for operator review (non-ASCII domain, or an RDAP
-- new-domain signal) is held as 'quarantined' instead of 'queued'. It is
-- deliberately excluded from messages_dispatch's partial index (status IN
-- ('queued', 'retry_waiting')) - no index change needed, only the status
-- vocabulary itself.
ALTER TABLE hailer.messages DROP CONSTRAINT messages_status_check;
ALTER TABLE hailer.messages ADD CONSTRAINT messages_status_check CHECK (status IN (
    'queued', 'claimed', 'dispatching', 'retry_waiting', 'held_unknown', 'quarantined',
    'accepted', 'rejected', 'suppressed', 'cancelled', 'expired'
));

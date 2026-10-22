ALTER TABLE hailer.attempts ADD COLUMN lease_until timestamptz;
CREATE INDEX attempts_unresolved_leases ON hailer.attempts(lease_until)
    WHERE phase IN ('reserved', 'dispatching');

ALTER TABLE hailer.recipient_facts ADD COLUMN message_id text REFERENCES hailer.messages(id);
UPDATE hailer.recipient_facts f SET message_id = a.message_id
    FROM hailer.attempts a WHERE a.id = f.attempt_id;
ALTER TABLE hailer.recipient_facts ALTER COLUMN message_id SET NOT NULL;
CREATE INDEX recipient_facts_message ON hailer.recipient_facts(message_id, occurred_at DESC, event_id);

-- Independent commit-ordered feed and atomic logical spool accounting.
CREATE TABLE hailer.outbound_counter (
    id smallint PRIMARY KEY CHECK (id = 1),
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    live_bytes bigint NOT NULL DEFAULT 0 CHECK (live_bytes >= 0),
    live_messages bigint NOT NULL DEFAULT 0 CHECK (live_messages >= 0)
);
INSERT INTO hailer.outbound_counter (id) VALUES (1);

CREATE TABLE hailer.messages (
    id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    submission_key text NOT NULL UNIQUE CHECK (octet_length(submission_key) BETWEEN 1 AND 256),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    receipt jsonb NOT NULL,
    envelope jsonb NOT NULL,
    rfc_message_id text,
    payload bytea,
    payload_bytes bigint NOT NULL CHECK (payload_bytes > 0),
    status text NOT NULL CHECK (status IN (
        'queued', 'claimed', 'dispatching', 'retry_waiting', 'held_unknown',
        'accepted', 'rejected', 'suppressed', 'cancelled', 'expired'
    )),
    lifecycle jsonb NOT NULL,
    row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    accepted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > accepted_at),
    next_attempt_at timestamptz NOT NULL,
    current_attempt_id text,
    lease_until timestamptz,
    provider_message_id text,
    body_purged_at timestamptz
);
CREATE INDEX messages_dispatch ON hailer.messages(next_attempt_at, accepted_at)
    WHERE status IN ('queued', 'retry_waiting');
CREATE INDEX messages_leases ON hailer.messages(lease_until)
    WHERE status IN ('claimed', 'dispatching');
CREATE INDEX messages_retention ON hailer.messages(accepted_at) WHERE payload IS NOT NULL;

CREATE TABLE hailer.attempts (
    id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    message_id text NOT NULL REFERENCES hailer.messages(id),
    ordinal integer NOT NULL CHECK (ordinal > 0),
    phase text NOT NULL CHECK (phase IN (
        'reserved', 'dispatching', 'accepted', 'not_accepted', 'unknown', 'abandoned'
    )),
    lifecycle jsonb NOT NULL,
    envelope jsonb,
    excluded jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatch_started_at timestamptz,
    finished_at timestamptz,
    provider_message_id text,
    outcome_code text,
    UNIQUE (message_id, ordinal)
);
CREATE UNIQUE INDEX attempts_provider_id ON hailer.attempts(provider_message_id)
    WHERE provider_message_id IS NOT NULL;
ALTER TABLE hailer.messages ADD CONSTRAINT messages_current_attempt
    FOREIGN KEY (current_attempt_id) REFERENCES hailer.attempts(id);

CREATE TABLE hailer.message_events (
    sequence bigint PRIMARY KEY CHECK (sequence > 0),
    message_id text NOT NULL REFERENCES hailer.messages(id),
    kind text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX message_events_message ON hailer.message_events(message_id, sequence);

CREATE TABLE hailer.recipient_facts (
    event_id text NOT NULL REFERENCES hailer.events(event_id),
    attempt_id text NOT NULL REFERENCES hailer.attempts(id),
    recipient text NOT NULL,
    kind text NOT NULL,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (event_id, attempt_id, recipient, kind)
);

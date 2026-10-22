CREATE TABLE hailer.event_counter (
    id smallint PRIMARY KEY CHECK (id = 1),
    value bigint NOT NULL DEFAULT 0 CHECK (value >= 0)
);
INSERT INTO hailer.event_counter (id) VALUES (1);

CREATE TABLE hailer.events (
    sequence bigint PRIMARY KEY CHECK (sequence > 0),
    event_id text NOT NULL UNIQUE,
    payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE hailer.suppressions (
    email_address text PRIMARY KEY,
    reason text NOT NULL CHECK (reason IN ('bounce', 'complaint')),
    suppressed_at timestamptz NOT NULL,
    originating_message_id text,
    source_event_id text,
    metadata jsonb NOT NULL DEFAULT 'null'::jsonb
);

CREATE TABLE hailer.sync_checkpoints (
    source text PRIMARY KEY,
    next_token text,
    completed boolean NOT NULL DEFAULT false,
    imported bigint NOT NULL DEFAULT 0,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

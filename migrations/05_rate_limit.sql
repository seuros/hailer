-- A single shared row so every replica draws down the same SES account
-- quota instead of each enforcing its own independent, process-local
-- budget. Seeded by application code (initial values are config-driven,
-- not fixed), never by this migration.
CREATE TABLE hailer.send_rate_limiter (
    id smallint PRIMARY KEY CHECK (id = 1),
    tokens double precision NOT NULL,
    last_refill double precision NOT NULL,
    capacity double precision NOT NULL,
    refill_rate double precision NOT NULL
);

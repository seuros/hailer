-- Fault-injection experiments armed by an operator, one per target. Every
-- replica with `[rogue]` configured polls this table; the expiry is
-- mandatory and bounded, so no experiment outlives its operator.
CREATE TABLE hailer.rogue_experiments (
    target text PRIMARY KEY CHECK (target IN ('provider', 'feedback', 'verifier')),
    fault text NOT NULL CHECK (fault IN ('raise', 'delay', 'garble')),
    delay_ms bigint NOT NULL DEFAULT 0 CHECK (delay_ms >= 0),
    fraction_ppm integer NOT NULL CHECK (fraction_ppm BETWEEN 1 AND 1000000),
    armed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > armed_at AND expires_at <= armed_at + interval '24 hours'),
    CHECK ((fault = 'delay') = (delay_ms > 0)),
    -- A garbled verdict would be cached past the experiment's expiry.
    CHECK (NOT (target = 'verifier' AND fault = 'garble'))
);

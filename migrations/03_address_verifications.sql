-- Cached third-party deliverability verdicts (Emailable today). Verification
-- costs money per lookup, so a verdict is reused until it expires rather than
-- re-checked on every send attempt/retry of the same address.
CREATE TABLE hailer.address_verifications (
    email_address text PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('deliverable', 'undeliverable', 'risky', 'unknown')),
    reason text,
    disposable boolean NOT NULL DEFAULT false,
    role boolean NOT NULL DEFAULT false,
    checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX address_verifications_expiry ON hailer.address_verifications(expires_at);

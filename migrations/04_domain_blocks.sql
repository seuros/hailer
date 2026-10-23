-- Operator- and repo-curated domain/pattern blocklist, checked before (and
-- cheaper than) paid address verification. A pattern is either an exact
-- domain or a `*.`-prefixed suffix glob (`*.il` matches that domain and any
-- subdomain under it). Matching logic lives in src/domain_block.rs, not SQL.
CREATE TABLE hailer.domain_blocks (
    pattern text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('operator', 'curated')),
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Hand-curated, started small; grown as real abuse is observed. Ships with
-- the repo and updated via normal Hailer releases - never fetched at
-- runtime from a third party.
INSERT INTO hailer.domain_blocks (pattern, source, reason) VALUES
    ('guerrillamail.com', 'curated', 'disposable mail provider'),
    ('guerrillamailblock.com', 'curated', 'disposable mail provider'),
    ('sharklasers.com', 'curated', 'disposable mail provider (guerrillamail alias)'),
    ('pokemail.net', 'curated', 'disposable mail provider (guerrillamail alias)'),
    ('mailinator.com', 'curated', 'disposable mail provider'),
    ('10minutemail.com', 'curated', 'disposable mail provider'),
    ('yopmail.com', 'curated', 'disposable mail provider'),
    ('trashmail.com', 'curated', 'disposable mail provider'),
    ('temp-mail.org', 'curated', 'disposable mail provider'),
    ('throwawaymail.com', 'curated', 'disposable mail provider'),
    ('getnada.com', 'curated', 'disposable mail provider'),
    ('maildrop.cc', 'curated', 'disposable mail provider'),
    ('dispostable.com', 'curated', 'disposable mail provider'),
    ('fakeinbox.com', 'curated', 'disposable mail provider'),
    ('mohmal.com', 'curated', 'disposable mail provider'),
    ('moakt.com', 'curated', 'disposable mail provider')
ON CONFLICT (pattern) DO NOTHING;

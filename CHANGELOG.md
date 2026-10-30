# Changelog

## Working tree — durable delivery (unreleased)

- Providers are cargo features: `ses` (AWS SDK) and `postmark`; platform gems build with both. A binary without the configured provider refuses to start.
- HTTP clients (Postmark, Emailable, RDAP) moved from reqwest to rama, with rustls on ring.
- The AWS SDK is gone: SESv2, SQS and STS go through hailer's own SigV4 client (verified against AWS's signing test suite) with env, shared-file, web identity, container and IMDSv2 credentials. The `ses` build loses 75 crates and aws-lc.

- Postmark provider (`provider = "postmark"`, `[postmark] message_stream`, `POSTMARK_SERVER_TOKEN`): Rails MIME parsed into Postmark's JSON, sync and queued sends, correlation as Metadata, `hailer postmark-server` token check. SES-only settings are refused with it.

- Opt-in `:hailer_queued` Rails adapter with stable job keys and frozen intake retries.
- PostgreSQL ownership receipts, immutable submission replay/conflict and explicit spool budgets.
- Supervised fenced dispatch, journaled attempts, safe retries and held unknown outcomes.
- Tagged SES feedback correlation, recipient facts, audited cancellation and resolution.
- Deadline/body retention, independent lifecycle feed and intake/dispatcher readiness.
- Native Rust future traits without `async-trait`; throttle-machines 0.3.1.
- Current-source CommonShare queued smoke with loopback SES; no production cutover.
- New Linux binary builds target Debian 13 Trixie (glibc 2.41), with locked GNU Ruby dependency variants.
- Opt-in pre-send address verification (Emailable first; swappable via `Verifier` enum), `[email_verification]` in `hailer.toml`, cached verdicts in `hailer.address_verifications`. Fails open by default - a verifier outage or `unknown`/`risky` result never blocks a send unless explicitly configured into `reject_states`.
- Domain/pattern blocklist (`hailer.domain_blocks`): exact domains or `*.`-suffix globs, always active. A small hand-curated disposable-provider list ships with the repo (source `curated`, only changes by upgrading); operators add their own via `hailer ban-domain`/`unban-domain` (source `operator`). Checked before suppression and verification - the cheapest, broadest filter runs first.
- New `quarantined` message status, queued pipeline only: a non-ASCII recipient domain, or (opt-in `[domain_intel]`, RDAP-backed) a newly registered one, holds the whole message for operator review (`hailer release-quarantine`/`cancel-message`) instead of a hard reject. Excluded from the dispatch index like `held_unknown`; expires on the normal deadline like `queued`.

## 0.2.0 (unreleased)

- PostgreSQL-only live state, with atomic feedback and commit-safe cursors.
- Resumable, read-only AWS suppression reconciliation scoped by account/region.
- Pure behavior admission circuit, per-process throttle, and SES circuit breaker.
- Single-attempt sends, explicit ambiguous outcomes, and typed rate-limit errors.
- Private health/readiness sensors, supervised workers, and bounded graceful drain.
- Explicit read-only migration of stopped 0.1 redb state into an empty database.

## 0.1.0

- Rails 8.1+/Ruby 4.0+ ActionMailer delivery with automatic defaults.
- Typed provider-neutral feedback client and Rails callback consumer.
- SES sender, SQS feedback ingestion, durable suppressions, and import tooling.
- Platform gems carrying the standalone Rust service with a verified launcher.
- Local CommonShare integration tested without sending real email.

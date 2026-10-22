# Changelog

## Working tree — durable delivery (unreleased)

- Opt-in `:hailer_queued` Rails adapter with stable job keys and frozen intake retries.
- PostgreSQL ownership receipts, immutable submission replay/conflict and explicit spool budgets.
- Supervised fenced dispatch, journaled attempts, safe retries and held unknown outcomes.
- Tagged SES feedback correlation, recipient facts, audited cancellation and resolution.
- Deadline/body retention, independent lifecycle feed and intake/dispatcher readiness.
- Native Rust future traits without `async-trait`; throttle-machines 0.3.1.
- Current-source CommonShare queued smoke with loopback SES; no production cutover.
- New Linux binary builds target Debian 13 Trixie (glibc 2.41), with locked GNU Ruby dependency variants.

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

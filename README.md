# Hailer

Rails writes the email. Hailer deals with sending it.

This repository is two things:

- **`hailer` gem** — an ActionMailer adapter, feedback client, and executable
  launcher. Platform gems bundle the Rust binary. No Ruby AWS SDK or native extension.
- **`hailer` crate** — a standalone Rust mail ship, supervised by Mothership.
  SESv2 is the first provider.

```text
ActionMailer → hailer gem → private Mothership route → hailer binary → SES
```

An **opt-in durable queued adapter** sits alongside the default synchronous
send path (`[queued]` in `hailer.example.toml`).

## Install

```ruby
gem "guardship"
gem "shuttlebay"
gem "hailer"
```

Mothership picks up the bay and starts sending mail. Keep using your
mailers, interceptors, observers, `deliver_now`, and `deliver_later` -
nothing else about how Rails sends mail changes.

Everything past this point is reference material: protocol internals,
TOML knobs, and operator commands for people running or debugging the
service, not required reading to use it.

## Rails

Requires Ruby 4.0+ and Rails 8.1 or newer within Rails 8.x.
Rails main (8.2 alpha) is also exercised. Older releases are not supported.

The Railtie registers and selects Hailer automatically. Set the private service
URL in the Rails process's environment:

```sh
HAILER_URL=http://127.0.0.1:9293/internal/mail/send
```

No application initializer is needed. To customize connection settings:

```ruby
config.action_mailer.hailer_settings = {
  endpoint: ENV.fetch("HAILER_URL"),
  open_timeout: 5,
  read_timeout: 30,
  write_timeout: 30
}
```

`HAILER_URL` is the complete private URL, for example
`http://127.0.0.1:9293/internal/mail/send`. It is also the default endpoint
when omitted from `hailer_settings`. There is no default host or fallback to SES.
The app can explicitly override the default, for example
`config.action_mailer.delivery_method = :test` in its test environment.

The adapter sends rendered MIME and separate envelope recipients, preserving
explicit `smtp_envelope_from` and `smtp_envelope_to` overrides. After SES
accepts the message, it sets `message.header[:ses_message_id]`.

## Rust service

Platform gems install the `hailer` executable, ready for Mothership to launch.
Installing the gem does not start the service or send mail.

The source (`ruby`) gem has the same Rails client and launcher, but no binary.
For source development or an unsupported platform:

```sh
cargo install --path . --locked
export HAILER_BINARY=/absolute/path/to/hailer
```

Run it as a Mothership HTTP bay. Merge this example into your fleet manifest;
the bind must stay private and must not be routed on a public listener:

```toml
[mothership.bind]
mail = "127.0.0.1:9293"

[[bays.http]]
name = "hailer"
command = "hailer"
workers = 0
threads = 2
routes = [{ bind = "mail", pattern = "^/internal/mail/(send|events|health|ready)$" }]
```

Mothership exports `MS_PID`, `MS_SHIP`, and `MS_SOCKET_DIR`. Hailer binds its
docking Unix socket; it does **not** run another HTTP server. For containers,
use a private network listener with network policy instead of a loopback address
if Rails runs in a different network namespace.

Give the **service**, not the Rails adapter, AWS region and credentials through
the standard AWS credential chain. Its identity needs `ses:SendEmail`, and the
SES account/identities must be configured for sending.

### PostgreSQL

PostgreSQL is the **only live backend**. Hailer owns a `hailer` schema,
not Rails tables. Use a separate database/role where practical:

```sh
export HAILER_DATABASE_URL='postgres://hailer:password@db/hailer?sslmode=verify-full'
hailer migrate
```

Supply the database CA through the normal PostgreSQL connection options.
Use certificate verification in deployment; a private network is not TLS.
Keep the URL in a secret manager, not TOML or a committed initializer.
Startup also applies embedded, versioned migrations. The bootstrap identity
needs database `CREATE` and ownership of its schema/tables. Runtime startup
currently runs that same bootstrap/migrator, so provision the service role for
it too; a separate DML-only runtime mode is not implemented.
The runtime never needs Rails schema ownership. Back up the database with your
normal PostgreSQL backup/restore tooling.

Configuration defaults: pool size 5 (range 1–64), shutdown timeout 30 seconds
(range 1–300). Every replica shares PostgreSQL; each has its own pool and optional
rate budget. Set your supervisor/container stop grace longer than Hailer's
shutdown timeout. PostgreSQL 18 is exercised locally and in CI.

## Bounces, complaints, and suppressions

```text
SES → SNS → dedicated SQS queue → Hailer → durable events + suppressions
```

Hailer atomically commits each event, deduplication key, and suppression change
to PostgreSQL **before** acknowledging SQS. Failed acknowledgements
can be retried without duplicating stored events. Invalid/untrusted messages
are left for SQS's dead-letter policy.

- Permanent bounces and complaints suppress recipients.
- Transient/undetermined bounces are recorded without permanent suppression.
- An explicit `not-spam` complaint does not create a new suppression.
- Suppression checks cover the actual To/Cc/Bcc envelope, including overrides.
  Matching is case-insensitive and ignores `+alias` tags.
- Mixed messages keep allowed recipients and their original addresses; MIME
  is unchanged. If every recipient is suppressed, Hailer returns HTTP 422
  without calling SES. A suppression lookup failure fails closed with HTTP 503.

Copy [hailer.example.toml](hailer.example.toml), set the real queue/topic, and
pass `--config /path/to/hailer.toml` or `HAILER_CONFIG`. Without `[feedback]`,
the consumer is disabled, but previously stored suppressions are still enforced.
`HAILER_DATABASE_URL` is required, even with feedback disabled.
The old `state_path`, `--state-path`, and `HAILER_STATE_PATH` are not supported.
Replicas may consume the same dedicated queue when they share the same Hailer
database. Commits serialize cursor allocation so a reader cannot skip an earlier
transaction that has not committed yet.

Event history and deduplication have **no automatic retention/pruning**.
Do not delete events casually: event IDs also carry the SQS deduplication
contract, and Rails consumers may still need their cursors. Plan retention
against consumer lag and redelivery windows before adding a pruning policy.

Hailer also exposes a provider-neutral, nondestructive event feed:

```http
POST /internal/mail/events
Content-Type: application/json

{"after":0,"limit":100}
```

Route it on the **same private bind**, if application tracking needs it:

```toml
routes = [{ bind = "mail", pattern = "^/internal/mail/(send|events)$" }]
```

The gem validates the page and exposes immutable `FeedbackEvent` objects:

```ruby
page = Hailer::FeedbackClient.new(endpoint: ENV.fetch("HAILER_URL")).poll(after: 0)
page.events.each { |event| puts [event.event_id, event.kind, event.message_id] }
```

Configure a Rails business-history handler (a callable or reloadable class name):

```ruby
Rails.application.config.hailer.feedback_handler = "Platform::MailFeedback"
```

Then `Hailer::FeedbackConsumer.new.call(after: cursor)` calls that handler for
each validated event and returns the next cursor only after all callbacks
succeed. A callback must **raise** on failure. Persist the cursor and domain
updates in one application database transaction. Retried pages require
idempotent handlers; no hidden poller, scheduling, or checkpoint database is
started by the gem. Reading does not acknowledge or erase events.

The SQS policy must allow only the intended SNS topic, using `aws:SourceArn`
and account restrictions. Subscribe with `RawMessageDelivery=false`, restrict
direct `sqs:SendMessage`, set a dead-letter queue, and monitor its depth.
The SNS topic allowlist in TOML is defense in depth, not signature verification.
Hailer needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on that queue, plus
applicable KMS decrypt permissions if customer-managed encryption is used.

### Import AWS suppressions

```sh
hailer sync-suppressions
# If AWS expires a saved pagination token:
hailer sync-suppressions --restart
# Existing application export, JSON array:
hailer import-suppressions --file suppressions.json
```

The service identity needs `ses:ListSuppressedDestinations`; STS
`GetCallerIdentity` identifies the account (AWS does not require an explicit
allow for that operation). The configured region is part of the source identity.
These commands **read AWS only**; they never mutate the account suppression list.
Missing AWS entries do not unsuppress local entries, and a bounce never
downgrades a complaint. Imports do not fabricate delivery-history events.
Account-level AWS data does not include every possible global SES suppression.

Enable `[suppression_sync]` as in the example to reconcile on startup and hourly.
The first complete sync gates send readiness. Later errors mark health degraded,
retain committed pages, and retry safe reads with bounded backoff. A token is
stored with its page in one transaction. Replicas coordinate a source-scoped
advisory lock; they cannot advance the same checkpoint concurrently.
An expired token deliberately requires the operator's `--restart`. Omitting
the section disables scheduled sync, not existing suppression enforcement.

### Control plane

The behavior circuit evaluates **pure admission decisions**: input validity,
service/storage readiness, suppression, and local rate admission. PostgreSQL,
AWS calls, clocks, and token reservations remain owned by the async host.
The SES circuit breaker counts transient/ambiguous failures, not bad addresses
or permanent request rejections. An optional `[rate_limit]` budgets envelope
recipients **per process**, not per account; it does not discover SES quotas.

Private `GET /internal/mail/health` returns sanitized component states and
counters. `GET /internal/mail/ready` returns 503 during initial sync or storage
failure, otherwise 200. A provider circuit can be degraded while readiness
remains true so protected half-open probes can recover it. No credentials,
recipient addresses, message bodies, or pagination tokens appear in these sensors.
They are operational signals, not a guarantee SES will accept a message.

TERM/ctrl-c stops admission and SQS receives, then drains owned requests and
the current feedback commit/ack within the configured deadline. Idle links
close immediately. Deadline expiry aborts remaining tasks and exits nonzero;
an interrupted send can have an unknown outcome.

**Security:** the route currently has no application-level authentication.
`/internal/` is a name, not an access control. Keep the bind private. The client
does not follow redirects or use environment HTTP proxies.

## Impossible addresses

Always on, no configuration: `toto@example.com` is syntactically a real
address, but RFC 2606 reserves `example.com`/`.net`/`.org` and the
`test`/`example`/`invalid`/`localhost` TLDs for documentation - no real
mailbox ever lives there. Rejected at the same boundary as a malformed
address (`SendRequest::validate`), before any guard in the chain runs, for
both the synchronous and queued adapters. Not a policy choice like the
domain blocklist below - there is nothing to configure or an operator could
turn off.

## Domain blocklist

```text
SendRequest -> impossible-address check -> domain blocklist (free, local) -> suppression (free, local) -> verification (paid, external) -> SES
```

Always active - an empty `hailer.domain_blocks` table is a no-op, there is
no toggle. A pattern is an exact domain (`guerrillamail.com`) or a
`*.`-prefixed suffix glob (`*.kp` matches that domain and any subdomain).
A small hand-curated list of known disposable-mail providers ships with the
repo (`source = 'curated'`, migration `05_domain_blocks.sql`); it only grows
by upgrading Hailer, never fetched from a third party at runtime. Operators
add their own:

```sh
hailer ban-domain --pattern "*.kp" --reason "sanctioned TLD"
hailer unban-domain --pattern "*.kp"
```

`unban-domain` only removes an `operator`-sourced pattern - a curated one
cannot be removed this way, by design.

## Address verification

```text
SendRequest -> suppression check (cheap, local) -> address verification (paid, external) -> SES
```

Opt-in pre-send deliverability check, off by default. Enable with
`[email_verification]` in `hailer.toml`:

```toml
[email_verification]
provider = "emailable"
reject_states = ["undeliverable"]  # risky/unknown fail open unless added here
cache_ttl_seconds = 604800
timeout_seconds = 5
```

Set `EMAILABLE_API_KEY` in the process environment, never in `hailer.toml` -
the same rule as `HAILER_DATABASE_URL` and AWS credentials. Suppression
filters first since it is a free local lookup; verification only runs for
recipients that survive it, so a suppressed address never costs a paid
lookup. Verdicts are cached in `hailer.address_verifications` (a week by
default) - a retried send against the same address reuses the cached verdict
instead of paying for another check.

A verifier outage, timeout, or an `unknown`/`risky` result all fail open: this
middleware is never a new reason a real send stops working unless a
deployment explicitly adds those states to `reject_states`. Adding another
provider (ZeroBounce, NeverBounce, ...) means a new `Verifier` enum variant in
`src/verify.rs`; nothing else in the send path changes.

### Bulk pre-screening (batch)

A separate concern from the real-time guard above: pre-verify a whole list
*before* ever trying to send to it, not a per-send check. Emailable's batch
API is async (submit, then poll) and only returns per-address results inline
for batches up to 1,000 - larger ones get a ZIP/CSV download link this client
deliberately does not fetch or parse, to avoid a zip/csv dependency:

```sh
hailer verify-batch --file addresses.txt   # one address per line, max 1000
hailer batch-status --id BATCH_ID          # poll; caches each verdict once complete
```

`batch-status` writes every returned verdict into `hailer.address_verifications`
(the same cache the real-time guard reads), so a later send to an
already-screened address reuses the cached verdict instead of paying for
another single-address check. A batch over 1,000 addresses is reported with
its `download_file` URL but is not cached - split into smaller batches
instead.

`verify-batch` never submits an address that does not need it: exact
duplicate lines (by the same normalized key the cache itself is keyed by -
`USER+alias@x` and `user@x` are the same address), and anything already in
`hailer.address_verifications` with an unexpired verdict, are dropped before
the batch is ever submitted - never spending a credit on them. Every
exclusion is counted and reported (`invalid_lines`, `duplicate_lines`,
`already_cached`), never silently dropped. `hailer emailable-account` checks
the Emailable account's remaining credits directly; `submit_batch` itself
also checks this before submitting and refuses (without ever reaching the
server) if the account has none left or fewer than the batch needs.

### SES account diagnostics

Operator-only, standalone (no Mothership, no docking - same tier as
`migrate`/`sync-suppressions`), for checking the AWS side of mail delivery
without ever sending anything:

```sh
hailer ses-account                                        # quota, reputation, enforcement status
hailer ses-identities --identities commonshare.com,insights.commonshare.com
```

`ses-identities` checks each comma-separated identity independently - one
unverified or nonexistent identity is reported inline (`error`/`code`) and
never stops the others from reporting.

## Failure semantics

An HTTP 200 means **SES accepted the message**, not that the recipient received
it. A successful response must include a nonblank `message_id`.

| Failure | Ruby exception |
| --- | --- |
| HTTP 400 / 422 | `Hailer::ValidationError` |
| HTTP 502 | `Hailer::RejectedError` |
| HTTP 429 | `Hailer::RateLimitedError` |
| HTTP 503, `code: "outcome_unknown"` | `Hailer::AmbiguousDeliveryError` |
| HTTP 503 | `Hailer::UnavailableError` |
| Other non-200 status | `Hailer::DeliveryError` |
| Invalid/oversized response, missing message ID | `Hailer::ProtocolError` |
| Connection, TLS, or timeout failure | `Hailer::TransportError` |
| Missing/invalid endpoint or timeout settings | `Hailer::ConfigurationError` |

The synchronous adapter does not retry. A lost response can happen **after** SES
accepted the message; retrying may duplicate it. This route does not use the
opt-in outbound spool or its submission-key deduplication.
Rails jobs can also retry, so configure them deliberately.
The SES SDK makes **one send attempt**. AWS throttling/transient failures return
503, permanent service rejections 502, and disconnects/timeouts/missing message
IDs carry the explicit unknown-outcome code. None of these responses authorizes
an automatic retry of an ambiguous send.

## Status

The delivery contract, Hailer-side feedback processing, domain blocklist,
impossible-address rejection, address verification, the queued/quarantine
pipeline, and SES account diagnostics are all implemented and shipped.
`[queued]`, `[domain_intel]`, `[suppression_sync]`, `[feedback]`, and
`[email_verification]` ship disabled by default - absent config, not
missing code.

## Development

```sh
bundle install
bundle exec rake
HAILER_TEST_DATABASE_URL=postgres://test_user:password@127.0.0.1/hailer_test cargo test --locked
cargo fmt --all -- --check
```

The Ruby suite exercises real local HTTP exchanges and a booted Rails app.
The Rust suite checks the same JSON fixture and the docking exchange with a
fake provider. Feedback tests cover redelivery, acknowledgement ordering,
atomic rollback, restart persistence, imports, and suppression enforcement.
Rust tests require an isolated PostgreSQL server and a test role with `CREATEDB`;
each test creates and drops its own database. Never point this variable at a
customer or Rails production database. Neither suite sends real mail or requires
AWS credentials.

For the opt-in CommonShare compatibility smoke, create a disposable Hailer
database, then run from that application's checkout:

```sh
RAILS_ENV=test HAILER_SMOKE_BINARY=/path/to/hailer/target/debug/hailer \
  HAILER_DATABASE_URL=postgres://test_user:password@127.0.0.1/hailer_smoke \
  bundle exec rails runner /path/to/hailer/test/commonshare_smoke.rb
```

It refuses a non-test app database, points the AWS SDK at a loopback fixture,
and rolls back application-history changes. It does not change the app's
dependency lockfile or send external mail.

`HAILER_BINARY` is an explicit, version-checked source-build override; there is
no download or PATH fallback.

Bundler selects the matching native platform gem automatically; do not set
`force_ruby_platform` in deployment if you want the bundled executable.

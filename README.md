# Hailer

Rails writes the email. Hailer deals with sending it.

This repository prepares two things at version **0.2.0** (not published yet):

- **`hailer` gem** — an ActionMailer adapter, feedback client, and executable
  launcher. Platform gems bundle the Rust binary. No Ruby AWS SDK or native extension.
- **`hailer` crate** — a standalone Rust mail ship, supervised by Mothership.
  SESv2 is the first provider.

```text
ActionMailer → hailer gem → private Mothership route → hailer binary → SES
```

## Rails

Requires Ruby 4.0+ and Rails 8.1 or newer within Rails 8.x.
Rails main (8.2 alpha) is also exercised in CommonShare. Older releases are not supported.

```ruby
# Gemfile (use path: "/path/to/hailer" while developing)
gem "hailer"
```

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

Keep using your mailers, interceptors, observers, `deliver_now`, and
`deliver_later`. Rails still owns job scheduling. The adapter sends rendered MIME
and separate envelope recipients, preserving explicit `smtp_envelope_from` and
`smtp_envelope_to` overrides. After SES accepts the message, it sets
`message.header[:ses_message_id]`.

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

PostgreSQL is the **only live backend** in 0.2. Hailer owns a `hailer` schema,
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
  Matching preserves CommonShare's case/plus-alias policy.
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

The gem does not retry. A lost response can happen **after** SES accepted the
message; retrying may duplicate it. There is no durable **outbound** queue or
send-idempotency store yet; feedback deduplication does not deduplicate sends.
Rails jobs can also retry, so configure them deliberately.
The SES SDK makes **one send attempt**. AWS throttling/transient failures return
503, permanent service rejections 502, and disconnects/timeouts/missing message
IDs carry the explicit unknown-outcome code. None of these responses authorizes
an automatic retry of an ambiguous send.

## Status and migration

The delivery contract and Hailer-side feedback processing are implemented. CommonShare's
existing sender is the compatibility reference, not a runtime dependency.

### Upgrade from 0.1 redb

1. Stop all old Hailer processes cleanly. Keep the source file and a backup.
2. Provision an **empty** Hailer PostgreSQL database and set its URL.
3. Run `hailer migrate-redb --file /private/path/state.redb`.
4. Check reported counts, event cursors, and suppression enforcement before
   starting 0.2. Keep the source backup; do not restart old writers after cutover.

Migration opens redb read-only, checks event/dedup mappings and suppression keys,
and copies bounded batches in **one destination transaction**. It preserves
event IDs, sequence values, suppression metadata, and next-cursor continuity.
It rejects missing/corrupt sources and nonempty targets without wiping anything.
An unclean redb file may require recovery with the old version against a backup
before migration. There is no implicit fallback and no dual-write period.

Before switching an existing app:

1. Provision PostgreSQL, run Hailer on a private Mothership bind, check readiness,
   and verify real SES delivery using a deliberate test.
2. Add the gem, set `HAILER_URL`, and remove the app's old delivery-method
   override so Hailer's default takes effect.
3. Subscribe a dedicated SQS queue before exporting and importing existing
   suppressions. Let Hailer catch up and verify the event feed.
4. Keep existing receivers/interceptors until routing, suppression import,
   and any provider-neutral app history consumer have been verified.
5. Only then remove the old sender and AWS-specific feedback handling.

CommonShare's local integration is tested with its real application, database,
Mothership, packaged Hailer binary, and a loopback SES fixture. Its existing AWS
receivers remain until the real subscription and suppression cutover is verified.
No deployed AWS routing has been changed.
Validation/behavior plugins, a durable outbound queue, distributed account-wide
quotas, and send idempotency remain later work. No plugin API is promised yet.

## Development

```sh
bundle install
bundle exec rake
HAILER_TEST_DATABASE_URL=postgres://test_user:password@127.0.0.1/hailer_test cargo test --locked
cargo fmt --all -- --check
gem build hailer.gemspec --output /tmp/hailer-0.2.0.gem
bundle exec rake native:package
```

The Ruby suite exercises real local HTTP exchanges and a booted Rails app.
The Rust suite checks the same JSON fixture and the docking exchange with a
fake provider. Feedback tests cover redelivery, acknowledgement ordering,
atomic rollback, restart persistence, imports, and suppression enforcement.
Rust tests require an isolated PostgreSQL server and a test role with `CREATEDB`;
each test creates and drops its own database. Never point this variable at a
customer or Rails production database. Neither suite sends real mail or requires
AWS credentials.

`native:package` builds a locked release and writes
`pkg/hailer-0.2.0-<platform>.gem`. Each platform gem contains exactly its host
binary and a version/platform/SHA-256 manifest. The launcher verifies that
manifest before `exec`, preserving arguments, exit status, and signal handling.
`HAILER_BINARY` is an explicit, version-checked source-build override; there is
no download or PATH fallback.

Build on each supported release host before publishing its artifact. Nothing
in the packaging task publishes a gem.

## Linux releases and RubyGems

The **Linux binary gems** GitHub Actions workflow builds `x86_64-linux-gnu`
and `aarch64-linux-gnu` natively against Debian 12 (glibc 2.36). These artifacts
require glibc 2.36 or newer; they are not Alpine/musl builds.

```sh
gh workflow run linux-gems.yml --repo seuros/hailer -f tag=v0.2.0
```

The workflow checks out the requested release tag, runs the Ruby and Rust
suites, and installs each binary gem in a separate slim Debian container.
It verifies the installed platform, bundled-binary manifest, executable version,
and PostgreSQL suppression import before attaching either Linux gem to the
existing release. Existing gem assets are never overwritten; `SHA256SUMS` is
updated to cover all release gems.

Publishing a GitHub release does **not** publish to RubyGems. Like Shuttlebay,
release assets can be fetched and published with Rake:

```sh
bundle exec rake gems:fetch
bundle exec rake gems:push_all
# Or select an explicit release:
bundle exec rake 'gems:push_all[v0.2.0]'
```

`gems:push_all` fetches fresh assets, checks every archive's SHA-256,
name/version/platform, and publishes only that verified set. It skips
already-published platforms only when RubyGems reports the identical archive
hash, so an interrupted publication can be resumed without replacing artifacts.
`gems:push` is an alias. No release task rebuilds or silently republishes gems.

RubyGems may require MFA or a suitably scoped API key. Keep credentials out of
the repository. Publishing makes the packaged Ruby code and binaries public,
even when the GitHub repository is private. Once published, Bundler selects
the matching native gem; do not set `force_ruby_platform` in deployment if
you want the bundled executable.

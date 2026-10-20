# Hailer

Rails writes the email. Hailer deals with sending it.

This repository ships two things at version **0.1.0**:

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
routes = [{ bind = "mail", pattern = "^/internal/mail/send$" }]
```

Mothership exports `MS_PID`, `MS_SHIP`, and `MS_SOCKET_DIR`. Hailer binds its
docking Unix socket; it does **not** run another HTTP server. For containers,
use a private network listener with network policy instead of a loopback address
if Rails runs in a different network namespace.

Give the **service**, not the Rails adapter, AWS region and credentials through
the standard AWS credential chain. Its identity needs `ses:SendEmail`, and the
SES account/identities must be configured for sending.

## Bounces, complaints, and suppressions

```text
SES → SNS → dedicated SQS queue → Hailer → durable events + suppressions
```

Hailer atomically commits each event, deduplication key, and suppression change
to a local redb database **before** acknowledging SQS. Failed acknowledgements
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
`HAILER_STATE_PATH` overrides `state_path`; the default is `data/hailer.redb`.
Use a persistent, private volume in production.

**One Hailer process owns one state file and one dedicated queue.** Run the
bay with `workers = 0`; this is not a distributed suppression database.
Do not run competing consumers with separate state files. Back up the state
file while Hailer is stopped; event history and deduplication currently have
no automatic retention/pruning.

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

See [feedback setup and migration](docs/feedback.md) before removing a receiver.

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
| HTTP 503 | `Hailer::UnavailableError` |
| Other non-200 status | `Hailer::DeliveryError` |
| Invalid/oversized response, missing message ID | `Hailer::ProtocolError` |
| Connection, TLS, or timeout failure | `Hailer::TransportError` |
| Missing/invalid endpoint or timeout settings | `Hailer::ConfigurationError` |

The gem does not retry. A lost response can happen **after** SES accepted the
message; retrying may duplicate it. There is no durable **outbound** queue or
send-idempotency store yet; feedback deduplication does not deduplicate sends.
Rails jobs can also retry, so configure them deliberately.
The current service maps all SES SDK send errors to HTTP 502; that status is
not yet a reliable distinction between permanent and transient AWS failures.

## Status and migration

The delivery contract and Hailer-side feedback processing are implemented. CommonShare's
existing sender is the compatibility reference, not a runtime dependency.
See [the contract](docs/delivery-contract.md).

Before switching an existing app:

1. Run Hailer on a private Mothership bind and verify real SES delivery.
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
Account diagnostics, validation/behavior plugins,
distributed state, rate limiting, graceful draining, and reliable send-error
classification are later work. No plugin API is promised yet.

## Development

```sh
bundle install
bundle exec rake
cargo test --locked
cargo fmt --all -- --check
gem build hailer.gemspec --output /tmp/hailer-0.1.0.gem
bundle exec rake native:package
```

The Ruby suite exercises real local HTTP exchanges and a booted Rails app.
The Rust suite checks the same JSON fixture and the docking exchange with a
fake provider. Feedback tests cover redelivery, acknowledgement ordering,
atomic rollback, restart persistence, imports, and suppression enforcement.
Neither suite sends real mail or requires AWS credentials.

`native:package` builds a locked release and writes
`pkg/hailer-0.1.0-<platform>.gem`. Each platform gem contains exactly its host
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
gh workflow run linux-gems.yml --repo seuros/hailer -f tag=v0.1.0
```

The workflow checks out the requested release tag, runs the Ruby and Rust
suites, and installs each binary gem in a separate slim Debian container.
It verifies the installed platform, bundled-binary manifest, executable version,
and offline suppression import before attaching either Linux gem to the
existing release. Existing gem assets are never overwritten; `SHA256SUMS` is
updated to cover all release gems.

Publishing a GitHub release does **not** publish to RubyGems. Download and
verify the release assets, then push each platform separately:

```sh
mkdir -p pkg/publish
gh release download v0.1.0 --repo seuros/hailer --dir pkg/publish \
  --pattern '*.gem' --pattern SHA256SUMS
cd pkg/publish
shasum -a 256 -c SHA256SUMS
gem push hailer-0.1.0.gem --host https://rubygems.org
gem push hailer-0.1.0-arm64-darwin.gem --host https://rubygems.org
gem push hailer-0.1.0-x86_64-linux-gnu.gem --host https://rubygems.org
gem push hailer-0.1.0-aarch64-linux-gnu.gem --host https://rubygems.org
```

RubyGems may require MFA or a suitably scoped API key. Keep credentials out of
the repository. Publishing makes the packaged Ruby code and binaries public,
even when the GitHub repository is private. Once published, Bundler selects
the matching native gem; do not set `force_ruby_platform` in deployment if
you want the bundled executable.

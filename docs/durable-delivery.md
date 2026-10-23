# Durable delivery

The queued Ruby interface, PostgreSQL spool and supervised dispatcher are
implemented and shipped (0.3.0+). It is opt-in: absent `[queued]` config,
Hailer stays on the synchronous send path and none of this runs.

## Opt-in Rails configuration

Keep `HAILER_URL` pointing to the existing private `/internal/mail/send` endpoint.
The queued client uses the same validated origin with `/internal/mail/messages`.

```ruby
# config/environments/production.rb
config.action_mailer.delivery_method = :hailer_queued
```

The initializer registers the adapter and selects `Hailer::MailDeliveryJob` for
this opt-in configuration. The default remains the synchronous `:hailer`
adapter, whose HTTP requests never retry.

For a per-mailer opt-in, select both:

```ruby
self.delivery_method = :hailer_queued
self.delivery_job = Hailer::MailDeliveryJob
```

An explicitly configured application delivery job is not replaced. It must
inherit `Hailer::MailDeliveryJob` or include `Hailer::DeliveryJobContext` to
obtain stable submission keys across job retries. This uses public ActiveJob
callbacks, not Rails monkey patches.

## Identities and retries

- Standard queued jobs use job ID plus delivery ordinal. Different jobs are
  different intents unless the application supplies a business key.
- Direct `deliver_now` calls generate a fresh key per invocation.
- Supply a business key through `delivery_method_options` when separate calls
  represent the same intended message:

  ```ruby
  mail(
    to: recipient,
    subject: 'Invoice',
    delivery_method_options: { submission_key: "invoice:#{invoice.id}:issued" }
  )
  ```

- Supply an optional ISO 8601 `expires_at` through those same options. The
  service contract defaults to 24 hours and allows shorter deadlines.
- A queued intake call serializes its command once. Up to three HTTP attempts
  reuse those exact bytes, key and deadline after transport loss or retryable
  admission errors (429/503). Set `intake_attempts: 1` to disable these retries.
- Validation, submission conflicts, malformed responses and provider-unknown
  errors do not retry.
- Rails re-renders mail when a job is performed again. If the bytes or envelope
  change under an accepted key, the service must return a conflict; the adapter
  raises `Hailer::SubmissionConflictError`. Job keys alone do not provide
  transparent exactly-once business delivery.

Submission keys are command metadata, not MIME headers.

## Private status and lifecycle feed

```ruby
client = Hailer::LifecycleClient.new # same HAILER_URL origin
status = client.status(hailer_message_id)
page = client.poll(after: saved_cursor, limit: 100)
```

Status distinguishes the original RFC Message-ID, Hailer ID and provider ID,
and never returns MIME. `status.attempts` and `status.recipient_outcomes` contain
the latest 100 records each, in chronological order. Their corresponding
`*_truncated` flags report older records; use the lifecycle feed for complete
observations. An accepted message can still have an uncertain historical
attempt after an operator-authorized retry. Lifecycle events have their own cursor space; do not
reuse an existing `FeedbackClient` cursor. Persist `page.next_cursor` only after
processing the page successfully. Re-reading a cursor does not delete events.

The service's `[queued]` configuration requires explicit `max_bytes` and
`max_messages`. Bytes measure canonical logical command payload, not PostgreSQL
table/index/WAL/backups or a physical disk quota. All replicas of a deployment
must use the same budgets. Admission is transactional; a replay does not consume
another slot, and accepted messages are never evicted to admit new ones.

## Acceptance is not delivery

HTTP 202 means the service durably owns the message. The adapter exposes its ID
through `message.header[:hailer_message_id]` before Rails observers run. It does
not fabricate `ses_message_id`; provider acceptance and recipient delivery
are later observations.

Application observers must distinguish durable ownership from provider
acceptance: persist the Hailer ID and an owned/queued state, then consume
lifecycle facts separately. CommonShare's existing observer still writes
`status: "sent"` and only the SES ID for every delivered callback. The isolated
queued smoke verifies transport compatibility, not that observer's readiness
for production opt-in. Keep its synchronous configuration until this application
cutover work is done.

Unknown provider outcomes are held without an automatic resend. Seven-day
unknown-body retention (measured from durable intake) does not extend the
24-hour send-start deadline. Body purge retains receipts and uncertainty.
Only positive evidence of no acceptance permits automatic retry, with jittered
backoff capped at 600 seconds and at most 255 automatic attempts per intent.
Expiry stops new attempts; it never proves a previous attempt was unsent.

Live bodies are purged after confirmed acceptance, terminal rejection or
suppression, cancellation, or safe queued expiry. Purge releases logical
capacity, not PostgreSQL allocated disk, backups or copies already in-flight.

## Feedback and private routing

Set root `ses_configuration_set = "hailer-feedback"` in Hailer's TOML, or use
an equivalent default configuration set on the SES sending identity. Configure
SES event publishing through the dedicated SNS/SQS feedback route, preserving
`mail.tags.hailer_message_id` and `mail.tags.hailer_attempt_id`. These unique tags
allow feedback to resolve an unknown SDK response. A provider ID already
captured from an accepted response also correlates later untagged feedback.
Without tags and without a captured provider ID, feedback cannot resolve
uncertainty. Hailer never guesses from recipient, timestamps or RFC Message-ID.
Feedback received before an untagged provider ID is captured is not retroactively
correlated; retain the tags for reliable recovery.

Mothership must route the new private paths as well as the old synchronous
ones. A suitable bind pattern is:

```toml
routes = [{ bind = "mail", pattern = "^/internal/mail/(send|events|health|ready|intake-ready|dispatch-ready|messages|messages/events|messages/[0-9a-f-]{36})$" }]
```

Keep this bind private. Intake readiness depends on durable writable capacity,
not SES health. `/internal/mail/dispatch-ready` separately reports worker,
bootstrap and control eligibility. Neither readiness response guarantees that
a later send will succeed.

## Operator control

Run against the intended database using the same `HAILER_DATABASE_URL`:

```sh
hailer message-status --id MESSAGE_UUID
hailer cancel-message --id MESSAGE_UUID --reason "intent withdrawn"
hailer resolve-message --id MESSAGE_UUID --outcome accepted \
  --provider-message-id VERIFIED_SES_ID --reason "verified provider evidence"
hailer resolve-message --id MESSAGE_UUID --outcome not-accepted \
  --reason "verified no provider acceptance"
hailer resolve-message --id MESSAGE_UUID --outcome retry \
  --acknowledge-duplicate-risk --reason "investigated; accept duplicate risk"
```

Cancellation is only possible before dispatch and cannot unsend mail.
Resolution defaults to the current held-unknown attempt. Use `--attempt-id
ATTEMPT_UUID` to resolve an older unknown journal entry without overwriting a
later attempt. Retry is allowed only for the current held intent, with an
available body, an unexpired deadline, no acceptance latch and explicit
duplicate-risk acknowledgement. A new attempt never erases the old uncertainty.
All decisions require an audit reason. After expiry or body purge, sending
requires a new intent/key.

## Quarantine

Opt-in, queued pipeline only - a synchronous `:hailer` send has no way to
hold a message for later review, so a non-ASCII domain or a newly
registered one makes the synchronous adapter reject outright. The queued
adapter holds instead:

```toml
[domain_intel]
provider = "rdap"
new_domain_threshold_days = 30
```

A recipient domain that is not plain ASCII (a malformed/unencoded IDN, or a
homograph-spoofing attempt - legitimate IDNs are transmitted as ASCII
punycode) is always checked, regardless of this block. `[domain_intel]`
additionally checks the domain's RDAP registration date; newer than the
threshold holds the message as `quarantined` instead of admitting it as
`queued`. The whole message is quarantined, not just the flagged recipient -
this is an operator review gate on intent, not a per-recipient filter like
suppression or the domain blocklist (`hailer ban-domain`). RDAP's bootstrap
registry is public; no credential is needed.

```sh
hailer release-quarantine --id MESSAGE_UUID --reason "reviewed, legitimate"
hailer cancel-message --id MESSAGE_UUID --reason "rejected on review"
```

Releasing resets `next_attempt_at` to now, so the message re-enters the
normal dispatch queue immediately. `cancel-message` also accepts a
quarantined message. An unactioned quarantine still expires on the same
deadline as a normal `queued` message.

## Local application verification

`test/commonshare_smoke.rb` runs inside CommonShare's test environment with a
real Mothership bay and loopback SES, never real AWS credentials. Use a fresh
loopback PostgreSQL database named `hailer_smoke_*` for each queued run.
The script intentionally reuses one business key to check replay after purge.

```sh
RAILS_ENV=test DISABLE_BOOTSNAP=1 \
HAILER_SMOKE_QUEUED=1 \
HAILER_SMOKE_BINARY=/absolute/path/to/current/hailer \
HAILER_DATABASE_URL=postgres://...@127.0.0.1:5432/hailer_smoke_queued \
bundle exec rails runner /absolute/path/to/hailer/test/commonshare_smoke.rb
```

An isolated temporary `BUNDLE_GEMFILE` can select the working-tree gem or locally
installed verification gem without editing CommonShare's Gemfile or lock.
Set `HAILER_SMOKE_EXPECTED_PLATFORM=arm64-darwin` to assert native-gem selection.
Omit `HAILER_SMOKE_QUEUED` for the unchanged synchronous contract. The test holds
the provider response until Rails has its receipt, then checks the journal,
correlation tags, envelope/attachment, body purge, replay, conflict and feeds.

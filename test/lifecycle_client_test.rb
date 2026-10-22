# frozen_string_literal: true

require_relative 'test_helper'

class LifecycleClientTest < Minitest::Test
  include HttpHarness

  ID = 'b74698dd-ecc2-45ac-a4ce-01abf5524578'

  def test_status_is_distinct_from_delivery_and_contains_no_body
    with_server(body: JSON.generate(status_document)) do |server|
      status = client(server).status(ID)
      assert_equal "GET /internal/mail/messages/#{ID} HTTP/1.1", server.request_line
      assert_equal ID, status.hailer_message_id
      assert_equal 'held_unknown', status.state
      assert_nil status.provider_message_id
      assert_equal '<original@example.com>', status.rfc_message_id
      refute status.body_present
      assert_instance_of Time, status.accepted_at
    end
  end

  def test_rejects_status_identity_state_type_and_timestamp_mismatches
    [
      status_document.merge(status: 'delivered'),
      status_document.merge(version: -1),
      status_document.merge(body_present: 'false'),
      status_document.merge(provider_message_id: 7),
      status_document.merge(receipt: status_document[:receipt].merge(hailer_message_id: 'another-id')),
      status_document.merge(receipt: status_document[:receipt].merge(accepted_at: 'invalid'))
    ].each do |document|
      assert_protocol_error(body: JSON.generate(document)) { |server| client(server).status(ID) }
    end
    ['../send', "#{ID}?secret=x", '', nil].each do |id|
      assert_raises(Hailer::ConfigurationError) do
        Hailer::LifecycleClient.new(endpoint: 'http://127.0.0.1:1/internal/mail/send').status(id)
      end
    end
  end

  def test_status_observations_are_bounded_deep_frozen_and_report_truncation
    document = status_document.merge(
      attempts: [{ id: ID, phase: 'unknown', envelope: { to_addresses: ['to@example.com'] } }],
      recipient_outcomes: [{ kind: 'bounce', recipient: 'to@example.com' }],
      attempts_truncated: true, recipient_outcomes_truncated: false
    )
    with_server(body: JSON.generate(document)) do |server|
      status = client(server).status(ID)
      assert status.attempts_truncated
      refute status.recipient_outcomes_truncated
      assert_equal 'bounce', status.recipient_outcomes.first['kind']
      assert status.attempts.frozen?
      assert status.attempts.first.frozen?
      assert status.attempts.first['envelope']['to_addresses'].first.frozen?
      assert_raises(FrozenError) { status.recipient_outcomes.first['kind'].replace('delivered') }
    end
    [
      { attempts: [{}] * 101 },
      { recipient_outcomes: [nil] },
      { attempts: {} },
      { attempts_truncated: 'true' },
      { recipient_outcomes_truncated: nil }
    ].each do |invalid|
      assert_protocol_error(body: JSON.generate(status_document.merge(invalid))) { |server| client(server).status(ID) }
    end
  end

  def test_separate_lifecycle_feed_preserves_cursor_and_freezes_details
    with_server(body: JSON.generate(events: [entry(3)], next_cursor: 3)) do |server|
      page = client(server).poll(after: 2, limit: 1)
      assert_equal 'POST /internal/mail/messages/events HTTP/1.1', server.request_line
      assert_equal({ 'after' => 2, 'limit' => 1 }, server.payload)
      assert_equal 3, page.next_cursor
      assert_equal 'queued', page.events.first.kind
      assert page.events.frozen?
      assert page.events.first.details.frozen?
      assert page.events.first.details['observations'].frozen?
    end
    with_server(body: '{"events":[],"next_cursor":7}') do |server|
      assert_equal 7, client(server).poll(after: 7).next_cursor
    end
  end

  def test_lifecycle_feed_rejects_cursor_skips_replays_and_invalid_events
    [
      { events: [entry(2)], next_cursor: 2 },
      { events: [entry(3), entry(3)], next_cursor: 3 },
      { events: [entry(3)], next_cursor: 4 },
      { events: [], next_cursor: 3 },
      { events: [entry(3).merge(event: {})], next_cursor: 3 }
    ].each do |document|
      assert_protocol_error(body: JSON.generate(document)) { |server| client(server).poll(after: 2) }
    end
  end

  private

  def client(server) = Hailer::LifecycleClient.new(endpoint: server.endpoint)

  def status_document
    {
      receipt: { hailer_message_id: ID, accepted_at: '2026-10-21T00:00:00Z', expires_at: '2026-10-22T00:00:00Z' },
      status: 'held_unknown', version: 3, body_present: false,
      provider_message_id: nil, rfc_message_id: '<original@example.com>'
    }
  end

  def entry(sequence)
    { sequence: sequence, event: {
      hailer_message_id: ID, kind: 'queued', occurred_at: '2026-10-21T00:00:00Z',
      details: { observations: ['durable'] }
    } }
  end
end

# frozen_string_literal: true

require_relative 'test_helper'

class FeedbackTest < Minitest::Test
  include HttpHarness

  def entry(sequence = 1, kind: 'bounced')
    {
      sequence: sequence,
      event: {
        event_id: "feedback-#{sequence}", kind: kind, provider: 'ses', provider_message_id: 'ses-id',
        message_id: '<rails-id@example.com>', occurred_at: '2026-10-20T12:00:00Z',
        recipients: ['recipient@example.com'], suppression: 'bounce'
      }
    }
  end

  def test_feedback_uses_the_same_private_origin_and_returns_typed_events
    with_server(body: JSON.generate(events: [entry], next_cursor: 1)) do |server|
      page = Hailer::FeedbackClient.new(endpoint: server.endpoint).poll(after: 0, limit: 10)
      assert_equal 'POST /internal/mail/events HTTP/1.1', server.request_line
      assert_equal({ 'after' => 0, 'limit' => 10 }, server.payload)
      assert_equal 1, page.next_cursor
      assert_equal 'bounced', page.events.first.kind
      assert_equal Time.utc(2026, 10, 20, 12), page.events.first.occurred_at
      assert_equal '<rails-id@example.com>', page.events.first.message_id
    end
  end

  def test_rejects_pages_that_would_skip_or_replay_sequence_positions
    [
      { events: [entry], next_cursor: 99 },
      { events: [], next_cursor: 99 },
      { events: [entry, entry], next_cursor: 1 },
      { events: [entry(0)], next_cursor: 0 },
      { events: [entry(kind: 'unknown')], next_cursor: 1 },
      { events: [{ sequence: 1, event: nil }], next_cursor: 1 }
    ].each do |document|
      with_server(body: JSON.generate(document)) do |server|
        assert_raises(Hailer::ProtocolError) do
          Hailer::FeedbackClient.new(endpoint: server.endpoint).poll(after: 0)
        end
      end
    end
  end

  def test_empty_page_keeps_the_requested_cursor
    with_server(body: '{"events":[],"next_cursor":7}') do |server|
      page = Hailer::FeedbackClient.new(endpoint: server.endpoint).poll(after: 7)
      assert_empty page.events
      assert_equal 7, page.next_cursor
    end
  end

  def test_handler_failure_prevents_returning_an_advanced_cursor
    seen = []
    handler = lambda do |event|
      seen << event.event_id
      raise 'domain transaction failed' if event.sequence == 2
    end
    with_server(body: JSON.generate(events: [entry, entry(2)], next_cursor: 2)) do |server|
      consumer = Hailer::FeedbackConsumer.new(client: Hailer::FeedbackClient.new(endpoint: server.endpoint), handler: handler)
      error = assert_raises(RuntimeError) { consumer.call(after: 0) }
      assert_equal 'domain transaction failed', error.message
      assert_equal %w[feedback-1 feedback-2], seen
    end
  end

  def test_consumer_returns_the_cursor_only_after_all_handlers_succeed
    seen = []
    with_server(body: JSON.generate(events: [entry], next_cursor: 1)) do |server|
      consumer = Hailer::FeedbackConsumer.new(
        client: Hailer::FeedbackClient.new(endpoint: server.endpoint), handler: ->(event) { seen << event.event_id }
      )
      assert_equal 1, consumer.call(after: 0)
      assert_equal ['feedback-1'], seen
    end
  end

  def test_missing_handler_fails_before_polling_or_discarding_events
    consumer = Hailer::FeedbackConsumer.new(client: Object.new, handler: Object.new)
    assert_raises(Hailer::ConfigurationError) { consumer.call(after: 0) }
  end
end

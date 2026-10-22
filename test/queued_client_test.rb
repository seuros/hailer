# frozen_string_literal: true

require_relative 'test_helper'

class QueuedClientTest < Minitest::Test
  include HttpHarness

  ACCEPTED = { status: 202, body: '{"hailer_message_id":"hailer-test-id"}' }.freeze

  def test_durable_acceptance_is_separate_from_provider_acceptance
    with_server(**ACCEPTED) do |server|
      receipt = client(server).submit(payload, submission_key: 'invoice-42')
      assert_equal 'hailer-test-id', receipt.hailer_message_id
      refute_respond_to receipt, :message_id
      assert_equal 'POST /internal/mail/messages HTTP/1.1', server.request_line
      assert_equal 'invoice-42', server.payload.fetch('submission_key')
      refute server.payload.key?('expires_at')
    end
  end

  def test_lost_acknowledgement_retries_identical_bytes_despite_caller_mutation
    mail = payload
    key = +'invoice-42'
    expiry = +'2026-10-22T00:00:00Z'
    mutate = lambda do |server|
      next unless server.requests.size == 1

      mail[:raw_mime].replace('changed after first request')
      mail[:to_addresses] << 'another@example.com'
      key.replace('another-intent')
      expiry.replace('2026-10-23T00:00:00Z')
    end
    with_server(responses: [{ body: nil }, ACCEPTED], on_request: mutate) do |server|
      receipt = client(server).submit(mail, submission_key: key, expires_at: expiry)
      assert_equal 'hailer-test-id', receipt.hailer_message_id
      assert_equal 2, server.requests.size
      assert_equal server.requests[0][:body], server.requests[1][:body]
      assert_equal 'invoice-42', server.payload.fetch('submission_key')
      assert_equal '2026-10-22T00:00:00Z', server.payload.fetch('expires_at')
      assert_equal ['to@example.com'], server.payload.fetch('to_addresses')
    end
  end

  def test_only_retryable_admission_errors_are_retried
    with_server(responses: [
      { status: 503, body: '{"error":"storage unavailable"}' },
      { status: 429, body: '{"error":"spool full"}' },
      ACCEPTED
    ]) do |server|
      assert_equal 'hailer-test-id', client(server).submit(payload, submission_key: 'intent').hailer_message_id
      assert_equal 3, server.requests.size
      assert_equal 1, server.requests.map { |request| request[:body] }.uniq.size
    end
  end

  def test_retries_are_bounded
    with_server(responses: Array.new(3) { { status: 503, body: '{"error":"unavailable"}' } }) do |server|
      assert_raises(Hailer::UnavailableError) { client(server).submit(payload, submission_key: 'intent') }
      assert_equal 3, server.requests.size
    end
    with_server(body: nil) do |server|
      assert_raises(Hailer::TransportError) do
        client(server, intake_attempts: 1).submit(payload, submission_key: 'intent')
      end
      assert_equal 1, server.requests.size
    end
  end

  def test_conflicts_validation_and_provider_uncertainty_are_not_retried
    [
      [409, '{"error":"submission conflict"}', Hailer::SubmissionConflictError],
      [422, '{"error":"invalid mail"}', Hailer::ValidationError],
      [502, '{"error":"rejected"}', Hailer::RejectedError],
      [503, '{"error":"unknown","code":"outcome_unknown"}', Hailer::AmbiguousDeliveryError]
    ].each do |status, body, error_class|
      with_server(status: status, body: body) do |server|
        error = assert_raises(error_class) { client(server).submit(payload, submission_key: 'intent') }
        assert_equal status, error.status
        assert_equal 1, server.requests.size
      end
    end
  end

  def test_requires_202_and_a_valid_durable_receipt
    with_server(status: 200, body: '{"hailer_message_id":"not-an-intake-receipt"}') do |server|
      assert_raises(Hailer::DeliveryError) { client(server).submit(payload, submission_key: 'intent') }
    end
    ['{}', '{"hailer_message_id":null}', '{"hailer_message_id":4}', '{"hailer_message_id":" "}', '[]', 'not json'].each do |body|
      intake = assert_protocol_error(status: 202, body: body) { |server| client(server).submit(payload, submission_key: 'intent') }
      assert_equal 1, intake.requests.size
    end
  end

  def test_preserves_the_response_limit_and_does_not_retry_protocol_failures
    oversized = JSON.generate(hailer_message_id: 'x' * Hailer::Transport::MAX_RESPONSE_BYTES)
    intake = assert_protocol_error(status: 202, body: oversized) { |server| client(server).submit(payload, submission_key: 'intent') }
    assert_equal 1, intake.requests.size
  end

  def test_rejects_invalid_keys_and_reserved_fields_before_connecting
    client = Hailer::QueuedClient.new(endpoint: 'http://127.0.0.1:1/internal/mail/send')
    [nil, '', ' ', 4, 'x' * 257].each do |key|
      assert_raises(Hailer::ConfigurationError) { client.submit(payload, submission_key: key) }
    end
    [:submission_key, 'submission_key', :expires_at, 'expires_at'].each do |key|
      assert_raises(Hailer::ConfigurationError) { client.submit(payload.merge(key => 'value'), submission_key: 'intent') }
    end
    [0, 4, nil, '3'].each do |attempts|
      assert_raises(Hailer::ConfigurationError) do
        Hailer::QueuedClient.new(endpoint: 'http://127.0.0.1:1/internal/mail/send', intake_attempts: attempts)
      end
    end
  end

  private

  def payload
    { raw_mime: +"From: from@example.com\r\n\r\nHello", to_addresses: ['to@example.com'] }
  end

  def client(server, **options)
    Hailer::QueuedClient.new(endpoint: server.endpoint, **options)
  end
end

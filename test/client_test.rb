# frozen_string_literal: true

require_relative 'test_helper'

class ClientTest < Minitest::Test
  include HttpHarness

  def test_posts_the_shared_rust_contract
    payload = JSON.parse(File.read(File.join(__dir__, 'fixtures/send_request.json')))
    with_server do |server|
      response = Hailer::Client.new(endpoint: server.endpoint).deliver(payload)
      assert_equal 'ses-test-id', response.message_id
      assert_equal 'POST /internal/mail/send HTTP/1.1', server.request_line
      assert_equal 'application/json', server.headers['content-type']
      assert_equal payload, server.payload
    end
  end

  def test_maps_http_failures_to_delivery_errors
    {
      400 => Hailer::ValidationError,
      422 => Hailer::ValidationError,
      429 => Hailer::RateLimitedError,
      502 => Hailer::RejectedError,
      503 => Hailer::UnavailableError,
      404 => Hailer::DeliveryError,
      302 => Hailer::DeliveryError
    }.each do |status, error_class|
      with_server(status: status, body: '{"error":"delivery failed"}') do |server|
        error = assert_raises(error_class) { client(server).deliver({}) }
        assert_equal status, error.status
        assert_equal 'delivery failed', error.message
      end
    end
  end

  def test_does_not_treat_other_success_statuses_as_delivery_acceptance
    with_server(status: 202, body: '{"message_id":"not-accepted-by-ses"}') do |server|
      assert_raises(Hailer::DeliveryError) { client(server).deliver({}) }
    end
  end

  def test_an_ambiguous_ses_outcome_is_distinct_from_an_unavailable_service
    with_server(status: 503, body: '{"error":"provider outcome unknown","code":"outcome_unknown"}') do |server|
      error = assert_raises(Hailer::AmbiguousDeliveryError) { client(server).deliver({}) }
      assert_equal 503, error.status
    end
  end

  def test_rejects_invalid_success_responses
    ['not json', '[]', 'null', '{}', '{"message_id":null}', '{"message_id":4}', '{"message_id":" "}'].each do |body|
      with_server(body: body) do |server|
        assert_raises(Hailer::ProtocolError) { client(server).deliver({}) }
      end
    end
  end

  def test_invalid_error_json_is_a_protocol_error_not_a_success
    with_server(status: 502, body: '<html>Bad gateway</html>') do |server|
      assert_raises(Hailer::ProtocolError) { client(server).deliver({}) }
    end
  end

  def test_limits_response_size
    with_server(body: JSON.generate(message_id: 'x' * Hailer::Transport::MAX_RESPONSE_BYTES)) do |server|
      assert_raises(Hailer::ProtocolError) { client(server).deliver({}) }
    end
  end

  def test_connection_loss_is_reported_as_an_ambiguous_transport_failure
    with_server(body: nil) do |server|
      error = assert_raises(Hailer::TransportError) { client(server).deliver({}) }
      assert_match(/outcome may be unknown/, error.message)
      assert_equal({}, server.payload)
    end
  end

  def test_requires_an_explicit_send_endpoint
    [nil, '', 'garbage', 'ftp://localhost/internal/mail/send', 'http://localhost/',
     'http://user:secret@localhost/internal/mail/send', 'http://localhost/internal/mail/send?secret=x',
     'http://localhost/internal/mail/send#fragment'].each do |endpoint|
      assert_raises(Hailer::ConfigurationError) { Hailer::Client.new(endpoint: endpoint) }
    end
  end

  def test_rejects_invalid_timeout_settings
    [0, -1, nil, '5', Float::INFINITY, Float::NAN].each do |value|
      assert_raises(Hailer::ConfigurationError) do
        Hailer::Client.new(endpoint: 'http://localhost/internal/mail/send', read_timeout: value)
      end
    end
    assert_raises(Hailer::ConfigurationError) do
      Hailer::Client.new(endpoint: 'http://localhost/internal/mail/send', unknown: 1)
    end
  end

  private

  def client(server)
    Hailer::Client.new(endpoint: server.endpoint)
  end
end

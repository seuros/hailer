# frozen_string_literal: true

require_relative 'test_helper'

class DeliveryMethodTest < Minitest::Test
  include HttpHarness

  def test_preserves_rendered_mime_and_recipient_groups
    message = build_message
    message.attachments['report.bin'] = "\x00\x01\xff".b
    raw_mime = message.to_s

    with_server do |server|
      response = delivery(server).deliver!(message)
      assert_equal raw_mime, server.payload.fetch('raw_mime')
      assert_equal ['to@example.com'], server.payload.fetch('to_addresses')
      assert_equal ['cc@example.com'], server.payload.fetch('cc_addresses')
      assert_equal ['bcc@example.com'], server.payload.fetch('bcc_addresses')
      refute server.payload.key?('from_email_address')
      assert_equal 'ses-test-id', response.message_id
      assert_equal 'ses-test-id', message.header[:ses_message_id].value
      decoded = Mail.read_from_string(server.payload.fetch('raw_mime'))
      assert_equal "\x00\x01\xff".b, decoded.attachments.first.decoded.b
      assert_equal ['Alice Example'], decoded[:from].display_names
    end
  end

  def test_preserves_explicit_envelope_overrides_without_rewriting_headers
    message = build_message
    message.smtp_envelope_from = 'bounce@example.com'
    message.smtp_envelope_to = ['override@example.com']

    with_server do |server|
      delivery(server).deliver!(message)
      assert_equal 'bounce@example.com', server.payload.fetch('from_email_address')
      assert_equal ['override@example.com'], server.payload.fetch('to_addresses')
      assert_equal ['cc@example.com'], server.payload.fetch('cc_addresses')
      assert_equal ['bcc@example.com'], server.payload.fetch('bcc_addresses')
      assert_equal ['to@example.com'], Mail.read_from_string(server.payload.fetch('raw_mime')).to
    end
  end

  def test_normalizes_missing_recipient_groups_to_arrays
    message = build_message
    message.to = nil
    message.cc = nil

    with_server do |server|
      delivery(server).deliver!(message)
      assert_equal [], server.payload.fetch('to_addresses')
      assert_equal [], server.payload.fetch('cc_addresses')
      assert_equal ['bcc@example.com'], server.payload.fetch('bcc_addresses')
    end
  end

  def test_leaves_recipient_validation_to_the_service
    message = build_message
    message.to = message.cc = message.bcc = nil

    with_server(status: 422, body: '{"error":"no recipients"}') do |server|
      assert_raises(Hailer::ValidationError) { delivery(server).deliver!(message) }
      assert_nil message.header[:ses_message_id]
    end
  end

  def test_failure_does_not_set_the_ses_message_id
    with_server(status: 503, body: '{"error":"circuit open"}') do |server|
      message = build_message
      assert_raises(Hailer::UnavailableError) { delivery(server).deliver!(message) }
      assert_nil message.header[:ses_message_id]
    end
  end

  def test_works_as_a_plain_mail_delivery_method
    with_server do |server|
      message = build_message
      message.delivery_method(Hailer::DeliveryMethod, endpoint: server.endpoint)
      assert_same message, message.deliver!
      assert_equal 'ses-test-id', message.header[:ses_message_id].value
    end
  end

  def test_settings_remain_configurable_between_deliveries
    adapter = Hailer::DeliveryMethod.new
    with_server do |server|
      adapter.settings[:endpoint] = server.endpoint
      adapter.deliver!(build_message)
      assert_equal ['to@example.com'], server.payload.fetch('to_addresses')
    end
  end

  private

  def delivery(server)
    Hailer::DeliveryMethod.new(endpoint: server.endpoint)
  end

  def build_message
    Mail.new do
      from 'Alice Example <alice@example.com>'
      to 'to@example.com'
      cc 'cc@example.com'
      bcc 'bcc@example.com'
      subject 'Delivery parity'
      body 'Hello from Rails.'
    end
  end
end

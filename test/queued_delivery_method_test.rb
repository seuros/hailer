# frozen_string_literal: true

require_relative 'test_helper'

class QueuedDeliveryMethodTest < Minitest::Test
  include HttpHarness
  include MailFixtures

  ACCEPTED = { status: 202, body: '{"hailer_message_id":"hailer-test-id"}' }.freeze

  def test_preserves_mime_attachments_and_explicit_envelopes_without_fabricating_ses_id
    mail = build_message
    mail.attachments['report.bin'] = "\x00\xff".b
    mail.smtp_envelope_from = 'bounce@example.com'
    mail.smtp_envelope_to = ['override@example.com']
    mime = mail.to_s
    with_server(**ACCEPTED) do |server|
      receipt = adapter(server).deliver!(mail)
      assert_equal mime, server.payload.fetch('raw_mime')
      assert_equal ['override@example.com'], server.payload.fetch('to_addresses')
      assert_equal ['cc@example.com'], server.payload.fetch('cc_addresses')
      assert_equal ['bcc@example.com'], server.payload.fetch('bcc_addresses')
      assert_equal 'bounce@example.com', server.payload.fetch('from_email_address')
      assert_equal "\x00\xff".b, Mail.read_from_string(server.payload.fetch('raw_mime')).attachments.first.decoded.b
      assert_equal 'hailer-test-id', receipt.hailer_message_id
      assert_equal 'hailer-test-id', mail.header[:hailer_message_id].value
      assert_nil mail.header[:ses_message_id]
      refute_includes server.payload.fetch('raw_mime'), 'hailer_message_id'
    end
  end

  def test_direct_calls_are_distinct_intents_unless_explicitly_keyed
    with_server(responses: [ACCEPTED, ACCEPTED]) do |server|
      delivery = adapter(server)
      2.times { delivery.deliver!(build_message) }
      keys = server.requests.map { |request| request[:payload].fetch('submission_key') }
      assert_equal 2, keys.uniq.size
      keys.each { |key| assert_match(/\A[0-9a-f-]{36}\z/, key) }
    end
    with_server(**ACCEPTED) do |server|
      adapter(server, submission_key: 'business-intent', expires_at: '2026-10-22T00:00:00Z').deliver!(build_message)
      assert_equal 'business-intent', server.payload.fetch('submission_key')
      assert_equal '2026-10-22T00:00:00Z', server.payload.fetch('expires_at')
    end
  end

  def test_job_identity_and_ordinal_reset_for_a_retry_and_explicit_keys_take_precedence
    with_server(responses: Array.new(4, ACCEPTED)) do |server|
      2.times do
        Hailer::DeliveryContext.set(job_id: 'stable-job', ordinal: 0) do
          adapter(server, submission_key: 'business-key').deliver!(build_message)
          adapter(server).deliver!(build_message)
        end
      end
      keys = server.requests.map { |request| request[:payload].fetch('submission_key') }
      assert_equal ['business-key', 'hailer:job:stable-job:2'] * 2, keys
    end
    assert_nil Hailer::DeliveryContext.job_id
  end

  def test_rejection_leaves_both_ids_unset
    with_server(status: 409, body: '{"error":"changed intent"}') do |server|
      mail = build_message
      assert_raises(Hailer::SubmissionConflictError) { adapter(server, submission_key: 'intent').deliver!(mail) }
      assert_nil mail.header[:hailer_message_id]
      assert_nil mail.header[:ses_message_id]
    end
  end

  private

  def adapter(server, **options)
    Hailer::QueuedDeliveryMethod.new(endpoint: server.endpoint, **options)
  end
end

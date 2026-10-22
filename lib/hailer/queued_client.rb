# frozen_string_literal: true

module Hailer
  Acceptance = Data.define(:hailer_message_id)

  # Acceptance means PostgreSQL owns the message, not that SES accepted it.
  class QueuedClient
    PATH = '/internal/mail/messages'

    def initialize(intake_attempts: 3, **settings)
      unless intake_attempts.is_a?(Integer) && (1..3).cover?(intake_attempts)
        raise ConfigurationError, 'intake_attempts must be in 1..3'
      end
      @intake_attempts = intake_attempts
      @transport = Transport.new(**settings)
    end

    def submit(payload, submission_key:, expires_at: nil)
      unless submission_key.is_a?(String) && !submission_key.strip.empty? && submission_key.bytesize <= 256
        raise ConfigurationError, 'submission_key must be nonempty text of at most 256 bytes'
      end
      unless payload.is_a?(Hash) && payload.keys.none? { |key| %w[submission_key expires_at].include?(key.to_s) }
        raise ConfigurationError, 'pass submission_key and expires_at separately from the mail payload'
      end
      command = payload.merge(submission_key: submission_key)
      command[:expires_at] = expires_at unless expires_at.nil?
      # Freeze serialization before the first request: retries must not re-render
      # MIME, regenerate keys, extend deadlines or observe caller mutations.
      body = JSON.generate(command).freeze
      document = submit_frozen(body)
      id = document['hailer_message_id']
      unless id.is_a?(String) && !id.strip.empty?
        raise ProtocolError, 'Hailer accepted the request without a hailer_message_id'
      end
      Acceptance.new(hailer_message_id: id.freeze)
    end

    private

    def submit_frozen(body)
      attempts = 0
      begin
        attempts += 1
        @transport.post_serialized(PATH, body, expected_status: 202)
      rescue TransportError, UnavailableError, RateLimitedError
        raise if attempts >= @intake_attempts

        sleep(0.05 * (2**(attempts - 1)))
        retry
      end
    end
  end
end

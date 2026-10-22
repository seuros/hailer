# frozen_string_literal: true

require 'mail'

module Hailer
  class DeliveryMethod
    attr_reader :settings

    def initialize(options = {})
      @settings = { endpoint: ENV['HAILER_URL'], **Transport::DEFAULTS }.merge(options)
    end

    def deliver!(message)
      Client.new(**settings).deliver(payload_for(message)).tap do |response|
        message.header[:ses_message_id] = response.message_id
      end
    end

    private

    def payload_for(message)
      payload = {
        raw_mime: message.to_s,
        to_addresses: Array(explicit_envelope?(message, :to) ? message.smtp_envelope_to : message.to),
        cc_addresses: Array(message.cc),
        bcc_addresses: Array(message.bcc)
      }
      # The default smtp_envelope_from drops display names. Let SES read the
      # MIME From header unless the caller explicitly overrode the envelope.
      if explicit_envelope?(message, :from)
        payload[:from_email_address] = message.smtp_envelope_from
      end

      payload
    end

    # Match the existing SESv2 adapter's distinction between Mail's computed
    # defaults and explicitly assigned SMTP envelope addresses.
    def explicit_envelope?(message, field)
      message.instance_variable_get(:"@smtp_envelope_#{field}")
    end
  end
end

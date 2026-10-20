# frozen_string_literal: true

module Hailer
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class ProtocolError < Error; end
  class BinaryError < Error; end

  # The request may already have reached SES. Retrying can send a duplicate.
  class TransportError < Error; end

  class DeliveryError < Error
    attr_reader :status

    def initialize(message, status:)
      @status = status
      super(message)
    end
  end

  class ValidationError < DeliveryError; end
  class RejectedError < DeliveryError; end
  class UnavailableError < DeliveryError; end
end

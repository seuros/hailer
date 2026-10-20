# frozen_string_literal: true

module Hailer
  Response = Data.define(:message_id)

  class Client
    def initialize(**settings)
      @transport = Transport.new(**settings)
    end

    def deliver(payload)
      document = @transport.post('/internal/mail/send', payload)
      message_id = document['message_id']
      unless message_id.is_a?(String) && !message_id.strip.empty?
        raise ProtocolError, 'Hailer accepted the request without a message_id'
      end
      Response.new(message_id: message_id)
    end
  end
end

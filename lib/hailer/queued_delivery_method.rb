# frozen_string_literal: true

require 'securerandom'

module Hailer
  class QueuedDeliveryMethod < DeliveryMethod
    def deliver!(message)
      job_key = DeliveryContext.next_submission_key
      key = settings.fetch(:submission_key) { job_key || SecureRandom.uuid }
      options = settings.except(:submission_key, :expires_at)
      QueuedClient.new(**options).submit(
        payload_for(message), submission_key: key, expires_at: settings[:expires_at]
      ).tap do |receipt|
        message.header[:hailer_message_id] = receipt.hailer_message_id
      end
    end
  end
end

# frozen_string_literal: true

module Hailer
  # The caller persists the returned cursor with its domain changes. An
  # exception returns no cursor; retrying the page requires idempotent handlers.
  class FeedbackConsumer
    def initialize(client: nil, handler: nil)
      @client = client || FeedbackClient.new(**ActionMailer::Base.hailer_settings)
      @handler = handler || Rails.application.config.hailer.feedback_handler
    end

    def call(after:, limit: 100)
      # Resolve a configured class name on every poll, respecting Rails reloads.
      handler = @handler.is_a?(String) ? @handler.constantize : @handler
      raise ConfigurationError, 'configure config.hailer.feedback_handler with a callable handler' unless handler.respond_to?(:call)

      page = @client.poll(after: after, limit: limit)
      page.events.each { |event| handler.call(event) }
      page.next_cursor
    end
  end
end

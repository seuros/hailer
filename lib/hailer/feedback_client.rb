# frozen_string_literal: true

require 'time'

module Hailer
  FeedbackEvent = Data.define(
    :sequence, :event_id, :kind, :provider, :provider_message_id,
    :message_id, :occurred_at, :recipients, :suppression
  )
  FeedbackPage = Data.define(:events, :next_cursor)

  class FeedbackClient
    KINDS = %w[sent delivered bounced complained rejected rendering_failed delayed opened clicked].freeze
    MAX_RESPONSE_BYTES = 2 * 1024 * 1024

    def initialize(**settings)
      @transport = Transport.new(**settings)
    end

    def poll(after:, limit: 100)
      unless after.is_a?(Integer) && after >= 0 && limit.is_a?(Integer) && (1..100).cover?(limit)
        raise ConfigurationError, 'feedback requires a nonnegative cursor and limit in 1..100'
      end
      document = @transport.post('/internal/mail/events', { after: after, limit: limit },
                                 max_response_bytes: MAX_RESPONSE_BYTES)
      entries = document['events']
      cursor = document['next_cursor']
      unless entries.is_a?(Array) && entries.size <= limit && cursor.is_a?(Integer) && cursor >= after
        raise ProtocolError, 'invalid Hailer feedback page'
      end
      previous = after
      events = entries.map do |entry|
        event = decode_event(entry)
        raise ProtocolError, 'feedback sequences must increase past the requested cursor' unless event.sequence > previous

        previous = event.sequence
        event
      end
      raise ProtocolError, 'feedback cursor does not match its page' unless cursor == previous

      FeedbackPage.new(events: events.freeze, next_cursor: cursor)
    end

    private

    def decode_event(entry)
      raise ProtocolError, 'invalid feedback entry' unless entry.is_a?(Hash) && entry['sequence'].is_a?(Integer)

      event = entry['event']
      unless event.is_a?(Hash) && KINDS.include?(event['kind']) &&
             %w[event_id provider provider_message_id occurred_at].all? { |key| text?(event[key]) } &&
             (event['message_id'].nil? || text?(event['message_id'])) &&
             [nil, 'bounce', 'complaint'].include?(event['suppression']) &&
             event['recipients'].is_a?(Array) && !event['recipients'].empty? &&
             event['recipients'].all? { |address| text?(address) }
        raise ProtocolError, 'invalid normalized Hailer event'
      end
      FeedbackEvent.new(
        sequence: entry['sequence'], event_id: event['event_id'].freeze,
        kind: event['kind'].freeze, provider: event['provider'].freeze,
        provider_message_id: event['provider_message_id'].freeze,
        message_id: event['message_id']&.freeze, occurred_at: Time.iso8601(event['occurred_at']).freeze,
        recipients: event['recipients'].map(&:freeze).freeze, suppression: event['suppression']&.freeze
      )
    rescue ArgumentError
      raise ProtocolError, 'invalid feedback event timestamp'
    end

    def text?(value) = value.is_a?(String) && !value.strip.empty?
  end
end

# frozen_string_literal: true

require 'time'

module Hailer
  MessageStatus = Data.define(
    :hailer_message_id, :state, :version, :body_present, :provider_message_id,
    :rfc_message_id, :accepted_at, :expires_at, :attempts, :recipient_outcomes,
    :attempts_truncated, :recipient_outcomes_truncated
  )
  MessageEvent = Data.define(:sequence, :hailer_message_id, :kind, :occurred_at, :details)
  MessageEventPage = Data.define(:events, :next_cursor)

  # Deliberately separate from the unchanged provider-feedback client/feed.
  class LifecycleClient
    ID = /\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/
    STATES = %w[queued claimed dispatching retry_waiting held_unknown accepted rejected suppressed cancelled expired].freeze
    MAX_RESPONSE_BYTES = 2 * 1024 * 1024

    def initialize(**settings)
      @transport = Transport.new(**settings)
    end

    def status(hailer_message_id)
      unless hailer_message_id.is_a?(String) && ID.match?(hailer_message_id)
        raise ConfigurationError, 'expected a Hailer message UUID'
      end
      document = @transport.get("/internal/mail/messages/#{hailer_message_id}",
                                max_response_bytes: MAX_RESPONSE_BYTES)
      receipt = document['receipt']
      unless receipt.is_a?(Hash) && receipt['hailer_message_id'] == hailer_message_id &&
             STATES.include?(document['status']) &&
             document['version'].is_a?(Integer) && document['version'] >= 0 &&
             [true, false].include?(document['body_present']) &&
             optional_text?(document['provider_message_id']) && optional_text?(document['rfc_message_id'])
        raise ProtocolError, 'invalid Hailer message status'
      end
      accepted_at = timestamp(receipt['accepted_at'])
      expires_at = timestamp(receipt['expires_at'])
      raise ProtocolError, 'invalid Hailer message deadline' unless expires_at > accepted_at

      MessageStatus.new(
        hailer_message_id: hailer_message_id.dup.freeze, state: document['status'].freeze,
        version: document['version'], body_present: document['body_present'],
        provider_message_id: document['provider_message_id']&.freeze,
        rfc_message_id: document['rfc_message_id']&.freeze,
        accepted_at: accepted_at, expires_at: expires_at,
        attempts: status_records(document, 'attempts'),
        recipient_outcomes: status_records(document, 'recipient_outcomes'),
        attempts_truncated: truncation_flag(document, 'attempts_truncated'),
        recipient_outcomes_truncated: truncation_flag(document, 'recipient_outcomes_truncated')
      )
    end

    def poll(after:, limit: 100)
      unless after.is_a?(Integer) && (0..9_223_372_036_854_775_807).cover?(after) &&
             limit.is_a?(Integer) && (1..100).cover?(limit)
        raise ConfigurationError, 'lifecycle requires a supported nonnegative cursor and limit in 1..100'
      end
      document = @transport.post('/internal/mail/messages/events', { after: after, limit: limit },
                                 max_response_bytes: MAX_RESPONSE_BYTES)
      entries = document['events']
      unless entries.is_a?(Array) && entries.size <= limit && document['next_cursor'].is_a?(Integer)
        raise ProtocolError, 'invalid Hailer lifecycle page'
      end
      previous = after
      events = entries.map do |entry|
        unless entry.is_a?(Hash) && entry['sequence'].is_a?(Integer) && entry['sequence'] > previous
          raise ProtocolError, 'lifecycle sequences must increase past the requested cursor'
        end
        event = entry['event']
        unless event.is_a?(Hash) && event['hailer_message_id'].is_a?(String) &&
               ID.match?(event['hailer_message_id']) && text?(event['kind']) && event['details'].is_a?(Hash)
          raise ProtocolError, 'invalid Hailer lifecycle event'
        end
        previous = entry['sequence']
        MessageEvent.new(
          sequence: previous, hailer_message_id: event['hailer_message_id'].freeze,
          kind: event['kind'].freeze, occurred_at: timestamp(event['occurred_at']),
          details: freeze_json(event['details'])
        )
      end
      raise ProtocolError, 'lifecycle cursor does not match its page' unless document['next_cursor'] == previous

      MessageEventPage.new(events: events.freeze, next_cursor: previous)
    end

    private

    def text?(value) = value.is_a?(String) && !value.strip.empty?
    def optional_text?(value) = value.nil? || text?(value)

    def status_records(document, name)
      records = document.fetch(name, [])
      unless records.is_a?(Array) && records.size <= 100 && records.all? { |record| record.is_a?(Hash) }
        raise ProtocolError, 'invalid Hailer status observations'
      end
      freeze_json(records)
    end

    def truncation_flag(document, name)
      flag = document.fetch(name, false)
      raise ProtocolError, 'invalid Hailer status truncation flag' unless [true, false].include?(flag)

      flag
    end

    def timestamp(value)
      raise ProtocolError, 'invalid Hailer lifecycle timestamp' unless text?(value)

      Time.iso8601(value).freeze
    rescue ArgumentError
      raise ProtocolError, 'invalid Hailer lifecycle timestamp'
    end

    def freeze_json(value)
      case value
      when Hash then value.each_value { |child| freeze_json(child) }
      when Array then value.each { |child| freeze_json(child) }
      end
      value.freeze
    end
  end
end

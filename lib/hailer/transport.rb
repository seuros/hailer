# frozen_string_literal: true

require 'json'
require 'net/http'
require 'uri'

module Hailer
  # No persistent connection across forks, redirects, HTTP_PROXY, or retries.
  class Transport
    DEFAULTS = { open_timeout: 5, read_timeout: 30, write_timeout: 30 }.freeze
    MAX_RESPONSE_BYTES = 65_536
    HTTP_ERRORS = {
      400 => ValidationError, 409 => SubmissionConflictError, 422 => ValidationError, 429 => RateLimitedError,
      502 => RejectedError, 503 => UnavailableError
    }.freeze

    def initialize(endpoint: ENV['HAILER_URL'], **timeouts)
      @uri = URI.parse(endpoint.to_s)
      unless @uri.is_a?(URI::HTTP) && @uri.host && !@uri.host.empty? &&
             !@uri.userinfo && !@uri.query && !@uri.fragment &&
             @uri.path == '/internal/mail/send'
        raise ConfigurationError, 'set HAILER_URL or endpoint to an HTTP(S) URL ending in /internal/mail/send'
      end
      @timeouts = DEFAULTS.merge(timeouts)
      raise ConfigurationError, 'unknown Hailer timeout setting' unless (@timeouts.keys - DEFAULTS.keys).empty?

      unless @timeouts.values.all? { |value| value.is_a?(Numeric) && value.finite? && value.positive? }
        raise ConfigurationError, 'Hailer timeouts must be positive, finite numbers'
      end
    rescue URI::InvalidURIError
      raise ConfigurationError, 'invalid Hailer endpoint URL'
    end

    def post(path, payload, max_response_bytes: MAX_RESPONSE_BYTES)
      post_serialized(path, JSON.generate(payload), max_response_bytes: max_response_bytes)
    end

    # Queued intake owns retries and passes the same serialized bytes each time.
    # Synchronous sends continue to require HTTP 200 and never retry.
    def post_serialized(path, body, expected_status: 200, max_response_bytes: MAX_RESPONSE_BYTES)
      request = Net::HTTP::Post.new(path)
      request.content_type = 'application/json'
      request.body = body
      exchange_document(request, expected_status, max_response_bytes)
    end

    def get(path, max_response_bytes: MAX_RESPONSE_BYTES)
      exchange_document(Net::HTTP::Get.new(path), 200, max_response_bytes)
    end

    private

    def exchange_document(request, expected_status, max_response_bytes)
      request['Accept'] = 'application/json'
      http = Net::HTTP.new(@uri.hostname, @uri.port, nil)
      http.use_ssl = @uri.scheme == 'https'
      http.max_retries = 0
      @timeouts.each { |name, value| http.public_send(:"#{name}=", value) }
      status, body = exchange(http, request, max_response_bytes)
      decode_response(status, body, expected_status)
    rescue Timeout::Error, IOError, EOFError, SystemCallError, SocketError,
           OpenSSL::SSL::SSLError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError => error
      raise TransportError, "Hailer transport failed (#{error.class}); delivery outcome may be unknown"
    end

    def exchange(http, request, limit)
      status = nil
      body = +''
      http.start do |connection|
        connection.request(request) do |response|
          status = response.code.to_i
          response.read_body do |chunk|
            raise ProtocolError, 'Hailer response exceeds the response size limit' if body.bytesize + chunk.bytesize > limit

            body << chunk
          end
        end
      end
      [status, body]
    end

    def decode_response(status, body, expected_status)
      document = JSON.parse(body)
      raise ProtocolError, 'Hailer response must be a JSON object' unless document.is_a?(Hash)

      unless status == expected_status
        message = document['error']
        message = "Hailer returned HTTP #{status}" unless message.is_a?(String) && !message.empty?
        if status == 503 && document['code'] == 'outcome_unknown'
          raise AmbiguousDeliveryError.new(message, status: status)
        end
        raise HTTP_ERRORS.fetch(status, DeliveryError).new(message, status: status)
      end
      document
    rescue JSON::ParserError
      raise ProtocolError, "Hailer returned invalid JSON (HTTP #{status})"
    end
  end
end

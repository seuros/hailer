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
      400 => ValidationError, 422 => ValidationError,
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
      request = Net::HTTP::Post.new(path)
      request.content_type = 'application/json'
      request['Accept'] = 'application/json'
      request.body = JSON.generate(payload)
      http = Net::HTTP.new(@uri.hostname, @uri.port, nil)
      http.use_ssl = @uri.scheme == 'https'
      http.max_retries = 0
      @timeouts.each { |name, value| http.public_send(:"#{name}=", value) }
      status, body = exchange(http, request, max_response_bytes)
      decode_response(status, body)
    rescue Timeout::Error, IOError, EOFError, SystemCallError, SocketError,
           OpenSSL::SSL::SSLError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError => error
      raise TransportError, "Hailer transport failed (#{error.class}); delivery outcome may be unknown"
    end

    private

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

    def decode_response(status, body)
      document = JSON.parse(body)
      raise ProtocolError, 'Hailer response must be a JSON object' unless document.is_a?(Hash)

      unless status == 200
        message = document['error']
        message = "Hailer returned HTTP #{status}" unless message.is_a?(String) && !message.empty?
        raise HTTP_ERRORS.fetch(status, DeliveryError).new(message, status: status)
      end
      document
    rescue JSON::ParserError
      raise ProtocolError, "Hailer returned invalid JSON (HTTP #{status})"
    end
  end
end

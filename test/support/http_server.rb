# frozen_string_literal: true

require 'socket'
require 'timeout'

# Scripted real HTTP exchanges, without a web server or mocking Net::HTTP.
class TestHttpServer
  attr_reader :endpoint, :request_line, :headers, :payload, :requests

  def initialize(status: 200, body: '{"message_id":"ses-test-id"}', responses: nil, on_request: nil)
    @listener = TCPServer.new('127.0.0.1', 0)
    @endpoint = "http://127.0.0.1:#{@listener.addr[1]}/internal/mail/send"
    @requests = []
    replies = responses || [{ status: status, body: body }]
    @thread = Thread.new do
      Timeout.timeout(10) do
        replies.each do |reply|
          @socket = @listener.accept
          @request_line = @socket.gets("\r\n").strip
          @headers = {}
          while (line = @socket.gets("\r\n")) != "\r\n"
            name, value = line.split(':', 2)
            @headers[name.downcase] = value.strip
          end
          raw_body = @socket.read(Integer(@headers.fetch('content-length', '0')))
          @payload = raw_body.empty? ? nil : JSON.parse(raw_body)
          @requests << { request_line: @request_line, headers: @headers, body: raw_body, payload: @payload }
          on_request&.call(self)
          if (response = reply[:body])
            @socket.write("HTTP/1.1 #{reply.fetch(:status)} Test\r\nContent-Type: application/json\r\n" \
                          "Content-Length: #{response.bytesize}\r\nConnection: close\r\n\r\n#{response}")
          end
          @socket.close
        end
      end
    end
    @thread.report_on_exception = false
  end

  def finish
    @thread.value
  end

  def close
    @listener.close
    @thread.kill
    @thread.join
    @socket&.close unless @socket&.closed?
  end
end

# frozen_string_literal: true

require 'socket'
require 'timeout'

# A single real HTTP exchange, without a web server or mocking Net::HTTP.
class TestHttpServer
  attr_reader :endpoint, :request_line, :headers, :payload

  def initialize(status: 200, body: '{"message_id":"ses-test-id"}')
    @listener = TCPServer.new('127.0.0.1', 0)
    @endpoint = "http://127.0.0.1:#{@listener.addr[1]}/internal/mail/send"
    @thread = Thread.new do
      Timeout.timeout(10) do
        @socket = @listener.accept
        @request_line = @socket.gets("\r\n").strip
        @headers = {}
        while (line = @socket.gets("\r\n")) != "\r\n"
          name, value = line.split(':', 2)
          @headers[name.downcase] = value.strip
        end
        @payload = JSON.parse(@socket.read(Integer(@headers.fetch('content-length'))))
        if body
          @socket.write("HTTP/1.1 #{status} Test\r\nContent-Type: application/json\r\n" \
                        "Content-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
        end
        @socket.close
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

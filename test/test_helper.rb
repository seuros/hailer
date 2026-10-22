# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

require 'hailer'
require 'minitest/autorun'
require_relative 'support/http_server'
require_relative 'support/mail_fixtures'

module HttpHarness
  def with_server(**options)
    server = TestHttpServer.new(**options)
    yield server
    server.finish
  ensure
    server&.close
  end

  # Scripts the server response, asserts the block raises ProtocolError, and
  # returns the finished server so callers can still inspect its requests.
  def assert_protocol_error(**server_options)
    finished = nil
    with_server(**server_options) do |server|
      assert_raises(Hailer::ProtocolError) { yield server }
      finished = server
    end
    finished
  end
end

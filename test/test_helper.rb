# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

require 'hailer'
require 'minitest/autorun'
require_relative 'support/http_server'

module HttpHarness
  def with_server(**options)
    server = TestHttpServer.new(**options)
    yield server
    server.finish
  ensure
    server&.close
  end
end

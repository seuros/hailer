# frozen_string_literal: true

require_relative 'test_helper'
require 'open3'
require 'rbconfig'
require 'tmpdir'

class RailtieTest < Minitest::Test
  include HttpHarness

  BOOT = <<~'RUBY'
    require 'hailer'
    require 'logger'

    class MailProbe < Rails::Application
      config.eager_load = false
      config.logger = Logger.new(nil)
      config.secret_key_base = 'x' * 64
      config.action_mailer.delivery_method = ARGV.first.to_sym unless ARGV.first.empty?
    end
    MailProbe.initialize!

    result = {
      registered: ActionMailer::Base.delivery_methods[:hailer] == Hailer::DeliveryMethod,
      selected: ActionMailer::Base.delivery_method
    }
    if %w[send later].include?(ARGV[1])
      class ProbeMailer < ActionMailer::Base
        def welcome
          mail(from: 'alice@example.com', to: 'to@example.com', subject: 'Rails handoff', body: 'Hello')
        end
      end
      class ProbeObserver
        def self.delivered_email(message)
          @message_id = message.header[:ses_message_id]&.value
        end

        def self.message_id = @message_id
      end
      ProbeMailer.register_observer(ProbeObserver)
      begin
        if ARGV[1] == 'later'
          ActiveJob::Base.queue_adapter = :test
          job = ProbeMailer.welcome.deliver_later
          result[:job_class] = job.class.name
          result[:jobs] = ActiveJob::Base.queue_adapter.enqueued_jobs.size
          result[:observer_message_id] = ProbeObserver.message_id
        else
          message = ProbeMailer.welcome.deliver_now
          result[:message_id] = message.header[:ses_message_id]&.value
          result[:observer_message_id] = ProbeObserver.message_id
          result[:test_deliveries] = ActionMailer::Base.deliveries.size
        end
      rescue Hailer::Error => error
        result[:error] = error.class.name
        result[:observer_message_id] = ProbeObserver.message_id
      end
    end
    print JSON.generate(result)
  RUBY

  def test_configures_hailer_without_an_application_initializer
    result = boot
    assert result.fetch('registered')
    assert_equal 'hailer', result.fetch('selected')
  end

  def test_rails_delivers_with_only_hailer_url_and_observers_see_the_provider_id
    with_server do |server|
      result = boot(endpoint: server.endpoint, deliver: true)
      assert result.fetch('registered')
      assert_equal 'hailer', result.fetch('selected')
      assert_equal 'ses-test-id', result.fetch('message_id')
      assert_equal 'ses-test-id', result.fetch('observer_message_id')
      assert_equal ['to@example.com'], server.payload.fetch('to_addresses')
    end
  end

  def test_test_delivery_can_be_selected_without_contacting_hailer
    result = boot(delivery_method: 'test', deliver: true)
    assert_equal 'test', result.fetch('selected')
    assert_equal 1, result.fetch('test_deliveries')
    assert_nil result.fetch('message_id')
  end

  def test_delivery_failures_propagate_and_do_not_notify_success_observers
    with_server(status: 503, body: '{"error":"circuit open"}') do |server|
      result = boot(endpoint: server.endpoint, deliver: true)
      assert_equal 'Hailer::UnavailableError', result.fetch('error')
      assert_nil result.fetch('observer_message_id')
    end
  end

  def test_deliver_later_enqueues_a_rails_job_without_contacting_hailer
    result = boot(later: true)
    assert_equal 'ActionMailer::MailDeliveryJob', result.fetch('job_class')
    assert_equal 1, result.fetch('jobs')
    assert_nil result.fetch('observer_message_id')
  end

  private

  def boot(delivery_method: '', endpoint: nil, deliver: false, later: false)
    Dir.mktmpdir('hailer-railtie') do |dir|
      out, err, status = Open3.capture3(
        { 'HAILER_URL' => endpoint }, RbConfig.ruby,
        '-I', File.expand_path('../lib', __dir__), '-e', BOOT,
        delivery_method, later ? 'later' : (deliver ? 'send' : 'boot'), chdir: dir
      )
      assert status.success?, "Rails boot failed:\n#{err}\n#{out}"
      JSON.parse(out)
    end
  end
end

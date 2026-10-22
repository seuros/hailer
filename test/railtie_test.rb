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

    class CustomMailJob < Hailer::MailDeliveryJob; end

    class MailProbe < Rails::Application
      config.eager_load = false
      config.logger = Logger.new(nil)
      config.secret_key_base = 'x' * 64
      config.action_mailer.delivery_method = ARGV.first.to_sym unless ARGV.first.empty?
      config.action_mailer.delivery_job = 'CustomMailJob' if ARGV[2] == 'custom'
    end
    MailProbe.initialize!

    result = {
      registered: ActionMailer::Base.delivery_methods[:hailer] == Hailer::DeliveryMethod,
      queued_registered: ActionMailer::Base.delivery_methods[:hailer_queued] == Hailer::QueuedDeliveryMethod,
      default_job: ActionMailer::Base.delivery_job.name,
      selected: ActionMailer::Base.delivery_method
    }
    if %w[send later perform retry].include?(ARGV[1])
      class ProbeMailer < ActionMailer::Base
        def welcome
          mail(from: 'alice@example.com', to: 'to@example.com', subject: 'Rails handoff', body: 'Hello')
        end
      end
      class ProbeObserver
        def self.delivered_email(message)
          @message_id = message.header[:ses_message_id]&.value
          @hailer_message_id = message.header[:hailer_message_id]&.value
        end

        def self.message_id = @message_id
        def self.hailer_message_id = @hailer_message_id
      end
      ProbeMailer.register_observer(ProbeObserver)
      begin
        if %w[later perform retry].include?(ARGV[1])
          ActiveJob::Base.queue_adapter = :test
          job = ProbeMailer.welcome.deliver_later
          result[:job_class] = job.class.name
          result[:jobs] = ActiveJob::Base.queue_adapter.enqueued_jobs.size
          result[:observer_message_id] = ProbeObserver.message_id
          if %w[perform retry].include?(ARGV[1])
            job.perform_now
            result[:job_id] = job.job_id
            result[:observer_hailer_message_id] = ProbeObserver.hailer_message_id
            job.perform_now if ARGV[1] == 'retry'
          end
        else
          message = ProbeMailer.welcome.deliver_now
          result[:message_id] = message.header[:ses_message_id]&.value
          result[:hailer_message_id] = message.header[:hailer_message_id]&.value
          result[:observer_message_id] = ProbeObserver.message_id
          result[:observer_hailer_message_id] = ProbeObserver.hailer_message_id
          result[:test_deliveries] = ActionMailer::Base.deliveries.size
        end
      rescue Hailer::Error => error
        result[:error] = error.class.name
        result[:observer_message_id] = ProbeObserver.message_id
      end
      result[:context_cleared] = Hailer::DeliveryContext.job_id.nil?
    end
    print JSON.generate(result)
  RUBY

  def test_configures_hailer_without_an_application_initializer
    result = boot
    assert result.fetch('registered')
    assert result.fetch('queued_registered')
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

  def test_queued_delivery_sets_only_the_durable_id_and_notifies_the_observer
    with_server(status: 202, body: '{"hailer_message_id":"queued-id"}') do |server|
      result = boot(delivery_method: 'hailer_queued', endpoint: server.endpoint, deliver: true)
      assert_equal 'queued-id', result.fetch('hailer_message_id')
      assert_equal 'queued-id', result.fetch('observer_hailer_message_id')
      assert_nil result.fetch('message_id')
      assert_equal 'POST /internal/mail/messages HTTP/1.1', server.request_line
    end
  end

  def test_selecting_queued_delivery_automatically_configures_the_public_job_subclass
    with_server(status: 202, body: '{"hailer_message_id":"queued-id"}') do |server|
      result = boot(delivery_method: 'hailer_queued', endpoint: server.endpoint, mode: 'perform')
      assert_equal 'Hailer::MailDeliveryJob', result.fetch('job_class')
      assert_equal "hailer:job:#{result.fetch('job_id')}:1", server.payload.fetch('submission_key')
      assert_equal 'queued-id', result.fetch('observer_hailer_message_id')
      assert result.fetch('context_cleared')
    end
  end

  def test_job_retry_uses_the_same_key_and_surfaces_a_changed_render_conflict
    with_server(responses: [
      { status: 202, body: '{"hailer_message_id":"queued-id"}' },
      { status: 409, body: '{"error":"rendered message changed"}' }
    ]) do |server|
      result = boot(delivery_method: 'hailer_queued', endpoint: server.endpoint, mode: 'retry')
      assert_equal 'Hailer::SubmissionConflictError', result.fetch('error')
      keys = server.requests.map { |request| request[:payload].fetch('submission_key') }
      assert_equal ["hailer:job:#{result.fetch('job_id')}:1"] * 2, keys
      refute_equal server.requests[0][:payload]['raw_mime'], server.requests[1][:payload]['raw_mime']
      assert result.fetch('context_cleared')
    end
  end

  def test_queued_configuration_does_not_replace_an_application_delivery_job
    result = boot(delivery_method: 'hailer_queued', custom_job: true)
    assert_equal 'CustomMailJob', result.fetch('default_job')
  end

  private

  def boot(delivery_method: '', endpoint: nil, deliver: false, later: false, mode: nil, custom_job: false)
    Dir.mktmpdir('hailer-railtie') do |dir|
      out, err, status = Open3.capture3(
        { 'HAILER_URL' => endpoint }, RbConfig.ruby,
        '-I', File.expand_path('../lib', __dir__), '-e', BOOT,
        delivery_method, mode || (later ? 'later' : (deliver ? 'send' : 'boot')), custom_job ? 'custom' : '', chdir: dir
      )
      assert status.success?, "Rails boot failed:\n#{err}\n#{out}"
      JSON.parse(out)
    end
  end
end

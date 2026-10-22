# frozen_string_literal: true

require 'action_mailer/railtie'
require_relative 'mail_delivery_job'

module Hailer
  class Railtie < ::Rails::Railtie
    # A default, not an after-boot override: an app's test environment can
    # still choose :test without contacting the service.
    config.action_mailer.delivery_method = :hailer
    config.hailer = ActiveSupport::OrderedOptions.new
    config.hailer.feedback_handler = nil

    initializer 'hailer.action_mailer', before: 'action_mailer.set_configs' do |app|
      if app.config.action_mailer.delivery_method == :hailer_queued
        app.config.action_mailer.delivery_job ||= 'Hailer::MailDeliveryJob'
      end
      ActiveSupport.on_load(:action_mailer) do
        add_delivery_method :hailer, Hailer::DeliveryMethod
        add_delivery_method :hailer_queued, Hailer::QueuedDeliveryMethod
      end
    end
  end
end

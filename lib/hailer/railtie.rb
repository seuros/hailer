# frozen_string_literal: true

require 'action_mailer/railtie'

module Hailer
  class Railtie < ::Rails::Railtie
    # A default, not an after-boot override: an app's test environment can
    # still choose :test without contacting the service.
    config.action_mailer.delivery_method = :hailer
    config.hailer = ActiveSupport::OrderedOptions.new
    config.hailer.feedback_handler = nil

    initializer 'hailer.action_mailer', before: 'action_mailer.set_configs' do
      ActiveSupport.on_load(:action_mailer) do
        add_delivery_method :hailer, Hailer::DeliveryMethod
      end
    end
  end
end

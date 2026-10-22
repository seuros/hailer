# frozen_string_literal: true

module Hailer
  # Custom application delivery jobs can include this public callback module.
  module DeliveryJobContext
    extend ActiveSupport::Concern

    included do
      around_perform do |job, perform|
        DeliveryContext.set(job_id: job.job_id, ordinal: 0) { perform.call }
      end
    end
  end

  class MailDeliveryJob < ActionMailer::MailDeliveryJob
    include DeliveryJobContext
  end
end

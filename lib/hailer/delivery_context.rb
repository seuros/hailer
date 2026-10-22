# frozen_string_literal: true

require 'active_support'
require 'active_support/current_attributes'

module Hailer
  # Scoped by the public ActiveJob callback, restored even on exceptions and
  # nested jobs. No submission key is injected into the rendered MIME.
  class DeliveryContext < ActiveSupport::CurrentAttributes
    attribute :job_id, :ordinal

    def next_submission_key
      return unless job_id

      self.ordinal = ordinal.to_i + 1
      "hailer:job:#{job_id}:#{ordinal}"
    end
  end
end

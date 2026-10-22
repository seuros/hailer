# frozen_string_literal: true

require_relative 'test_helper'

class DeliveryJobTest < Minitest::Test
  class ContextProbe < ActiveJob::Base
    include Hailer::DeliveryJobContext
    class_attribute :observed_keys, default: []

    def perform(nested: false, fail_after: false)
      self.class.observed_keys += [Hailer::DeliveryContext.next_submission_key]
      ContextProbe.perform_now if nested
      self.class.observed_keys += [Hailer::DeliveryContext.next_submission_key]
      raise 'probe failure' if fail_after
    end
  end

  def setup
    ContextProbe.observed_keys = []
    @logger = ActiveJob::Base.logger
    ActiveJob::Base.logger = Logger.new(nil)
  end

  def teardown
    ActiveJob::Base.logger = @logger
    Hailer::DeliveryContext.reset
  end

  def test_public_callback_preserves_identity_across_serialized_job_retries
    job = ContextProbe.new
    serialized = job.serialize
    2.times { ActiveJob::Base.deserialize(serialized).perform_now }
    assert_equal ["hailer:job:#{job.job_id}:1", "hailer:job:#{job.job_id}:2"] * 2, ContextProbe.observed_keys
    assert_nil Hailer::DeliveryContext.job_id
  end

  def test_nested_jobs_restore_the_outer_ordinal
    job = ContextProbe.new(nested: true)
    job.perform_now
    keys = ContextProbe.observed_keys
    assert_equal "hailer:job:#{job.job_id}:1", keys[0]
    assert_equal "hailer:job:#{job.job_id}:2", keys[3]
    refute_equal keys[0].split(':')[2], keys[1].split(':')[2]
    assert_nil Hailer::DeliveryContext.job_id
  end

  def test_exception_restores_an_enclosing_context
    Hailer::DeliveryContext.set(job_id: 'outer', ordinal: 9) do
      assert_raises(RuntimeError) { ContextProbe.perform_now(fail_after: true) }
      assert_equal 'hailer:job:outer:10', Hailer::DeliveryContext.next_submission_key
    end
    assert_nil Hailer::DeliveryContext.job_id
  end

  def test_concurrent_jobs_do_not_share_identity_or_ordinal
    ready = Queue.new
    proceed = Queue.new
    results = Queue.new
    workers = %w[first second].map do |id|
      Thread.new do
        Hailer::DeliveryContext.set(job_id: id, ordinal: 0) do
          ready << true
          proceed.pop
          results << [Hailer::DeliveryContext.next_submission_key, Hailer::DeliveryContext.next_submission_key]
        end
      end
    end
    2.times { ready.pop }
    2.times { proceed << true }
    workers.each(&:value)
    assert_equal [
      %w[hailer:job:first:1 hailer:job:first:2],
      %w[hailer:job:second:1 hailer:job:second:2]
    ], 2.times.map { results.pop }.sort
    assert_nil Hailer::DeliveryContext.job_id
  ensure
    workers&.each { |worker| worker.kill if worker.alive? }
    workers&.each(&:join)
  end
end

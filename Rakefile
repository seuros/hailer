# frozen_string_literal: true

require 'rake/testtask'

Rake::TestTask.new do |test|
  test.libs << 'test'
  test.pattern = 'test/**/*_test.rb'
end

task default: :test

Dir[File.join(__dir__, 'tasks/*.rake')].sort.each { |task_file| load task_file }

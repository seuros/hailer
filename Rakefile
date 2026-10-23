# frozen_string_literal: true

desc 'Run the Ruby test suite'
task :test do
  sh 'bundle exec padded_cell'
end

task default: :test

Dir[File.join(__dir__, 'tasks/*.rake')].sort.each { |task_file| load task_file }

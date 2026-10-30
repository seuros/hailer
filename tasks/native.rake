# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'open3'
require 'tmpdir'
require_relative '../lib/hailer/binary'

module HailerPackaging
  ROOT = File.expand_path('..', __dir__)
  TARGETS = {
    'arm64-darwin' => 'aarch64-apple-darwin',
    'x86_64-darwin' => 'x86_64-apple-darwin',
    'aarch64-linux-gnu' => 'aarch64-unknown-linux-gnu',
    'x86_64-linux-gnu' => 'x86_64-unknown-linux-gnu',
    'aarch64-linux-musl' => 'aarch64-unknown-linux-musl',
    'x86_64-linux-musl' => 'x86_64-unknown-linux-musl'
  }.freeze

  module_function

  def metadata
    output, error, status = Open3.capture3('cargo', 'metadata', '--no-deps', '--format-version=1', chdir: ROOT)
    raise error unless status.success?

    JSON.parse(output)
  end

  def build
    data = metadata
    crate = data.fetch('packages').find { |package| package['name'] == 'hailer' }
    raise 'gem/crate version mismatch' unless crate.fetch('version') == Hailer::VERSION

    platform = Hailer::Binary.platform
    target = TARGETS.fetch(platform)
    rustc, status = Open3.capture2e('rustc', '-vV')
    raise 'Rust toolchain host does not match the Ruby gem platform' unless status.success? && rustc.lines.include?("host: #{target}\n")
    raise 'cargo build failed' unless system('cargo', 'build', '--release', '--locked', '--features', 'ses,postmark', chdir: ROOT)

    File.join(data.fetch('target_directory'), 'release', 'hailer')
  end

  def package(binary)
    platform = Hailer::Binary.platform
    version, status = Open3.capture2e(binary, '--version')
    raise 'wrong binary version' unless status.success? && version.strip == "hailer #{Hailer::VERSION}"

    spec = Gem::Specification.load(File.join(ROOT, 'hailer.gemspec'))
    output = File.join(ROOT, 'pkg', "hailer-#{Hailer::VERSION}-#{platform}.gem")
    FileUtils.mkdir_p(File.dirname(output))
    Dir.mktmpdir('hailer-package') do |stage|
      FileUtils.cp(File.join(ROOT, 'hailer.gemspec'), File.join(stage, 'hailer.gemspec'))
      spec.files.each do |relative|
        destination = File.join(stage, relative)
        FileUtils.mkdir_p(File.dirname(destination))
        FileUtils.cp(File.join(ROOT, relative), destination)
      end
      relative_binary = "vendor/#{platform}/bin/hailer"
      manifest = "vendor/#{platform}/manifest.json"
      FileUtils.mkdir_p(File.dirname(File.join(stage, relative_binary)))
      FileUtils.cp(binary, File.join(stage, relative_binary))
      FileUtils.chmod(0o755, [File.join(stage, relative_binary), File.join(stage, 'exe/hailer')])
      File.write(File.join(stage, manifest), JSON.pretty_generate(
        version: Hailer::VERSION, platform: platform, target: TARGETS.fetch(platform),
        sha256: Digest::SHA256.file(binary).hexdigest
      ))
      # Build outside Bundler's runtime so the archive uses RubyGems' own
      # dependency/URI implementation and only the staged platform's files.
      require 'bundler'
      Bundler.with_unbundled_env do
        success = system({ 'HAILER_PLATFORM' => platform }, Gem.ruby, '-S', 'gem', 'build',
                         'hailer.gemspec', '--output', output, chdir: stage)
        raise 'gem build failed' unless success
      end
    end
    puts output
  end
end

namespace :native do
  desc 'Build and package the release binary for this host (no publication)'
  task :package do
    HailerPackaging.package(HailerPackaging.build)
  end
end

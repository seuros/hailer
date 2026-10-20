# frozen_string_literal: true

require_relative 'test_helper'
require 'hailer/binary'
require 'tmpdir'
require 'fileutils'
require 'rbconfig'

class BinaryTest < Minitest::Test
  def test_source_override_checks_the_executable_version
    Dir.mktmpdir('hailer-binary') do |directory|
      binary = File.join(directory, 'hailer')
      File.write(binary, "#!#{RbConfig.ruby}\nputs 'hailer #{Hailer::VERSION}'\n")
      FileUtils.chmod(0o755, binary)
      assert_equal binary, Hailer::Binary.path(env: { 'HAILER_BINARY' => binary })
      File.write(binary, "#!#{RbConfig.ruby}\nputs 'not-hailer'\n")
      assert_raises(Hailer::BinaryError) { Hailer::Binary.path(env: { 'HAILER_BINARY' => binary }) }
    end
  end

  def test_missing_binary_never_falls_back_to_path_or_a_download
    Dir.mktmpdir('hailer-binary') do |directory|
      assert_raises(Hailer::BinaryError) { Hailer::Binary.path(env: {}, root: directory) }
    end
  end

  def test_modified_vendored_binary_is_rejected
    Dir.mktmpdir('hailer-binary') do |directory|
      path = File.join(directory, 'vendor', Hailer::Binary.platform, 'bin', 'hailer')
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, 'tampered')
      FileUtils.chmod(0o755, path)
      File.write(File.join(File.dirname(File.dirname(path)), 'manifest.json'), JSON.generate(
        version: Hailer::VERSION, platform: Hailer::Binary.platform, sha256: 'wrong-digest'
      ))
      assert_raises(Hailer::BinaryError) { Hailer::Binary.path(env: {}, root: directory) }
    end
  end
end

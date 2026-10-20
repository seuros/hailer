# frozen_string_literal: true

require_relative 'test_helper'
require_relative '../tasks/release'

class ReleaseTest < Minitest::Test
  def test_verifies_source_and_native_packages_and_puts_source_first
    with_release do |directory|
      artifacts = HailerRelease.verify(directory, Hailer::VERSION)
      assert_equal %w[ruby x86_64-linux-gnu], artifacts.map(&:platform)
      assert artifacts.all? { it.sha256 == Digest::SHA256.file(it.path).hexdigest }
    end
  end

  def test_rejects_corrupted_archives
    with_release do |directory|
      File.open(Dir["#{directory}/*.gem"].first, 'ab') { it.write('tampered') }
      assert_raises(HailerRelease::Error) { HailerRelease.verify(directory, Hailer::VERSION) }
    end
  end

  def test_rejects_missing_or_unlisted_gems_before_publication
    with_release do |directory|
      path = Dir["#{directory}/*.gem"].first
      File.rename(path, "#{directory}/unexpected.gem")
      assert_raises(HailerRelease::Error) { HailerRelease.verify(directory, Hailer::VERSION) }
    end
  end

  def test_rejects_a_different_release_even_when_its_checksums_are_valid
    with_release do |directory|
      assert_raises(HailerRelease::Error) { HailerRelease.verify(directory, '9.9.9') }
    end
  end

  def test_rejects_duplicate_checksum_entries
    with_release do |directory|
      path = File.join(directory, 'SHA256SUMS')
      File.open(path, 'a') { it.write(File.readlines(path).first) }
      assert_raises(HailerRelease::Error) { HailerRelease.verify(directory, Hailer::VERSION) }
    end
  end

  def test_resumes_only_when_published_bytes_match
    source = HailerRelease::Artifact.new(path: 'source.gem', platform: 'ruby', sha256: 'a' * 64)
    native = HailerRelease::Artifact.new(path: 'native.gem', platform: 'x86_64-linux-gnu', sha256: 'b' * 64)
    published = [{ 'number' => Hailer::VERSION, 'platform' => 'ruby', 'sha' => source.sha256 }]
    capture_io do
      assert_equal [native], HailerRelease.pending([source, native], published, Hailer::VERSION)
    end
    published.first['sha'] = 'c' * 64
    assert_raises(HailerRelease::Error) { HailerRelease.pending([source, native], published, Hailer::VERSION) }
  end

  def test_release_tags_cannot_escape_the_release_or_package_paths
    assert_equal Hailer::VERSION, HailerRelease.version
    assert_equal Hailer::VERSION, HailerRelease.version("v#{Hailer::VERSION}")
    assert_raises(HailerRelease::Error) { HailerRelease.version('../0.1.0') }
  end

  private

  def with_release
    Dir.mktmpdir('hailer-release-test') do |directory|
      %w[ruby x86_64-linux-gnu].each do |platform|
        spec = Gem::Specification.new do |gem|
          gem.name = 'hailer'
          gem.version = Hailer::VERSION
          gem.platform = platform
          gem.summary = 'Release verification fixture'
          gem.authors = ['Test']
        end
        path = File.join(directory, spec.file_name)
        capture_io { Gem::Package.build(spec, true, false, path) }
      end
      checksums = Dir["#{directory}/*.gem"].map { "#{Digest::SHA256.file(it).hexdigest}  #{File.basename(it)}\n" }.join
      File.write(File.join(directory, 'SHA256SUMS'), checksums)
      yield directory
    end
  end
end

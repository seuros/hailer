# frozen_string_literal: true

require 'digest'
require 'json'
require 'open3'
require_relative 'errors'
require_relative 'version'

module Hailer
  # Used by the executable without booting Rails. Never download a binary or
  # select an unrelated executable from PATH.
  module Binary
    ROOT = File.expand_path('../..', __dir__)

    module_function

    def platform(local = Gem::Platform.local)
      case local.os
      when 'darwin'
        cpu = %w[arm64 aarch64].include?(local.cpu) ? 'arm64' : local.cpu
        "#{cpu}-darwin"
      when 'linux'
        cpu = %w[arm64 aarch64].include?(local.cpu) ? 'aarch64' : local.cpu
        "#{cpu}-linux-#{local.version == 'musl' ? 'musl' : 'gnu'}"
      else
        raise BinaryError, "unsupported Hailer platform: #{local}"
      end
    end

    def path(env: ENV, root: ROOT)
      if env.key?('HAILER_BINARY')
        candidate = File.expand_path(env.fetch('HAILER_BINARY'))
        executable!(candidate)
        output, status = Open3.capture2e(candidate, '--version')
        unless status.success? && output.strip == "hailer #{VERSION}"
          raise BinaryError, "HAILER_BINARY must be Hailer #{VERSION}"
        end
        return candidate
      end

      directory = File.join(root, 'vendor', platform)
      candidate = File.join(directory, 'bin', 'hailer')
      executable!(candidate)
      manifest = JSON.parse(File.read(File.join(directory, 'manifest.json')))
      unless manifest['version'] == VERSION && manifest['platform'] == platform &&
             manifest['sha256'] == Digest::SHA256.file(candidate).hexdigest
        raise BinaryError, 'bundled Hailer executable does not match its release manifest'
      end
      candidate
    rescue Errno::ENOENT, JSON::ParserError
      raise BinaryError, 'missing or invalid Hailer binary manifest; reinstall the platform gem'
    end

    def executable!(candidate)
      return if File.file?(candidate) && File.executable?(candidate)

      raise BinaryError, "no executable Hailer binary at #{candidate}; install the platform gem or set HAILER_BINARY"
    end
  end
end

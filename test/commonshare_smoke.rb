# frozen_string_literal: true

# Opt-in compatibility smoke, run from CommonShare with:
# RAILS_ENV=test HAILER_SMOKE_BINARY=/path/to/hailer/target/debug/hailer \
# HAILER_DATABASE_URL=postgres://.../disposable_hailer_database \
# bundle exec rails runner /path/to/hailer/test/commonshare_smoke.rb
# Uses the application's installed client, a real bay, and loopback SES only.
# Set HAILER_SMOKE_QUEUED=1 and use an isolated Bundler Gemfile pointing to this
# working tree to verify durable dispatch. The application's Gemfile is untouched.
require 'socket'
require 'tmpdir'
require 'timeout'
require 'fileutils'
require 'base64'
require 'net/http'
require 'uri'

abort 'test environment required' unless Rails.env.test?
abort 'CommonShare test database required' unless ActiveRecord::Base.connection_db_config.database == 'commonshare_test'
binary = File.realpath(ENV.fetch('HAILER_SMOKE_BINARY'))
database_uri = URI.parse(ENV.fetch('HAILER_DATABASE_URL'))
unless %w[postgres postgresql].include?(database_uri.scheme) &&
       %w[127.0.0.1 localhost [::1]].include?(database_uri.host) &&
       %r{\A/hailer_[a-z0-9_]*(?:test|smoke|commonshare)[a-z0-9_]*\z}.match?(database_uri.path)
  abort 'a loopback disposable Hailer test/smoke database is required'
end
queued = ENV['HAILER_SMOKE_QUEUED'] == '1'
abort 'queued smoke needs the current client' if queued && !defined?(Hailer::QueuedClient)
if (expected_platform = ENV['HAILER_SMOKE_EXPECTED_PLATFORM'])
  abort 'unexpected installed Hailer gem platform' unless Gem.loaded_specs.fetch('hailer').platform.to_s == expected_platform
end

class HailerControlProbeMailer < ActionMailer::Base
  def probe
    attachments['report.bin'] = "\x00\x01\xff".b
    mail(from: 'Alice <alice@example.com>', to: 'recipient@example.com',
         bcc: 'copy@example.com', subject: 'Hailer controller smoke',
         body: 'Loopback only.')
  end
end

def smoke_assert(condition, label)
  raise label unless condition
end

def smoke_port
  socket = TCPServer.new('127.0.0.1', 0)
  socket.addr[1]
ensure
  socket&.close
end

def smoke_ready?(port, path)
  response = Net::HTTP.new('127.0.0.1', port, nil).get(path)
  response.code == '200' && JSON.parse(response.body).fetch('ready')
end

directory =Dir.mktmpdir('hailer-ctl-', '/tmp')
listener = TCPServer.new('127.0.0.1', 0)
requests = Queue.new
release_provider = Queue.new
fixture = Thread.new do
  socket = nil
  loop do
    socket = listener.accept
    line = socket.gets("\r\n")
    headers = {}
    while (header = socket.gets("\r\n")) != "\r\n"
      key, value = header.split(':', 2)
      headers[key.downcase] = value.strip
    end
    requests << [line, JSON.parse(socket.read(Integer(headers.fetch('content-length'))))]
    # Hold SES's response until Rails has received durable ownership.
    release_provider.pop if queued
    response = '{"MessageId":"commonshare-control-id"}'
    socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{response.bytesize}\r\nConnection: close\r\n\r\n#{response}")
    socket.close
  end
ensure
  socket&.close
end
fixture.report_on_exception = false

begin
  port = smoke_port
  endpoint = "http://127.0.0.1:#{port}/internal/mail/send"
  config = File.join(directory, 'hailer.toml')
  settings = "threads = 2\nshutdown_timeout_seconds = 5\n"
  settings += "ses_configuration_set = \"hailer-feedback\"\n[queued]\nmax_bytes = 1048576\nmax_messages = 10\n" if queued
  File.write(config, settings)
  manifest = File.join(directory, 'manifest.toml')
  File.write(manifest, <<~TOML)
    [mothership]
    metrics_port = #{smoke_port}
    [mothership.bind]
    mail = "127.0.0.1:#{port}"
    [[bays.http]]
    name = "hailer"
    command = #{binary.to_json}
    cwd = #{directory.to_json}
    workers = 0
    threads = 2
    boot_timeout = 30
    routes = [{ bind = "mail", pattern = "^/internal/mail/(send|events|health|ready|intake-ready|dispatch-ready|messages|messages/events|messages/[0-9a-f-]{36})$" }]
  TOML
  mothership = File.join(Gem.loaded_specs.fetch('guardship').full_gem_path, 'libexec/mothership')
  log = File.join(directory, 'mothership.log')
  pid = Process.spawn({
    'MS_SOCKET_DIR' => "#{directory}/sockets", 'HAILER_CONFIG' => config,
    'AWS_REGION' => 'us-east-1', 'AWS_ACCESS_KEY_ID' => 'fixture-key',
    'AWS_SECRET_ACCESS_KEY' => 'fixture-secret', 'AWS_SESSION_TOKEN' => nil,
    'AWS_PROFILE' => nil, 'AWS_CONFIG_FILE' => File::NULL,
    'AWS_SHARED_CREDENTIALS_FILE' => File::NULL, 'AWS_EC2_METADATA_DISABLED' => 'true',
    'AWS_IGNORE_CONFIGURED_ENDPOINT_URLS' => 'false',
    'AWS_ENDPOINT_URL' => "http://127.0.0.1:#{listener.addr[1]}"
  }, mothership, 'run', '-c', manifest, out: log, err: log, pgroup: true)
  Timeout.timeout(30) do
    loop do
      raise 'Mothership exited before readiness' if Process.wait(pid, Process::WNOHANG)

      begin
        break if smoke_ready?(port, '/internal/mail/ready')
      rescue Errno::ECONNREFUSED, EOFError, IOError
        # Bay is still starting.
      end
      sleep 0.1
    end
  end
  HailerControlProbeMailer.delivery_method = queued ? :hailer_queued : :hailer
  if queued
    HailerControlProbeMailer.hailer_queued_settings = {
      endpoint: endpoint, read_timeout: 5, submission_key: 'commonshare-loopback-probe'
    }
    %w[intake-ready dispatch-ready].each do |path|
      Timeout.timeout(10) do
        sleep 0.1 until smoke_ready?(port, "/internal/mail/#{path}")
      end
    end
  else
    HailerControlProbeMailer.hailer_settings = { endpoint: endpoint, read_timeout: 5 }
  end
  ActiveRecord::Base.transaction(requires_new: true) do
    message = HailerControlProbeMailer.probe.deliver_now
    line, request = Timeout.timeout(5) { requests.pop }
    smoke_assert(line.include?('/v2/email/outbound-emails'), 'SES route')
    smoke_assert(request.dig('Destination', 'ToAddresses') == ['recipient@example.com'], 'To envelope')
    smoke_assert(request.dig('Destination', 'BccAddresses') == ['copy@example.com'], 'Bcc envelope')
    parsed = Mail.read_from_string(Base64.strict_decode64(request.dig('Content', 'Raw', 'Data')))
    smoke_assert(parsed.attachments.first.decoded.b == "\x00\x01\xff".b, 'attachment integrity')
    history = Platform::OutgoingEmail.find_by!(message_id: message.message_id)
    if queued
      id = message.header[:hailer_message_id]&.value
      smoke_assert(id && Hailer::LifecycleClient::ID.match?(id), 'durable ownership ID')
      smoke_assert(message.header[:ses_message_id].nil?, 'no fabricated SES ID')
      smoke_assert(history.headers['ses_message_id'].nil?, 'legacy observer must not invent acceptance')
      lifecycle = Hailer::LifecycleClient.new(endpoint: endpoint)
      smoke_assert(lifecycle.status(id).state == 'dispatching', '202 before provider acceptance')
      smoke_assert(request.fetch('ConfigurationSetName') == 'hailer-feedback', 'configuration set')
      tags = request.fetch('EmailTags').to_h { |tag| [tag.fetch('Name'), tag.fetch('Value')] }
      smoke_assert(tags['hailer_message_id'] == id && Hailer::LifecycleClient::ID.match?(tags.fetch('hailer_attempt_id')), 'correlation tags')
      release_provider << true
      Timeout.timeout(10) do
        sleep 0.05 until lifecycle.status(id).state == 'accepted'
      end
      status = lifecycle.status(id)
      smoke_assert(!status.body_present && status.provider_message_id == 'commonshare-control-id', 'accepted and purged')
      smoke_assert(status.attempts.one? && status.attempts.first['phase'] == 'accepted', 'attempt journal')
      payload = {
        raw_mime: Base64.strict_decode64(request.dig('Content', 'Raw', 'Data')),
        to_addresses: ['recipient@example.com'], cc_addresses: [], bcc_addresses: ['copy@example.com']
      }
      client = Hailer::QueuedClient.new(endpoint: endpoint)
      replay = client.submit(payload, submission_key: 'commonshare-loopback-probe')
      smoke_assert(replay.hailer_message_id == id, 'same receipt after purge')
      begin
        client.submit(payload.merge(raw_mime: "#{payload[:raw_mime]}changed"),
                      submission_key: 'commonshare-loopback-probe')
        raise 'changed replay was not rejected'
      rescue Hailer::SubmissionConflictError
        # Immutable intent cannot be overwritten.
      end
      page = lifecycle.poll(after: 0)
      smoke_assert(page.events.any? { |event| event.kind == 'body_purged' }, 'lifecycle route')
      sleep 0.5
      smoke_assert(requests.empty?, 'replay and conflict never resend')
    else
      smoke_assert(message.header[:ses_message_id].value == 'commonshare-control-id', 'acceptance ID')
      smoke_assert(history.headers['ses_message_id'] == 'commonshare-control-id', 'CommonShare observer')
    end
    raise ActiveRecord::Rollback
  end
  smoke_assert(Hailer::FeedbackClient.new(endpoint: endpoint).poll(after: 0).events.empty?, 'feedback contract')
  health = JSON.parse(Net::HTTP.new('127.0.0.1', port, nil).get('/internal/mail/health').body)
  smoke_assert(health.fetch('version') == '0.2.0' && health.fetch('ready'), 'controller health')
  smoke_assert(fixture.alive?, 'loopback fixture failed')
  puts "CommonShare #{queued ? 'queued' : 'sync'} smoke passed: Rails #{Rails.version}, Ruby #{RUBY_VERSION}, client #{Hailer::VERSION} (#{Gem.loaded_specs.fetch('hailer').platform}), service #{health.fetch('version')}; no external mail."
rescue StandardError
  warn File.read(log) if log && File.exist?(log)
  raise
ensure
  if pid
    begin
      # A failed bay can make Mothership exit before readiness. Reap it
      # without signaling a process group that no longer belongs to us.
      unless Process.wait(pid, Process::WNOHANG)
        Process.kill('TERM', pid)
        Timeout.timeout(10) { Process.wait(pid) }
      end
    rescue Timeout::Error
      unless Process.wait(pid, Process::WNOHANG)
        Process.kill('KILL', -pid)
        Process.wait(pid)
      end
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end
  fixture.kill
  fixture.join
  listener.close
  FileUtils.remove_entry(directory)
end

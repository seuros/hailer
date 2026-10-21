# frozen_string_literal: true

# Opt-in compatibility smoke, run from CommonShare with:
# RAILS_ENV=test HAILER_SMOKE_BINARY=/path/to/hailer/target/debug/hailer \
# HAILER_DATABASE_URL=postgres://.../disposable_hailer_database \
# bundle exec rails runner /path/to/hailer/test/commonshare_smoke.rb
# Uses the application's installed client, a real bay, and loopback SES only.
require 'socket'
require 'tmpdir'
require 'timeout'
require 'fileutils'
require 'base64'
require 'net/http'

abort 'test environment required' unless Rails.env.test?
abort 'CommonShare test database required' unless ActiveRecord::Base.connection_db_config.database == 'commonshare_test'
binary = File.realpath(ENV.fetch('HAILER_SMOKE_BINARY'))
abort 'isolated Hailer database URL required' if ENV.fetch('HAILER_DATABASE_URL').empty?

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

directory = Dir.mktmpdir('hailer-ctl-', '/tmp')
listener = TCPServer.new('127.0.0.1', 0)
requests = Queue.new
fixture = Thread.new do
  socket = listener.accept
  line = socket.gets("\r\n")
  headers = {}
  while (header = socket.gets("\r\n")) != "\r\n"
    key, value = header.split(':', 2)
    headers[key.downcase] = value.strip
  end
  requests << [line, JSON.parse(socket.read(Integer(headers.fetch('content-length'))))]
  response = '{"MessageId":"commonshare-control-id"}'
  socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{response.bytesize}\r\nConnection: close\r\n\r\n#{response}")
ensure
  socket&.close
end
fixture.report_on_exception = false

begin
  port = smoke_port
  endpoint = "http://127.0.0.1:#{port}/internal/mail/send"
  config = File.join(directory, 'hailer.toml')
  File.write(config, "threads = 2\nshutdown_timeout_seconds = 5\n")
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
    routes = [{ bind = "mail", pattern = "^/internal/mail/(send|events|health|ready)$" }]
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
      begin
        response = Net::HTTP.new('127.0.0.1', port, nil).get('/internal/mail/ready')
        break if response.code == '200' && JSON.parse(response.body).fetch('ready')
      rescue Errno::ECONNREFUSED, EOFError, IOError
        # Bay is still starting.
      end
      sleep 0.1
    end
  end
  HailerControlProbeMailer.delivery_method = :hailer
  HailerControlProbeMailer.hailer_settings = { endpoint: endpoint, read_timeout: 5 }
  ActiveRecord::Base.transaction(requires_new: true) do
    message = HailerControlProbeMailer.probe.deliver_now
    line, request = Timeout.timeout(5) { requests.pop }
    smoke_assert(line.include?('/v2/email/outbound-emails'), 'SES route')
    smoke_assert(request.dig('Destination', 'ToAddresses') == ['recipient@example.com'], 'To envelope')
    smoke_assert(request.dig('Destination', 'BccAddresses') == ['copy@example.com'], 'Bcc envelope')
    parsed = Mail.read_from_string(Base64.strict_decode64(request.dig('Content', 'Raw', 'Data')))
    smoke_assert(parsed.attachments.first.decoded.b == "\x00\x01\xff".b, 'attachment integrity')
    smoke_assert(message.header[:ses_message_id].value == 'commonshare-control-id', 'acceptance ID')
    history = Platform::OutgoingEmail.find_by!(message_id: message.message_id)
    smoke_assert(history.headers['ses_message_id'] == 'commonshare-control-id', 'CommonShare observer')
    raise ActiveRecord::Rollback
  end
  smoke_assert(Hailer::FeedbackClient.new(endpoint: endpoint).poll(after: 0).events.empty?, 'feedback contract')
  health = JSON.parse(Net::HTTP.new('127.0.0.1', port, nil).get('/internal/mail/health').body)
  smoke_assert(health.fetch('version') == '0.2.0' && health.fetch('ready'), 'controller health')
  fixture.value
  puts "CommonShare smoke passed: Rails #{Rails.version}, Ruby #{RUBY_VERSION}, client #{Hailer::VERSION}, service #{health.fetch('version')}; no external mail."
rescue StandardError
  warn File.read(log) if log && File.exist?(log)
  raise
ensure
  if pid
    begin
      Process.kill('TERM', -pid)
      Timeout.timeout(10) { Process.wait(pid) }
    rescue Timeout::Error
      Process.kill('KILL', -pid)
      Process.wait(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end
  listener.close
  fixture.kill
  fixture.join
  FileUtils.remove_entry(directory)
end

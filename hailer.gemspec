# frozen_string_literal: true

require_relative 'lib/hailer/version'

Gem::Specification.new do |spec|
  spec.name = 'hailer'
  spec.version = Hailer::VERSION
  spec.authors = ['Abdelkader Boudih']
  spec.email = ['oss@seuros.com']
  spec.summary = 'ActionMailer delivery through the Hailer mail ship'
  spec.description = 'A thin ActionMailer adapter for the standalone Hailer Rust service. ' \
                     'Rails renders the message; Hailer owns SES delivery.'
  spec.homepage = 'https://github.com/seuros/hailer'
  spec.license = 'BSD-3-Clause'
  spec.required_ruby_version = '>= 4.0'
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = "#{spec.homepage}/blob/master/CHANGELOG.md"
  spec.metadata['rubygems_mfa_required'] = 'true'
  spec.files = Dir['lib/**/*.rb', 'docs/**/*.md'] + %w[README.md CHANGELOG.md LICENSE.txt exe/hailer hailer.example.toml]
  if (platform = ENV['HAILER_PLATFORM'])
    spec.platform = Gem::Platform.new(platform)
    spec.files += ["vendor/#{platform}/bin/hailer", "vendor/#{platform}/manifest.json"]
  end
  spec.bindir = 'exe'
  spec.executables = ['hailer']
  spec.require_paths = ['lib']

  spec.add_dependency 'actionmailer', '>= 8.1', '< 9'
  spec.add_dependency 'json', '>= 2.18', '< 4'
  spec.add_dependency 'mail', '>= 2.9', '< 3'
  spec.add_dependency 'net-http', '>= 0.6', '< 1'
  spec.add_dependency 'railties', '>= 8.1', '< 9'

  spec.add_development_dependency 'minitest', '~> 6.0'
  spec.add_development_dependency 'padded_cell'
  spec.add_development_dependency 'rake', '~> 13.0'
end

# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_downloader_sailthru/version'

Gem::Specification.new do |spec|
  spec.name = 'gooddata_connectors_downloader_sailthru'
  spec.version = GoodData::Connectors::DownloaderSailThru::VERSION
  spec.authors = ['Adrian Toman']
  spec.email = ['adrian.toman@gooddata.com']
  spec.description = 'The gem for downloading CSV files process by Sailthru EMR process based on Gooddata Connectors infrastructure'
  spec.summary = ''
  spec.homepage = ''
  spec.license = 'BSD'

  spec.files = `git ls-files`.split($INPUT_RECORD_SEPARATOR)
  spec.executables = spec.files.grep(/^bin/) { |f| File.basename(f) }
  spec.test_files = spec.files.grep(/^(test|spec|features)/)
  spec.require_paths = ['lib']

  spec.add_dependency 'gooddata', '~> 0.6', '>= 0.6.12'
  spec.add_dependency 'gooddata_connectors_base'
  spec.add_dependency 'gooddata_connectors_metadata'
end

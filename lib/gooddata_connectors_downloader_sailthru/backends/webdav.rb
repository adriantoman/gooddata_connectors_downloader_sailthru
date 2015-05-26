# encoding: utf-8

require_relative 'base_backend'

module GoodData
  module Connectors
    module SailThruDownloader
      module Backend
        class Webdav < BaseBackend
          def initialize(downloader, opts = {})
            super(downloader, opts)
          end

          def connect(opts = nil)
            super(opts)
          end
        end
      end
    end
  end
end

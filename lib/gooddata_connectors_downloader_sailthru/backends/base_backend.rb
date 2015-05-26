# encoding: utf-8

module GoodData
  module Connectors
    module SailThruDownloader
      module Backend
        class BaseBackend
          attr_reader :downloader, :options

          def initialize(downloader, opts = {})
            @downloader = downloader
            @options = opts
          end

          def connect(opts = nil)
            @options = opts if opts
          end

          def list(path)
            fail NotImplementedError, 'Must be implemented in subclass'
          end

          def read(remote_path, local_path)
            fail NotImplementedError, 'Must be implemented in subclass'
          end

          def rename(orig_path, new_path)
            fail NotImplementedError, 'Must be implemented in subclass'
          end
        end
      end
    end
  end
end

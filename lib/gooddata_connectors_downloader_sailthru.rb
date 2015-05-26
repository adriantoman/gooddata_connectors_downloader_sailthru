require 'gooddata'
require 'gooddata_connectors_base'


require 'gooddata_connectors_downloader_sailthru/version'
require 'gooddata_connectors_downloader_sailthru/sailthru'


module GoodData
  module Connectors
    module SailThruDownloader
      # Middleware wrapper of CsvDownloader
      class SailThruDownloaderMiddleWare < GoodData::Bricks::Middleware
        def call(params)
          # Setup logger
          $log = params['GDC_LOGGER']
          $log.info "Initializing #{self.class.to_s.split('::').last}" if $log

          # Create downloader instance
          "Initializing SailThru downloader."
          downloader = SailThru.new(params['metadata_wrapper'], params)

          # Call implementation
          @app.call(params.merge('csv_downloader_wrapper' => downloader))
        end
      end
    end
  end
end

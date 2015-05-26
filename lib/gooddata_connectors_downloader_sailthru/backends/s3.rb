# encoding: utf-8

require 'aws-sdk-v1'

require_relative 'base_backend'

module GoodData
  module Connectors
    module SailThruDownloader
      module Backend
        class S3 < BaseBackend
          attr_reader :bucket, :s3

          def initialize(downloader, opts = {})
            super(downloader, opts)
          end

          def connect(opts = nil)
            super(opts)
            @logger = options[:logger] || Logger.new(STDOUT)
            args = {
              access_key_id: options['access_key'],
              secret_access_key: options['secret_key'],
              http_read_timeout: 120,
              http_open_timeout: 120
            }
            @s3 = AWS::S3.new(args)
            @bucket = s3.buckets[options['bucket']]
            self
          end

          def list(remote_path)
            @bucket.objects.with_prefix(remote_path)
          end

          def read(remote_path, local_path)
            FileUtils.mkdir_p(File.dirname(local_path))
            obj = bucket.objects[remote_path]
            File.open(local_path, 'w') do |file|
              obj.read do |chunk|
                file.write(chunk)
              end
            end
          end

          def rename(orig_path, new_path)
            obj = bucket.objects[orig_path]
            obj.rename_to(new_path)
          end
        end
      end
    end
  end
end

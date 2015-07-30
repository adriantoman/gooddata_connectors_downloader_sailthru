# encoding: utf-8

require 'gooddata_connectors_base/downloaders/base_downloader'

require_relative 'backends/backends'
require_relative 'extensions/extensions'

module GoodData
  module Connectors
    module SailThruDownloader
      class SailThru < Base::BaseDownloader
        attr_reader :backend, :data_structure

        def initialize(metadata, options = {})
          @data_structure = nil
          @type = 'sailthru'
          @group_id = options["GROUP_ID"]
          @timestamp = options["TIMESTAMP"].nil? ? Date.today.strftime('%Y%m%d') : options["TIMESTAMP"]
          super(metadata, options)
        end

        class << self
          def create_backend(downloader, class_type, options)
            class_name = "#{GoodData::Connectors::SailThruDownloader::Backend}::#{class_type.camelize}"
            klass = Object.class_from_string(class_name)
            klass.new(downloader, options)
          end
        end

        def connect
          puts '[SAILTHRU_LOG][INFO] Running the Sailthru downloader'
          # database_type = @metadata.get_configuration_by_type_and_key(@type, 'type')
          source_type = @metadata.get_configuration_by_type_and_key(@type, 'type')
          @backend_opts = @metadata.get_configuration_by_type_and_key(@type, 'options')
          @opts = @metadata.get_configuration_by_type_and_key(@type, 'options')
          @backend = SailThru.create_backend(self, source_type, @backend_opts)
          @backend.connect

          #We can use connector ID as batch identification
          batch_id = @metadata.get_configuration_by_type_and_key('global', 'ID')
          #Lets create batch

          # Lets create new dummy batch
          @batch = Metadata::Batch.new(batch_id)
        end

        def load_metadata(entity_name)
          metadata_entity = @metadata.get_entity(entity_name)
          puts "[SAILTHRU_LOG][INFO] Load metadata for entity #{entity_name}."
          #current_entity_version = @manifests_data[entity_name].first[:version]
          if (!metadata_entity.disabled?)
            temporary_fields = []
            source_description = metadata_entity.custom["fields"]
            # Lets replace # in field names as _
            source_description.each do |field|
              field_name  = field["name"]
              type = nil
              case field["type"]
                when /^varchar\((\d*)\)/
                  type = "string-#{$1}"
                when /^string-(\d*)/
                  type = "string-#{$1}"
                when "string","Varchar","varchar"
                  type = "string-255"
                when "integer"
                  type = "integer"
                when /^decimal\((\d*),(\d*)\)/
                  type = "decimal-#{$1}-#{$2}"
                when /^decimal-(\d*)-(\d*)/
                  type = "decimal-#{$1}-#{$2}"
                when "boolean"
                  type = "boolean"
                when "date","time-false"
                  type = "date-false"
                when "time","time-true"
                  type = "date-true"
                else
                  $log.info "Unsupported type #{field["type"]} - using string(255) as default value"
                  type = "string-255"
              end

              field = Metadata::Field.new({
                                              "id" => field_name,
                                              "name" => field_name,
                                              "type" => type,
                                              "custom" => {"order" => field["order"]}
                                          })
              temporary_fields << field
            end
            
            diff = metadata_entity.diff_fields(temporary_fields)

            # Merging entity and disabling add of new fields
            if (@metadata.load_fields_from_source?(metadata_entity.id))
              diff["only_in_target"].each do |target_field|
                configuration_field = metadata_entity.custom["fields"].find{|f| f["name"] = target_field.name}
                # The field is not in current entity, we need to create it
                $log.info "Adding new field #{target_field.name} to entity #{metadata_entity.id}"
                target_field.order = configuration_field["order"]
                metadata_entity.add_field(target_field)
                metadata_entity.make_dirty()
              end
            end

            diff["only_in_source"].each do |source_field|
              if (!source_field.disabled?)
                $log.info "Disabling field #{source_field.name} in entity #{metadata_entity.id}"
                source_field.disable("From synchronization with source system")
                metadata_entity.make_dirty()
              end
            end

            if !metadata_entity.custom.include?("download_by") or metadata_entity.custom["download_by"] != @type
              metadata_entity.custom["download_by"] = @type
              metadata_entity.make_dirty()
            end

            # Lets set parsing information about file to entity metadata
            files_structure_settings = @backend_opts["files_structure"]
            ["skip_rows","column_separator","escape_as","file_format","db_parser","enclosed_by"].each do |opt|
              if (files_structure_settings.include?(opt))
                if (metadata_entity.custom.include?(opt))
                  if files_structure_settings[opt] != metadata_entity.custom[opt]
                    metadata_entity.custom[opt] = files_structure_settings[opt]
                    metadata_entity.make_dirty()
                  end
                else
                  metadata_entity.custom[opt] = files_structure_settings[opt]
                  metadata_entity.make_dirty()
                end
              end
            end
            puts "[SAILTHRU_LOG][INFO] Saving entity #{metadata_entity}."
            metadata.save_entity(metadata_entity)
          end
        end

        def create_link_file(entity)

          link_file = Metadata::LinkFile.new()

          collection = 'results-' + entity.custom['collection'] if entity.custom.include?('collection')
          prefix = '/' + entity.custom['prefix'] + '/[^\/]*\.gz$' if entity.custom.include?('prefix')

          # missing prefix
          raise Exception, "Entity is part of a collection (#{collection}) but has no folder prefix." if !collection.nil? && prefix.nil?

          # skip entities that are not part of any collection
          return link_file if collection.nil?

          puts "[SAILTHRU_LOG][INFO] Creating link file for entity #{entity}. Collection: #{collection}. Prefix: #{prefix}."

          remote_folder = @backend_opts["folder"] + @timestamp + '/' + @group_id + '/' + collection + '/'
          remote_files = @backend.list(remote_folder)

          remote_files.each do |a|
            if a.key[/#{prefix}/] && !a.key[/\/_temporary\//]
              puts "[SAILTHRU_LOG][DEBUG] Adding file #{a.key} to link file."
              link_file.add_file(a.key)
            end
          end
          link_file
        end

        # TODO: Implement
        def download_entity_data(entity_name)
          metadata_entity = @metadata.get_entity(entity_name)
          local_path = @backend_opts["local_path"] || "source/" + entity_name + "/"
          link_file = create_link_file(metadata_entity)
          if (!link_file.files.empty? || !@opts['mandatory_entities'].include?(entity_name))
            local_path = link_file.create_file
            metadata_entity.store_runtime_param("source_filename",local_path)
            metadata_entity.store_runtime_param("batch",@batch.get_remote_filename)
            metadata_entity.store_runtime_param("type","link")
            metadata_entity.store_runtime_param("full",true)
            # Upload data from local storage to S3
            response = @metadata.save_data(metadata_entity)
            @batch.add_file(entity_name,response[:path])
          else
            raise Exception, "Error. Missing CSV files. Group ID: #{@group_id}. Timestamp: #{@timestamp}."
          end

        end

        def load_last_unprocessed_manifest
          local_path = "source/"
          #Set BATCH sequence number and filename
          #@batch.sequence = (Date.strptime(@timestamp,'%Y%m%d') - Date.strptime('2010-01-01','%Y-%m-%d')).to_i # number of days since timestamp
          @batch.filename = "#{@group_id}_#{@timestamp}" # manifest path
          @manifests_data = {}
        end

        # TODO: Implement
        def define_default_entities
          []
        end

        def finish_load
         @metadata.save_batch(@batch)
        end


      end
    end
  end
end

require 'lhm/id_set_chunk_insert'

module Lhm
  class IdSetChunkFinder
    LOG_PREFIX = "Chunker"

    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @ids = options[:ids]
      @throttler = options[:throttler]
      @processed_rows = 0
    end

    def table_empty?
      ids.nil? || ids.empty?
    end

    def validate
    end

    def each_chunk
      @processed_rows = 0
      while @processed_rows < ids.length
        next_idx = [@processed_rows + @throttler.stride, @ids.length].min
        range = @processed_rows...next_idx
        ids_to_insert = ids[range]
        @processed_rows = next_idx
        yield ChunkInsert.new(@migration, @connection, ids_to_insert[0], ids_to_insert[-1], expected_rows: range.count)
      end
    end

    def max_rows
      ids.length
    end

    def processed_rows
      @processed_rows
    end

    private

    def ids
      @ids ||= select_ids_from_db
    end

    def select_ids_from_db
      @connection.select_values("select id from `#{ @migration.origin_name }` order by id asc", should_retry: true, log_prefix: LOG_PREFIX)
    end
  end
end

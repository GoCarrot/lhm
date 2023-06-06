module Lhm
  class ChunkFinder
    LOG_PREFIX = "Chunker"

    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @start = options[:start] || select_start_from_db
      @limit = options[:limit] || select_limit_from_db
      @throttler = options[:throttler]
      @processed_rows = 0
    end

    def table_empty?
      start.nil? && limit.nil?
    end

    def validate
      if start > limit
        raise ArgumentError, "impossible chunk options (limit (#{limit.inspect} must be greater than start (#{start.inspect})"
      end
    end

    def each_chunk
      next_id = @start
      @processed_rows = 0
      while next_id <= @limit
        top = upper_id(next_id)
        @processed_rows += @throttler.stride
        yield ChunkInsert.new(@migration, @connection, next_id, top)
        next_id = top + 1
      end
    end

    def max_rows
      @limit - @start + 1
    end

    def processed_rows
      @processed_rows
    end

    private

    attr_reader :start, :limit

    def select_start_from_db
      @connection.select_value("select min(id) from `#{ @migration.origin_name }`")
    end

    def select_limit_from_db
      @connection.select_value("select max(id) from `#{ @migration.origin_name }`")
    end

    def upper_id(next_id)
      sql = "select id from `#{ @migration.origin_name }` where id >= #{ next_id } order by id limit 1 offset #{ @throttler.stride - 1}"
      top = @connection.select_value(sql, should_retry: true, log_prefix: LOG_PREFIX)

      [top ? top.to_i : @limit, @limit].min
    end
  end
end

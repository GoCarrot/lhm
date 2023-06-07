require 'lhm/sql_retry'
require 'lhm/proxysql_helper'

module Lhm
  class IdSetChunkInsert

    LOG_PREFIX = "ChunkInsert"

    def initialize(migration, connection, ids, retry_options = {})
      @migration = migration
      @connection = connection
      @ids = ids
      @retry_options = retry_options
    end

    def insert_and_return_count_of_rows_created
      @connection.update(sql, should_retry: true, log_prefix: LOG_PREFIX)
    end

    def bottom
      @ids[0]
    end

    def top
      @ids[-1]
    end

    def expected_rows
      @ids.length
    end

    private

    def sql
      "insert ignore into `#{ @migration.destination_name }` (#{ @migration.destination_columns }) " \
      "select #{ @migration.origin_columns } from `#{ @migration.origin_name }` " \
      "#{ conditions } `#{ @migration.origin_name }`.`id` in (#{@ids.join(',')})"
    end

    # XXX this is extremely brittle and doesn't work when filter contains more
    # than one SQL clause, e.g. "where ... group by foo". Before making any
    # more changes here, please consider either:
    #
    # 1. Letting users only specify part of defined clauses (i.e. don't allow
    # `filter` on Migrator to accept both WHERE and INNER JOIN
    # 2. Changing query building so that it uses structured data rather than
    # strings until the last possible moment.
    def conditions
      if @migration.conditions
        @migration.conditions.
          # strip ending paren
          sub(/\)\Z/, '').
          # put any where conditions in parens
          sub(/where\s(\w.*)\Z/, 'where (\\1)') + ' and'
      else
        'where'
      end
    end
  end
end

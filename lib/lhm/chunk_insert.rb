require 'lhm/sql_retry'
require 'lhm/proxysql_helper'

module Lhm
  class ChunkInsert

    LOG_PREFIX = "ChunkInsert"

    def initialize(migration, connection, lowest, highest, options = {})
      @migration = migration
      @connection = connection
      @lowest = lowest
      @highest = highest
      @options = options
    end

    def insert_and_return_count_of_rows_created
      @connection.update(sql, should_retry: true, log_prefix: LOG_PREFIX)
    end

    def bottom
      @lowest
    end

    def top
      @highest
    end

    def expected_rows
      @options.fetch(:expected_rows, top - bottom + 1)
    end

    private

    def sql
      "insert ignore into `#{ @migration.destination_name }` (#{ @migration.destination_columns }) " \
      "select #{ @migration.origin_columns } from `#{ @migration.origin_name }` " \
      "#{ conditions } `#{ @migration.origin_name }`.`id` between #{ @lowest } and #{ @highest }"
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

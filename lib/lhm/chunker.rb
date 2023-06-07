# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'lhm/command'
require 'lhm/sql_helper'
require 'lhm/printer'
require 'lhm/chunk_insert'
require 'lhm/chunk_finder'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    LOG_PREFIX = "Chunker"

    # Copy from origin to destination in chunks of size `stride`.
    # Use the `throttler` class to sleep between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @chunk_finder = options.fetch(:chunk_finder, ChunkFinder).new(migration, connection, options)
      @options = options
      @raise_on_warnings = options.fetch(:raise_on_warnings, false)
      @verifier = options[:verifier]
      if @throttler = options[:throttler]
        @throttler.connection = @connection if @throttler.respond_to?(:connection=)
      end
      @printer = options[:printer] || Printer::Percentage.new
      @pause_before_switch = options[:pause_before_switch]
      @retry_options = options[:retriable] || {}
      @retry_helper = SqlRetry.new(
        @connection,
        retry_options: @retry_options
      )
    end

    def execute
      @start_time = Time.now

      return if @chunk_finder.table_empty?
      @chunk_finder.each_chunk do |chunk|
        verify_can_run

        affected_rows = chunk.insert_and_return_count_of_rows_created

        # Only log the chunker progress every 5 minutes instead of every iteration
        current_time = Time.now
        if current_time - @start_time > (5 * 60)
          Lhm.logger.info("Inserted #{affected_rows} rows into the destination table from #{chunk.bottom} to #{chunk.top}")
          @start_time = current_time
        end

        if affected_rows < chunk.expected_rows
          raise_on_non_pk_duplicate_warning
        end

        if @throttler && affected_rows > 0
          @throttler.run
        end

        @printer.notify(@chunk_finder.processed_rows, @chunk_finder.max_rows)
      end
      @printer.end
      sleep @pause_before_switch if @pause_before_switch
    rescue => e
      @printer.exception(e) if @printer.respond_to?(:exception)
      raise
    end

    private

    def raise_on_non_pk_duplicate_warning
      @connection.execute("show warnings", should_retry: true, log_prefix: LOG_PREFIX).each do |level, code, message|
        unless message.match?(/Duplicate entry .+ for key 'PRIMARY'/)
          m = "Unexpected warning found for inserted row: #{message}"
          Lhm.logger.warn(m)
          raise Error.new(m) if @raise_on_warnings
        end
      end
    end

    def verify_can_run
      return unless @verifier
      @retry_helper.with_retries(log_prefix: LOG_PREFIX) do |retriable_connection|
        raise "Verification failed, aborting early" if !@verifier.call(retriable_connection)
      end
    end

    def validate
      return if @chunk_finder.table_empty?
      @chunk_finder.validate
    end

  end
end

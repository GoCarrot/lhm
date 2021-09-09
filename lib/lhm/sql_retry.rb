require 'retriable'
require 'lhm/sql_helper'

module Lhm
  # SqlRetry standardizes the interface for retry behavior in components like
  # Entangler, AtomicSwitcher, ChunkerInsert.
  #
  # By default if an error includes the message "Lock wait timeout exceeded", or
  # "Deadlock found when trying to get lock", SqlRetry will retry again
  # once the MySQL client returns control to the caller, plus one second.
  # It will retry a total of 10 times and output to the logger a description
  # of the retry with error information, retry count, and elapsed time.
  #
  # This behavior can be modified by passing `options` that are documented in
  # https://github.com/kamui/retriable. Additionally, a "log_prefix" option,
  # which is unique to SqlRetry can be used to prefix log output.
  class SqlRetry
    def initialize(connection, options = {})
      @connection = connection
      @global_retry_config = default_retry_config.dup.merge!(options)
      @log_prefix = options.delete(:log_prefix)
      @initial_host = hostname
      @retry_config = default_retry_config.dup.merge!(options)
    end

    def with_retries(retry_config = {})
      cnf = @global_retry_config.dup.merge!(retry_config)
      @log_prefix = cnf.delete(:log_prefix) || "SQL Retry"
      Retriable.retriable(cnf) do
        yield(@connection)
      # rescue ConnectionError
      #   with_retry_for_host(retry_config) do
      #     @connection.reconnect!
      #   end
      end
    end

    attr_reader :global_retry_config

    private

    def log_with_prefix(message, level = :info)
      message.prepend("[#{@log_prefix}] ") if @log_prefix
      Lhm.logger.send(level, message)
    end

    def hostname
      @connection&.query("SELECT @@hostname as host, @@port as port;").first.tap do |record|
        record&.[]("host") + ":" + record&.[]("port")
      end
    end

    def with_retry_for_host(retry_config)
      Retriable.retriable(retry_config) do
        yield
        raise "Different MySQL server host than the initial host" if hostname != @initial_host
      end
    #  TODO add unless raise exception
    end

    # For a full list of configuration options see https://github.com/kamui/retriable
    def default_retry_config
      {
        on: {
          StandardError => [
            /Lock wait timeout exceeded/,
            /Timeout waiting for a response from the last query/,
            /Deadlock found when trying to get lock/,
            /Query execution was interrupted/,
            /Lost connection to MySQL server during query/,
            /Max connect timeout reached/,
            /Unknown MySQL server host/,
            /Different MySQL server host than the initial host/,
          ]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 1, # the initial interval in seconds between tries.
        tries: 20, # Number of attempts to make at running your code block (includes initial attempt).
        rand_factor: 0, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try_number, total_elapsed_time, next_interval|
          log = "#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try."
          log_with_prefix(log, :info)
        end
      }.freeze
    end
  end
end

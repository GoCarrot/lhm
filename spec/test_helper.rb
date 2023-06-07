# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

if ENV['COV']
  require 'simplecov'
  SimpleCov.start do
    enable_coverage :branch
  end
end

require 'minitest/autorun'
require 'minitest/spec'
require 'minitest/mock'
require 'mocha/minitest'
require 'after_do'
require 'byebug'
require 'pathname'
require 'lhm'

$project = Pathname.new(File.dirname(__FILE__) + '/..').cleanpath
$spec = $project.join('spec')
$fixtures = $spec.join('fixtures')

$db_name = 'test'

require 'active_record'
require 'mysql2'

logger = Logger.new STDOUT
logger.level = Logger::WARN
Lhm.logger = logger

# Want test to be efficient without having to wait the normal value of 120s
Lhm::SqlRetry::RECONNECT_RETRY_MAX_ITERATION = 4

def without_verbose(&block)
  old_verbose, $VERBOSE = $VERBOSE, nil
  yield
ensure
  $VERBOSE = old_verbose
end

def printer
  printer = Lhm::Printer::Base.new

  def printer.notify(*) ;end
  def printer.end(*) [] ;end

  printer
end

def throttler
  Lhm::Throttler::Time.new(:stride => 100)
end

def init_test_db
  db_config = YAML.load_file(File.expand_path(File.dirname(__FILE__)) + '/integration/database.yml')
  conn = Mysql2::Client.new(
    :host => '127.0.0.1',
    :username => db_config['master']['user'],
    :password => db_config['master']['password'],
    :port => db_config['master']['port']
  )

  conn.query("DROP DATABASE IF EXISTS #{$db_name}")
  conn.query("CREATE DATABASE #{$db_name}")
end

init_test_db



# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/chunker'
require 'lhm/throttler'
require 'lhm/connection'
require 'lhm/id_set_chunk_finder'

describe Lhm::IdSetChunkFinder do
  include UnitHelper

  EXPECTED_RETRY_FLAGS_CHUNKER = {:should_retry => true, :log_prefix => "Chunker"}
  EXPECTED_RETRY_FLAGS_CHUNK_INSERT = {:should_retry => true, :log_prefix => "ChunkInsert"}

  before(:each) do
    @origin = Lhm::Table.new('foo')
    @destination = Lhm::Table.new('bar')
    @migration = Lhm::Migration.new(@origin, @destination)
    @connection = mock()
    @connection.stubs(:execute).returns([["dummy"]])
    # This is a poor man's stub
    @throttler = Object.new
    def @throttler.run
      # noop
    end
    def @throttler.stride
      1
    end

    @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                         :chunk_finder => Lhm::IdSetChunkFinder)
  end

  describe '#run' do
    it 'chunks the result set according to the stride size' do
      def @throttler.stride
        2
      end

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns((1..20).select(&:odd?))

      @connection.expects(:update).with(regexp_matches(/`id` in \(1,3\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(5,7\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(9,11\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(13,15\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(17,19\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)

      @chunker.run
    end

    it 'handles stride changes during execution' do
      # roll our own stubbing
      def @throttler.stride
        @run_count ||= 0
        @run_count = @run_count + 1
        if @run_count > 1
          3
        else
          2
        end
      end

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns((1..20).select(&:odd?))

      @connection.expects(:update).with(regexp_matches(/`id` in \(1,3\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(5,7,9\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(11,13,15\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(17,19\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)

      @connection.expects(:execute).twice.with(regexp_matches(/show warnings/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([])

      @chunker.run
    end

    it 'correctly copies single record tables' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :chunk_finder => Lhm::IdSetChunkFinder)

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([1])
      @connection.expects(:update).with(regexp_matches(/`id` in \(1\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(1)

      @chunker.run
    end

    it 'copies the last record of a table, even it is the start of the last chunk' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :chunk_finder => Lhm::IdSetChunkFinder)
      def @throttler.stride
        2
      end

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns((2..10).to_a)

      @connection.expects(:update).with(regexp_matches(/`id` in \(2,3\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(4,5\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(6,7\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(8,9\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(2)
      @connection.expects(:update).with(regexp_matches(/`id` in \(10\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(1)

      @chunker.run
    end


    it 'separates filter conditions from chunking conditions' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :chunk_finder => Lhm::IdSetChunkFinder)
      def @throttler.stride
        2
      end

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([1, 2])
      @connection.expects(:update).with(regexp_matches(/where \(foo.created_at > '2013-07-10' or foo.baz = 'quux'\) and `foo`.*`id` in \(1,2\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(1)
      @connection.expects(:execute).with(regexp_matches(/show warnings/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([])

      def @migration.conditions
        "where foo.created_at > '2013-07-10' or foo.baz = 'quux'"
      end

      @chunker.run
    end

    it "doesn't mess with inner join filters" do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :chunk_finder => Lhm::IdSetChunkFinder)

      def @throttler.stride
        2
      end

      @connection.expects(:select_values).with(regexp_matches(/order by id asc/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([1,2])
      @connection.expects(:update).with(regexp_matches(/inner join bar on foo.id = bar.foo_id and.*`id` in \(1,2\)/),EXPECTED_RETRY_FLAGS_CHUNK_INSERT).returns(1)
      @connection.expects(:execute).with(regexp_matches(/show warnings/),EXPECTED_RETRY_FLAGS_CHUNKER).returns([])

      def @migration.conditions
        'inner join bar on foo.id = bar.foo_id'
      end

      @chunker.run
    end
  end
end

# frozen_string_literal: true

require_relative "litequeue/version"

require "singleton"
require "yaml"
require "litedb"

class Litequeue
  include Singleton

  DEFAULT_QUEUE = "default"

  Configuration = Struct.new(:path, :synchronous, :mmap_size, :journal_size_limit)

  def self.configuration
    @configuration ||= Configuration.new(
      _path = "queue.sqlite3",
      _synchronous = :OFF,
      _mmap_size = 32 * 1024
    )
  end

  def self.configure
    yield(configuration)
  end

  def self.migrations
    YAML.load_file("#{__dir__}/litequeue/migrations.sql.yml")
  end

  def self.statements
    YAML.load_file("#{__dir__}/litequeue/statements.sql.yml")
  end

  def initialize
    configuration = self.class.configuration
    file = configuration.path
    options = configuration.to_h
      .slice(:synchronous, :mmap_size, :journal_size_limit)
      .merge(migrations: self.class.migrations,
        statements: self.class.statements)

    @db = Litedb::Connection.new(file, options)
    # Once the instance has been initialized, don't allow the configuration to be changed
    # as it won't have any effect.
    configuration.freeze
  end

  def push(value, queue: DEFAULT_QUEUE, delay: 0)
    results = @db.run_statement(:push, queue, delay, value) # [["{id}", "{name}"]]
    extract_row(results)
  end

  def pop(queue: DEFAULT_QUEUE, limit: 1)
    results = @db.run_statement(:pop, queue, limit)

    return extract_row(results) if limit == 1

    results
  end

  def repush(id, value, queue: DEFAULT_QUEUE, delay: 0)
    results = @db.run_statement(:repush, id, queue, delay, value)
    extract_value(results)
  end

  def delete(id)
    results = @db.run_statement(:delete, id)
    extract_value(results)
  end

  def count(queue: nil)
    results = @db.run_statement(:count, queue)
    extract_value(results)
  end

  def clear(queue: nil)
    results = @db.run_statement(:clear, queue)
    results.count
  end

  def empty?
    count.zero?
  end

  private

  def extract_value(results) # [["{value}"]] || []
    return if results.empty?

    results
      .first # [[value]] -> [value]
      .first # [value] -> value
  end

  def extract_row(results) # [[{value}, {value}]] || []
    return if results.empty?

    results
      .first # [[value, value]] -> [value, value]
  end
end

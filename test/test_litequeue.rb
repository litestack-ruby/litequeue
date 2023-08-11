# frozen_string_literal: true

require "test_helper"

describe Litequeue do
  before do
    # we don't want to pollute the repo with SQLite files, so we ensure that we set the path
    # here to the /test directory.
    Litequeue.configure do |config|
      config.path = "test/queue.sqlite3"
    end
  end

  after do
    Litequeue.instance_variable_set :@configuration, nil
  end

  describe ".configuration" do
    it "has default path as queue database" do
      Litequeue.instance_variable_set :@configuration, nil
      assert_equal "queue.sqlite3", Litequeue.configuration.path
    end

    it "has default synchronous off" do
      Litequeue.instance_variable_set :@configuration, nil
      assert_equal :OFF, Litequeue.configuration.synchronous
    end

    it "has default mmap_size as 32 kilobyes" do
      Litequeue.instance_variable_set :@configuration, nil
      assert_equal 32768, Litequeue.configuration.mmap_size
    end

    it "is frozen after the instance has been initialized" do
      # need to clone the class to ensure that we can test a separate `instance`
      # from `Litequeue.instance`, which might have already been set by a preceding test,
      # since tests are ran in a random order.
      testable = Litequeue.clone
      # we don't want to pollute the repo with SQLite files, so we ensure that we set the path
      # here to the /test directory.
      testable.configure do |config|
        config.path = "test/queue.sqlite3"
      end

      testable.instance

      assert testable.configuration.frozen?
      assert_raises(FrozenError) { testable.configuration.path = "" }
    end
  end

  describe ".instance" do
    it "is a singleton" do
      assert_same Litequeue.instance, Litequeue.instance
    end
  end

  describe ".configure" do
    it "can set path" do
      Litequeue.configure do |config|
        config.path = "path/to/db.sqlite3"
      end

      assert_equal "path/to/db.sqlite3", Litequeue.configuration.path
      assert_equal :OFF, Litequeue.configuration.synchronous
      assert_equal 32768, Litequeue.configuration.mmap_size
    end

    it "can set synchronous" do
      Litequeue.configure do |config|
        config.synchronous = :NORMAL
      end

      assert_equal :NORMAL, Litequeue.configuration.synchronous
      assert_equal "test/queue.sqlite3", Litequeue.configuration.path
      assert_equal 32768, Litequeue.configuration.mmap_size
    end

    it "can set synchronous" do
      Litequeue.configure do |config|
        config.mmap_size = 0
      end

      assert_equal 0, Litequeue.configuration.mmap_size
      assert_equal "test/queue.sqlite3", Litequeue.configuration.path
      assert_equal :OFF, Litequeue.configuration.synchronous
    end

    it "can set all 3" do
      Litequeue.configure do |config|
        config.path = "path/to/db.sqlite3"
        config.synchronous = :NORMAL
        config.mmap_size = 0
      end

      assert_equal "path/to/db.sqlite3", Litequeue.configuration.path
      assert_equal :NORMAL, Litequeue.configuration.synchronous
      assert_equal 0, Litequeue.configuration.mmap_size
    end
  end

  describe ".migrations" do
    it "returns Hash from YAML file" do
      expected = {
        "create_table_queue" => "CREATE TABLE IF NOT EXISTS queue(\n  id TEXT PRIMARY KEY NOT NULL ON CONFLICT REPLACE,\n  name TEXT NOT NULL ON CONFLICT REPLACE,\n  fire_at INTEGER NOT NULL ON CONFLICT REPLACE,\n  value TEXT,\n  created_at INTEGER DEFAULT(UNIXEPOCH()) NOT NULL ON CONFLICT REPLACE\n) WITHOUT ROWID;\n",
        "create_index_queue_by_name" => "CREATE INDEX IF NOT EXISTS idx_queue_by_name ON queue(name, fire_at ASC);\n"
      }

      assert_equal expected, Litequeue.migrations
    end
  end

  describe ".statements" do
    it "returns Hash from YAML file" do
      expected = {
        "push" => "INSERT INTO queue(id, name, fire_at, value) VALUES (HEX(RANDOMBLOB(32)), $1, (UNIXEPOCH('subsec') + $2), $3) RETURNING id, name;\n",
        "repush" => "INSERT INTO queue(id, name, fire_at, value) VALUES ($1, $2, (UNIXEPOCH('subsec') + $3), $4) RETURNING name;\n",
        "pop" => "DELETE FROM queue WHERE name != '_dead' AND (name, fire_at, id) IN (\n    SELECT name, fire_at, id FROM queue\n    WHERE name = IFNULL($1, 'default')\n    AND fire_at <= (UNIXEPOCH('subsec'))\n    ORDER BY fire_at ASC\n    LIMIT IFNULL($2, 1)\n) RETURNING id, value;\n",
        "delete" => "DELETE FROM queue WHERE id = $1 RETURNING value;\n",
        "count" => "SELECT COUNT(*) FROM queue WHERE IIF($1 IS NULL, 1, name = $1);\n",
        "clear" => "DELETE FROM queue WHERE IIF($1 IS NULL, 1, name = $1) RETURNING id;\n",
        "info" => "SELECT \n  name, \n  COUNT(*) AS count, \n  AVG(UNIXEPOCH('subsec') - created_at) AS avg, \n  MIN(UNIXEPOCH('subsec') - created_at) AS min, \n  MAX(UNIXEPOCH('subsec') - created_at) AS max\nFROM queue  GROUP BY name  ORDER BY count DESC;\n"
      }

      assert_equal expected, Litequeue.statements
    end
  end

  describe "#push" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @custom_queue_name = "QUEUE_NAME"
    end

    after do
      @litequeue.clear
    end

    describe "with no queue name provided" do
      it "returns id and queue is 'default'" do
        id, queue = @litequeue.push(@value)

        assert id
        assert_equal Litequeue::DEFAULT_QUEUE, queue
      end

      it "successfully pushes item to the 'default' queue" do
        assert_equal 0, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)

        _id, queue = @litequeue.push(@value)

        assert_equal Litequeue::DEFAULT_QUEUE, queue
        assert_equal 1, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)
      end
    end

    describe "with a queue name provided" do
      it "returns id and queue is the provided name" do
        id, queue = @litequeue.push(@value, queue: @custom_queue_name)

        assert id
        assert_equal @custom_queue_name, queue
      end

      it "successfully pushes item to the provided queue" do
        assert_equal 0, @litequeue.count(queue: @custom_queue_name)

        _id, queue = @litequeue.push(@value, queue: @custom_queue_name)

        assert_equal @custom_queue_name, queue
        assert_equal 1, @litequeue.count(queue: @custom_queue_name)
      end
    end

    describe "with a delay provided" do
      it "sets a `fire_at` value that many seconds in the future" do
        id, _queue = @litequeue.push(@value, delay: 123)

        # we need to do some hacking with the internals of the class
        # because the class doesn't publicly expose this information.
        db = @litequeue.instance_variable_get(:@db)
        fire_at, created_at = db.get_first_row("SELECT fire_at, created_at FROM queue WHERE id = ?", id)

        assert (fire_at - created_at) >= 123
      end
    end
  end

  describe "#pop" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @custom_queue_name = "QUEUE_NAME"
    end

    after do
      @litequeue.clear
    end

    it "return nil when no items present" do
      result = @litequeue.pop

      assert_nil result
    end

    describe "with no queue name provided" do
      it "returns id and value" do
        @litequeue.push(@value)

        id, value = @litequeue.pop

        assert id
        assert_equal @value, value
      end

      it "successfully pops item from the 'default' queue" do
        @litequeue.push(@value)
        assert_equal 1, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)

        _id, value = @litequeue.pop

        assert_equal @value, value
        assert_equal 0, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)
      end
    end

    describe "with a queue name provided" do
      it "returns id and value" do
        @litequeue.push(@value, queue: @custom_queue_name)

        id, value = @litequeue.pop(queue: @custom_queue_name)

        assert id
        assert_equal @value, value
      end

      it "successfully pops item from the provided queue" do
        @litequeue.push(@value, queue: @custom_queue_name)
        assert_equal 1, @litequeue.count(queue: @custom_queue_name)

        _id, value = @litequeue.pop(queue: @custom_queue_name)

        assert_equal @value, value
        assert_equal 0, @litequeue.count(queue: @custom_queue_name)
      end
    end

    describe "with a limit provided" do
      it "returns an Array of ids and values" do
        2.times do |i|
          @litequeue.push(@value + i.to_s)
        end

        result = @litequeue.pop(limit: 2)

        assert result.is_a?(Array)
        assert_equal 2, result.size
        # results are in a random order depending on the alphanumeric sort of the hexblob ID
        # sort by the value to enforce a predictable order for the test
        result.sort_by(&:last).each.with_index do |(_id, value), i|
          assert_equal @value + i.to_s, value
        end
      end

      it "only returns as many values as the limit, even if more present" do
        5.times do |i|
          @litequeue.push(@value + i.to_s)
        end

        result = @litequeue.pop(limit: 2)

        assert result.is_a?(Array)
        assert_equal 2, result.size
      end

      it "only returns as many values as present, even if limit is higher" do
        2.times do |i|
          @litequeue.push(@value + i.to_s)
        end

        result = @litequeue.pop(limit: 5)

        assert result.is_a?(Array)
        assert_equal 2, result.size
      end
    end
  end

  describe "#repush" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @id = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      @custom_queue_name = "QUEUE_NAME"
      @custom_delay = 123
    end

    after do
      @litequeue.clear
    end

    it "raises an exception when repushing an existing item" do
      id, _queue = @litequeue.push(@value)

      assert_raises(SQLite3::ConstraintException) { @litequeue.repush(id, "NEW VALUE") }
    end

    describe "with no queue name provided" do
      it "returns queue" do
        queue = @litequeue.repush(@id, @value)

        assert_equal Litequeue::DEFAULT_QUEUE, queue
      end

      it "successfully pushes item to the default queue" do
        assert_equal 0, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)

        queue = @litequeue.repush(@id, @value)

        assert_equal Litequeue::DEFAULT_QUEUE, queue
        assert_equal 1, @litequeue.count(queue: Litequeue::DEFAULT_QUEUE)
      end
    end

    describe "with a queue name provided" do
      it "returns the provided queue" do
        queue = @litequeue.repush(@id, @value, queue: @custom_queue_name)

        assert_equal @custom_queue_name, queue
      end

      it "successfully pushes item to the provided queue" do
        assert_equal 0, @litequeue.count(queue: @custom_queue_name)

        queue = @litequeue.repush(@id, @value, queue: @custom_queue_name)

        assert_equal @custom_queue_name, queue
        assert_equal 1, @litequeue.count(queue: @custom_queue_name)
      end
    end

    describe "with a delay provided" do
      it "sets a `fire_at` value that many seconds in the future" do
        @litequeue.repush(@id, @value, delay: @custom_delay)

        # we need to do some hacking with the internals of the class
        # because the class doesn't publicly expose this information.
        db = @litequeue.instance_variable_get(:@db)
        fire_at, created_at = db.get_first_row("SELECT fire_at, created_at FROM queue WHERE id = ?", @id)

        assert (fire_at - created_at) >= @custom_delay
      end
    end
  end

  describe "#delete" do
    before do
      @litequeue = Litequeue.instance
      @non_existent_id = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      @value = "VALUE"
    end

    after do
      @litequeue.clear
    end

    it "returns nil when you try to delete a non-existent item" do
      assert_nil @litequeue.delete(@non_existent_id)
    end

    it "returns the value when you delete an existing item" do
      id, _queue = @litequeue.push(@value)

      value = @litequeue.delete(id)

      assert_equal @value, value
    end
  end

  describe "#count" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @custom_queue_name = "QUEUE_NAME"
    end

    after do
      @litequeue.clear
    end

    describe "with no queue name provided" do
      it "returns zero when table is empty" do
        assert_equal 0, @litequeue.count
      end

      it "returns correct number of items across all queues" do
        @litequeue.push(@value)
        @litequeue.push(@value, queue: @custom_queue_name)

        assert_equal 2, @litequeue.count
      end
    end

    describe "with a queue name provided" do
      it "returns zero when table is empty" do
        assert_equal 0, @litequeue.count(queue: @custom_queue_name)
      end

      it "returns correct number of items for provided queue" do
        @litequeue.push(@value)
        @litequeue.push(@value, queue: @custom_queue_name)

        assert_equal 1, @litequeue.count(queue: @custom_queue_name)
      end
    end
  end

  describe "#clear" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @custom_queue_name = "QUEUE_NAME"
    end

    after do
      @litequeue.clear
    end

    describe "with no queue name provided" do
      it "returns zero when table is empty" do
        assert_equal 0, @litequeue.clear
      end

      it "returns correct number of deleted items across all queues" do
        @litequeue.push(@value)
        @litequeue.push(@value, queue: @custom_queue_name)

        assert_equal 2, @litequeue.clear
      end
    end

    describe "with a queue name provided" do
      it "returns zero when table is empty" do
        assert_equal 0, @litequeue.clear(queue: @custom_queue_name)
      end

      it "returns correct number of items for provided queue" do
        @litequeue.push(@value)
        @litequeue.push(@value, queue: @custom_queue_name)

        assert_equal 1, @litequeue.clear(queue: @custom_queue_name)
      end
    end
  end

  describe "#empty?" do
    before do
      @litequeue = Litequeue.instance
      @value = "VALUE"
      @custom_queue_name = "QUEUE_NAME"
    end

    after do
      @litequeue.clear
    end

    it "returns true when table is empty" do
      assert @litequeue.empty?
    end

    it "returns false when table not empty" do
      @litequeue.push(@value)
      @litequeue.push(@value, queue: @custom_queue_name)

      refute @litequeue.empty?
    end
  end
end

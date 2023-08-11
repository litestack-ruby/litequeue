# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "simplecov"
SimpleCov.start do
  enable_coverage :branch
end

require "litequeue"

require "minitest/autorun"

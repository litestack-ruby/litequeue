# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/test_*.rb"]
end

desc "Update the README code coverage badge"
task :update_readme_coverage_badge do
  require "json"

  next unless File.exist?("coverage/.last_run.json")

  last_run_coverage = JSON.load_file("coverage/.last_run.json")
  line_coverage = last_run_coverage.dig("result", "line")
  branch_coverage = last_run_coverage.dig("result", "branch")
  average_coverage = [(branch_coverage * 1), (line_coverage * 1.5)].sum.fdiv(2.5).round
  badge_color = if average_coverage >= 75
    :brightgreen
  else
    :red
  end

  coverage_badge_re = /!\[Coverage\]\(https:\/\/img.shields.io\/badge\/code_coverage-(.*?)\)/
  last_run_coverage_badge = "![Coverage](https://img.shields.io/badge/code_coverage-#{average_coverage}%25-#{badge_color})"

  new_readme = File.read("README.md").gsub(coverage_badge_re, last_run_coverage_badge)

  File.write("README.md", new_readme)

  puts "Updated README code coverage badge to show #{average_coverage}% coverage."
end

task cov: %i[test update_readme_coverage_badge]

require "standard/rake"

task default: %i[test standard]

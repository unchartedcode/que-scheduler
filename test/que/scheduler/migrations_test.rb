require 'test_helper'

describe Que::Scheduler::Migrations do
  before do
    DB.drop_table? :que_jobs
    DB.drop_table? :que_scheduler
    Que.migrate!
  end

  it "it starts out at 0" do
    Que::Scheduler::Migrations.db_version.must_equal 0
  end

  it "can migrate to 1" do
    Que::Scheduler.migrate!
    Que::Scheduler::Migrations.db_version.must_equal 1
  end

  it "can migrate back down to 0" do
    Que::Scheduler.migrate!
    Que::Scheduler::Migrations.db_version.must_equal 1
    Que::Scheduler.migrate! version: 0
    Que::Scheduler::Migrations.db_version.must_equal 0
  end
end

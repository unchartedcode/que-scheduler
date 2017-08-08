require 'test_helper'

describe Que::Scheduler::Migrations do
  before do
    DB.drop_table? :que_jobs
    DB.drop_table? :que_scheduler
    DB.drop_function :que_scheduler_insert_job, if_exists: true
    DB.drop_function :que_scheduler_update_job, if_exists: true
    DB.drop_function :que_jobs_reschedule_job, if_exists: true
    DB.drop_function :que_scheduler_parse_cron, args: [:text], if_exists: true
    Que.migrate!
  end

  after do
    # We must reset everything so other tests don't start failing
    DB.drop_table? :que_jobs
    DB.drop_table? :que_scheduler
    DB.drop_function :que_scheduler_insert_job, if_exists: true
    DB.drop_function :que_scheduler_update_job, if_exists: true
    DB.drop_function :que_jobs_reschedule_job, if_exists: true
    DB.drop_function :que_scheduler_parse_cron, args: [:text], if_exists: true
    Que.migrate!
    Que::Data.migrate!
    Que::Scheduler.migrate!
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

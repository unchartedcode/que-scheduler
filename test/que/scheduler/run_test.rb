require 'test_helper'

describe Que::Job, '.run' do
  before do
    DB[:que_scheduler].delete
    DB[:que_jobs].delete
    ArgsJob.passed_args = nil
    ArgsJob.last_execution = nil
  end

  it "should immediately process the job with the arguments given to it" do
    result = ArgsJob.run 1, 'two', {:three => 3}
    result.must_be_instance_of ArgsJob
    result.attrs[:args].must_equal [1, 'two', {:three => 3}]

    DB[:que_jobs].count.must_equal 0
    ArgsJob.passed_args.must_equal [1, 'two', {:three => 3}]
  end

  describe "with disabled schedule" do
    let(:job_schedule) do
      {
        'test_job' => {
          'expression' => '*/10 * * * *',
          'job_class' => 'SkipDestroyJob',
          'args' => [1, 'two', {three: 3}],
          'enabled' => false
        }
      }
    end

    before do
      Que::Scheduler.load_schedule!(job_schedule)
    end

    it "should have schedule" do
      DB[:que_scheduler].count.must_equal 1

      row = DB[:que_scheduler].first
      row[:expression].must_equal '*/10 * * * *'
      row[:job_class].must_equal 'SkipDestroyJob'
      JSON.parse(row[:args]).must_equal [1,"two",{"three" => 3}]
      row[:enabled].must_equal false
    end

    it "should not have a job" do
      DB[:que_jobs].count.must_equal 0
    end

    it "will process if enabled" do
      DB[:que_scheduler].update(enabled: true)
      DB[:que_jobs].count.must_equal 1

      row = DB[:que_jobs].first
      row[:priority].must_equal 100
      row[:run_at].wont_be_nil
      row[:job_id].wont_be_nil
      row[:job_class].must_equal 'SkipDestroyJob'
      row[:error_count].must_equal 0
      row[:last_error].must_be_nil
      row[:queue].must_equal ''
      JSON.parse(row[:data]).must_equal({
        "scheduler" => {
          "name" => "test_job"
        }
      })
    end
  end

  describe "with enabled schedule" do
    let(:job_schedule) do
      {
        'test_job' => {
          'expression' => '* * * * *',
          'job_class' => 'SkipDestroyJob',
          'args' => [1, 'two', {three: 3}],
          'enabled' => true
        }
      }
    end

    before do
      Que::Scheduler.load_schedule!(job_schedule)
    end

    it "should have a job" do
      DB[:que_jobs].count.must_equal 1

      row = DB[:que_jobs].first
      row[:priority].must_equal 100
      row[:run_at].wont_be_nil
      row[:job_id].wont_be_nil
      row[:job_class].must_equal 'SkipDestroyJob'
      row[:error_count].must_equal 0
      row[:last_error].must_be_nil
      row[:queue].must_equal ''
      JSON.parse(row[:data]).must_equal({
        "scheduler" => {
          "name" => "test_job"
        }
      })
    end

    it "should process the job" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked, result[:error]
      result[:job][:job_class].must_equal 'SkipDestroyJob'

      # It wont schedule a second copy since we didn't destroy
      DB[:que_jobs].count.must_equal 1
    end
  end

  describe "with arg job" do
    let(:job_schedule) do
      {
        'test_job' => {
          'expression' => '* * * * *',
          'job_class' => 'ArgsJob',
          'args' => [1, 'two', {three: 3}],
          'enabled' => true
        }
      }
    end

    before do
      Que::Scheduler.load_schedule!(job_schedule)
    end

    it "should process the job with the arguments given to it" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked, result[:error]
      result[:job][:job_class].must_equal 'ArgsJob'

      ArgsJob.passed_args.must_equal [1, 'two', {'three' => 3}]
    end

    it "should reschedule the job with the same arguments" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked, result[:error]
      result[:job][:job_class].must_equal 'ArgsJob'

      DB[:que_jobs].count.must_equal 1
      queued_job = DB[:que_jobs].first
      queued_job[:job_class].must_equal 'ArgsJob'
      JSON.parse(queued_job[:args]).must_equal [1, 'two', {'three' => 3}]
    end

    it "should skip the process if enabled is later set to false" do
      DB[:que_jobs].count.must_equal 1
      DB[:que_scheduler].update(enabled: false)
      DB[:que_jobs].count.must_equal 0
    end

    it "should process the job and reschedule it" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked, result[:error]

      # It should schedule a second copy
      DB[:que_jobs].count.must_equal 1
      row = DB[:que_jobs].first
      row[:run_at].must_be_close_to Time.now - Time.now.sec + 60, 5
      row[:job_class].must_equal 'ArgsJob'
      row[:error_count].must_equal 0
      row[:last_error].must_be_nil
      data = JSON.parse(row[:data])
      data.dig('scheduler', 'name').must_equal 'test_job'
      data.dig('scheduler', 'last_executed_at').must_be_close_to Time.now.to_i
    end
  end

  describe "job processing" do
    let(:job_schedule) do
      {
        'test_job' => {
          'expression' => '0 */2 * * *',
          'job_class' => 'ArgsJob',
          'args' => [1, 'two', {three: 3}],
          'enabled' => true
        }
      }
    end

    before do
      Que::Scheduler.load_schedule!(job_schedule)
    end

    it "should immediately reschedule the job if the schedule is changed" do
      DB[:que_scheduler].count.must_equal 1
      DB[:que_scheduler].first[:expression].must_equal '0 */2 * * *'

      DB[:que_jobs].count.must_equal 1
      DB[:que_jobs].first[:run_at].must_be_close_to Time.now - (Time.now.hour * 60 * 60) - (Time.now.min * 60) - Time.now.sec + ((((Time.now.hour / 2) + 1) * 2) * 60 * 60), 5

      # Update the schedule
      Que.set_schedule('test_job', job_schedule['test_job'].merge({ 'expression' => '@hourly' }))
      DB[:que_scheduler].first[:expression].must_equal '@hourly'

      # It resets the run_at to an hour from now
      DB[:que_jobs].first[:run_at].must_be_close_to Time.now - (Time.now.hour * 60 * 60) - (Time.now.min * 60) - Time.now.sec + ((Time.now.hour + 1) * 60 * 60), 5

      # It won't run the job
      result = Que::Job.work
      result[:event].must_equal :job_unavailable, result[:error]
    end

    it "should immediately reschedule the job if enabled is changed" do
      DB[:que_scheduler].count.must_equal 1
      DB[:que_scheduler].first[:enabled].must_equal true

      DB[:que_jobs].count.must_equal 1
      DB[:que_jobs].first[:run_at].must_be_close_to Time.now - (Time.now.hour * 60 * 60) - (Time.now.min * 60) - Time.now.sec + ((((Time.now.hour / 2) + 1) * 2) * 60 * 60), 5

      # Update the schedule
      DB[:que_scheduler].update(enabled: false)
      DB[:que_scheduler].first[:enabled].must_equal false
      DB[:que_jobs].count.must_equal 0
    end
  end
end

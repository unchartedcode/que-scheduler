require 'test_helper'

describe Que::Job, '.run' do
  before do
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

  describe "with enabled schedule" do
    let(:job_schedule) do
      {
        'test_job' => {
          'every' => '10m',
          'class' => 'ArgsJob',
          'args' => [1, 'two', {three: 3}],
          'enabled' => true
        }
      }
    end

    before do
      Que::Scheduler.enabled = true
      Que.schedule = job_schedule
      Que::Scheduler.load_schedule!
      $passed_args = nil
    end

    it "should process the job with the arguments given to it" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked
      result[:job][:job_class].must_equal 'ArgsJob'

      ArgsJob.passed_args.must_equal [1, 'two', {'three' => 3}]
    end

    it "should reschedule the job with the same arguments" do
      DB[:que_jobs].count.must_equal 1

      result = Que::Job.work
      result[:event].must_equal :job_worked
      result[:job][:job_class].must_equal 'ArgsJob'

      DB[:que_jobs].count.must_equal 1
      queued_job = DB[:que_jobs].first
      queued_job[:job_class].must_equal 'ArgsJob'
      JSON.parse(queued_job[:args]).must_equal [1, 'two', {'three' => 3}]
    end

    it "should skip the process if enabled is later set to false" do
      DB[:que_jobs].count.must_equal 1
      Que.set_schedule('test_job', job_schedule['test_job'].merge({ 'enabled' => false }))

      result = Que::Job.work
      result[:event].must_equal :job_worked
      result[:job][:job_class].must_equal 'ArgsJob'
      DB[:que_jobs].count.must_equal 0
      ArgsJob.last_execution.must_be_nil
    end
  end
end

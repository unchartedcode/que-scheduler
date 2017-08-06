require 'test_helper'
require 'byebug'

describe Que::Scheduler do
  it 'has a version' do
    refute_nil ::Que::Scheduler::VERSION
  end

  describe 'load_schedule' do
    before do
      DB[:que_jobs].delete
      DB[:que_scheduler].delete
    end

    describe 'when job enabled option is false' do
      let(:job_schedule) do
        {
          'some_ivar_job' => {
            'every' => '10m',
            'job_class' => 'SomeIvarJob',
            'args' => [ '/tmp' ],
            'enabled' => false
          }
        }
      end

      before do
        Que::Scheduler.load_schedule!(job_schedule)
      end

      it 'does not increase the jobs amount' do
        jobs = Que.job_stats.select { |s| s['job_class'] == 'SomeIvarJob' }
        assert_equal 0, jobs.size
      end

      it 'does include in scheduled jobs' do
        refute_nil Que.get_schedule('some_ivar_job')
      end
    end

    describe 'when job enabled option is true' do
      let(:job_schedule) do
        {
          'some_ivar_job' => {
            'every' => '10m',
            'job_class' => 'SomeIvarJob',
            'args' => { path: '/tmp ' },
            'enabled' => true
          }
        }
      end

      before do
        Que::Scheduler.load_schedule!(job_schedule)
      end

      it 'does increase the jobs amount' do
        jobs = Que.job_stats.select { |s| s['job_class'] == 'SomeIvarJob' }
        assert_equal 1, jobs.size
      end

      it 'does include in scheduled jobs' do
        schedule = Que.get_schedule('some_ivar_job')
        refute_nil schedule
        assert_equal true, schedule['enabled']
      end
    end
  end

  describe '.enqueue_job' do
    let(:schedule_time) { Time.now }
    let(:args) { '/tmp' }
    let(:scheduler_config) do
      { 'job_class' => 'TestJob', 'queue' => 'high', 'args'  => args, 'every' => '1d' }
    end

    describe 'when it is a que job' do
      it 'prepares the parameters' do
        mock_enqueue = MiniTest::Mock.new
        mock_enqueue.expect :call, nil, ['/tmp', {
          :job_class => 'TestJob',
          :queue => 'high'
        }]
        
        Que::Job.stub(:enqueue, mock_enqueue) do
          Que.set_schedule('test', scheduler_config)
        end
      end

      it 'sends to execute' do
        mock_execute = MiniTest::Mock.new
        mock_execute.expect :call, [{}], [
          Que::Scheduler::SQL[:get_schedule_by_name],
          ['test']
        ]
        mock_execute.expect :call, [{}], [
          Que::Scheduler::SQL[:insert_schedule],
          [
            'test',
            'TestJob',
            ['/tmp'],
            nil,
            '1d',
            nil
          ]
        ]
        mock_execute.expect :call, [
          {
            queue: 'high', 
            priority: 100, 
            run_at: DateTime.now, 
            job_class: 'TestJob', 
            args: ['/tmp']
          }
        ], [
          :insert_job, 
          ["high", nil, nil, "TestJob", ["/tmp"]]
        ]
        
        Que.stub(:execute, mock_execute) do
          Que.set_schedule('test', scheduler_config)
        end
      end
    end
  end
end

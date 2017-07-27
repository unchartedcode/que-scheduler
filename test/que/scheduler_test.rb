require 'test_helper'
require 'byebug'

describe Que::Scheduler do
  it 'has a version' do
    refute_nil ::Que::Scheduler::VERSION
  end

  describe 'load_schedule' do
    before do
      DB[:que_jobs].delete
    end

    describe 'when scheduler enabled option is false' do
      let(:job_schedule) do
        {
          'some_ivar_job' => {
            'every' => '10m',
            'class' => 'SomeIvarJob',
            'args' => { path: '/tmp ' }
          }
        }
      end

      before do
        Que::Scheduler.enabled = false
        Que.schedule = job_schedule
        Que::Scheduler.load_schedule!
      end

      it 'does not increase the jobs amount' do
        jobs = Que.job_stats.select { |s| s['job_class'] == 'SomeIvarJob' }
        assert_equal 0, jobs.size
      end

      it 'does include in scheduled jobs' do
        refute_nil Que.get_schedule('some_ivar_job')
      end
    end

    describe 'when job enabled option is false' do
      let(:job_schedule) do
        {
          'some_ivar_job' => {
            'every' => '10m',
            'class' => 'SomeIvarJob',
            'args' => [ '/tmp' ],
            'enabled' => false
          }
        }
      end

      before do
        Que::Scheduler.enabled = true
        Que.schedule = job_schedule
        Que::Scheduler.load_schedule!
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
            'class' => 'SomeIvarJob',
            'args' => { path: '/tmp ' },
            'enabled' => true
          }
        }
      end

      before do
        Que::Scheduler.enabled = true
        Que.schedule = job_schedule
        Que::Scheduler.load_schedule!
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
end

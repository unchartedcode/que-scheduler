module Que
  module Scheduler
    module Scheduled
      # Default repetition interval in seconds. Can be overridden in
      # subclasses. Can use 1.minute if using Rails.
      @interval = 60

      class << self
        attr_reader :interval
        def every(interval)
          @interval = interval
        end

        def schedule(klass, args = {})
          unless QueJob.where(job_class: klass.to_s).exists?
            puts 'Scheduling'
            klass.enqueue args.merge({ start_at: (Time.now - TypeformUpdate.interval).to_f, end_at: Time.now.to_f })
          end
        end
      end

      attr_reader :start_at, :end_at, :run_again_at, :time_range

      def _run
        schedule = Que.execute(Que::Scheduler::SQL[:get_schedule_by_job_id], [attrs[:job_id]]).first
        if schedule && !schedule['enabled']
          destroy unless @destroyed
          return
        end

        data = JSON.parse(schedule.dig('data') || '{}')
        data['scheduled'] = {} if data['scheduled'].nil?

        @start_at = Time.at(data['scheduled']['start_at'])
        @end_at = Time.at(data['scheduled']['end_at'])
        @run_again_at = @end_at + Que::Scheduler.parse_in(schedule['every'])
        @time_range = @start_at...@end_at

        super

        data['scheduled']['start_at'] = @end_at.to_f
        data['scheduled']['end_at']   = @run_again_at.to_f
        Que.execute(Que::Scheduler::SQL[:update_data], [attrs[:job_id], data])
        self.class.enqueue(*attrs[:args], run_at: @run_again_at)
      end
    end
  end
end

Que::Job.prepend Que::Scheduler::Scheduled

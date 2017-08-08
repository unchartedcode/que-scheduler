module Que
  module Scheduler
    module Scheduled
      attr_reader :schedule

      def scheduled?
        !@data['scheduler'].nil? && !@data['scheduler']['name'].nil?
      end

      def _run
        unless scheduled?
          super
          return
        end

        @schedule = Que.execute(Que::Scheduler::SQL[:get_schedule_by_name], [@data['scheduler']['name']]).first || {}
        if @schedule && !@schedule['enabled']
          destroy unless @destroyed
          return
        end

        # next_executed_at = Que.execute(Que::Scheduler::SQL[:parse_cron], [@schedule['expression']]).first

        # if (next_execution_at - 5) <= Time.now
          super
        # else
        #   destroy unless @destroyed
        # end

        if Que.execute(Que::Scheduler::SQL[:check_job], [@data['scheduler']['name']]).none?
          run_again_at = Que.execute(Que::Scheduler::SQL[:parse_cron], [@schedule['expression']]).first[:next_at]
          new_job = self.class.enqueue(*attrs[:args], run_at: run_again_at)
          @data['scheduler']['last_executed_at'] = Time.now.to_i
          Que::Job.update_data(new_job.attrs[:job_id], @data['scheduler'], section: 'scheduler')
        end
      end
    end
  end
end

if defined? Que::Data::Extension
  fail "que/data must be defined after que/scheduler"
end

Que::Job.prepend Que::Scheduler::Scheduled
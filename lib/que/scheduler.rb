require "que/job"
require "que/scheduler/version"
require "que/scheduler/migrations"
require "que/scheduler/util"
require "que/scheduler/sql"
require "que/scheduler/schedule"
require "que/scheduler/scheduled"

module Que
  module Scheduler
    class << self
      def enabled?
        @enabled
      end

      def enabled=(value)
        renenabled = @enabled == false && value == true
        @enabled = value

        if renenabled
          Que.reload_schedules!
        end

        @enabled
      end

      def load_schedule!(schedule)
        Que.log message: 'Loading Schedule'

        Que.schedule = schedule

        if Que.schedule.empty?
          Que.log message: 'Schedule empty!' 
        else
          Que.log message: "#{Que.schedule.size} schedules loaded"
        end
      end
    end
  end
end

require "que/job"
require "que/scheduler/util"
require "que/scheduler/schedule"
require "que/scheduler/scheduled"

module Que
  module Scheduler
    autoload :Migrations, 'que/scheduler/migrations'
    autoload :SQL, 'que/scheduler/sql'
    autoload :Version, 'que/scheduler/version'

    class << self
      def migrate!(version = {:version => Migrations::CURRENT_VERSION})
        Migrations.migrate!(version)
      end

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

require "que/adapters/base"
Que::Adapters::Base::CAST_PROCS[16] = true.method(:==)
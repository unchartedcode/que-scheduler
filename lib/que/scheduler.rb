require "que/scheduler/version"
require "que/scheduler/migrations"
require "que/scheduler/util"
require "que/scheduler/sql"
require "que/scheduler/schedule"
require "que/scheduler/scheduled"

module Que
  module Scheduler
    class << self
      # Set to enable or disable the scheduler.
      attr_accessor :enabled

      # Pulls the schedule from Sidekiq.schedule and loads it into the
      # rufus scheduler instance
      def load_schedule!
        if enabled
          Que.log message: 'Loading Schedule'

          # Load schedule from redis for the first time if dynamic
          # if dynamic
          #   Sidekiq.reload_schedule!
          #   @current_changed_score = Time.now.to_f
          #   rufus_scheduler.every('5s') do
          #     update_schedule
          #   end
          # end

          # logger.info 'Schedule empty! Set Sidekiq.schedule' if Sidekiq.schedule.empty?


          # @@scheduled_jobs = {}
          # queues = sidekiq_queues

          # Sidekiq.schedule.each do |name, config|
          #   if !listened_queues_only || enabled_queue?(config['queue'].to_s, queues)
          #     load_schedule_job(name, config)
          #   else
          #     logger.info { "Ignoring #{name}, job's queue is not enabled." }
          #   end
          # end

          # Sidekiq.redis { |r| r.del(:schedules_changed) unless r.type(:schedules_changed) == 'zset' }

          Que.log message: 'Schedules Loaded'
        else
          Que.log message: 'SidekiqScheduler is disabled'
        end
      end
    end
  end
end

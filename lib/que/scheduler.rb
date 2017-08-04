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

      def try_to_constantize(klass)
        klass.is_a?(String) ? klass.constantize : klass
      rescue NameError
        klass
      end

      def symbolize_keys!(hash)
        hash.keys.each do |key|
          hash[(key.to_sym rescue key) || key] = hash.delete(key)
        end
      end

      # Convert the given arguments in the format expected to be enqueued.
      #
      # @param [Hash] config the options to be converted
      # @option config [String] class the job class
      # @option config [Hash/Array] args the arguments to be passed to the job
      #   class
      #
      # @return [Hash]
      def prepare_arguments(config)
        config['job_class'] = try_to_constantize(config['job_class'])

        if config['args'].is_a?(Hash)
          config['args'].symbolize_keys! if config['args'].respond_to?(:symbolize_keys!)
        else
          config['args'] = Array(config['args'])
        end

        config.delete('cron')
        config.delete('every')

        symbolize_keys!(config)

        config
      end

      # Enqueue a job based on a config hash
      #
      # @param job_config [Hash] the job configuration
      # @param time [Time] time the job is enqueued
      def enqueue_job(job_config, time = Time.now)
        config = prepare_arguments(job_config.dup)

        if config.delete('include_metadata')
          config['args'] = arguments_with_metadata(config['args'], scheduled_at: time.to_f)
        end

        args = config.delete(:args)
        Que::Job.enqueue *args, config
      end
    end
  end
end

module Que
  module Scheduler
    module Schedule
      # Accepts a new schedule configuration of the form:
      #
      #   {
      #     "MakeTea" => {
      #       "every" => "1m" 
      #     },
      #     "some_name" => {
      #       "cron"        => "5/* * * *",
      #       "class"       => "DoSomeWork",
      #       "args"        => "work on this string",
      #       "description" => "this thing works it"s butter off" },
      #     ...
      #   }
      #
      # Hash keys can be anything and are used to describe and reference
      # the scheduled job. If the "class" argument is missing, the key
      # is used implicitly as "class" argument - in the "MakeTea" example,
      # "MakeTea" is used both as job name and sidekiq worker class.
      #
      # :cron can be any cron scheduling string
      #
      # :every can be used in lieu of :cron. see rufus-scheduler's 'every' usage
      # for valid syntax. If :cron is present it will take precedence over :every.
      #
      # :class must be a sidekiq worker class. If it is missing, the job name (hash key)
      # will be used as :class.
      #
      # :args can be any yaml which will be converted to a ruby literal and
      # passed in a params. (optional)
      #
      # :description is just that, a description of the job (optional). If
      # params is an array, each element in the array is passed as a separate
      # param, otherwise params is passed in as the only parameter to perform.
      def schedule=(schedule_hash)
        schedule_hash = prepare_schedule(schedule_hash)
        to_remove = get_all_schedules.keys - schedule_hash.keys.map(&:to_s)

        schedule_hash.each do |name, job_spec|
          set_schedule(name, job_spec)
        end

        to_remove.each do |name|
          remove_schedule(name)
        end

        @schedule = schedule_hash
      end

      def schedule
        @schedule
      end

      # Reloads the schedule from Redis and return it.
      #
      # @return Hash
      def reload_schedule!
        @schedule = get_schedule
      end
      alias_method :schedule!, :reload_schedule!

      # Retrive the schedule configuration for the given name
      # if the name is nil it returns a hash with all the
      # names end their schedules.
      def get_schedule(name = nil)
        if name.nil?
          get_all_schedules
        else
          convert_result(Que.execute(Que::Scheduler::SQL[:get_schedule_by_name], [name]))[name]
        end
      end

      def get_all_schedules
        convert_result(Que.execute(Que::Scheduler::SQL[:get_all]))
      end

      def convert_result(result)
        Hash[result.map { |row| [row['name'], Hash[(row.keys - ['name']).map { |k| [k, row[k]] }]] }]
      end

      # Create or update a schedule with the provided name and configuration.
      #
      # Note: values for class and custom_job_class need to be strings,
      # not constants.
      #
      #    Sidekiq.set_schedule('some_job', { :class => 'SomeJob',
      #                                       :every => '15mins',
      #                                       :queue => 'high',
      #                                       :args => '/tmp/poop' })
      def set_schedule(name, config)
        existing_config = get_schedule(name)
        unless existing_config && existing_config == config
          Que.execute Que::Scheduler::SQL[:insert_schedule], [
            name,
            config['class'],
            [config['args']],
            config['description'],
            config['every'],
            config['enabled']
          ]
        end

        scheduled_job = Que.execute(Que::Scheduler::SQL[:get_scheduled_job], [name]).first
        if config['enabled']
          if scheduled_job
            # Update?
          else
            # args = config['args'].merge({
            #                    start_at: (Time.now - Que::Scheduler.parse_in(config['every'])).to_f,
            #                    end_at: Time.now.to_f
            #                  })
            job = Que::Job.enqueue(
              *config['args'],
              queue: config['queue'],
              job_class: config['class']
            )

            data = {
              scheduled: {
                name: name,
                start_at: (Time.now - Que::Scheduler.parse_in(config['every'])).to_f,
                end_at: Time.now.to_f
              }
            }

            Que.execute(Que::Scheduler::SQL[:update_data], [job.attrs[:job_id], data])
          end
        else
          # Remove...
        end

        config
      end

      # remove a given schedule by name
      def remove_schedule(name)
        Que.execute Que::Scheduler::SQL[:destroy_schedule], [name]
      end

    private

      def prepare_schedule(schedule_hash)
        schedule_hash = Que.json_converter.call(schedule_hash)

        prepared_hash = {}

        schedule_hash.each do |name, job_spec|
          job_spec = job_spec.dup

          job_class = job_spec.fetch('class', name)
          inferred_queue = infer_queue(job_class)

          job_spec['class'] ||= job_class
          job_spec['queue'] ||= inferred_queue unless inferred_queue.nil?

          prepared_hash[name] = job_spec
        end
        prepared_hash
      end

      def infer_queue(_klass)
        # Wait until official queue support in 1.0
        ''
      end

      def try_to_constantize(klass)
        klass.is_a?(String) ? klass.constantize : klass
      rescue NameError
        klass
      end
    end
  end
end

Que.extend Que::Scheduler::Schedule

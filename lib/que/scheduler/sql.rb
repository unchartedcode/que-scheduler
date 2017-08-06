module Que
  module Scheduler
    SQL = {
      :get_all => %{
        SELECT que_scheduler.name
             , que_scheduler.job_class
             , que_scheduler.args
             , que_scheduler.description
             , que_scheduler.every
             , que_scheduler.enabled
        FROM que_scheduler
      }.freeze,

      :get_schedule_by_name => %{
        SELECT que_scheduler.name
             , que_scheduler.job_class
             , que_scheduler.args
             , que_scheduler.description
             , que_scheduler.every
             , que_scheduler.enabled
        FROM que_scheduler
        WHERE name = $1::text
      },

      :get_schedule => %{
        SELECT que_scheduler.name
             , que_scheduler.job_class
             , que_scheduler.args
             , que_scheduler.description
             , que_scheduler.every
             , que_scheduler.enabled
        FROM que_scheduler
        WHERE job_class = $1::text
      },

      :insert_schedule => %{
        INSERT INTO que_scheduler
        (
          name,
          job_class,
          args,
          description,
          every,
          enabled
        )
        VALUES
        (
          $1::text,
          coalesce($2, '')::text,
          coalesce($3, '[]')::json,
          coalesce($4, '')::text,
          coalesce($5, '')::text,
          coalesce($6, false)::boolean
        )
        ON CONFLICT(name) DO UPDATE
        SET job_class = coalesce($2, '')::text
          , args = coalesce($3, '[]')::json
          , description = coalesce($4, '')::text
          , every = coalesce($5, '')::text
        RETURNING *
      }.freeze,

      :destroy_schedule => %{
        DELETE FROM que_scheduler
        WHERE name = $1::text
      }.freeze,

      :check_job => %{
        SELECT 1
        FROM que_jobs
        WHERE que_jobs.data->'scheduler'->>'name' = $1::text
      }.freeze
    }.freeze
  end
end

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

      :get_schedule_by_job_id => %{
        SELECT que_scheduler.name
             , que_scheduler.job_class
             , que_scheduler.args
             , que_scheduler.description
             , que_scheduler.every
             , que_scheduler.enabled
             , que_jobs.data
        FROM que_scheduler
        JOIN que_jobs
          ON que_jobs.data->'scheduled'->>'name' = que_scheduler.name
        WHERE que_jobs.job_id = $1::integer
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
          , enabled = coalesce($6, false)::boolean
        RETURNING *
      }.freeze,

      :destroy_schedule => %{
        DELETE FROM que_scheduler
        WHERE name = $1::text
      }.freeze,

      :check_job_scheduled => %{
        SELECT 1 AS one
        FROM   que_jobs
        WHERE  job_class = $1::text
        AND    queue     = $2::text
      }.freeze,

      :get_scheduled_job => %{
        SELECT que_scheduler.name
             , que_scheduler.job_class
             , que_scheduler.args
             , que_scheduler.description
             , que_scheduler.every
             , que_scheduler.enabled
             , que_jobs.job_id
        FROM que_scheduler
        JOIN que_jobs
          ON que_jobs.data->'scheduled'->>'name' = que_scheduler.name
        WHERE que_scheduler.name = $1::text
      }.freeze,

      :get_data => %{
        SELECT job_id
             , data
        FROM que_jobs
        WHERE job_id = $1::integer
      }.freeze,

      :update_data => %{
        UPDATE que_jobs
        SET data = $2::jsonb
        WHERE job_id = $1::integer
      }.freeze
    }.freeze
  end
end

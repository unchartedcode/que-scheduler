CREATE OR REPLACE FUNCTION que_jobs_reschedule_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    schedule record;
    row_count int;

  BEGIN
    IF OLD.data->'scheduler'->'name' IS NULL THEN
      RETURN NULL;
    END IF;

    INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
    SELECT
      que_scheduler.queue,
      que_scheduler.priority,
      que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute'),
      que_scheduler.job_class,
      que_scheduler.args,
      ('{"scheduler": {"name":"' || que_scheduler.name || '"}}')::jsonb
    FROM que_scheduler
    WHERE que_scheduler.name = OLD.data->'scheduler'->>'name'
    AND que_scheduler.enabled = true
    AND NOT EXISTS (
      SELECT 1
      FROM que_jobs
      WHERE data->'scheduler'->>'name' = que_scheduler.name
    );

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Rescheduled % job(s) for schedule %', row_count, schedule.name;
    END IF;

    RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_unschedule_jobs(schedule_name text)
RETURNS TABLE(job_id bigint, success bool)
LANGUAGE plpgsql
AS $$
  BEGIN
    RETURN QUERY
      DELETE FROM que_jobs
      USING (
        SELECT qj.job_id, pg_try_advisory_lock(qj.job_id) AS locked
        FROM que_jobs qj
        WHERE qj.data->'scheduler'->>'name' = schedule_name
      ) target
      WHERE target.job_id = que_jobs.job_id
      and target.locked
      RETURNING target.job_id, pg_advisory_unlock(target.job_id) as unlocked;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_reschedule_jobs(schedule_name text)
RETURNS TABLE(job_id bigint, success bool)
LANGUAGE plpgsql
AS $$
  BEGIN
    -- Remove extra jobs
    DELETE
    FROM que_jobs
    USING (
      SELECT MIN(qj.job_id) as job_id, schedule_name as schedule_name
      FROM que_jobs qj
      WHERE qj.data->'scheduler'->>'name' = schedule_name
      GROUP BY qj.data->'scheduler'->>'name'
      HAVING COUNT(qj.job_id) > 1
    ) duplicates
    WHERE duplicates.schedule_name = que_jobs.data->'scheduler'->>'name'
    AND que_jobs.job_id != duplicates.job_id;

    RETURN QUERY
      UPDATE que_jobs
      SET run_at = que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute')
        , job_class = que_scheduler.job_class
        , args = que_scheduler.args
      FROM (
        SELECT qj.job_id, pg_try_advisory_lock(qj.job_id) AS locked
        FROM que_jobs qj
        WHERE qj.data->'scheduler'->>'name' = schedule_name
      ) target
      JOIN que_scheduler ON que_scheduler.name = schedule_name
      WHERE target.job_id = que_jobs.job_id
      AND target.locked
      AND que_scheduler.enabled
      RETURNING target.job_id, pg_advisory_unlock(target.job_id) as unlocked;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_schedule_jobs(schedule_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
    SELECT
      que_scheduler.queue,
      que_scheduler.priority,
      que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute'),
      que_scheduler.job_class,
      que_scheduler.args,
      ('{"scheduler": {"name":"' || que_scheduler.name || '"}}')::jsonb
    FROM que_scheduler
    LEFT JOIN que_jobs ON que_jobs.data->'scheduler'->>'name' = que_scheduler.name
    WHERE que_scheduler.name = schedule_name
    AND que_scheduler.enabled = true
    AND que_jobs.job_id IS NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_update_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked bool[];
    row_count int;

  BEGIN
    IF NEW.name != OLD.name THEN
      PERFORM que_scheduler_unschedule_jobs(OLD.name);

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Unscheduled % jobs(s) FROM que_jobs', row_count;
      END IF;
    END IF;

    IF NEW.enabled THEN
      PERFORM que_scheduler_reschedule_jobs(OLD.name);

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Rescheduled % jobs(s) FROM que_jobs', row_count;
      ELSE
        PERFORM que_scheduler_schedule_jobs(OLD.name);

        IF FOUND THEN
          GET DIAGNOSTICS row_count = ROW_COUNT;
          RAISE NOTICE 'Scheduled % jobs(s) FROM que_jobs', row_count;
        END IF;
      END IF;

      IF NOT EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
        RAISE EXCEPTION 'Unable to schedule jobs for schedule %', NEW.name;
      END IF;
    ELSE
      PERFORM que_scheduler_unschedule_jobs(NEW.name);

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Unscheduled % jobs(s) FROM que_jobs', row_count;
      END IF;

      IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = OLD.name) THEN
        RAISE EXCEPTION 'Unable to unschedule jobs for schedule %', OLD.name;
      END IF;
    END IF;

    RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked record;
    row_count int;

  BEGIN
    IF NOT NEW.enabled THEN
      PERFORM que_scheduler_unschedule_jobs(NEW.name);

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Unscheduled % jobs(s) FROM que_jobs', row_count;
      END IF;

      RETURN NULL;
    END IF;

    PERFORM que_scheduler_reschedule_jobs(NEW.name);

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Rescheduled % jobs(s) FROM que_jobs', row_count;
    END IF;

    PERFORM que_scheduler_schedule_jobs(NEW.name);

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Scheduled % jobs(s) FROM que_jobs', row_count;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
      RAISE EXCEPTION 'Unable to schedule jobs for schedule %', NEW.name;
    END IF;

    RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_delete_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked record;
    row_count int;

  BEGIN
    PERFORM que_scheduler_unschedule_jobs(OLD.name);

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Unscheduled % jobs(s) FROM que_jobs', row_count;
    END IF;

    IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = OLD.name) THEN
      RAISE EXCEPTION 'Unable to remove jobs for schedule %', OLD.name;
    END IF;

    RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_jobs_reschedule_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    schedule record;
    row_count int;

  BEGIN
    IF OLD.data->'scheduler'->'name' IS NULL THEN
      RETURN NULL;
    END IF;

    PERFORM que_scheduler_schedule_jobs(OLD.data->'scheduler'->>'name');

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Scheduled % jobs(s) FROM que_jobs', row_count;
    END IF;

    RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked record;
    row_count int;

  BEGIN
    IF NOT NEW.enabled THEN
      RETURN NULL;
    END IF;

    WITH target AS (
      SELECT job_id, pg_try_advisory_lock(job_id) AS locked
      FROM que_jobs
      WHERE data->'scheduler'->>'name' = NEW.name
    )
    UPDATE que_jobs
    SET run_at = CASE WHEN OLD.expression != NEW.expression
                      THEN que_scheduler_parse_cron(NEW.expression, now())
                      ELSE run_at
                 END
      , job_class = NEW.job_class
      , args = NEW.args
    FROM target
    WHERE target.job_id = que_jobs.job_id
    AND target.locked
    RETURNING pg_advisory_unlock(target.job_id) INTO unlocked;

    IF FOUND THEN
      GET DIAGNOSTICS row_count = ROW_COUNT;
      RAISE NOTICE 'Rescheduled % jobs(s) FROM que_jobs', row_count;
    ELSE
      INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
      VALUES (
        NEW.queue,
        NEW.priority,
        que_scheduler_parse_cron(NEW.expression, now() - interval '1 minute'),
        NEW.job_class,
        NEW.args,
        ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb
      );

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Scheduled % jobs(s) FROM que_jobs', row_count;
      END IF;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
      RAISE EXCEPTION 'Unable to schedule jobs for schedule %', NEW.name;
    END IF;

    RETURN NULL;
  END;
$$;

DROP TRIGGER que_scheduler_update_job ON que_scheduler;

CREATE TRIGGER que_scheduler_update_job AFTER UPDATE OF name, expression, enabled ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_update_job();

CREATE OR REPLACE FUNCTION que_scheduler_update_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked record;
    row_count int;

  BEGIN
    IF NEW.name != OLD.name THEN
      DELETE FROM que_jobs
      USING (
        SELECT job_id, pg_try_advisory_lock(job_id) AS locked
        FROM que_jobs
        WHERE data->'scheduler'->>'name' = OLD.name
      ) target
      WHERE target.job_id = que_jobs.job_id
      and target.locked
      RETURNING pg_advisory_unlock(target.job_id) INTO unlocked;

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Unscheduled % jobs(s) FROM que_jobs', row_count;
      END IF;
    END IF;

    IF NEW.enabled THEN
      WITH target AS (
        SELECT job_id, pg_try_advisory_lock(job_id) AS locked
        FROM que_jobs
        WHERE data->'scheduler'->>'name' = NEW.name
      )
      UPDATE que_jobs
      SET run_at = CASE WHEN OLD.expression != NEW.expression
                        THEN que_scheduler_parse_cron(NEW.expression, now())
                        ELSE run_at
                   END
        , job_class = NEW.job_class
        , args = NEW.args
      FROM target
      WHERE target.job_id = que_jobs.job_id
      AND target.locked
      RETURNING pg_advisory_unlock(target.job_id) INTO unlocked;

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Rescheduled % jobs(s) FROM que_jobs', row_count;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (
          NEW.queue,
          NEW.priority,
          que_scheduler_parse_cron(NEW.expression, now() - interval '1 minute'),
          NEW.job_class,
          NEW.args,
          ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb
        );

        IF FOUND THEN
          GET DIAGNOSTICS row_count = ROW_COUNT;
          RAISE NOTICE 'Scheduled % jobs(s) FROM que_jobs', row_count;
        END IF;
      END IF;

      IF NOT EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
        RAISE EXCEPTION 'Unable to schedule jobs for schedule %', NEW.name;
      END IF;
    ELSE
      DELETE FROM que_jobs
      WHERE data->'scheduler'->>'name' = NEW.name;

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

DROP TRIGGER que_scheduler_delete_job ON que_scheduler;

CREATE TRIGGER que_scheduler_delete_job AFTER DELETE ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_delete_job();

CREATE OR REPLACE FUNCTION que_scheduler_delete_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    unlocked record;
    row_count int;

  BEGIN
    DELETE FROM que_jobs
    USING (
      SELECT job_id, pg_try_advisory_lock(job_id) AS locked
      FROM que_jobs
      WHERE data->'scheduler'->>'name' = OLD.name
    ) target
    WHERE target.job_id = que_jobs.job_id
    and target.locked
    RETURNING pg_advisory_unlock(target.job_id) INTO unlocked;

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

DROP TRIGGER que_jobs_reschedule_job ON que_jobs;

CREATE TRIGGER que_jobs_reschedule_job AFTER DELETE ON que_jobs
  FOR EACH ROW EXECUTE PROCEDURE que_jobs_reschedule_job();

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

    SELECT *
    INTO schedule
    FROM que_scheduler
    WHERE que_scheduler.name = OLD.data->'scheduler'->>'name'
    AND enabled = true;

    IF FOUND THEN
      INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
      VALUES (
        schedule.queue,
        schedule.priority,
        que_scheduler_parse_cron(schedule.expression, now() - interval '1 minute'),
        schedule.job_class,
        schedule.args,
        ('{"scheduler": {"name":"' || schedule.name || '"}}')::jsonb
      );

      IF FOUND THEN
        GET DIAGNOSTICS row_count = ROW_COUNT;
        RAISE NOTICE 'Rescheduled % job(s) for schedule %', row_count, schedule.name;
      ELSE
        RAISE EXCEPTION 'Unable to reschedule job for schedule %', schedule.name;
      END IF;
    END IF;

    RETURN NULL;
  END;
$$;

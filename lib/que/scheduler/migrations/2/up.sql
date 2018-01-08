CREATE SEQUENCE que_scheduler_id_seq;

ALTER TABLE que_scheduler
  ADD COLUMN schedule_id int not null default nextval('que_scheduler_id_seq'),
  ADD COLUMN queue text not null default '',
  ADD COLUMN priority integer not null default 100;

ALTER SEQUENCE que_scheduler_id_seq OWNED BY que_scheduler.schedule_id;

CREATE OR REPLACE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    JOB record;

  BEGIN
    PERFORM pg_advisory_xact_lock(NEW.schedule_id);

    IF NEW.enabled THEN
      SELECT *
      INTO JOB
      FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name;

      IF FOUND THEN
        UPDATE que_jobs
        SET run_at = que_scheduler_parse_cron(NEW.expression, now())
          , job_class = NEW.job_class
          , args = NEW.args
        WHERE que_jobs.job_id = JOB.job_id;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        SELECT
          que_scheduler.queue,
          que_scheduler.priority,
          que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute'),
          que_scheduler.job_class,
          que_scheduler.args,
          ('{"scheduler": {"name":"' || que_scheduler.name || '"}}')::jsonb
        FROM que_scheduler
        WHERE que_scheduler.name = NEW.name;
      END IF;
    ELSE
      -- Do nothing
    END IF;

    RETURN NEW;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_update_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    JOB record;

  BEGIN
    PERFORM pg_advisory_xact_lock(NEW.schedule_id);

    IF NEW.enabled THEN
      SELECT *
      INTO JOB
      FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name;

      IF FOUND THEN
        PERFORM pg_advisory_xact_lock(JOB.job_id);

        UPDATE que_jobs
        SET run_at = CASE WHEN OLD.expression != NEW.expression
                          THEN que_scheduler_parse_cron(NEW.expression, now())
                          ELSE run_at
                    END
          , job_class = NEW.job_class
          , args = NEW.args
        WHERE que_jobs.job_id = JOB.job_id;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        SELECT
          que_scheduler.queue,
          que_scheduler.priority,
          que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute'),
          que_scheduler.job_class,
          que_scheduler.args,
          ('{"scheduler": {"name":"' || que_scheduler.name || '"}}')::jsonb
        FROM que_scheduler
        WHERE que_scheduler.name = NEW.name;
      END IF;
    ELSE
      DELETE FROM que_jobs
      WHERE data->'scheduler'->>'name' = NEW.name;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE OR REPLACE FUNCTION que_scheduler_delete_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    JOB record;

  BEGIN
    PERFORM pg_advisory_xact_lock(OLD.schedule_id);

    WITH target AS (
      SELECT job_id, pg_try_advisory_lock(job_id) AS locked
      FROM que_jobs
      WHERE data->'scheduler'->>'name' = OLD.name
    )
    DELETE FROM que_jobs
    USING target
    WHERE target.job_id = que_jobs.job_id
    and target.locked
    RETURNING pg_advisory_unlock(target.job_id) INTO JOB;

    RETURN OLD;
  END;
$$;

CREATE TRIGGER que_scheduler_delete_job BEFORE DELETE ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_delete_job();

CREATE OR REPLACE FUNCTION que_jobs_reschedule_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    PERFORM pg_advisory_xact_lock(OLD.job_id);

    IF OLD.data->'scheduler'->'name' IS NULL THEN
      -- Do nothing
    ELSE
      IF NOT EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = OLD.data->'scheduler'->>'name') THEN
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        SELECT
          que_scheduler.queue,
          que_scheduler.priority,
          que_scheduler_parse_cron(que_scheduler.expression, now() - interval '1 minute'),
          que_scheduler.job_class,
          que_scheduler.args,
          ('{"scheduler": {"name":"' || que_scheduler.name || '"}}')::jsonb
        FROM que_scheduler
        WHERE que_scheduler.name = OLD.data->'scheduler'->>'name';
      END IF;
    END IF;

    RETURN OLD;
  END;
$$;

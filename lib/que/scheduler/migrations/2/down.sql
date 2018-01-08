ALTER TABLE que_scheduler
  DROP COLUMN schedule_id,
  DROP COLUMN queue,
  DROP COLUMN priority;

DROP TRIGGER que_scheduler_delete_job ON que_scheduler;

CREATE OR REPLACE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    IF NEW.enabled THEN
      IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
        UPDATE que_jobs
            SET run_at = que_scheduler_parse_cron(NEW.expression, now())
              , job_class = NEW.job_class
              , args = NEW.args
          WHERE data->'scheduler'->>'name' = NEW.name;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (''::text, 100, que_scheduler_parse_cron(NEW.expression, now() - interval '1 minute'), NEW.job_class, NEW.args, ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb);
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
  BEGIN
    IF NEW.enabled THEN
      IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
        UPDATE que_jobs
            SET run_at = CASE WHEN OLD.expression != NEW.expression
                              THEN que_scheduler_parse_cron(NEW.expression, now())
                              ELSE run_at
                        END
              , job_class = NEW.job_class
              , args = NEW.args
          WHERE data->'scheduler'->>'name' = NEW.name;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (''::text, 100, que_scheduler_parse_cron(NEW.expression, now() - interval '1 minute'), NEW.job_class, NEW.args, ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb);
      END IF;
    ELSE
      DELETE FROM que_jobs
      WHERE data->'scheduler'->>'name' = NEW.name;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE OR REPLACE FUNCTION que_jobs_reschedule_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    schedule RECORD;
  BEGIN
    IF OLD.data->'scheduler'->'name' IS NULL THEN
      -- Do nothing
    ELSE
      SELECT *
      INTO schedule
      FROM que_scheduler
      WHERE name = OLD.data->'scheduler'->>'name'
      AND enabled = true;

      IF schedule IS NULL THEN
        -- Do nothing
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (
          OLD.queue,
          OLD.priority,
          que_scheduler_parse_cron(schedule.expression, now()),
          OLD.job_class,
          OLD.args,
          jsonb_build_object(
            'scheduler',
            jsonb_build_object(
              'name', schedule.name,
              'last_executed_at', extract(epoch from date_trunc('second', now()))
            )
          )
        );
      END IF;
    END IF;

    RETURN OLD;
  END;
$$;

CREATE TABLE que_scheduler
(
  name        text        NOT NULL,
  job_class   text        NOT NULL,
  args        json        NOT NULL DEFAULT '[]'::json,
  description text        NOT NULL DEFAULT '',
  every       text        NOT NULL,
  enabled     boolean     NOT NULL DEFAULT false,

  CONSTRAINT que_scheduler_pkey PRIMARY KEY (name)
);

CREATE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    IF NEW.enabled THEN
      IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
        UPDATE que_jobs
           SET run_at = now()
             , job_class = NEW.job_class
             , args = NEW.args
         WHERE data->'scheduler'->>'name' = NEW.name;
      ELSE
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (''::text, 100, now(), NEW.job_class, NEW.args, ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb);
      END IF;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER que_scheduler_insert_job AFTER INSERT OR UPDATE ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_insert_job();


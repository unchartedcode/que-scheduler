CREATE EXTENSION IF NOT EXISTS intarray;

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

CREATE OR REPLACE FUNCTION que_scheduler_parse_cron(text, timestamptz DEFAULT now())
RETURNS timestamptz
LANGUAGE plpgsql
AS $$
  DECLARE
    cron text;
    next_time timestamptz := $2;
    time_diff int[];
    time_specs RECORD;
  BEGIN
    -- Interpret Vixieisms
    cron := replace($1,   '@yearly',  '0 0 1 1 *');
    cron := replace(cron, '@monthly', '0 0 1 * *');
    cron := replace(cron, '@weekly',  '0 0 * * 0');
    cron := replace(cron, '@daily',   '0 0 * * *');
    cron := replace(cron, '@hourly',  '0 * * * *');

    WITH parsed as (
      SELECT
        MAX(CASE WHEN key = 'minute' THEN value END) as minute
      , MAX(CASE WHEN key = 'hour' THEN value END) as hour
      , MAX(CASE WHEN key = 'dom' THEN value END) as dom
      , MAX(CASE WHEN key = 'month' THEN value END) as month
      , MAX(CASE WHEN key = 'dow' THEN value END) as dow
        FROM (
          select key
               , CASE 
                    WHEN value[2] IS NOT NULL THEN
                      CASE
                        WHEN value[1] = '*' THEN
                          CASE key
                            WHEN 'minute' THEN array(select generate_series(0,59))
                            WHEN 'hour' THEN array(select generate_series(0,23))
                            WHEN 'dom' THEN array(select generate_series(1,31))
                            WHEN 'month' THEN array(select generate_series(1,12))
                            WHEN 'dow' THEN array(select generate_series(0,6))
                          END
                        WHEN value[1] ilike '%-%' THEN
                          array(select generate_series(x[1]::int, x[2]::int) FROM regexp_matches(value[1], '([^\-]*)\-([^\-]*)') x)
                        ELSE
                          null
                      END & array[CASE key
                                    WHEN 'minute' THEN array(select x from generate_series(0,59) x where x % value[2]::int = 0)
                                    WHEN 'hour' THEN array(select x from generate_series(0,23) x where x % value[2]::int = 0)
                                    WHEN 'dom' THEN array(select x from generate_series(1,31) x where x % value[2]::int = 0)
                                    WHEN 'month' THEN array(select x from generate_series(1,12) x where x % value[2]::int = 0)
                                    WHEN 'dow' THEN array(select x from generate_series(0,6) x where x % value[2]::int = 0)
                                  END]
                    WHEN value[1] = '*' THEN
                      CASE key
                        WHEN 'minute' THEN array(select generate_series(0,59))
                        WHEN 'hour'   THEN array(select generate_series(0,23))
                        WHEN 'dom'    THEN array(select generate_series(1,31))
                        WHEN 'month'  THEN array(select generate_series(1,12))
                        WHEN 'dow'    THEN array(select generate_series(0,6))
                      END
                    WHEN value[1] ilike '%,%' then
                      array(select x::integer from regexp_split_to_table(value[1], ',') x)
                    WHEN value[1] ilike '%-%' then 
                      array(select generate_series(x[1]::int, x[2]::int) FROM regexp_matches(value[1], '([^\-]*)\-([^\-]*)') x)
                    ELSE array(select generate_series(value[1]::int, value[1]::int))
                 END as value
          from (
            select case row_number() over() 
                     when 1 then 'minute'
                     when 2 then 'hour'
                     when 3 then 'dom'
                     when 4 then 'month'
                     when 5 then 'dow'
                   end as key
                 , regexp_split_to_array(t[1], '/') as value
              from regexp_matches(cron, '[^\s]+', 'g') as t
          ) matches
        ) values
    ), dates as (
      select date_trunc('second', generate_series($2, $2 + interval '1 year', '1 minute'::interval)) date
    )
    select dates.date
    into time_specs
    from dates, parsed
    where parsed.month && array[date_part('month', date)::int] 
      and parsed.hour && array[date_part('hour', date)::int]
      and parsed.minute && array[date_part('minute', date)::int]
      and parsed.dom && array[date_part('day', date)::int]
      and parsed.dow && array[date_part('dow', date)::int]
    order by date asc
    limit 1;

    RETURN time_specs.date;
  END;
$$;

CREATE FUNCTION que_scheduler_insert_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    IF EXISTS (SELECT 1 FROM que_jobs WHERE data->'scheduler'->>'name' = NEW.name) THEN
      UPDATE que_jobs
          SET run_at = now()
            , job_class = NEW.job_class
            , args = NEW.args
        WHERE data->'scheduler'->>'name' = NEW.name;
    ELSE
      IF NEW.enabled THEN
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (''::text, 100, now(), NEW.job_class, NEW.args, ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb);
      END IF;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER que_scheduler_insert_job AFTER INSERT OR UPDATE OF every, enabled ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_insert_job();


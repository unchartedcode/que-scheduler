CREATE EXTENSION IF NOT EXISTS intarray;

CREATE TABLE que_scheduler
(
  name        text        NOT NULL,
  job_class   text        NOT NULL,
  args        json        NOT NULL DEFAULT '[]'::json,
  description text        NOT NULL DEFAULT '',
  expression  text        NOT NULL,
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

    WITH RECURSIVE parsed_line as (
      SELECT CASE row_number() over() 
               WHEN 1 THEN 'minute'
               WHEN 2 THEN 'hour'
               WHEN 3 THEN 'dom'
               WHEN 4 THEN 'month'
               WHEN 5 THEN 'dow'
             END as key
           , t[1] as value
      FROM regexp_matches(cron, '[^\s]+', 'g') as t
    ), parsed_comma(key, value, modifier) as (
      SELECT  key
           ,  r.r
           ,  regexp_split_to_array(r.r, '/')
      FROM parsed_line, regexp_split_to_table(value, ',') r
    ), parsed_values as (
      SELECT key
           , value
           , CASE 
                WHEN value = '*' THEN
                  CASE key
                    WHEN 'minute' THEN ARRAY(SELECT generate_series(0,59))
                    WHEN 'hour'   THEN ARRAY(SELECT generate_series(0,23))
                    WHEN 'dom'    THEN ARRAY(SELECT generate_series(1,31))
                    WHEN 'month'  THEN ARRAY(SELECT generate_series(1,12))
                    WHEN 'dow'    THEN ARRAY(SELECT generate_series(0,6))
                  END
                WHEN value ~ '/' THEN
                  CASE
                    WHEN modifier[1] = '*' THEN
                      CASE key
                        WHEN 'minute' THEN array(select generate_series(0,59))
                        WHEN 'hour'   THEN array(select generate_series(0,23))
                        WHEN 'dom'    THEN array(select generate_series(1,31))
                        WHEN 'month'  THEN array(select generate_series(1,12))
                        WHEN 'dow'    THEN array(select generate_series(0,6))
                      END
                    ELSE
                      ARRAY(
                        SELECT generate_series(y[1]::int, y[2]::int) 
                        FROM regexp_matches(modifier[1], '([^\-]*)\-([^\-]*)') y
                      )
                  END & 
                  ARRAY(
                    SELECT value
                    FROM (
                      SELECT y[1]::int as start, generate_series(y[1]::int, y[2]::int) as value
                      FROM regexp_matches(modifier[1], '([^\-]*)\-([^\-]*)') y
                      WHERE modifier[1] ~ '-'
                      UNION ALL
                      SELECT CASE key
                               WHEN 'minute' THEN 0
                               WHEN 'hour'   THEN 0
                               WHEN 'dom'    THEN 1
                               WHEN 'month'  THEN 1
                               WHEN 'dow'    THEN 0
                             END as start, 
                             generate_series(
                               CASE key
                                 WHEN 'minute' THEN 0
                                 WHEN 'hour'   THEN 0
                                 WHEN 'dom'    THEN 1
                                 WHEN 'month'  THEN 1
                                 WHEN 'dow'    THEN 0
                               END, 
                               CASE key
                                 WHEN 'minute' THEN 59
                                 WHEN 'hour'   THEN 23
                                 WHEN 'dom'    THEN 31
                                 WHEN 'month'  THEN 12
                                 WHEN 'dow'    THEN 6
                               END
                             ) as value
                      WHERE modifier[1] !~ '-'
                    ) value
                    WHERE (value - start) % modifier[2]::int = 0
                  )
                WHEN value ~ '-' THEN
                  ARRAY(
                    SELECT generate_series(y[1]::int, y[2]::int) 
                    FROM regexp_matches(value, '([^\-]*)\-([^\-]*)') y
                  )
                ELSE
                  ARRAY(SELECT generate_series(
                    CASE
                      WHEN key = 'dow' THEN
                        CASE 
                          WHEN value ILIKE 'SUN' THEN '0'
                          WHEN value ILIKE 'MON' THEN '1'
                          WHEN value ILIKE 'TUE' THEN '2'
                          WHEN value ILIKE 'WED' THEN '3'
                          WHEN value ILIKE 'THU' THEN '4'
                          WHEN value ILIKE 'FRI' THEN '5'
                          WHEN value ILIKE 'SAT' THEN '6'
                          ELSE value
                        END
                      WHEN key = 'month' THEN
                        CASE 
                          WHEN value ILIKE 'JAN' THEN '1'
                          WHEN value ILIKE 'FEB' THEN '2'
                          WHEN value ILIKE 'MAR' THEN '3'
                          WHEN value ILIKE 'APR' THEN '4'
                          WHEN value ILIKE 'MAY' THEN '5'
                          WHEN value ILIKE 'JUN' THEN '6'
                          WHEN value ILIKE 'JUL' THEN '7'
                          WHEN value ILIKE 'AUG' THEN '8'
                          WHEN value ILIKE 'SEP' THEN '9'
                          WHEN value ILIKE 'OCT' THEN '10'
                          WHEN value ILIKE 'NOV' THEN '11'
                          WHEN value ILIKE 'DEC' THEN '12'
                          ELSE value
                        END
                      ELSE value
                    END::int
                    ,
                    CASE
                      WHEN key = 'dow' THEN
                        CASE 
                          WHEN value ILIKE 'SUN' THEN '0'
                          WHEN value ILIKE 'MON' THEN '1'
                          WHEN value ILIKE 'TUE' THEN '2'
                          WHEN value ILIKE 'WED' THEN '3'
                          WHEN value ILIKE 'THU' THEN '4'
                          WHEN value ILIKE 'FRI' THEN '5'
                          WHEN value ILIKE 'SAT' THEN '6'
                          ELSE value
                        END
                      WHEN key = 'month' THEN
                        CASE 
                          WHEN value ILIKE 'JAN' THEN '1'
                          WHEN value ILIKE 'FEB' THEN '2'
                          WHEN value ILIKE 'MAR' THEN '3'
                          WHEN value ILIKE 'APR' THEN '4'
                          WHEN value ILIKE 'MAY' THEN '5'
                          WHEN value ILIKE 'JUN' THEN '6'
                          WHEN value ILIKE 'JUL' THEN '7'
                          WHEN value ILIKE 'AUG' THEN '8'
                          WHEN value ILIKE 'SEP' THEN '9'
                          WHEN value ILIKE 'OCT' THEN '10'
                          WHEN value ILIKE 'NOV' THEN '11'
                          WHEN value ILIKE 'DEC' THEN '12'
                          ELSE value
                        END
                      ELSE value
                    END::int)
                  )
              END as includes
      FROM parsed_comma
    ), dates(date,n) as (
      SELECT date_trunc('minute', $2 + interval '1 minute') date, 1 as n
      UNION ALL
      SELECT date_trunc('minute', $2 + interval '1 minute' * (n+1)) date, n+1 as n
      FROM dates
      -- To avoid an infinite loop (pretty high number of iterations here)
      -- This should effectively allow for looking ahead a full year. 
      WHERE n < 525600 -- (60*24*365)
    )
    SELECT dates.date
    INTO time_specs
    FROM dates
    WHERE EXISTS (
      SELECT 1
      FROM parsed_values
      WHERE parsed_values.key = 'month' and (parsed_values.includes && array[date_part('month', date)::int])
    ) AND EXISTS (
      SELECT 1
      FROM parsed_values
      WHERE parsed_values.key = 'hour' and (parsed_values.includes && array[date_part('hour', date)::int])
    ) AND EXISTS (
      SELECT 1
      FROM parsed_values
      WHERE parsed_values.key = 'minute' and (parsed_values.includes && array[date_part('minute', date)::int])
    ) AND EXISTS (
      SELECT 1
      FROM parsed_values
      WHERE parsed_values.key = 'dom' and (parsed_values.includes && array[date_part('day', date)::int])
    ) AND EXISTS (
      SELECT 1
      FROM parsed_values
      WHERE parsed_values.key = 'dow' and (parsed_values.includes && array[date_part('dow', date)::int])
    )
    LIMIT 1;

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
          SET run_at = CASE WHEN OLD.expression != NEW.expression
                            THEN que_scheduler_parse_cron(NEW.expression, now())
                            ELSE run_at
                       END
            , job_class = NEW.job_class
            , args = NEW.args
        WHERE data->'scheduler'->>'name' = NEW.name;
    ELSE
      IF NEW.enabled THEN
        INSERT INTO que_jobs (queue, priority, run_at, job_class, args, data)
        VALUES (''::text, 100, que_scheduler_parse_cron(NEW.expression, now() - interval '1 minute'), NEW.job_class, NEW.args, ('{"scheduler": {"name":"' || NEW.name || '"}}')::jsonb);
      END IF;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER que_scheduler_insert_job AFTER INSERT OR UPDATE OF expression, enabled ON que_scheduler
  FOR EACH ROW EXECUTE PROCEDURE que_scheduler_insert_job();


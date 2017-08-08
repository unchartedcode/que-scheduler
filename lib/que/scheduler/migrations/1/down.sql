-- Remove the Triggers
DROP TRIGGER IF EXISTS que_scheduler_insert_job ON que_scheduler;
DROP TRIGGER IF EXISTS que_scheduler_update_job ON que_scheduler;
DROP TRIGGER IF EXISTS que_jobs_reschedule_job ON que_jobs;

-- Remove the Functions
DROP FUNCTION IF EXISTS que_scheduler_insert_job();
DROP FUNCTION IF EXISTS que_scheduler_update_job();
DROP FUNCTION IF EXISTS que_jobs_reschedule_job();
DROP FUNCTION IF EXISTS que_scheduler_parse_cron(text);

-- Drop our Table
DROP TABLE que_scheduler;

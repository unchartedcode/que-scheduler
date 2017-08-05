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

-- Durable schedules: cron or fixed-interval triggers that create workflow instances.
--
-- A schedule is pure data. The scheduler runtime leases a due row via
-- locked_until (SELECT ... FOR UPDATE SKIP LOCKED), creates a workflow
-- instance, then advances next_run_at. If a process crashes mid-fire the lease
-- expires and another process retries; deterministic instance IDs make the retry
-- idempotent.
CREATE TABLE schedules (
  id varchar(200) NOT NULL PRIMARY KEY,
  workflow_name varchar(128) NOT NULL,
  queue varchar(128) NOT NULL DEFAULT '',
  inputs bytea NULL,                    -- JSON-encoded []payload.Payload
  cron_expression varchar(255) NULL,
  interval_ns bigint NULL,              -- fixed delay in nanoseconds, when cron_expression is NULL
  timezone varchar(64) NOT NULL DEFAULT 'UTC',
  start_at timestamptz NULL,
  end_at timestamptz NULL,
  enabled boolean NOT NULL DEFAULT true,
  overlap_policy int NOT NULL DEFAULT 0,
  catchup_policy int NOT NULL DEFAULT 0,
  next_run_at timestamptz NOT NULL,
  last_run_at timestamptz NULL,
  last_instance_id varchar(256) NULL,
  last_execution_id varchar(128) NULL,
  num_fired bigint NOT NULL DEFAULT 0,
  locked_until timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_schedules_due ON schedules (next_run_at) WHERE enabled;

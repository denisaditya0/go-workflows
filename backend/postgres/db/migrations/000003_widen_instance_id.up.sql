-- Widen the user-controlled instance_id columns from varchar(128) to varchar(256)
-- so composite dedup-derived instance IDs don't overflow. UUID columns
-- (execution_id, event_id, activity_id) are unchanged. Widening a varchar is a
-- metadata-only change in Postgres (no table rewrite).
ALTER TABLE instances      ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE instances      ALTER COLUMN parent_instance_id TYPE varchar(256);
ALTER TABLE pending_events ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE history        ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE activities     ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE attributes     ALTER COLUMN instance_id        TYPE varchar(256);

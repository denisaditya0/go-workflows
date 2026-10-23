-- Revert instance_id columns to varchar(128). Fails if any existing value exceeds
-- 128 characters.
ALTER TABLE attributes     ALTER COLUMN instance_id        TYPE varchar(128);
ALTER TABLE activities     ALTER COLUMN instance_id        TYPE varchar(128);
ALTER TABLE history        ALTER COLUMN instance_id        TYPE varchar(128);
ALTER TABLE pending_events ALTER COLUMN instance_id        TYPE varchar(128);
ALTER TABLE instances      ALTER COLUMN parent_instance_id TYPE varchar(128);
ALTER TABLE instances      ALTER COLUMN instance_id        TYPE varchar(128);

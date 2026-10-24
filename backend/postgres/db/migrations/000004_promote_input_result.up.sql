-- Phase 1: denormalize a workflow's initial input and final result/error onto the
-- instances row for cheap reads (no full-history decode). These duplicate data that
-- also lives in the history/attributes blobs; the engine still replays from history.
-- bytea to match the existing opaque payload storage; nullable (result/error only set
-- on terminal completion).
ALTER TABLE instances ADD COLUMN workflow_name varchar(128) NULL;
ALTER TABLE instances ADD COLUMN input bytea NULL;
ALTER TABLE instances ADD COLUMN result bytea NULL;
ALTER TABLE instances ADD COLUMN error bytea NULL;

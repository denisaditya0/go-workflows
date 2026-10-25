-- Backward lineage pointer for ContinueAsNew: the execution_id of the previous
-- execution of the SAME instance (NULL for a first/original run). Distinct from
-- parent_* (which is the sub-workflow relationship to a different instance). Lets a
-- continued execution's predecessor be read from the instances row without walking
-- forward links, and survives even if cleaning deletes the prior execution row.
ALTER TABLE instances ADD COLUMN previous_execution_id varchar(128) NULL;

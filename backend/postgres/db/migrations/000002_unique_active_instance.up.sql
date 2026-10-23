-- Enforce at most one ACTIVE instance per instance_id, atomically.
-- state = 0 is core.WorkflowInstanceStateActive. Completed/ContinuedAsNew/Finished
-- rows are exempt, so an instance_id can be reused once its previous run is no
-- longer active. This closes the check-then-insert race in createInstance where
-- two concurrent CreateWorkflowInstance calls could both create an active row.
CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_active_instance_id
  ON instances (instance_id)
  WHERE state = 0;

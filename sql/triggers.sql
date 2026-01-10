-- PostgreSQL Triggers for BANZAI Calibration Cache
-- This file contains only the filepath preservation trigger needed
-- to prevent replication from overwriting local file paths.
--
-- Installation:
--   psql -U banzai -d banzai_local -f sql/triggers.sql

-- =============================================================================
-- TRIGGER: Preserve Local Filepath
-- =============================================================================
-- Purpose: Prevent PostgreSQL replication from overwriting local filepath updates
-- Fires: BEFORE UPDATE on calimages
-- Logic:
--   - If OLD.filepath exists (local file downloaded) and NEW.filepath is NULL
--   - Keep the OLD.filepath value
--   - This prevents replication from AWS (where filepath=NULL) from clearing
--     the local filepath that was set by the download worker

CREATE OR REPLACE FUNCTION preserve_local_filepath()
RETURNS TRIGGER AS $$
BEGIN
    -- If local filepath exists, don't let replication overwrite it
    IF OLD.filepath IS NOT NULL AND NEW.filepath IS NULL THEN
        NEW.filepath := OLD.filepath;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to calimages table
DROP TRIGGER IF EXISTS trigger_preserve_filepath ON calimages;
CREATE TRIGGER trigger_preserve_filepath
    BEFORE UPDATE ON calimages
    FOR EACH ROW
    EXECUTE FUNCTION preserve_local_filepath();

-- Enable trigger for logical replication
ALTER TABLE calimages ENABLE ALWAYS TRIGGER trigger_preserve_filepath;


-- =============================================================================
-- Verification Query
-- =============================================================================
-- Use this query to verify trigger is installed:
-- SELECT trigger_name, event_manipulation, action_timing
-- FROM information_schema.triggers
-- WHERE event_object_table = 'calimages';

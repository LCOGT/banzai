-- PostgreSQL Logical Replication Calibration Cache Triggers
-- This file contains trigger functions for the BANZAI calibration cache system
--
-- Installation:
--   psql -U banzai -d banzai_local -f sql/triggers.sql
--
-- Description:
--   These triggers work together to implement an event-driven calibration cache:
--   1. queue_calibration_download: Queues calibrations for download based on filters
--   2. cleanup_old_calibration_versions: Maintains only 2 most recent versions
--   3. preserve_local_filepath: Prevents replication from overwriting local paths

-- =============================================================================
-- TRIGGER 1: Queue Calibration Download
-- =============================================================================
-- Purpose: Automatically queue calibrations for download when they match site filters
-- Fires: AFTER INSERT OR UPDATE on calimages
-- Logic:
--   - Only processes master calibrations (is_master=true, is_bad=false)
--   - Reads cache_config for site_id and instrument_types filters
--   - Checks if calibration's instrument matches filters
--   - Inserts into pending_downloads if all criteria pass

CREATE OR REPLACE FUNCTION queue_calibration_download()
RETURNS TRIGGER AS $$
DECLARE
    instrument_record RECORD;
    configured_site TEXT;
    configured_instrument_types TEXT[];
BEGIN
    -- Only process master calibrations
    IF NEW.is_master != true OR NEW.is_bad = true THEN
        RETURN NEW;
    END IF;

    -- Get filter configuration
    SELECT site_id, instrument_types
    INTO configured_site, configured_instrument_types
    FROM cache_config LIMIT 1;

    -- If no config exists, don't queue anything
    IF configured_site IS NULL THEN
        RETURN NEW;
    END IF;

    -- Get instrument details
    SELECT * INTO instrument_record
    FROM instruments
    WHERE id = NEW.instrument_id;

    -- Filter by site
    IF instrument_record.site != configured_site THEN
        RETURN NEW;  -- Wrong site, don't queue
    END IF;

    -- Filter by instrument type
    IF configured_instrument_types[1] != '*' AND
       NOT (instrument_record.type = ANY(configured_instrument_types)) THEN
        RETURN NEW;  -- Wrong instrument type, don't queue
    END IF;

    -- All filters passed - check if already cached or pending
    IF NOT EXISTS (
        SELECT 1 FROM calimages
        WHERE id = NEW.id AND filepath IS NOT NULL
    ) AND NOT EXISTS (
        SELECT 1 FROM pending_downloads
        WHERE calimage_id = NEW.id
        AND status IN ('pending', 'downloading')
    ) THEN
        -- Queue for download
        INSERT INTO pending_downloads (calimage_id, status)
        VALUES (NEW.id, 'pending');

        RAISE NOTICE 'Queued calibration: % (type: %, instrument: %, site: %)',
            NEW.filename, NEW.type, instrument_record.type, instrument_record.site;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to calimages table
DROP TRIGGER IF EXISTS trigger_queue_calibration_download ON calimages;
CREATE TRIGGER trigger_queue_calibration_download
    AFTER INSERT OR UPDATE ON calimages
    FOR EACH ROW
    EXECUTE FUNCTION queue_calibration_download();


-- =============================================================================
-- TRIGGER 2: Cleanup Old Calibration Versions
-- =============================================================================
-- Purpose: Maintain only 2 most recent versions of each calibration configuration
-- Fires: AFTER INSERT OR UPDATE on calimages
-- Logic:
--   - Only processes master calibrations that match site filters
--   - Groups calibrations by: instrument, type, config_mode, binning, filter
--   - Ranks by dateobs (most recent = 1)
--   - Sets filepath=NULL for versions beyond 2nd most recent
--   - Download worker will delete files with NULL filepath

CREATE OR REPLACE FUNCTION cleanup_old_calibration_versions()
RETURNS TRIGGER AS $$
DECLARE
    old_cal_ids INTEGER[];
    configured_site TEXT;
    configured_instrument_types TEXT[];
    instrument_record RECORD;
BEGIN
    -- Only cleanup master calibrations
    IF NEW.is_master != true THEN
        RETURN NEW;
    END IF;

    -- Get configuration
    SELECT site_id, instrument_types
    INTO configured_site, configured_instrument_types
    FROM cache_config LIMIT 1;

    -- If no config exists, don't cleanup
    IF configured_site IS NULL THEN
        RETURN NEW;
    END IF;

    -- Get instrument details
    SELECT * INTO instrument_record
    FROM instruments WHERE id = NEW.instrument_id;

    -- Only cleanup if this matches our filter criteria
    IF instrument_record.site != configured_site OR
       (configured_instrument_types[1] != '*' AND
        NOT (instrument_record.type = ANY(configured_instrument_types))) THEN
        RETURN NEW;  -- Not in our cache scope, ignore
    END IF;

    -- Find calibrations older than 2nd most recent
    WITH ranked_cals AS (
        SELECT
            c.id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    c.instrument_id,
                    c.type,
                    c.attributes->>'configuration_mode',
                    c.attributes->>'binning',
                    CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                        THEN c.attributes->>'filter'
                        ELSE NULL
                    END
                ORDER BY c.dateobs DESC
            ) as version_rank
        FROM calimages c
        JOIN instruments i ON c.instrument_id = i.id
        WHERE
            c.instrument_id = NEW.instrument_id
            AND c.type = NEW.type
            AND c.attributes->>'configuration_mode' = NEW.attributes->>'configuration_mode'
            AND c.attributes->>'binning' = NEW.attributes->>'binning'
            AND (
                NEW.type NOT IN ('SKYFLAT', 'FLAT')
                OR c.attributes->>'filter' = NEW.attributes->>'filter'
            )
            AND c.is_master = true
            AND i.site = configured_site
            AND (configured_instrument_types[1] = '*' OR
                 i.type = ANY(configured_instrument_types))
    )
    SELECT ARRAY_AGG(id) INTO old_cal_ids
    FROM ranked_cals
    WHERE version_rank > 2;  -- Keep 2 versions

    -- Mark for deletion (reset filepath to NULL)
    IF old_cal_ids IS NOT NULL THEN
        UPDATE calimages
        SET filepath = NULL
        WHERE id = ANY(old_cal_ids)
        AND filepath IS NOT NULL;

        RAISE NOTICE 'Marked % calibrations for cleanup',
            array_length(old_cal_ids, 1);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to calimages table
DROP TRIGGER IF EXISTS trigger_cleanup_old_versions ON calimages;
CREATE TRIGGER trigger_cleanup_old_versions
    AFTER INSERT OR UPDATE ON calimages
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_old_calibration_versions();


-- =============================================================================
-- TRIGGER 3: Preserve Local Filepath
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


-- =============================================================================
-- Verification Queries
-- =============================================================================
-- Use these queries to verify triggers are installed and working

-- Check that all trigger functions exist
-- SELECT routine_name
-- FROM information_schema.routines
-- WHERE routine_schema = 'public'
-- AND routine_type = 'FUNCTION'
-- AND routine_name IN (
--     'queue_calibration_download',
--     'cleanup_old_calibration_versions',
--     'preserve_local_filepath'
-- );

-- Check that all triggers are attached to calimages
-- SELECT trigger_name, event_manipulation, action_timing
-- FROM information_schema.triggers
-- WHERE event_object_table = 'calimages'
-- ORDER BY trigger_name;

-- Monitor trigger activity (check NOTICE messages in PostgreSQL logs)
-- SET client_min_messages = NOTICE;

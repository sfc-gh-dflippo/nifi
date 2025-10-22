-- Initialize PostgreSQL database with pg_parquet extension
-- This script is automatically executed by the PostgreSQL Docker entrypoint
-- when the container starts for the first time

-- Create pg_parquet extension
DO $$
BEGIN
    -- Try to create the pg_parquet extension
    CREATE EXTENSION IF NOT EXISTS pg_parquet;
    RAISE NOTICE 'pg_parquet extension created successfully';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Failed to create pg_parquet extension: %', SQLERRM;
        -- Don't fail the container startup if extension creation fails
        -- This allows tests to run even if pg_parquet isn't available
END
$$;

-- Verify the extension is loaded and log the result
DO $$
DECLARE
    extension_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet'
    ) INTO extension_exists;
    
    IF extension_exists THEN
        RAISE NOTICE 'pg_parquet extension is available and ready for use';
    ELSE
        RAISE WARNING 'pg_parquet extension is NOT available';
    END IF;
END
$$;

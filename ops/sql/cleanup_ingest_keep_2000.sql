-- Drop old Spark batch staging tables, keeping the 2000 most recent.
-- Staging tables accumulate (one per Spark micro-batch) and are NOT dropped
-- by the streaming job itself to preserve crash-recovery durability.
-- 2000 is about 8 days of batches at 6-min intervals - enough for debugging.

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM (
            SELECT
                schemaname,
                tablename,
                row_number() OVER (
                    ORDER BY tablename DESC
                ) AS rn
            FROM pg_tables
            WHERE schemaname = 'ingest'
              AND tablename LIKE 'raw_prices_ingest_%'
        ) t
        WHERE rn > 2000
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.schemaname, r.tablename);
    END LOOP;
END $$;

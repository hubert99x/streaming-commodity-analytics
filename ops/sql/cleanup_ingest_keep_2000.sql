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
                    ORDER BY
                        substring(tablename from 'raw_prices_ingest_stream1_([0-9]+)$')::bigint DESC
                ) AS rn
            FROM pg_tables
            WHERE schemaname = 'ingest'
              AND tablename LIKE 'raw_prices_ingest_stream1_%'
        ) t
        WHERE rn > 2000
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.schemaname, r.tablename);
    END LOOP;
END $$;

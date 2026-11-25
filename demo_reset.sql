-- Reset the demo to "empty table, watching one small folder".
SELECT ingest.stop();
TRUNCATE ingest.files, ingest.green_trips;
UPDATE ingest.watches SET prefix = 'green/puYear=2019/puMonth=8/', last_error = NULL, last_error_at = NULL;

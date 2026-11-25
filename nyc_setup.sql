-- Real-data watch: NYC green-taxi Parquet files in the public azureopendatastorage/nyctlc container.
SET search_path = ingest, public;
CREATE TYPE green_rec AS (
    vendor_id int, pickup timestamp, dropoff timestamp, passenger_count int,
    trip_distance float8, pu_location_id text, do_location_id text,
    pickup_lon float8, pickup_lat float8, dropoff_lon float8, dropoff_lat float8,
    rate_code_id int, store_and_fwd_flag text, payment_type int, fare_amount float8,
    extra float8, mta_tax float8, improvement_surcharge text, tip_amount float8,
    tolls_amount float8, ehail_fee float8, total_amount float8, trip_type int
);
CREATE TABLE green_trips (LIKE green_rec, source text NOT NULL, loaded_at timestamptz NOT NULL);
CREATE INDEX ON green_trips (source);
INSERT INTO watches (name, account, container, prefix, suffix, target, rectype, decoder)
VALUES ('green', 'azureopendatastorage', 'nyctlc', 'green/puYear=2019/puMonth=8/', '.parquet',
        'green_trips', 'green_rec', 'parquet');
-- Row counts read from the Parquet footers, to check exactly-once after a crash.
CREATE TABLE expected (month int PRIMARY KEY, rows bigint);
INSERT INTO expected VALUES (1,630889),(2,575678),(3,601071),(4,514392),(5,504898),(6,471023),(7,15),(8,1);
CREATE VIEW check_counts AS
SELECT e.month, e.rows AS expected, coalesce(a.rows, 0) AS loaded, e.rows = coalesce(a.rows, 0) AS ok
FROM expected e
LEFT JOIN (SELECT substring(source FROM 'puMonth=(\d+)')::int AS month, count(*) AS rows
           FROM green_trips GROUP BY 1) a USING (month)
ORDER BY 1;

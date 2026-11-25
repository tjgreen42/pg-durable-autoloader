-- Scale lab target: tiny CSV event files under events/{yyyy}/{mm}/{dd}/ in mock account mocklanding.
SET search_path = lab, public;
CREATE TYPE event_rec AS (id bigint, ts timestamptz, device text, value float8);
CREATE TABLE events (LIKE event_rec, source text NOT NULL, loaded_at timestamptz NOT NULL);
CREATE INDEX ON events (source);
INSERT INTO watches (name, account, container, prefix, lookback, suffix, target, rectype, decoder, options)
VALUES ('events', 'mocklanding', 'landing', 'events/{yyyy}/{mm}/{dd}/', '1 day', '.csv',
        'events', 'event_rec', 'csv', azure_storage.options_csv_get(header => true));

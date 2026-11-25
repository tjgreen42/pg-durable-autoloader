-- Autoloader: keep Postgres tables in sync with files landing in Azure Blob Storage.
-- azure_storage reads and decodes blobs; pg_durable decides when, retries, and survives restarts.
--
-- Install into a schema owned by the calling (non-superuser) role:
--   psql -d pg_durable -U durable_dev -v schema=ingest -f autoloader.sql
\set ON_ERROR_STOP on
DROP SCHEMA IF EXISTS :"schema" CASCADE;
CREATE SCHEMA :"schema";
SET search_path = :"schema", public;

CREATE TABLE settings (key text PRIMARY KEY, value text NOT NULL);
INSERT INTO settings VALUES
    ('max_inflight', '50'),   -- per_file mode: cap on concurrently loading files
    ('max_attempts', '3'),
    ('retry_base',   '5 seconds');

CREATE FUNCTION setting(p_key text) RETURNS text LANGUAGE sql STABLE
SET search_path FROM CURRENT AS $$ SELECT value FROM settings WHERE key = p_key $$;

-- A watch maps a (possibly date-templated) prefix to a target table.
-- Templates: {yyyy} {mm} {dd} {hh}, expanded over [now() - lookback, now()] in UTC.
CREATE TABLE watches (
    name      text PRIMARY KEY,
    account   text NOT NULL,
    container text NOT NULL,
    prefix    text NOT NULL,
    lookback  interval NOT NULL DEFAULT '1 day',
    suffix    text NOT NULL DEFAULT '',
    target    regclass NOT NULL,  -- columns of rectype, then source text, loaded_at timestamptz
    rectype   regtype NOT NULL,
    decoder   text NOT NULL DEFAULT 'auto',
    options   jsonb,
    enabled   boolean NOT NULL DEFAULT true,
    last_listed_at timestamptz,
    last_found     int,
    last_error     text,
    last_error_at  timestamptz
);

-- Manifest: one row per blob version (path + ETag).
CREATE TABLE files (
    watch           text NOT NULL REFERENCES watches ON DELETE CASCADE,
    path            text NOT NULL,
    etag            text NOT NULL,
    bytes           bigint,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'loading', 'loaded', 'failed', 'superseded')),
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    instance_id     text,
    rows_loaded     bigint,
    discovered_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    loaded_at       timestamptz,
    PRIMARY KEY (watch, path, etag)
);
CREATE INDEX files_pending ON files (discovered_at) WHERE status = 'pending';
CREATE INDEX files_loading ON files (instance_id) WHERE status = 'loading';

CREATE FUNCTION hot_prefixes(p_prefix text, p_lookback interval, p_full boolean DEFAULT false)
RETURNS SETOF text LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN strpos(p_prefix, '{') = 0 THEN p_prefix
                ELSE left(p_prefix, strpos(p_prefix, '{') - 1) END
    WHERE p_full OR strpos(p_prefix, '{') = 0
    UNION
    SELECT replace(replace(replace(replace(p_prefix,
               '{yyyy}', to_char(t, 'YYYY')), '{mm}', to_char(t, 'MM')),
               '{dd}', to_char(t, 'DD')), '{hh}', to_char(t, 'HH24'))
    FROM generate_series((now() AT TIME ZONE 'UTC') - p_lookback, now() AT TIME ZONE 'UTC',
                         CASE WHEN p_prefix LIKE '%{hh}%' THEN interval '1 hour'
                              ELSE interval '1 day' END) t
    WHERE NOT p_full AND strpos(p_prefix, '{') > 0
$$;

-- Load one blob version. Delete-then-insert by source plus the manifest update commit
-- together, so retries and replays are harmless. Versions of one path are serialized,
-- and a version older than the newest discovered one is skipped.
CREATE FUNCTION load_file(p_watch text, p_path text, p_etag text) RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    w watches;
    n bigint;
    src text := p_watch || ':' || p_path;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(src, 0));
    SELECT * INTO STRICT w FROM watches WHERE name = p_watch;
    IF EXISTS (SELECT 1 FROM files f, files me
               WHERE (me.watch, me.path, me.etag) = (p_watch, p_path, p_etag)
                 AND f.watch = p_watch AND f.path = p_path
                 AND f.discovered_at > me.discovered_at) THEN
        UPDATE files SET status = 'superseded'
        WHERE (watch, path, etag) = (p_watch, p_path, p_etag);
        RETURN 0;
    END IF;
    EXECUTE format('DELETE FROM %s WHERE source = $1', w.target) USING src;
    EXECUTE format('INSERT INTO %s SELECT r.*, $4, now()
                    FROM azure_storage.blob_get($1, $2, $3, NULL::%s,
                         decoder => $5, options => $6) r', w.target, w.rectype)
        USING w.account, w.container, p_path, src, w.decoder, w.options;
    GET DIAGNOSTICS n = ROW_COUNT;
    UPDATE files SET status = 'loaded', rows_loaded = n, loaded_at = now(),
                     attempts = attempts + 1, last_error = NULL
    WHERE (watch, path, etag) = (p_watch, p_path, p_etag);
    RETURN n;
END $$;

CREATE FUNCTION record_failure(p_watch text, p_path text, p_etag text, p_error text)
RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
    UPDATE files
    SET attempts = attempts + 1,
        last_error = p_error,
        status = CASE WHEN attempts + 1 >= setting('max_attempts')::int THEN 'failed' ELSE 'pending' END,
        next_attempt_at = now() + setting('retry_base')::interval * power(2, attempts)
    WHERE (watch, path, etag) = (p_watch, p_path, p_etag)
$$;

-- List hot prefixes and record unseen blob versions. Returns the number of new files.
-- Each watch is isolated: a failing account or container is recorded and skipped.
CREATE FUNCTION discover(p_full boolean DEFAULT false) RETURNS int
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE w watches; n int; total int := 0;
BEGIN
    FOR w IN SELECT * FROM watches
             WHERE enabled
               AND (last_error_at IS NULL OR last_error_at < now() - interval '5 minutes')
    LOOP
        BEGIN
            INSERT INTO files (watch, path, etag, bytes)
            SELECT w.name, b.path, b.etag, b.bytes
            FROM hot_prefixes(w.prefix, w.lookback, p_full) p(prefix)
            CROSS JOIN LATERAL azure_storage.blob_list(w.account, w.container, p.prefix) b
            WHERE right(b.path, length(w.suffix)) = w.suffix
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS n = ROW_COUNT;
            UPDATE watches SET last_listed_at = now(), last_found = n, last_error = NULL
            WHERE name = w.name;
            total := total + n;
        EXCEPTION WHEN OTHERS THEN
            UPDATE watches SET last_error = SQLERRM, last_error_at = now() WHERE name = w.name;
        END;
    END LOOP;
    RETURN total;
END $$;

-- per_file mode: one durable instance per file, at most max_inflight at a time.
-- Starting instances and marking files 'loading' commit together.
CREATE FUNCTION dispatch() RETURNS int LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE r record; n int := 0;
BEGIN
    -- Fold finished-but-failed instances back into the manifest.
    FOR r IN SELECT f.watch, f.path, f.etag, i.status
             FROM files f JOIN df.instances i ON i.id = f.instance_id
             WHERE f.status = 'loading' AND i.status IN ('failed', 'cancelled')
    LOOP
        PERFORM record_failure(r.watch, r.path, r.etag, 'instance ' || r.status);
    END LOOP;

    FOR r IN SELECT watch, path, etag FROM files
             WHERE status = 'pending' AND next_attempt_at <= now()
             ORDER BY discovered_at
             LIMIT greatest(0, setting('max_inflight')::int
                               - (SELECT count(*) FROM files WHERE status = 'loading'))
             FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            UPDATE files
            SET status = 'loading',
                instance_id = df.start(
                    format('SELECT %I.load_file(%L, %L, %L) AS rows',
                           current_schema(), r.watch, r.path, r.etag),
                    format('autoloader:%s:file:%s:%s', current_schema(), r.watch, r.path))
            WHERE (watch, path, etag) = (r.watch, r.path, r.etag);
        EXCEPTION WHEN SQLSTATE '53400' THEN  -- pg_durable instance quota: retry next poll
            EXIT;
        END;
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

-- pool mode: claim due files one at a time (so large files spread across workers) and load
-- each in its own subtransaction; stop after p_batch files or p_budget.
CREATE FUNCTION work(p_batch int DEFAULT 500, p_budget interval DEFAULT '10 seconds') RETURNS int
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE r record; n int := 0; t0 timestamptz := clock_timestamp();
BEGIN
    LOOP
        SELECT watch, path, etag INTO r FROM files
        WHERE status = 'pending' AND next_attempt_at <= now()
        ORDER BY discovered_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        EXIT WHEN NOT FOUND;
        BEGIN
            PERFORM load_file(r.watch, r.path, r.etag);
        EXCEPTION WHEN OTHERS THEN
            PERFORM record_failure(r.watch, r.path, r.etag, SQLERRM);
        END;
        n := n + 1;
        EXIT WHEN n >= p_batch OR clock_timestamp() - t0 > p_budget;
    END LOOP;
    RETURN n;
END $$;

-- Re-queue failed files, e.g. after fixing the blob or the target table.
CREATE FUNCTION retry(p_watch text DEFAULT NULL) RETURNS int
LANGUAGE sql SET search_path FROM CURRENT AS $$
    WITH r AS (
        UPDATE files SET status = 'pending', attempts = 0, last_error = NULL, next_attempt_at = now()
        WHERE status = 'failed' AND (p_watch IS NULL OR watch = p_watch)
        RETURNING 1)
    SELECT count(*)::int FROM r
$$;

-- Start the durable loops. Labels are 'autoloader:<schema>:...'. pg_durable runs at most two
-- activities at once, so extra workers only overlap orchestration overhead (4 measured best).
CREATE FUNCTION start(mode text DEFAULT 'pool', workers int DEFAULT 4,
                      poll_seconds int DEFAULT 5, batch int DEFAULT 500)
RETURNS SETOF text LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s text := current_schema(); i int;
BEGIN
    IF mode NOT IN ('pool', 'per_file') THEN
        RAISE EXCEPTION 'mode must be pool or per_file';
    END IF;
    RETURN NEXT df.start(
        df.loop(CASE WHEN mode = 'per_file'
                     THEN format('SELECT %I.discover() AS found', s)
                          ~> format('SELECT %I.dispatch() AS launched', s)
                     ELSE format('SELECT %I.discover() AS found', s) END
                ~> df.sleep(poll_seconds),
                continue_on_failure => true),
        format('autoloader:%s:discover', s));
    RETURN NEXT df.start(
        df.loop(df.wait_for_schedule('0 3 * * *')
                ~> format('SELECT %I.discover(true) AS found', s),
                continue_on_failure => true),
        format('autoloader:%s:backstop', s));
    IF mode = 'pool' THEN
        FOR i IN 1..workers LOOP
            RETURN NEXT df.start(
                df.loop(format('SELECT %I.work(%s) AS n', s, batch) |=> 'w'
                        ~> df.if('SELECT $w.n = 0', df.sleep(1), 'SELECT 1'),
                        continue_on_failure => true),
                format('autoloader:%s:worker-%s', s, i));
        END LOOP;
    END IF;
END $$;

CREATE FUNCTION stop() RETURNS int LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE r record; n int := 0;
BEGIN
    FOR r IN SELECT id FROM df.instances
             WHERE label LIKE 'autoloader:' || current_schema() || ':%'
               AND status IN ('pending', 'running')
    LOOP
        PERFORM df.cancel(r.id);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

CREATE VIEW status AS
SELECT w.name AS watch,
       count(f.*) FILTER (WHERE f.status = 'pending')    AS pending,
       count(f.*) FILTER (WHERE f.status = 'loading')    AS loading,
       count(f.*) FILTER (WHERE f.status = 'loaded')     AS loaded,
       count(f.*) FILTER (WHERE f.status = 'failed')     AS failed,
       count(f.*) FILTER (WHERE f.status = 'superseded') AS superseded,
       coalesce(sum(f.rows_loaded) FILTER (WHERE f.status = 'loaded'), 0) AS rows_loaded,
       max(f.loaded_at)                                  AS last_loaded_at,
       w.last_listed_at,
       w.last_error
FROM watches w LEFT JOIN files f ON f.watch = w.name
GROUP BY w.name;

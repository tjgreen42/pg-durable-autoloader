# Autoloader

Keep Postgres tables in sync with files landing in Azure Blob Storage: exactly once, across crashes, with nothing running outside the database

A field report on building a real applet with [pg_durable](https://github.com/microsoft/pg_durable) and [azure_storage](https://learn.microsoft.com/azure/postgresql/extensions/concepts-storage-extension), including what broke along the way

- [Why](#why)
- [Without and With pg_durable](#without-and-with-pg_durable)
- [Architecture](#architecture)
- [The pg_durable Code](#the-pg_durable-code)
- [Results](#results)
- [Lessons Learned](#lessons-learned)
- [Issues to Fix](#issues-to-fix)
- [Try It](#try-it)

## Why

A lot of data arrives as files: exports, partner drops, IoT batches, and data-lake partitions. Loading it into Postgres continuously usually means a pipeline outside the database plus glue to make it exactly once. That pipeline might be Data Factory, Databricks Auto Loader, an Azure Function, or a cron box.

Most of that pipeline is state the database could hold: which files exist, which are loaded, what failed, and when to retry. azure_storage already gives Postgres the data path with `blob_list` and `blob_get`. What's missing is something that runs continuously, retries, and survives restarts. pg_durable provides that.

Ingestion is also a good stress test for a durable task framework. It is long-running, I/O-bound, and failure-prone. It also scales two ways: number of files and size of files.

## Without and With pg_durable

Without pg_durable, the loader lives outside the database:

```text
timer (Function, cron, k8s) ──► worker process ──► Blob SDK ──► bytes ──► Postgres
                                 secrets, deploys, retries, restarts, logs, scaling
```

With pg_durable, it's a schema:

```sh
psql -v schema=ingest -f autoloader.sql
```

```sql
INSERT INTO ingest.watches (name, account, container, prefix, suffix, target, rectype, decoder)
VALUES ('green', 'azureopendatastorage', 'nyctlc', 'green/puYear=2019/', '.parquet',
        'green_trips', 'green_rec', 'parquet');

SELECT ingest.start();
```

Concern | Without | With
--- | --- | ---
Where it runs | A service you build, deploy, and keep alive | pg_durable background workers inside Postgres
Restarts | A process supervisor, and you recover state yourself | Loops resume on their own (tested with immediate-mode restarts)
Scheduling | cron or a timer trigger | `df.sleep(5)` and `df.wait_for_schedule('0 3 * * *')`
Parallelism | Threads or processes, plus a connection pool | N `df.loop`s claiming work with `SKIP LOCKED`
Data path | Blob → worker → network → Postgres | Blob → Postgres (`INSERT ... SELECT FROM blob_get(...)`)
Credentials | In the worker and the database | In the database only
Observability | Logs in another system | SQL: `TABLE ingest.status` and `df.instances`
Privileges | A service identity with write access | Runs as the owning role; no superuser

Two caveats:

- **Exactly-once comes from plain Postgres, not from pg_durable.** It rests on a manifest table and one transaction per load. The applet never relies on durable replay for correctness.
- **For a simple poll-and-load loop, pg_cron could do much of this**, with jobs every few seconds calling `discover()` and `work()`. pg_durable pulls ahead when a pipeline has steps (load → embed → index), waits (schedules, signals, HTTP), or per-item workflows. Those are also where its current limits bite (see [Issues to Fix](#issues-to-fix)).

## Architecture

```text
 Blob Storage            Postgres (one schema per install)
 ┌───────────┐  list   ┌─────────────┐  claim one at a time  ┌──────────┐ INSERT ┌─────────────┐
 │ prefix/   │ ──────► │ files       │ ────────────────────► │ worker   │ ─────► │ target      │
 │ *.parquet │ discover│ (manifest)  │ ◄──────────────────── │ loop × 4 │        │ table       │
 └───────────┘  loop   └─────────────┘   loaded / backoff    └──────────┘        └─────────────┘
       ▲                      ▲
       └──── backstop loop ───┘  daily full listing, for late or out-of-order files
```

Tables:

- **`watches`**: one row per source, with account, container, prefix, suffix, target table, record type, and decoder.
  - Prefixes can be date-templated, such as `events/{yyyy}/{mm}/{dd}/`.
  - Each poll lists only the folders within `lookback`, which defaults to 1 day.
- **`files`**: the manifest, with one row per blob version.
  - Keyed by `(watch, path, etag)`, so an overwritten blob gets a new row.
  - Tracks `status` (pending, loading, loaded, failed, superseded), `attempts`, `next_attempt_at`, `last_error`, and `rows_loaded`.

Loops (each one is a pg_durable instance):

- **discover**, every 5 s: lists the hot prefixes and runs `INSERT ... ON CONFLICT DO NOTHING` into `files`.
- **worker** × 4: claims the oldest due file with `FOR UPDATE SKIP LOCKED` and loads it. It repeats until it has loaded 500 files or spent 10 s, then goes around again.
- **backstop**, daily at 03:00: lists everything under each watch.

Each worker batch is one transaction, and each file loads in a subtransaction (`load_file`, abridged):

```sql
PERFORM pg_advisory_xact_lock(hashtextextended(src, 0));   -- serialize versions of one path
-- if a newer version of this path was discovered: mark this one superseded, return 0
EXECUTE format('DELETE FROM %s WHERE source = $1', w.target) USING src;
EXECUTE format('INSERT INTO %s SELECT r.*, $4, now()
                FROM azure_storage.blob_get($1, $2, $3, NULL::%s,
                     decoder => $5, options => $6) r', w.target, w.rectype)
    USING w.account, w.container, p_path, src, w.decoder, w.options;
UPDATE files SET status = 'loaded', rows_loaded = n, loaded_at = now(), ...
WHERE (watch, path, etag) = (p_watch, p_path, p_etag);
```

How this behaves under failure:

- **Crash:** the in-flight batch rolls back, and those files are claimed again.
- **Replay:** a replayed file is deleted by `source` first, so running a file twice is harmless.
- **Error:** the error is recorded in `last_error` and the file backs off (5 s, then 10 s). After 3 attempts it is `failed` until `retry()`.

Two modes:

- **`pool`** (the default) uses long-lived workers that claim files in batches. This is the mode that scales.
- **`per_file`** starts one durable instance per file, capped at `max_inflight`. It's easy to inspect file by file, but it runs into pg_durable's quotas and throughput limits.

## The pg_durable Code

A pg_durable workflow is a value built from SQL strings and combinators, then started with `df.start(workflow, label)`. The applet uses:

Construct | Meaning
--- | ---
`a ~> b` | Run `a`, then `b`
`a \|=> 'w'` | Name `a`'s result `w`; later SQL can use `$w.col`
`df.loop(body, continue_on_failure => true)` | Repeat `body` forever; a failed iteration doesn't end the loop
`df.sleep(n)` | Durable sleep that holds no connection
`df.wait_for_schedule(cron)` | Durable wait until a cron time
`df.if(cond, then, else)` | Branch on a SQL condition
`df.instances`, `df.cancel(id)` | Inspect and stop instances

This is all of `start()`, with comments added:

```sql
CREATE FUNCTION start(mode text DEFAULT 'pool', workers int DEFAULT 4,
                      poll_seconds int DEFAULT 5, batch int DEFAULT 500)
RETURNS SETOF text LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s text := current_schema(); i int;
BEGIN
    IF mode NOT IN ('pool', 'per_file') THEN
        RAISE EXCEPTION 'mode must be pool or per_file';
    END IF;
    -- discover: list (and in per_file mode, launch), sleep, repeat
    RETURN NEXT df.start(
        df.loop(CASE WHEN mode = 'per_file'
                     THEN format('SELECT %I.discover() AS found', s)
                          ~> format('SELECT %I.dispatch() AS launched', s)
                     ELSE format('SELECT %I.discover() AS found', s) END
                ~> df.sleep(poll_seconds),
                continue_on_failure => true),
        format('autoloader:%s:discover', s));
    -- backstop: full listing once a day
    RETURN NEXT df.start(
        df.loop(df.wait_for_schedule('0 3 * * *')
                ~> format('SELECT %I.discover(true) AS found', s),
                continue_on_failure => true),
        format('autoloader:%s:backstop', s));
    -- workers: load a batch; sleep 1 s only when there was nothing to do
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
```

`stop()` calls `df.cancel` on every running instance whose label starts with `autoloader:<schema>:`.

In `per_file` mode, `dispatch()` starts one instance per file from inside a SQL node. `df.start` commits atomically with the node's manifest update. A quota error on one start is caught in a subtransaction, so it doesn't roll back the whole node:

```sql
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
END LOOP;
```

Observability is plain SQL:

```sql
TABLE ingest.status;
```

```text
 watch | pending | loading | loaded | failed | superseded | rows_loaded |        last_loaded_at         |        last_listed_at         | last_error
-------+---------+---------+--------+--------+------------+-------------+-------------------------------+-------------------------------+------------
 green |       0 |       0 |      8 |      0 |          0 |     3297967 | 2026-11-25 10:57:26.732103-08 | 2026-11-25 14:54:28.418914-08 |
```

```sql
SELECT label, status FROM df.instances WHERE label LIKE 'autoloader:ingest:%' AND status = 'running' ORDER BY label;
```

```text
           label            | status
----------------------------+---------
 autoloader:ingest:backstop | running
 autoloader:ingest:discover | running
 autoloader:ingest:worker-1 | running
 autoloader:ingest:worker-2 | running
 autoloader:ingest:worker-3 | running
 autoloader:ingest:worker-4 | running
```

pg_durable's HTTP client can also list blobs, without azure_storage:

```sql
SELECT df.start(
    df.http('https://azureopendatastorage.blob.core.windows.net/nyctlc?restype=container&comp=list&prefix=green/puYear=2019/', 'GET')
    |=> 'r'
    ~> $$SELECT count(*) AS blobs FROM XMLTABLE('//Blob' PASSING (ltrim($r.body, chr(65279))::xml)
                                                COLUMNS name text PATH 'Name')$$);
-- df.result: {"rows": [{"blobs": 56}], "row_count": 1}
```

This works, but it needs the `ltrim` to strip a byte-order mark, and it pastes the whole response body into the SQL text (issues 5 and 6). For listing, `azure_storage.blob_list` is the better tool.

## Results

These ran on Postgres 18.7 under WSL2 Ubuntu on aarch64, with pg_durable 0.2.10 (`6416a44`) and azure_storage 1.11.

Test | Result
--- | ---
NYC green taxi, real Azure: watch one month, widen to all of 2019, crash Postgres mid-load | 8 Parquet files and 3,297,967 rows in 50 s. Every month's count is exact and every file has `attempts = 1`.
Scale on mock storage: 10,000 CSV files in one folder, crash a third of the way through | 106 s, 100,000 rows, 0 duplicate ids
Throughput without a crash (`pool`, 4 workers × 500 files) | About 130 files/s
Throughput with one instance per file (`per_file`, 50 in flight) | About 19 files/s
Discovering 100,000 new files | 1.1 s; a repeat poll takes 0.7 s
Bad file | Retried after 5 s and 10 s, then `failed` with the parse error. Fixing the file and calling `retry()` loads it.
Overwritten blob | The new ETag loads; the stale version is marked `superseded` and loads 0 rows.
Unreachable account | Recorded in `watches.last_error`; that watch is skipped for 5 minutes and the other watches keep running.

"Crash" means `pg_ctlcluster 18 main restart -m immediate`. Nothing was restarted by hand afterward.

## Lessons Learned

1. **Put exactly-once in the data, not the workflow.** The manifest has one row per blob version, and each load is one transaction, so correctness doesn't depend on how pg_durable replays. Every activity is idempotent: `load_file` deletes by source before inserting and skips stale versions. The crash tests passed with no special handling.

2. **Batch the work inside each activity.** pg_durable runs at most 2 activities at a time server-wide, and each orchestration step costs about 100 ms.
   - One instance per file reached only about 20 files/s.
   - Four long-lived worker loops, each loading up to 500 files per activity, reached about 130 files/s.
   - One SQL node is one transaction, so cap each batch by time as well (10 s here).

3. **Per-item instances need quota handling.**
   - At 100 concurrent instances per user, `df.start` fails with SQLSTATE 53400.
   - The retained-instance quota (10,000 per user, kept 30 days) caps sustained fan-out at about 333 items a day.
   - A node that starts too many instances rolls back completely, and a `continue_on_failure` loop then retries it forever.
   - The fix is to catch 53400 for each `df.start` in a subtransaction.

4. **`continue_on_failure` loops are cheap but quiet.** Their iterations are child orchestrations and don't count against the retained quota: `pool` mode used about 58 instances for 10,000 files. But failed iterations don't show up in `df.instances` or `df.instance_nodes`, so record health in your own tables (`watches.last_error`, `files.last_error`).

5. **Listing scales badly; loading doesn't.** azure_storage can only list a prefix from the beginning, so every poll re-lists the hot folders, even when nothing is new.
   - On real Azure a listing costs about 1.2 s per call plus about 0.5 ms per blob.
   - Date-templated prefixes bound that cost, and a daily backstop catches late files.
   - A `startFrom` watermark or Event Grid notifications would make each poll cost proportional to the new files only.

6. **Claim one row at a time.** A PL/pgSQL `FOR r IN SELECT ... FOR UPDATE SKIP LOCKED` loop fetches 10 rows at once, so one worker locked all the large files. A `SELECT ... LIMIT 1 FOR UPDATE SKIP LOCKED` in a loop spreads them across workers.

7. **Isolate each source.** One bad account shouldn't stall every watch; a mistyped account name alone retries for 84 s. `discover()` wraps each watch in its own exception block and skips a failing watch for 5 minutes.

8. **Workflows run as the user, with the user's settings.** SQL nodes run as the role that started them, and role-level settings apply. Setting `azure_storage.use_mock_blob_storage = on` for a test role gave us a mock-backed scale lab with no code changes.

## Issues to Fix

We found 13 issues: 6 in pg_durable and 7 in azure_storage. [FINDINGS.md](FINDINGS.md) has the evidence and a repro for each. These are worth fixing first:

\# | Component | Issue | Suggested fix
--- | --- | --- | ---
1 | pg_durable | **Only 2 activities run at a time, server-wide**, and this can't be configured. `worker.rs:796` uses duroxide's `RuntimeOptions::default()`. | GUCs for worker and orchestration concurrency
2 | pg_durable | **Rolled-back `df.start` calls still use up the 30-day retained quota**, so a runaway fan-out loop can lock a role out without any visible sign. Both quotas raise SQLSTATE 53400. | Don't charge rolled-back starts. Use distinct error codes. Add a `df.capacity()` view, plus a failure count and last error on loop nodes.
3 | pg_durable | Each orchestration step costs about 100 ms: the dispatcher polls every 100 ms, with no LISTEN/NOTIFY wakeup. | Wake dispatchers with NOTIFY
7 | azure_storage | Every non-Parquet `blob_get` makes an extra List Blobs call, only to log the size. | Drop the probe, or use `Content-Length`
8 | azure_storage | Every `blob_list` makes 2 requests (likely Get Container Properties, then List) on new TLS connections, with no reuse: about 1.2 s fixed per call. | A keep-alive client; skip the properties call
9 | azure_storage | A mistyped account name retries for 84 s, then reports "internal error while connecting". | Fail fast on NXDOMAIN, and name the host
10 | azure_storage | `blob_list` has no `startFrom` or marker. It also sends API version 2022-11-02, which silently ignores `startFrom`. | A `start_from` argument and a newer API version
11 | azure_storage | `blob_get` can't pin a read to the ETag that was listed. | An `if_match` argument

Smaller issues:

- **pg_durable:**
  - `df.instance_nodes` shows cancelled loops as `running` (4).
  - `df.http` keeps the UTF-8 byte-order mark (5).
  - `$var` pastes large bodies into the SQL text and the log (6).
- **azure_storage:**
  - Progress lines are logged at LOG level on every call (12).
  - The mock backend's ETag never changes (13).

## Next Steps

- **Chain steps after each load**, such as azure_ai embeddings or a pg_textsearch index refresh. This is where pg_durable's step model should pay off.
- **Event-driven discovery:** Event Grid → Storage Queue, polled with `df.http`.
- **Re-measure throughput** once the concurrency cap can be configured.

## Try It

Requires Postgres 18 with pg_durable and azure_storage, and a role with `df.grant_usage`.

```sh
# Real data: NYC green-taxi Parquet files in the public azureopendatastorage account
psql -d pg_durable -U durable_dev -v schema=ingest -f autoloader.sql -f nyc_setup.sql
psql -d pg_durable -U durable_dev -c "SELECT ingest.start()"
```

[DEMO.md](DEMO.md) has a 10-minute runbook (point it at a folder, add files, crash Postgres, scale, handle a bad file). It also covers setting up the mock-storage scale lab.

## Files

File | Purpose
--- | ---
[autoloader.sql](autoloader.sql) | The applet (about 270 lines). Install with `-v schema=...`.
[nyc_setup.sql](nyc_setup.sql) | Real-data watch, target table, and `check_counts` view
[lab_setup.sql](lab_setup.sql) | Mock-storage watch and target table
[gen_events.py](gen_events.py) | Writes N small CSV files into mock storage
[demo_reset.sql](demo_reset.sql), [demo_nyc.sh](demo_nyc.sh), [demo_scale.sh](demo_scale.sh) | Demo reset and unattended rehearsals
[DEMO.md](DEMO.md) | Demo runbook
[Autoloader-Summary.pdf](Autoloader-Summary.pdf) | Four-page summary of this report
[FINDINGS.md](FINDINGS.md) | All 13 issues, with evidence and repros

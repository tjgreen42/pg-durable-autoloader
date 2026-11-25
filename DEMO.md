# Autoloader Demo

Runbook for demoing the Autoloader. See [README.md](README.md) for the write-up.

Keep Postgres tables in sync with files landing in Azure Blob Storage, exactly once, across crashes, in about 270 lines of SQL

- [azure_storage](https://learn.microsoft.com/azure/postgresql/extensions/concepts-storage-extension) lists, reads, and decodes blobs (CSV, Parquet, ...)
- [pg_durable](https://github.com/microsoft/pg_durable) decides when, retries, and survives restarts

No external scheduler, queue, or worker process. Everything is a table you can query.

## How It Works

```text
 Blob Storage           Postgres
 ┌──────────┐  list   ┌──────────────┐  claim (SKIP LOCKED)  ┌─────────┐  INSERT  ┌──────────┐
 │ folder/  │ ──────► │ files        │ ────────────────────► │ worker  │ ───────► │ target   │
 │  *.csv   │ discover│ (manifest)   │ ◄──────────────────── │ loop ×4 │          │ table    │
 └──────────┘  loop   └──────────────┘   mark loaded/failed  └─────────┘          └──────────┘
```

- **Watch**: account, container, prefix, suffix, target table, and record type. Prefixes can be date-templated (`events/{yyyy}/{mm}/{dd}/`) so each poll lists only the last `lookback` of folders. A daily backstop lists everything.
- **Manifest** (`files`): one row per blob version `(watch, path, etag)`. Overwriting a blob adds a new row, and older versions become `superseded`.
- **Exactly once**: claiming a file, `DELETE ... WHERE source = file`, `INSERT ... FROM azure_storage.blob_get(...)`, and marking it `loaded` all commit in one transaction. A crash rolls back the in-flight batch and the file is simply claimed again.
- **Failures**: each file loads in a subtransaction. An error records `last_error` and backs off (5 s, 10 s, ...). After `max_attempts` the file is `failed` until `retry()`.
- **Durable loops**: `start()` launches a discover loop, a backstop loop, and N worker loops with `df.loop(..., continue_on_failure => true)`. They resume on their own after a restart.

Two modes:

- `pool` (default): N long-lived workers claim files in batches. Scales to large folders.
- `per_file`: one durable instance per file, capped at `max_inflight`. Easier to inspect per file, but it hits pg_durable instance quotas (see [FINDINGS.md](FINDINGS.md)).

## API

```sql
SELECT ingest.start();                       -- pool mode, 4 workers, poll every 5 s
SELECT ingest.start('per_file', poll_seconds => 1);
SELECT ingest.stop();
SELECT ingest.retry();                       -- requeue failed files (optionally for one watch)
SELECT ingest.discover(true);                -- list everything now (normally the daily backstop)
TABLE ingest.status;                         -- per watch: pending, loaded, failed, rows, last_error
```

Settings live in `ingest.settings`: `max_attempts` (3), `retry_base` (5 seconds), and `max_inflight` (50, per_file mode only).

## Setup

Requires Postgres 18 with pg_durable 0.2.10 and azure_storage 1.11.

```sh
# Real data: NYC green-taxi Parquet files in the public azureopendatastorage account
psql -d pg_durable -U durable_dev -v schema=ingest -f autoloader.sql -f nyc_setup.sql

# Scale lab: azure_storage's mock backend (files under /tmp/azure_storage_test_data/mock/)
psql -d pg_durable -c "CREATE ROLE ingest_lab LOGIN" \
                   -c "ALTER ROLE ingest_lab SET azure_storage.use_mock_blob_storage = on" \
                   -c "GRANT CREATE ON DATABASE pg_durable TO ingest_lab" \
                   -c "SELECT df.grant_usage('ingest_lab')"
psql -d pg_durable -U ingest_lab -v schema=lab -f autoloader.sql -f lab_setup.sql
```

`autoloader.sql` drops and recreates the schema named by `-v schema=...`.

## Demo

About 10 minutes. Use two terminals: **A** runs commands and **B** watches.

Before you start, run `psql -d pg_durable -U durable_dev -f demo_reset.sql`. This stops the loops, empties the table, and points the watch at one month.

### 1. Point it at a folder

B:

```sql
SELECT watch, pending, loaded, failed, rows_loaded, last_loaded_at FROM ingest.status \watch 1
```

A:

```sql
SELECT prefix FROM ingest.watches;                   -- green/puYear=2019/puMonth=8/
SELECT ingest.start(poll_seconds => 2);              -- 6 loop instances
```

`loaded` reaches 1 within about 5 s. August 2019 has a single row in this dataset.

Talking point: that is the whole deployment. Run `SELECT id, label, status FROM df.instances WHERE label LIKE 'autoloader:%'` to show it.

### 2. New files arrive

Widen the watch to simulate a year of files landing:

```sql
UPDATE ingest.watches SET prefix = 'green/puYear=2019/';
```

Seven new files and 3.3M rows load in about 45 s, two files at a time.

### 3. Pull the plug

When `loaded` reaches 4 or 5, run this from a shell:

```sh
sudo pg_ctlcluster 18 main restart -m immediate
```

B reconnects. Nobody restarts anything and the loops pick up where they left off. When `pending = 0`:

```sql
TABLE ingest.check_counts;   -- every month: expected = loaded, ok = t
SELECT path, attempts FROM ingest.files;   -- attempts = 1 everywhere: the crashed batch rolled back
```

`demo_nyc.sh` runs acts 1 to 3 unattended (last run: 50 s end to end, crash at 5 of 8 files, all counts exact).

### 4. Scale

```sh
./demo_scale.sh 10000
```

This writes 10,000 CSV files into one mock folder, starts 4 workers, crashes Postgres a third of the way through, and checks the result:

```text
106s  pending=0 loaded=10000
 files_loaded | manifest_rows | table_rows | duplicate_ids
--------------+---------------+------------+---------------
        10000 |        100000 |     100000 |             0
```

That is about 95 files/s including the crash, and about 130 files/s without one. Discovery is cheap:

```sh
python3 gen_events.py $(date -u +%F) 100000
psql -U ingest_lab -c '\timing' -c 'SELECT lab.discover()'   -- 100,000 new files in ~1.1 s
```

Talking points:

- Listing is bounded by date-templated hot prefixes, so polling a 5-year archive lists only yesterday's and today's folders.
- Workers claim with `FOR UPDATE SKIP LOCKED`, one file at a time, so large files spread out.
- Throughput is capped by pg_durable running only 2 activities at a time (see FINDINGS #1).

### 5. Bad file

```sh
D=/tmp/azure_storage_test_data/mock/mocklanding/landing/events/$(date -u +%Y/%m/%d)
printf 'id,ts,device,value\nNaN-id,2026-11-25T12:00:00Z,dev-1,1.0\n' > $D/part-bad.csv
```

```sql
SELECT lab.start(workers => 1, poll_seconds => 1);
SELECT status, attempts, next_attempt_at, last_error FROM lab.files WHERE path LIKE '%bad%' \watch 2
```

The file retries at +5 s and +10 s, then becomes `failed`:

```text
failed | 3 | invalid input syntax for type bigint: "NaN-id"
```

Fix the file, then requeue it:

```sh
printf 'id,ts,device,value\n999999,2026-11-25T12:00:00Z,dev-1,1.0\n' > $D/part-bad.csv
```

```sql
SELECT lab.retry();   -- 1; the file is loaded a few seconds later
```

### 6. Wrap up

- Everything is SQL: the manifest, retries, health (`watches.last_error`), and the loops (`df.instances`).
- Next steps:
  - Chain a step after each load, for example `azure_ai` embeddings or a `pg_textsearch` index refresh, as another node in the worker loop.
  - Trigger on Event Grid → Storage Queue instead of polling.

## Reset

```sh
psql -d pg_durable -U durable_dev -f demo_reset.sql
psql -d pg_durable -U ingest_lab -c "SELECT lab.stop()"
```

## Files

File | Purpose
--- | ---
`autoloader.sql` | The applet. Install with `-v schema=...`
`nyc_setup.sql` | Real-data watch, `green_trips` table, and `check_counts` view
`lab_setup.sql` | Mock-backend watch and `events` table
`gen_events.py` | Writes N small CSV files into the mock: `gen_events.py DAY FILES [ROWS] [START]`
`demo_reset.sql` | Resets the NYC demo
`demo_nyc.sh` | Runs acts 1 to 3 unattended
`demo_scale.sh` | Runs act 4 unattended
`README.md` | Write-up: motivation, architecture, lessons, and issues
`FINDINGS.md` | Bugs and limitations found along the way

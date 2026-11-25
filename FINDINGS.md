# Findings

Bugs and limitations found while building the [Autoloader](README.md) on pg_durable and azure_storage

Environment:

- Postgres 18.7 on aarch64 (WSL2 Ubuntu)
- pg_durable 0.2.10 (`6416a44`), with duroxide 0.1.30 and duroxide-pg 0.1.34
- azure_storage 1.11
- pgvector 0.8.7, pg_textsearch 1.6.0, azure_ai 2.5.7

Severity | pg_durable | azure_storage
--- | --- | ---
High | 2 | -
Medium | 1 | 5
Low | 3 | 2

## pg_durable

### 1. Only 2 activities run at a time, server-wide (high)

`worker.rs:796` starts the runtime with `RuntimeOptions::default()`. That sets `worker_concurrency = 2` and `orchestration_concurrency = 2`, and neither can be changed. Meanwhile `max_user_connections` (10) and `df.max_concurrent_per_user` (100) suggest much more parallelism is possible.

- With pool mode at 1, 4, and 8 worker loops, sampling `pg_stat_activity` every 0.5 s never showed more than 2 concurrent loads.
- Real data: eight Parquet files load strictly two at a time.
- This caps every applet on the server, for all users combined.

Suggestion: expose GUCs for worker and orchestration concurrency, or derive them from `max_worker_processes`.

### 2. Instance quotas: a fan-out loop livelocks and quietly burns retained quota (high)

When `df.max_concurrent_per_user` (100) is reached, `df.start` raises SQLSTATE 53400. There is no backpressure or queueing.

- **Livelock.** A SQL node that starts more instances than the limit allows rolls back entirely, including its other writes such as manifest inserts. A `continue_on_failure` loop then retries forever. Observed: 0 files/s and an empty manifest with `max_inflight = 200`. Meanwhile `df.instances` shows the loop as `running`, and `df.instance_nodes` shows no failure count or last error.
- **Phantom retained quota.** Rolled-back starts still count against `df.max_instances_per_user` (10,000 retained for 30 days). After about 30 s of livelock, `_duroxide._submissions` held 3,949 rows for the role while `df.instances` showed 1,791. At that rate a role is locked out of `df.start` for 30 days within minutes, and users can't see why.
- **Same error code.** Both quotas raise 53400, and only the HINT differs. Callers can't tell "retry in a second" from "blocked for 30 days".
- **Low ceiling for fan-out.** Even without the livelock, one instance per item caps sustained use at about 333 items a day (10,000 / 30 days).

Workaround used by the applet: catch 53400 around each `df.start` in a subtransaction, and stop launching for this poll. Catching it this way leaks no instances: exactly 100 were started.

Suggestions:

- Don't charge retained quota for rolled-back starts.
- Use distinct error codes for the two quotas.
- Add a `df.capacity()` or quota view.
- Add a failed-iteration count and `last_error` to loop nodes.

### 3. About 100 ms per orchestration step; a loop iteration costs about 1.8 s (medium)

The duroxide dispatcher polls every 100 ms, and duroxide-pg has no LISTEN/NOTIFY wakeup.

- A loop of `work() |=> 'w' ~> df.if('SELECT $w.n = 0', df.sleep(1), 'SELECT 1')` takes about 2.1 s per iteration when `work()` itself takes 0.33 s.
- Throughput on 10-row CSV files:

  Configuration | files/s
  --- | ---
  1 worker × batch 20 | 9.8
  4 × 20 | 33.8
  8 × 20 | 55.3
  4 × 200 | 91.0
  4 × 500 | 129.9
  per_file, 50 in flight | 19.1

Workaround: large batches per activity.

### 4. `df.instance_nodes` still reports `running` after `df.cancel` (low)

After a running loop is cancelled, `df.status()` returns `cancelled`. However, `df.instance_nodes(id)` still reports the LOOP and THEN nodes as `running`, with `inferred_status` also `running`. Dashboards built on `instance_nodes` therefore show cancelled work as live.

```sql
SELECT df.start(df.loop('SELECT 1' ~> df.sleep(1))) AS id \gset
SELECT df.cancel(:'id');
SELECT node_type, status, inferred_status FROM df.instance_nodes(:'id');
--  THEN  | running   | running
--  SLEEP | completed | pending
--  SQL   | completed | completed
--  LOOP  | running   | running
```

### 5. `df.http` keeps the UTF-8 BOM in text bodies (low)

Azure Storage XML responses start with U+FEFF, so `($r.body)::xml` fails with "invalid XML content".

```sql
df.http('https://azureopendatastorage.blob.core.windows.net/nyctlc?restype=container&comp=list&prefix=green/puYear=2019/', 'GET')
  |=> 'r' ~> $$SELECT ($r.body)::xml$$
```

Workaround: `ltrim($r.body, chr(65279))::xml`. Suggestion: strip the BOM when decoding text bodies, or document it.

### 6. `$var` substitution inlines whole HTTP bodies into SQL text (low)

A 93 KB List Blobs page became a 93,788-character statement, and it was logged in full with `log_workflow_sql = on`. A 5,000-blob page would be about 2 MB per statement. Paging with `NextMarker` also needs state carried between iterations through a user table.

## azure_storage

### 7. Every non-Parquet `blob_get` sends an extra List Blobs call (medium)

Each non-Parquet `blob_get` first sends a List Blobs request, just to log the blob size, even when `log_level` hides that line.

- On Azure this doubles the requests per file, and List is billed at the higher write-class rate.
- On the mock backend the probe walks the whole container: 300 gets of one 250-byte CSV took 44 ms in a 1-blob container and 4,662 ms in a 10,000-blob container.

```sql
SELECT count(*) FROM generate_series(1, 300) g,
  azure_storage.blob_get('mocklanding', 'landing', CASE WHEN g > 0 THEN 'events/.../part-00000.csv' END,
                         NULL::lab.event_rec, decoder => 'csv');
```

Suggestion: drop the probe, or take the size from `Content-Length` or `Content-Range` on the first GET.

### 8. `blob_list` opens 2 new TLS connections per call, with no reuse (medium)

Three `blob_list` calls on an empty prefix made 6 TCP connections to port 443 and 6 DNS lookups. Each call takes about 1.1 s, compared with about 0.6 s for a cold `curl` of the same URL. Real listing times:

Blobs | Time
--- | ---
6 | 1.20 s
576 | 1.74 s
8,819 | 5.92 s

Per-call setup dominates hot-prefix polling.

```sh
strace -f -e connect -p <backend pid>   # while running:
```

```sql
SELECT count(*) FROM generate_series(1, 3) g,
  azure_storage.blob_list('azureopendatastorage', 'nyctlc', CASE WHEN g > 0 THEN 'zzz-none/' END);
```

The second request is likely a Get Container Properties call before every listing.

Suggestion: a keep-alive client per backend, and skip or cache the properties call.

### 9. A nonexistent account retries for 84 s, then reports "internal error while connecting" (medium)

A DNS NXDOMAIN (for example a typo in the account name) is retried with azure_core's default backoff for 83.6 s. The resulting message names neither the host nor the cause. Under pg_durable this also ties up one of only 2 activity slots (#1) for every poll.

```sql
SELECT count(*) FROM azure_storage.blob_list('nosuchacct9z8y7x', 'landing', 'x/');
```

### 10. No paged or incremental listing (medium)

`blob_list(account, container, prefix)` always enumerates the whole prefix from the beginning; there is no marker, `startFrom`, or `max_results`. Watchers must re-list everything on each poll:

- A poll that found no new files re-listed a 10,000-file folder in full.
- On real Azure, listing costs about 1.2 s per call plus about 0.46 ms per blob: 56 blobs take 1.27 s, 576 take 2.0 s, and 8,819 take 5.3 s.
- `gfsweatherdatacontainer` did not finish listing in over 5 minutes.

The REST API can resume from a key with `startFrom` (version 2023-05-03 and later). On `nyctlc`, `prefix=green/` returns 576 blobs; adding `startFrom=green/puYear=2019/puMonth=6/` returns 44. azure_storage can't use it today: `blob_list` has no such argument, and the extension sends `x-ms-version` 2022-11-02. At that version Azure silently ignores `startFrom` and returns all 576 blobs.

The applet works around this with date-templated hot prefixes and a daily full backstop.

Suggestion: add an optional `start_from` argument to `blob_list` (and send API version 2023-05-03 or later), so watchers can keep a per-prefix watermark.

### 11. `blob_get` can't pin a read to the ETag it listed (medium)

Loaders list `(path, etag)` and read later. With no `if_match` or version argument, the bytes loaded may come from a newer version than the manifest row records.

Suggestion: add an optional `etag` or `if_match` argument to `blob_get` and COPY.

### 12. `log_level` defaults to LOG (low)

azure_storage writes 1 to 3 progress lines per `blob_get` or `blob_list`, for about 34,000 lines in a 10,000-file run. The GUC is superuser-only. Suggestion: default to DEBUG1.

### 13. The mock backend reports a constant ETag and `last_modified` (low)

Every blob reports `0x123` and `2000-01-01`, so overwrites and change detection can't be tested against the mock. Suggestion: derive both from the file's mtime and size.

## What Worked Well

- **Crash recovery is exact.** Several `-m immediate` restarts in the middle of loads (10,000 files, and 3.3M Parquet rows) ended with exact counts, 0 duplicates, and every file at `attempts = 1`. The loops resumed without intervention.
- **pg_durable SQL nodes compose with transactions.** `df.start` inside a SQL node commits atomically with the node's other writes, and 53400 can be caught per call in a subtransaction.
- **`continue_on_failure` loops are cheap.** Their iterations are child orchestrations and don't consume retained quota: pool mode used about 58 instances in total for 10,000 files.
- **Role-level GUCs reach worker sessions.** A role-level `azure_storage.use_mock_blob_storage` applies to that role's pg_durable SQL nodes, which makes a mock-backed scale lab possible.
- **azure_storage Parquet decoding is fast.** 3.3M rows across 8 files load in about 45 s, with a typed `INSERT ... SELECT` from `blob_get`.

#!/bin/bash
# Rehearsal of the NYC acts: point at one month, widen to 2019, crash mid-load, check counts.
F=$(dirname "$0"); P="psql -X -q -d pg_durable -U durable_dev"
$P -f $F/demo_reset.sql >/dev/null
t0=$(date +%s); el(){ echo "$(( $(date +%s) - t0 ))s"; }
$P -Atc "SELECT count(*) || ' loops started' FROM ingest.start(poll_seconds => 2)"
until [ "$($P -Atc "SELECT loaded FROM ingest.status")" = 1 ]; do sleep 1; done; echo "$(el) month 8 loaded"
$P -c "UPDATE ingest.watches SET prefix = 'green/puYear=2019/'"
crashed=
while :; do sleep 3
  s=$($P -At -F' ' -c "SELECT pending, loading, loaded, rows_loaded FROM ingest.status" 2>/dev/null || echo down); echo "$(el) $s"
  set -- $s
  if [ -z "$crashed" ] && [ "${3:-0}" -ge 4 ]; then sudo -n pg_ctlcluster 18 main restart -m immediate; crashed=1; echo "*** crash ***"; fi
  [ "$1 $2 $3" = "0 0 8" ] && break
done
$P -c "TABLE ingest.check_counts" -c "SELECT path, attempts, rows_loaded FROM ingest.files ORDER BY path"
$P -Atc "SELECT ingest.stop() || ' loops stopped'"

#!/bin/bash
# Scale act: 10,000 files land at once in the mock account; 4 durable workers drain them;
# Postgres is crashed (-m immediate) mid-drain; the table ends up exact, with no duplicates.
set -e
F=$(dirname "$0"); N=${1:-10000}; DAY=$(date -u +%F)
P="psql -X -q -d pg_durable -U ingest_lab"
$P -v schema=lab -f $F/autoloader.sql -f $F/lab_setup.sql 2>/dev/null
rm -rf /tmp/azure_storage_test_data/mock/mocklanding/landing/events
python3 $F/gen_events.py $DAY $N
$P -Atc "SELECT count(*) || ' loops started' FROM lab.start('pool', 4, 1)"
t0=$(date +%s); crashed=
while :; do
    sleep 3
    read pend loaded <<<$($P -At -F' ' -c "SELECT pending, loaded FROM lab.status WHERE watch = 'events'" 2>/dev/null || echo "- -")
    echo "$(( $(date +%s) - t0 ))s  pending=$pend loaded=$loaded"
    if [ -z "$crashed" ] && [ "$loaded" != "-" ] && [ "$loaded" -gt $(( N / 3 )) ]; then
        sudo -n pg_ctlcluster 18 main restart -m immediate; crashed=1; echo "*** crashed and restarted Postgres ***"
    fi
    [ "$loaded" = "$N" ] && break
done
$P -c "SELECT count(*) AS files_loaded, sum(rows_loaded) AS manifest_rows,
              (SELECT count(*) FROM lab.events) AS table_rows,
              (SELECT count(*) FROM (SELECT id FROM lab.events GROUP BY id HAVING count(*) > 1) d) AS duplicate_ids
       FROM lab.files WHERE status = 'loaded'"
$P -Atc "SELECT lab.stop() || ' loops stopped'"

#!/usr/bin/env python3
"""Write tiny CSV event files into the azure_storage mock: gen_events.py DAY FILES [ROWS] [START]"""
import os, sys, datetime as dt
root = '/tmp/azure_storage_test_data/mock/mocklanding/landing'
day = dt.date.fromisoformat(sys.argv[1]); nfiles = int(sys.argv[2])
rows = int(sys.argv[3]) if len(sys.argv) > 3 else 10
start = int(sys.argv[4]) if len(sys.argv) > 4 else 0
d = f'{root}/events/{day:%Y/%m/%d}'; os.makedirs(d, exist_ok=True)
for f in range(start, start + nfiles):
    with open(f'{d}/part-{f:05d}.csv', 'w') as fh:
        fh.write('id,ts,device,value\n')
        for r in range(rows):
            fh.write(f'{f * rows + r},{day}T12:00:00Z,dev-{r % 7},{(f * 31 + r) % 100 / 3:.3f}\n')

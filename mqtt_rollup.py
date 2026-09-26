#!/usr/bin/env python3
"""
mqtt_rollup.py
Maintain mqtt_readings_1m: one row per (numeric topic, minute) holding
n / sum / min / max of that minute's readings from mqtt_readings.

Why: the dashboard's trend charts (6h/24h/7d/30d) used to AVG() raw readings
per topic. mqtt_readings is clustered by insertion order, so one topic's rows
are scattered across the whole table -- every row a random page read -- and a
7-day wind chart took ~30s (30 days: minutes, nginx 504). Reading the same
chart from the rollup is a few thousand rows out of a small table.

Sums (not averages) are stored so any bucket that is a whole number of minutes
can be recombined exactly: AVG over a bucket == SUM(sum_value)/SUM(n).

Incremental and idempotent: each run recomputes only whole, settled minutes
between the stored watermark (mqtt_rollup_state.covered_to) and "now - 10s",
replacing those rows, so re-running or overlapping ranges can't double count.
The first run backfills --backfill-days (default 31, a little more than the
dashboard's longest 30d range). A named DB lock stops overlapping runs.

Only topics whose mqtt_topics.data_type is int/float are rolled up, and only
readings that are plain decimal numbers (string values like "OK" are skipped
rather than aborting the INSERT under strict SQL mode).

Usage:
  python3 mqtt_rollup.py                      # catch up to now (what the timer runs)
  python3 mqtt_rollup.py --backfill-days 2    # first run: smaller backfill (for testing)
  python3 mqtt_rollup.py --pause 2            # sleep 2s between chunks (gentle backfill)
  python3 mqtt_rollup.py --status             # print coverage and exit
"""
import argparse
import sys
import time
from datetime import datetime, timedelta
import mysql.connector

SETTLE_SECONDS = 10       # a minute is only rolled up this long after it ends
LOCK_NAME = 'mqtt_rollup'

DDL = [
    """CREATE TABLE IF NOT EXISTS mqtt_readings_1m (
         topic_id   INT UNSIGNED NOT NULL,
         minute_ts  DATETIME     NOT NULL,
         n          INT UNSIGNED NOT NULL,
         sum_value  DOUBLE       NOT NULL,
         min_value  DOUBLE       NOT NULL,
         max_value  DOUBLE       NOT NULL,
         PRIMARY KEY (topic_id, minute_ts),
         KEY idx_rollup_minute (minute_ts)
       ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS mqtt_rollup_state (
         id            TINYINT UNSIGNED NOT NULL PRIMARY KEY,
         covered_from  DATETIME NOT NULL,
         covered_to    DATETIME NOT NULL
       ) ENGINE=InnoDB""",
]

ROLLUP_CHUNK_SQL = """
    INSERT INTO mqtt_readings_1m (topic_id, minute_ts, n, sum_value, min_value, max_value)
    SELECT topic_id, m, COUNT(*), SUM(v), MIN(v), MAX(v)
    FROM (
        -- Plan is pinned on purpose: scan mqtt_readings by ts (ids are in ts order, so
        -- that's a near-sequential read) and look topics up by PK. Left to itself the
        -- optimizer can drive from the tiny mqtt_topics table instead and hit the
        -- (topic_id, ts) index, i.e. one random page read per row -- 78s vs 0.3s for
        -- the same 10 minutes of data.
        SELECT STRAIGHT_JOIN r.topic_id,
               TIMESTAMP(DATE(r.ts), MAKETIME(HOUR(r.ts), MINUTE(r.ts), 0)) AS m,
               CAST(r.value AS DECIMAL(20,4)) AS v
        FROM mqtt_readings r FORCE INDEX (idx_readings_ts)
        JOIN mqtt_topics t ON t.id = r.topic_id
        WHERE r.ts >= %s AND r.ts < %s
          AND t.data_type IN ('int', 'float')
          AND r.value REGEXP '^[-+]?[0-9]*[.]?[0-9]+$'
    ) x
    GROUP BY topic_id, m
    ON DUPLICATE KEY UPDATE n = VALUES(n), sum_value = VALUES(sum_value),
                            min_value = VALUES(min_value), max_value = VALUES(max_value)
"""


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def floor_minute(dt):
    return dt.replace(second=0, microsecond=0)


def rollup_range(cur, conn, start, end, chunk, pause, on_chunk_done=None):
    """Recompute whole minutes in [start, end) in chunk-sized transactions. Returns rows written."""
    total = 0
    while start < end:
        chunk_end = min(start + chunk, end)
        ct = time.time()
        cur.execute(ROLLUP_CHUNK_SQL, (start, chunk_end))
        total += cur.rowcount
        if on_chunk_done:
            on_chunk_done(chunk_end)
        conn.commit()
        if chunk_end - start > timedelta(minutes=30):    # only chatty for backfill-sized chunks
            print(f"  {start} -> {chunk_end}  ({time.time() - ct:.1f}s)", flush=True)
        start = chunk_end
        if pause and start < end:
            time.sleep(pause)
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--backfill-days', type=int, default=31)
    ap.add_argument('--keep-days', type=int, default=90, help='prune rollup rows older than this')
    ap.add_argument('--chunk-minutes', type=int, default=360, help='minutes of raw data per transaction')
    ap.add_argument('--pause', type=float, default=0.0, help='seconds to sleep between chunks')
    ap.add_argument('--status', action='store_true')
    args = ap.parse_args()

    conn = mysql.connector.connect(
        unix_socket='/run/mysqld/mysqld.sock', user='mikemc',
        password=get_secrets()['DB_PASS'], database='boat_monitoring', autocommit=False)
    cur = conn.cursor()
    # READ COMMITTED makes INSERT ... SELECT read mqtt_readings without shared
    # next-key locks, so a rollup can never block the logger's inserts.
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    for ddl in DDL:
        cur.execute(ddl)
    conn.commit()

    if args.status:
        cur.execute("SELECT covered_from, covered_to FROM mqtt_rollup_state WHERE id = 1")
        row = cur.fetchone()
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT topic_id) FROM mqtt_readings_1m")
        rows, topics = cur.fetchone()
        print(f"covered: {row[0]} -> {row[1]}" if row else "covered: (nothing yet)")
        print(f"rollup rows: {rows}, topics: {topics}")
        return 0

    cur.execute("SELECT GET_LOCK(%s, 0)", (LOCK_NAME,))
    if cur.fetchone()[0] != 1:
        print("another mqtt_rollup run is in progress; exiting")
        return 0

    now = datetime.now()
    end = floor_minute(now - timedelta(seconds=SETTLE_SECONDS))
    window_start = floor_minute(now - timedelta(days=args.backfill_days))

    cur.execute("SELECT covered_from, covered_to FROM mqtt_rollup_state WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        covered_from = start = window_start
        print(f"first run: backfilling from {window_start}")
    elif row[1] < window_start:
        covered_from = start = window_start
        print(f"WARNING: watermark {row[1]} is older than the backfill window; "
              f"restarting coverage from {window_start} (earlier data left as-is)")
    else:
        covered_from, start = row

    chunk = timedelta(minutes=args.chunk_minutes)
    t0 = time.time()
    total_rows = 0

    # A wider --backfill-days than what's already covered (e.g. after a small
    # test run): fill in [window_start, covered_from) first. covered_from only
    # moves once the whole back-range is done, so an interrupted run never
    # advertises coverage it doesn't have.
    if row is not None and row[0] > window_start and row[1] >= window_start:
        print(f"extending coverage backwards: {window_start} -> {row[0]}")
        total_rows += rollup_range(cur, conn, window_start, row[0], chunk, args.pause)
        covered_from = window_start
        cur.execute("UPDATE mqtt_rollup_state SET covered_from = %s WHERE id = 1", (covered_from,))
        conn.commit()

    def save_state(chunk_end):
        cur.execute(
            """INSERT INTO mqtt_rollup_state (id, covered_from, covered_to) VALUES (1, %s, %s)
               ON DUPLICATE KEY UPDATE covered_from = VALUES(covered_from), covered_to = VALUES(covered_to)""",
            (covered_from, chunk_end))

    total_rows += rollup_range(cur, conn, start, end, chunk, args.pause, on_chunk_done=save_state)

    # Retention: cheap because of idx_rollup_minute; bounded per run.
    prune_before = floor_minute(now - timedelta(days=args.keep_days))
    cur.execute("DELETE FROM mqtt_readings_1m WHERE minute_ts < %s LIMIT 20000", (prune_before,))
    if cur.rowcount:
        cur.execute("UPDATE mqtt_rollup_state SET covered_from = GREATEST(covered_from, %s) WHERE id = 1",
                    (prune_before,))
    conn.commit()

    if total_rows or (time.time() - t0) > 5:
        print(f"rolled up through {end}: {total_rows} minute-rows written in {time.time() - t0:.1f}s")
    cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
    cur.fetchall()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
Downsample old boat_monitoring readings to one sample per minute.

mqtt_readings (from mqtt_logger.py) and victron_readings (same logger,
Victron VRM data) log every value change with no retention limit -- as of
2026-08-21, mqtt_readings alone was 8.85M rows and growing, going back to
2026-06-27, with victron_readings at 191M rows. Nothing else in this project
prunes or compacts them (checked: no cron, no systemd timer, no MariaDB
event, no code in mqtt_logger.py/dashboard_api.py).

This script keeps everything newer than --retention-days at full
resolution, and for anything older, replaces however many samples fell in
each calendar minute with exactly one row for that minute -- an average for
numeric topics (data_type float/int), the most recent value in that minute
for status/string topics (data_type string), since those can't be averaged.
A minute that already has only one sample is left alone.

Safety notes:
  - Defaults to a dry run (reports what it would do). Pass --apply to
    actually delete/insert.
  - Idempotent and safe to re-run or schedule on a timer: already-compacted
    minutes have exactly one row and are skipped (HAVING COUNT(*) > 1), and
    every query is bounded to ts < the retention cutoff, so it can never
    touch recent data.
  - Each (topic, day) chunk is one transaction, so an interrupted run just
    leaves the remaining days for next time rather than corrupting anything.

Usage:
  python3 db_downsample.py                                   # dry run, mqtt, 30 days
  python3 db_downsample.py --apply --archive-dir ~/db_archive # compact, with a rollback copy
  python3 db_downsample.py --apply                            # compact mqtt_readings, no archive
  python3 db_downsample.py --source victron --apply            # same, for victron_readings
  python3 db_downsample.py --topic boat/watermaker/pressure/hp --apply   # one topic, for testing
  python3 db_downsample.py --retention-days 14 --apply
"""
import argparse
import csv
import gzip
import os
import sys
from datetime import datetime, timedelta
import mysql.connector

SOURCES = {
    'mqtt':    {'readings': 'mqtt_readings',    'topics': 'mqtt_topics'},
    'victron': {'readings': 'victron_readings', 'topics': 'victron_topics'},
}


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def get_db():
    s = get_secrets()
    return mysql.connector.connect(
        host='localhost', user='mikemc', password=s.get('DB_PASS', ''),
        database='boat_monitoring', connection_timeout=10,
    )


def numeric_buckets(cur, readings_table, topic_id, day_start, day_end):
    """Minutes with >1 numeric sample in [day_start, day_end): (bucket_ts, avg_value, n)."""
    cur.execute(f"""
        SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts)/60)*60) AS bucket_ts,
               AVG(CAST(value AS DECIMAL(20,4))) AS avg_value,
               COUNT(*) AS n
        FROM {readings_table}
        WHERE topic_id = %s AND ts >= %s AND ts < %s
        GROUP BY bucket_ts
        HAVING COUNT(*) > 1
    """, (topic_id, day_start, day_end))
    return cur.fetchall()


def string_buckets(cur, readings_table, topic_id, day_start, day_end):
    """Minutes with >1 string sample: (bucket_ts, most_recent_value, n). Window functions,
    not GROUP_CONCAT, so a busy minute can't silently truncate past group_concat_max_len."""
    cur.execute(f"""
        SELECT bucket_ts, value, n FROM (
            SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts)/60)*60) AS bucket_ts, value,
                   ROW_NUMBER() OVER (PARTITION BY FLOOR(UNIX_TIMESTAMP(ts)/60) ORDER BY ts DESC) AS rn,
                   COUNT(*)     OVER (PARTITION BY FLOOR(UNIX_TIMESTAMP(ts)/60)) AS n
            FROM {readings_table}
            WHERE topic_id = %s AND ts >= %s AND ts < %s
        ) ranked
        WHERE rn = 1 AND n > 1
    """, (topic_id, day_start, day_end))
    return cur.fetchall()


def archive_day_rows(cur, readings_table, topic_id, topic_name, day, day_end, archive_dir, source):
    """Writes every raw row about to be compacted for [day, day_end) to a gzip CSV
    before any DELETE happens, as a rollback path for this still-new script's first
    real runs -- deterministic filename, so re-running a day just overwrites with
    the same content rather than piling up duplicates."""
    cur.execute(f"""
        SELECT ts, value FROM (
            SELECT ts, value,
                   COUNT(*) OVER (PARTITION BY FLOOR(UNIX_TIMESTAMP(ts)/60)) AS n
            FROM {readings_table}
            WHERE topic_id = %s AND ts >= %s AND ts < %s
        ) t WHERE n > 1
        ORDER BY ts
    """, (topic_id, day, day_end))
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(archive_dir, exist_ok=True)
    safe_topic = topic_name.replace('/', '_')
    path = os.path.join(archive_dir, f"{source}_{safe_topic}_{day:%Y-%m-%d}.csv.gz")
    with gzip.open(path, 'wt', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ts', 'value'])
        writer.writerows(rows)
    return len(rows)


def apply_bucket(cur, readings_table, topic_id, bucket_ts, value):
    bucket_end = bucket_ts + timedelta(minutes=1)
    cur.execute(
        f"DELETE FROM {readings_table} WHERE topic_id = %s AND ts >= %s AND ts < %s",
        (topic_id, bucket_ts, bucket_end),
    )
    deleted = cur.rowcount
    cur.execute(
        f"INSERT INTO {readings_table} (ts, topic_id, value) VALUES (%s, %s, %s)",
        (bucket_ts, topic_id, str(value)),
    )
    return deleted


def process_topic(conn, cur, readings_table, topic_id, topic_name, data_type, cutoff, apply,
                   archive_dir=None, source=None):
    cur.execute(
        f"SELECT MIN(ts), MAX(ts) FROM {readings_table} WHERE topic_id = %s AND ts < %s",
        (topic_id, cutoff),
    )
    min_ts, max_ts = cur.fetchone()
    if min_ts is None:
        return 0, 0  # nothing older than the cutoff for this topic

    total_old, total_new = 0, 0
    day = min_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    end = min(max_ts, cutoff)
    get_buckets = string_buckets if data_type == 'string' else numeric_buckets

    while day < end:
        day_end = min(day + timedelta(days=1), cutoff)
        if apply and archive_dir:
            # Archived and compacted inside the same transaction as the deletes
            # below (committed together) -- if this run is interrupted before
            # commit, the archive file may exist without the DB having changed
            # yet, which is harmless: the next run just re-archives (overwriting
            # the same deterministic filename) and compacts as normal.
            archive_day_rows(cur, readings_table, topic_id, topic_name, day, day_end, archive_dir, source)
        for bucket_ts, value, n in get_buckets(cur, readings_table, topic_id, day, day_end):
            if apply:
                deleted = apply_bucket(cur, readings_table, topic_id, bucket_ts, value)
                total_old += deleted
            else:
                total_old += n
            total_new += 1
        if apply:
            conn.commit()  # one transaction per (topic, day) -- bounded, resumable if interrupted
        day = day_end  # day_end > day always holds here since day < end <= cutoff

    return total_old, total_new


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--source', choices=sorted(SOURCES), default='mqtt', help='Which logger\'s tables to compact (default: mqtt)')
    ap.add_argument('--retention-days', type=int, default=30, help='Keep full resolution for this many days (default: 30)')
    ap.add_argument('--apply', action='store_true', help='Actually delete/insert. Without this, only reports what would happen.')
    ap.add_argument('--topic', help='Only process one topic (exact match), useful for testing before a full run')
    ap.add_argument('--archive-dir', help='Write every raw row to a gzip CSV here before compacting it (only takes '
                                           'effect with --apply). A rollback path for early runs -- skip it once '
                                           'you trust the script, since archiving here defeats the space savings.')
    args = ap.parse_args()

    cfg = SOURCES[args.source]
    cutoff = datetime.now() - timedelta(days=args.retention_days)
    mode = 'APPLY' if args.apply else 'DRY RUN'
    print(f"[{mode}] source={args.source} retention_days={args.retention_days} cutoff={cutoff}")
    if args.archive_dir and not args.apply:
        print(f"  (--archive-dir ignored in dry run -- nothing is deleted, so nothing to archive)")
    elif args.archive_dir:
        print(f"  archiving raw rows to {args.archive_dir} before each delete")

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    query = f"SELECT id, topic, data_type FROM {cfg['topics']} WHERE data_type IN ('float','int','string')"
    params = ()
    if args.topic:
        query += " AND topic = %s"
        params = (args.topic,)
    cur.execute(query, params)
    topics = cur.fetchall()
    if args.topic and not topics:
        print(f"No topic '{args.topic}' found in {cfg['topics']} (or it has no data_type set)")
        sys.exit(1)

    archive_dir = args.archive_dir if args.apply else None  # dry runs never archive -- nothing gets deleted

    grand_old, grand_new = 0, 0
    for topic_id, topic_name, data_type in topics:
        old, new = process_topic(conn, cur, cfg['readings'], topic_id, topic_name, data_type, cutoff, args.apply,
                                  archive_dir=archive_dir, source=args.source)
        if old:
            print(f"  {topic_name}: {old} rows -> {new} rows ({old - new} removed)")
        grand_old += old
        grand_new += new

    cur.close()
    conn.close()
    print(f"[{mode}] done. {grand_old} old rows -> {grand_new} compacted rows "
          f"(~{grand_old - grand_new} rows {'removed' if args.apply else 'would be removed'})")


if __name__ == '__main__':
    main()

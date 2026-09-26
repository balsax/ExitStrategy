"""
Electrical tab history recorder (siloed addition, used by dashboard_api.py).

VRM keeps history for the devices on the Cerbo (48V bank, MultiPlus, 12V house
SmartShunt), but nothing stored the rest of the Electrical tab: the simulated
shunts/MPPTs/alternator (boat/sim/victron/*, deliberately kept out of MariaDB
by mqtt_logger.py), the Orions over Bluetooth (boat/victron/*), and the DC
loads the dashboard derives. This samples all of them every SAMPLE_S seconds
into a small SQLite file next to this module:

  raw     every SAMPLE_S seconds, kept RAW_KEEP_S (2 days)
  rollup  5-minute averages, kept ROLLUP_KEEP_S (30 days)

Delete electrical_history.db (and this module's hooks in dashboard_api.py) to
back it out; nothing else reads it.
"""
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'electrical_history.db')
SAMPLE_S = 20
ROLLUP_S = 300
RAW_KEEP_S = 2 * 86400
ROLLUP_KEEP_S = 30 * 86400
STALE_S = 60          # ignore MQTT values older than this (source stopped)

# MQTT prefix -> key prefix stripped (keys come out as <device>_<field>,
# same as /api/victron/sim and /api/victron/ble)
SOURCES = ('boat/sim/victron/', 'boat/victron/')

# Chart range -> bucket seconds
RANGE_SECONDS = {'10m': 600, '1h': 3600, '6h': 21600, '24h': 86400, '7d': 604800, '30d': 2592000}
RANGE_BUCKET = {'10m': 20, '1h': 60, '6h': 300, '24h': 900, '7d': 3600, '30d': 14400}


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def _init():
    with _connect() as c:
        c.execute('CREATE TABLE IF NOT EXISTS raw (k TEXT NOT NULL, ts INTEGER NOT NULL, v REAL NOT NULL)')
        c.execute('CREATE INDEX IF NOT EXISTS raw_k_ts ON raw (k, ts)')
        c.execute('CREATE TABLE IF NOT EXISTS rollup (k TEXT NOT NULL, ts INTEGER NOT NULL, v REAL NOT NULL, '
                  'PRIMARY KEY (k, ts))')


def _snapshot(mqtt_state, mqtt_lock):
    """Current numeric values, flattened like the /api/victron/sim|ble routes, plus derived DC loads."""
    cutoff = time.time() - STALE_S
    with mqtt_lock:
        items = [(t, dict(v)) for t, v in mqtt_state['topics'].items() if t.startswith(SOURCES)]
    out = {}
    for topic, entry in items:
        try:
            if datetime.fromisoformat(entry['time']).timestamp() < cutoff:
                continue
            val = float(entry['value'])
        except (KeyError, ValueError, TypeError):
            continue
        prefix = next(p for p in SOURCES if topic.startswith(p))
        out[topic[len(prefix):].replace('/', '_')] = val

    # DC loads, same rule as the Electrical tab: a battery's discharge;
    # the house uses the (simulated) house load total.
    for k, i_key, v_key in (('12s', 'shunt_diesel_current', 'shunt_diesel_voltage'),
                            ('bt', 'shunt_thruster_current', 'shunt_thruster_voltage')):
        if i_key in out and v_key in out:
            out[f'load_{k}_w'] = max(0.0, -out[i_key]) * out[v_key]
    if 'house_load_power' in out:
        out['load_12h_w'] = out['house_load_power']
    if 'dc48_load_power' in out:
        out['load_48_w'] = out['dc48_load_power']
    return out


def _rollup_and_prune(conn, now):
    # Fold finished 5-minute buckets from raw into rollup (idempotent upsert).
    last = conn.execute('SELECT MAX(ts) FROM rollup').fetchone()[0] or 0
    upto = int(now // ROLLUP_S) * ROLLUP_S
    conn.execute(f'''INSERT OR REPLACE INTO rollup (k, ts, v)
                     SELECT k, (ts / {ROLLUP_S}) * {ROLLUP_S} AS b, AVG(v) FROM raw
                     WHERE ts >= ? AND ts < ? GROUP BY k, b''', (last, upto))
    conn.execute('DELETE FROM raw WHERE ts < ?', (now - RAW_KEEP_S,))
    conn.execute('DELETE FROM rollup WHERE ts < ?', (now - ROLLUP_KEEP_S,))


def _loop(mqtt_state, mqtt_lock):
    _init()
    last_rollup = 0
    while True:
        time.sleep(SAMPLE_S)
        try:
            now = int(time.time())
            snap = _snapshot(mqtt_state, mqtt_lock)
            with _connect() as conn:
                if snap:
                    conn.executemany('INSERT INTO raw (k, ts, v) VALUES (?, ?, ?)',
                                     [(k, now, v) for k, v in snap.items()])
                if now - last_rollup >= ROLLUP_S:
                    _rollup_and_prune(conn, now)
                    last_rollup = now
        except Exception as e:  # never let the recorder take the dashboard down
            print(f'electrical_history: {e}')


def start(mqtt_state, mqtt_lock):
    threading.Thread(target=_loop, args=(mqtt_state, mqtt_lock), daemon=True).start()


def series(keys, range_val):
    """{key: {'times': [...local ISO...], 'values': [...]}} bucketed for the chart range."""
    seconds = RANGE_SECONDS.get(range_val, 3600)
    bucket = RANGE_BUCKET.get(range_val, 60)
    now = int(time.time())
    start_ts = now - seconds
    raw_from = now - RAW_KEEP_S
    out = {}
    if not os.path.exists(DB_PATH):
        return {k: {'times': [], 'values': []} for k in keys}
    with _connect() as conn:
        for k in keys:
            rows = []
            if start_ts < raw_from:  # older part from the 5-minute rollup
                rows += conn.execute(f'''SELECT (ts / {bucket}) * {bucket} AS b, AVG(v) FROM rollup
                                         WHERE k = ? AND ts >= ? AND ts < ? GROUP BY b ORDER BY b''',
                                     (k, start_ts, raw_from)).fetchall()
            rows += conn.execute(f'''SELECT (ts / {bucket}) * {bucket} AS b, AVG(v) FROM raw
                                     WHERE k = ? AND ts >= ? GROUP BY b ORDER BY b''',
                                 (k, max(start_ts, raw_from))).fetchall()
            out[k] = {
                'times': [datetime.fromtimestamp(b).strftime('%Y-%m-%dT%H:%M:%S') for b, _ in rows],
                'values': [round(v, 3) for _, v in rows],
            }
    return out

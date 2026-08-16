from flask import Flask, jsonify, request, send_from_directory
import requests
import os
import time
import json
import math
import threading
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

@app.route('/')
def index():
    return send_from_directory('static-src', 'index.html')

@app.route('/assets/<path:filename>')
def assets(filename):
    return send_from_directory('static-src/assets', filename)

def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets

# ─── MQTT diagnostics ────────────────────────────────────────────────────────
# Background subscriber that mirrors every retained/live topic on the broker
# into memory so the dashboard can poll a REST snapshot of it.
mqtt_state = {
    'connected': False,
    'topics': {},       # topic -> {value, time, qos, retain}
    'message_count': 0,
}
mqtt_lock = threading.Lock()
mqtt_client = None  # set once the background client connects; used to publish commands

def start_mqtt_listener():
    def on_connect(client, userdata, flags, reason_code, properties=None):
        mqtt_state['connected'] = (str(reason_code) == 'Success' or reason_code == 0)
        client.subscribe('#')

    def on_disconnect(client, userdata, flags, reason_code=None, properties=None):
        mqtt_state['connected'] = False

    def on_message(client, userdata, msg):
        try:
            value = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            value = repr(msg.payload)
        with mqtt_lock:
            mqtt_state['topics'][msg.topic] = {
                'value': value,
                'time': datetime.now(timezone.utc).isoformat(),
                'qos': msg.qos,
                'retain': msg.retain,
            }
            mqtt_state['message_count'] += 1

    def run():
        global mqtt_client
        s = get_secrets()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if s.get('MQTT_USER'):
            client.username_pw_set(s['MQTT_USER'], s.get('MQTT_PASS', ''))
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        mqtt_client = client
        while True:
            try:
                client.connect('localhost', 1883, keepalive=30)
                client.loop_forever(retry_first_connection=True)
            except Exception:
                mqtt_state['connected'] = False
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()

def query_influx(flux):
    s = get_secrets()
    res = requests.post(
        f"{s['INFLUX_URL']}/api/v2/query?org={s['INFLUX_ORG']}",
        headers={
            'Authorization': f"Token {s['INFLUX_TOKEN']}",
            'Content-Type': 'application/vnd.flux',
            'Accept': 'application/csv'
        },
        data=flux
    )
    return res.text

def parse_last(text):
    header = None
    for line in text.strip().split('\n'):
        if line.startswith('#') or not line.strip():
            continue
        cols = line.split(',')
        if '_value' in cols:
            header = cols
            continue
        if header:
            row = dict(zip(header, cols))
            try:
                return float(row['_value'])
            except:
                continue
    return None

def parse_series(text):
    header = None
    times = []
    values = []
    for line in text.strip().split('\n'):
        if line.startswith('#') or not line.strip():
            continue
        cols = line.split(',')
        if '_value' in cols:
            header = cols
            continue
        if header:
            row = dict(zip(header, cols))
            try:
                times.append(row['_time'])
                values.append(float(row['_value']))
            except:
                continue
    return times, values

def get_vrm_data():
    s = get_secrets()
    res = requests.get(
        f"https://vrmapi.victronenergy.com/v2/installations/{s['VRM_INSTALL_ID']}/diagnostics?count=500",
        headers={'x-authorization': f"Token {s['VRM_TOKEN']}"},
        timeout=10
    )
    data = res.json()
    if not data.get('success'):
        return {}

    # Extended attribute map - covers all cards
    want = {
        # 48V Battery / BMS
        'bs':  'soc',
        'bv':  'voltage',
        'bc':  'current',
        'bp':  'power',
        'bst': 'state',
        'bT':  'temp',
        'SOH': 'soh',
        'mcV': 'cell_min',
        'McV': 'cell_max',
        'tTTG': 'time_to_go',
        'bAC': 'consumed_ah',
        # Multiplus / AC
        'a1':  'ac_load',
        'g1':  'grid',
        'mV':  'mp_voltage_in',
        'mA':  'mp_current_in',
        'mVO': 'mp_voltage_out',
        'mAO': 'mp_current_out',
        'ms':  'mp_state',
        # 12V House SmartShunt
        'Bv':  'v_12v',
        'Bc':  'i_12v',
        'Bs':  'soc_12v',
        'BT':  'temp_12v',
        'BTTG':'ttg_12v',
        'BAh': 'cah_12v',
        # Orion DC-DC (may appear as separate devices)
        'o1s': 'orion1_state',
        'o2s': 'orion2_state',
    }

    result = {}
    for r in data.get('records', []):
        code = r.get('code')
        if code in want:
            key = want[code]
            raw = r.get('rawValue')
            fmt = r.get('formattedValue', '')
            try:
                result[key] = float(raw)
            except:
                result[key] = fmt

    # Stash raw records for diagnostics endpoint
    result['_raw_codes'] = [
        {'code': r.get('code'), 'desc': r.get('description', ''), 'val': r.get('formattedValue', '')}
        for r in data.get('records', [])
    ]

    return result

@app.route('/api/sensor')
def sensor():
    s = get_secrets()
    data = {}
    for field in ['temp_f', 'humidity', 'pressure', 'iaq', 'co2_ppm', 'voc_ppm']:
        flux = f'''from(bucket:"{s['INFLUX_BUCKET']}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._field == "{field}")
  |> last()'''
        val = parse_last(query_influx(flux))
        if val is not None:
            data[field] = round(val, 2)
    return jsonify(data)

@app.route('/api/victron')
def victron():
    data = get_vrm_data()
    # Don't send raw codes to the main dashboard
    data.pop('_raw_codes', None)
    return jsonify(data)

@app.route('/api/victron/diagnostics')
def victron_diagnostics():
    """Returns all available VRM attribute codes — useful for discovery"""
    data = get_vrm_data()
    return jsonify(data.get('_raw_codes', []))

@app.route('/api/victron/history')
def victron_history():
    """
    Fetches time-series data for a VRM attribute from the Graph widget endpoint.
    Query params:
      code  - VRM attribute code (e.g. 'bs' for SOC, 'bv' for voltage)
      range - 1h | 6h | 24h | 7d | 30d
    """
    s = get_secrets()
    code = request.args.get('code', 'bs')
    range_val = request.args.get('range', '1h')

    range_seconds = {
        '1h':  3600,
        '6h':  21600,
        '24h': 86400,
        '7d':  604800,
        '30d': 2592000,
    }
    seconds = range_seconds.get(range_val, 3600)

    now = int(datetime.now(timezone.utc).timestamp())
    start = now - seconds

    try:
        res = requests.get(
            f"https://vrmapi.victronenergy.com/v2/installations/{s['VRM_INSTALL_ID']}/widgets/Graph",
            headers={'x-authorization': f"Token {s['VRM_TOKEN']}"},
            params={
                'attributeCodes[]': code,
                'start': start,
                'end': now,
                'type': 'custom',
            },
            timeout=15
        )
        data = res.json()

        if not data.get('success'):
            return jsonify({'times': [], 'values': [], 'error': 'VRM API error'})

        records = data.get('records', {})
        # VRM Graph returns: records -> {code -> [[timestamp_ms, value], ...]}
        series = None
        for key, val in records.items():
            if isinstance(val, list) and len(val) > 0:
                series = val
                break

        if not series:
            return jsonify({'times': [], 'values': []})

        times = []
        values = []
        for point in series:
            if len(point) >= 2 and point[1] is not None:
                ts_ms = point[0]
                dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                times.append(dt.isoformat())
                values.append(float(point[1]))

        return jsonify({'times': times, 'values': values})

    except Exception as e:
        return jsonify({'times': [], 'values': [], 'error': str(e)})

@app.route('/api/trend')
def trend():
    field = request.args.get('field', 'temp_f')
    range_val = request.args.get('range', '1h')
    s = get_secrets()

    window_map = {
        '1h': '2m', '6h': '10m', '24h': '30m',
        '7d': '3h', '30d': '12h'
    }
    window = window_map.get(range_val, '5m')

    flux = f'''from(bucket:"{s['INFLUX_BUCKET']}")
  |> range(start: -{range_val})
  |> filter(fn: (r) => r._field == "{field}")
  |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)'''

    text = query_influx(flux)
    times, values = parse_series(text)
    return jsonify({'times': times, 'values': values})

@app.route('/api/mqtt/topics')
def mqtt_topics():
    with mqtt_lock:
        topics = dict(mqtt_state['topics'])
    return jsonify({
        'connected': mqtt_state['connected'],
        'count': len(topics),
        'message_count': mqtt_state['message_count'],
        'topics': topics,
    })

WATERMAKER_MODES = {'start', 'stop', 'flush', 'auto', 'manual'}
WATERMAKER_DEVICES = {'pump', 'boost_pump', 'divert', 'flush'}

@app.route('/api/watermaker/control', methods=['POST'])
def watermaker_control():
    data = request.get_json(silent=True) or {}
    mode = data.get('mode')
    if mode not in WATERMAKER_MODES:
        return jsonify({'error': f'mode must be one of {sorted(WATERMAKER_MODES)}'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/watermaker/cmd/mode', mode)
    return jsonify({'status': 'sent', 'topic': 'boat/watermaker/cmd/mode', 'mode': mode})

@app.route('/api/watermaker/device', methods=['POST'])
def watermaker_device():
    data = request.get_json(silent=True) or {}
    device = data.get('device')
    state = data.get('state')
    if device not in WATERMAKER_DEVICES:
        return jsonify({'error': f'device must be one of {sorted(WATERMAKER_DEVICES)}'}), 400
    if str(state) not in ('0', '1'):
        return jsonify({'error': 'state must be 0 or 1'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    payload = str(state)
    topic = f'boat/watermaker/cmd/{device}'
    mqtt_client.publish(topic, payload)
    return jsonify({'status': 'sent', 'topic': topic, 'state': payload})

@app.route('/api/watermaker/pump_speed', methods=['POST'])
def watermaker_pump_speed():
    data = request.get_json(silent=True) or {}
    try:
        speed = int(data.get('speed'))
    except (TypeError, ValueError):
        return jsonify({'error': 'speed must be an integer 0-100'}), 400
    if not (0 <= speed <= 100):
        return jsonify({'error': 'speed must be between 0 and 100'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/watermaker/cmd/pump_speed', str(speed))
    return jsonify({'status': 'sent', 'topic': 'boat/watermaker/cmd/pump_speed', 'speed': speed})

# ─── Smart relay (boat/power/relay1) ───────────────────────────────────────────
# Command payloads ('1' for on, 'o' for off) match this relay's firmware exactly
# as given. Status is read by the frontend from boat/power/relay1/0/get (value
# '0'/'1') — confirmed live on the broker, a different topic and vocabulary
# than the /set command side.
@app.route('/api/relay/set', methods=['POST'])
def relay_set():
    data = request.get_json(silent=True) or {}
    state = data.get('state')
    if state not in ('on', 'off'):
        return jsonify({'error': "state must be 'on' or 'off'"}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    payload = '1' if state == 'on' else 'o'
    topic = 'boat/power/relay1/0/set'
    mqtt_client.publish(topic, payload)
    return jsonify({'status': 'sent', 'topic': topic, 'state': state, 'payload': payload})

@app.route('/api/relay/query', methods=['POST'])
def relay_query():
    # boat/power/relay1/0/get isn't retained, so a dashboard that just loaded
    # has no way to know current state until something happens to trigger a
    # fresh publish. Confirmed live: publishing an empty payload to that same
    # /get topic (not /set — never touches the command side) makes the device
    # report its real status right back on it.
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/power/relay1/0/get', payload=None)
    return jsonify({'status': 'queried', 'topic': 'boat/power/relay1/0/get'})

# ─── Engine compartment fan control (boat/engine/fan) ──────────────────────────
# Setpoint/timeout topics are both the status and the config channel — this
# device reads back its own accepted value on the same topic it's set on
# (confirmed live: relay/command already echoes 'AUTO' after being set).
@app.route('/api/engine_fan/command', methods=['POST'])
def engine_fan_command():
    data = request.get_json(silent=True) or {}
    command = data.get('command')
    if command not in ('AUTO', 'MANUAL_ON', 'MANUAL_OFF'):
        return jsonify({'error': "command must be 'AUTO', 'MANUAL_ON', or 'MANUAL_OFF'"}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/engine/fan/relay/command', command)
    return jsonify({'status': 'sent', 'topic': 'boat/engine/fan/relay/command', 'command': command})

@app.route('/api/engine_fan/setpoints', methods=['POST'])
def engine_fan_setpoints():
    data = request.get_json(silent=True) or {}
    try:
        on_f = float(data.get('on_f'))
        off_f = float(data.get('off_f'))
    except (TypeError, ValueError):
        return jsonify({'error': 'on_f and off_f must be numbers'}), 400
    if off_f >= on_f:
        return jsonify({'error': 'off_f must be less than on_f'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/engine/fan/setpoint/on', str(on_f))
    mqtt_client.publish('boat/engine/fan/setpoint/off', str(off_f))
    return jsonify({'status': 'sent', 'on_f': on_f, 'off_f': off_f})

@app.route('/api/engine_fan/timeout', methods=['POST'])
def engine_fan_timeout():
    data = request.get_json(silent=True) or {}
    try:
        minutes = int(data.get('minutes'))
    except (TypeError, ValueError):
        return jsonify({'error': 'minutes must be an integer'}), 400
    if minutes < 0:
        return jsonify({'error': 'minutes must be 0 or positive'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/engine/fan/manual/timeout', str(minutes))
    return jsonify({'status': 'sent', 'minutes': minutes})

# ─── Watermaker trend history (reads from the existing MariaDB MQTT logger) ────
# boat_monitoring.mqtt_readings is populated by a pre-existing logger service —
# this dashboard only reads from it, it does not write.
WATERMAKER_METRIC_TOPICS = {
    'membrane':   'boat/watermaker/pressure/hp',
    'feed':       'boat/watermaker/pressure/postfilter',
    'flow':       'boat/watermaker/flow/rate',          # mL/min, converted to gph below
    'cond':       'boat/watermaker/flow/conductivity_comp',
    'rpm':        'boat/watermaker/pump/rpm',
    'current':    'boat/watermaker/pump/current',
    'efficiency': 'boat/watermaker/efficiency',
    'tank':       'boat/watermaker/tank/level',
}
WATERMAKER_RANGE_SECONDS = {'1h': 3600, '6h': 21600, '24h': 86400, '7d': 604800, '30d': 2592000}
WATERMAKER_RANGE_BUCKET = {'1h': 30, '6h': 120, '24h': 600, '7d': 3600, '30d': 14400}

def get_boat_db():
    s = get_secrets()
    return mysql.connector.connect(
        host='localhost', user='mikemc', password=s.get('DB_PASS', ''),
        database='boat_monitoring', connection_timeout=5,
    )

def query_bucketed_series(cursor, topic, start_dt, bucket_seconds):
    cursor.execute("SELECT id FROM mqtt_topics WHERE topic = %s", (topic,))
    row = cursor.fetchone()
    if not row:
        return {}
    topic_id = row[0]
    cursor.execute("""
        SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts)/%s)*%s) AS bucket_ts,
               AVG(CAST(value AS DECIMAL(20,4))) AS avg_value
        FROM mqtt_readings
        WHERE topic_id = %s AND ts >= %s
        GROUP BY bucket_ts
        ORDER BY bucket_ts
    """, (bucket_seconds, bucket_seconds, topic_id, start_dt))
    return {r[0]: float(r[1]) for r in cursor.fetchall() if r[1] is not None}

@app.route('/api/watermaker/history')
def watermaker_history():
    metric = request.args.get('metric', 'membrane')
    range_val = request.args.get('range', '1h')
    if metric != 'filterdp' and metric not in WATERMAKER_METRIC_TOPICS:
        return jsonify({'times': [], 'values': [], 'error': 'unknown metric'}), 400

    bucket = WATERMAKER_RANGE_BUCKET.get(range_val, 30)
    seconds = WATERMAKER_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)  # mqtt_readings.ts is local time, not UTC

    try:
        conn = get_boat_db()
        cur = conn.cursor()

        if metric == 'filterdp':
            pre = query_bucketed_series(cur, 'boat/watermaker/pressure/prefilter', start_dt, bucket)
            post = query_bucketed_series(cur, 'boat/watermaker/pressure/postfilter', start_dt, bucket)
            keys = sorted(set(pre.keys()) & set(post.keys()))
            times = [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys]
            values = [round(pre[k] - post[k], 2) for k in keys]
        else:
            series = query_bucketed_series(cur, WATERMAKER_METRIC_TOPICS[metric], start_dt, bucket)
            keys = sorted(series.keys())
            times = [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys]
            values = [round(series[k], 2) for k in keys]
            if metric == 'flow':
                values = [round(v * 60 / 3785.411784, 2) for v in values]  # mL/min -> gph

        conn.close()
        return jsonify({'times': times, 'values': values})
    except Exception as e:
        return jsonify({'times': [], 'values': [], 'error': str(e)})

# ─── Anchor watch ────────────────────────────────────────────────────────────
# Anchor position/radius is persisted to a small JSON file (not a DB — this is
# a single current value, not a time series) so it survives page reloads,
# different devices, and API restarts while the boat is actually anchored.
ANCHOR_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anchor_state.json')
ANCHOR_STATE_LOCK = threading.Lock()

def load_anchor_state():
    with ANCHOR_STATE_LOCK:
        if os.path.exists(ANCHOR_STATE_FILE):
            with open(ANCHOR_STATE_FILE) as f:
                return json.load(f)
    return {'active': False, 'anchor_lat': None, 'anchor_lon': None, 'radius_m': 30,
            'dropped_at': None, 'depth_alarm_m': 1.8288}

def save_anchor_state(state):
    with ANCHOR_STATE_LOCK:
        with open(ANCHOR_STATE_FILE, 'w') as f:
            json.dump(state, f)

GPS_STALE_THRESHOLD_S = 30  # GPS publishes ~1/s — this is a generous margin before treating it as lost

def get_gps_age_s():
    """Seconds since the last GPS fix was received, or None if one has never been seen."""
    with mqtt_lock:
        lat_rec = mqtt_state['topics'].get('boat/nav/gps/latitude')
        lon_rec = mqtt_state['topics'].get('boat/nav/gps/longitude')
    if not lat_rec or not lon_rec:
        return None
    try:
        newest = min(datetime.fromisoformat(lat_rec['time']), datetime.fromisoformat(lon_rec['time']))
    except (KeyError, ValueError):
        return None
    return (datetime.now(timezone.utc) - newest).total_seconds()

def get_gps_position(max_age_s=None):
    with mqtt_lock:
        lat_rec = mqtt_state['topics'].get('boat/nav/gps/latitude')
        lon_rec = mqtt_state['topics'].get('boat/nav/gps/longitude')
    if not lat_rec or not lon_rec:
        return None
    if max_age_s is not None:
        age_s = get_gps_age_s()
        if age_s is None or age_s > max_age_s:
            return None  # stale — refuse to hand back a frozen position as if it were current
    return float(lat_rec['value']), float(lon_rec['value'])

# In-memory breadcrumb trail, recorded by the background monitor thread every
# ANCHOR_MONITOR_INTERVAL_S regardless of whether a browser tab is open, so the
# track doesn't skip straight from wherever it was when a tab was last closed
# to wherever the boat is now. Not persisted to disk — it's only meaningful
# for the current anchoring session, and gets cleared on drop/raise.
ANCHOR_TRAIL_MAX_POINTS = 5000  # ~7 hours of history at the 5s monitor interval
ANCHOR_TRAIL_LOCK = threading.Lock()
_anchor_trail = []

def clear_anchor_trail():
    with ANCHOR_TRAIL_LOCK:
        _anchor_trail.clear()

def record_anchor_trail_point(lat, lon):
    with ANCHOR_TRAIL_LOCK:
        _anchor_trail.append({'lat': lat, 'lon': lon, 't': datetime.now(timezone.utc).isoformat()})
        if len(_anchor_trail) > ANCHOR_TRAIL_MAX_POINTS:
            del _anchor_trail[:len(_anchor_trail) - ANCHOR_TRAIL_MAX_POINTS]

@app.route('/api/anchor/state')
def anchor_state():
    return jsonify(load_anchor_state())

@app.route('/api/anchor/trail')
def anchor_trail_route():
    with ANCHOR_TRAIL_LOCK:
        return jsonify(list(_anchor_trail))

@app.route('/api/anchor/drop', methods=['POST'])
def anchor_drop():
    data = request.get_json(silent=True) or {}
    try:
        radius_m = float(data.get('radius_m', 30))
    except (TypeError, ValueError):
        return jsonify({'error': 'radius_m must be a number'}), 400
    if radius_m <= 0:
        return jsonify({'error': 'radius_m must be positive'}), 400

    # An explicit anchor_lat/anchor_lon (e.g. the user clicked a spot ~100ft
    # ahead of the boat on the overhead view, since that's where the anchor
    # actually ends up, not at the boat's own GPS position) overrides the
    # boat's current position.
    anchor_lat = data.get('anchor_lat')
    anchor_lon = data.get('anchor_lon')
    if anchor_lat is not None or anchor_lon is not None:
        try:
            anchor_lat = float(anchor_lat)
            anchor_lon = float(anchor_lon)
        except (TypeError, ValueError):
            return jsonify({'error': 'anchor_lat/anchor_lon must both be numbers'}), 400
        if not (-90 <= anchor_lat <= 90 and -180 <= anchor_lon <= 180):
            return jsonify({'error': 'anchor_lat/anchor_lon out of range'}), 400
    else:
        pos = get_gps_position(max_age_s=GPS_STALE_THRESHOLD_S)
        if pos is None:
            return jsonify({'error': 'No live GPS position available (missing or stale)'}), 503
        anchor_lat, anchor_lon = pos

    prior = load_anchor_state()
    state = {
        'active': True,
        'anchor_lat': anchor_lat,
        'anchor_lon': anchor_lon,
        'radius_m': radius_m,
        'dropped_at': datetime.now(timezone.utc).isoformat(),
        'depth_alarm_m': prior.get('depth_alarm_m', 1.8288),
    }
    save_anchor_state(state)
    clear_anchor_trail()
    return jsonify(state)

@app.route('/api/anchor/radius', methods=['POST'])
def anchor_set_radius():
    data = request.get_json(silent=True) or {}
    try:
        radius_m = float(data.get('radius_m'))
    except (TypeError, ValueError):
        return jsonify({'error': 'radius_m must be a number'}), 400
    if radius_m <= 0:
        return jsonify({'error': 'radius_m must be positive'}), 400
    state = load_anchor_state()
    state['radius_m'] = radius_m
    save_anchor_state(state)
    return jsonify(state)

@app.route('/api/anchor/depth_alarm', methods=['POST'])
def anchor_set_depth_alarm():
    data = request.get_json(silent=True) or {}
    try:
        depth_alarm_m = float(data.get('depth_alarm_m'))
    except (TypeError, ValueError):
        return jsonify({'error': 'depth_alarm_m must be a number'}), 400
    if depth_alarm_m <= 0:
        return jsonify({'error': 'depth_alarm_m must be positive'}), 400
    state = load_anchor_state()
    state['depth_alarm_m'] = depth_alarm_m
    save_anchor_state(state)
    return jsonify(state)

@app.route('/api/anchor/raise', methods=['POST'])
def anchor_raise():
    state = load_anchor_state()
    state['active'] = False
    save_anchor_state(state)
    clear_anchor_trail()
    return jsonify(state)

# ─── Anchor alarm dispatch (ntfy push + optional GPIO buzzer) ──────────────────
# Runs in a background thread independent of any browser tab, so drag/depth
# alarms still fire with no dashboard open. Notifies immediately on a state
# transition, then re-notifies every ANCHOR_NOTIFY_REPEAT_S while still
# tripped, rather than either spamming every poll or going silent.
ANCHOR_MONITOR_INTERVAL_S = 5
ANCHOR_NOTIFY_REPEAT_S = 180
M_PER_FT = 0.3048
ANCHOR_GPIO_PIN = int(os.environ.get('ANCHOR_GPIO_PIN', 17))  # BCM numbering — GPIO7-11 and GPIO25 are used by the CAN HAT (SPI0 + interrupt), avoid those

_anchor_monitor = {'drag_notified_at': 0, 'depth_notified_at': 0, 'gps_notified_at': 0, 'gpio_device': None}

try:
    from gpiozero import DigitalOutputDevice
    GPIO_AVAILABLE = True
except ImportError:
    GPIO_AVAILABLE = False

def send_ntfy(title, message, priority='urgent', tags='warning'):
    # Header values must be Latin-1 (HTTP spec) — emoji/non-ASCII in the Title
    # header raises UnicodeEncodeError in requests, so keep the title plain
    # ASCII and let ntfy's Tags header supply the icon instead.
    topic = get_secrets().get('NTFY_TOPIC')
    if not topic:
        return False
    try:
        r = requests.post(
            f'https://ntfy.sh/{topic}',
            data=message.encode('utf-8'),
            headers={'Title': title, 'Priority': priority, 'Tags': tags},
            timeout=5,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f'send_ntfy failed: {e}', flush=True)  # best-effort — a failed push must never take down the monitor loop
        return False

def set_alarm_gpio(active):
    if not GPIO_AVAILABLE:
        return
    try:
        if _anchor_monitor['gpio_device'] is None:
            _anchor_monitor['gpio_device'] = DigitalOutputDevice(ANCHOR_GPIO_PIN)
        _anchor_monitor['gpio_device'].value = 1 if active else 0
    except Exception:
        pass

def anchor_monitor_loop():
    while True:
        time.sleep(ANCHOR_MONITOR_INTERVAL_S)
        try:
            state = load_anchor_state()
            gps_age_s = get_gps_age_s()
            # A stale/missing fix must never be treated as a valid current position —
            # get_gps_position() returns None here rather than handing back a frozen
            # lat/lon that would silently look "not dragging" forever.
            pos = get_gps_position(max_age_s=GPS_STALE_THRESHOLD_S)
            gps_lost = state.get('active') and (gps_age_s is None or gps_age_s > GPS_STALE_THRESHOLD_S)
            with mqtt_lock:
                depth_rec = mqtt_state['topics'].get('boat/nav/depth')
            depth_m = float(depth_rec['value']) if depth_rec else None

            dragging = False
            if state.get('active') and pos is not None and state.get('anchor_lat') is not None:
                record_anchor_trail_point(pos[0], pos[1])
                m_per_deg_lat = 111320.0
                m_per_deg_lon = 111320.0 * math.cos(math.radians(state['anchor_lat']))
                dx = (pos[1] - state['anchor_lon']) * m_per_deg_lon
                dy = (pos[0] - state['anchor_lat']) * m_per_deg_lat
                dist_m = math.hypot(dx, dy)
                dragging = dist_m > state['radius_m']

            depth_alarm_m = state.get('depth_alarm_m', 1.8288)
            shallow = depth_m is not None and depth_m < depth_alarm_m

            now = time.time()
            if dragging:
                if now - _anchor_monitor['drag_notified_at'] > ANCHOR_NOTIFY_REPEAT_S:
                    dist_ft = dist_m / M_PER_FT
                    radius_ft = state['radius_m'] / M_PER_FT
                    send_ntfy('Anchor dragging', f"⚓ Exit Strategy is {dist_ft:.0f} ft from the anchor — outside its {radius_ft:.0f} ft swing radius.", tags='anchor,warning')
                    _anchor_monitor['drag_notified_at'] = now
            else:
                _anchor_monitor['drag_notified_at'] = 0

            if shallow:
                if now - _anchor_monitor['depth_notified_at'] > ANCHOR_NOTIFY_REPEAT_S:
                    depth_ft = depth_m / M_PER_FT
                    depth_alarm_ft = depth_alarm_m / M_PER_FT
                    send_ntfy('Shallow water', f'🌊 Depth is {depth_ft:.1f} ft, below the {depth_alarm_ft:.1f} ft alarm threshold.', tags='ocean,warning')
                    _anchor_monitor['depth_notified_at'] = now
            else:
                _anchor_monitor['depth_notified_at'] = 0

            if gps_lost:
                if now - _anchor_monitor['gps_notified_at'] > ANCHOR_NOTIFY_REPEAT_S:
                    if gps_age_s is None:
                        send_ntfy('GPS signal lost', '📡 No GPS fix has been received — anchor watch cannot verify position.', tags='satellite,warning')
                    else:
                        send_ntfy('GPS signal lost', f'📡 Last GPS fix was {gps_age_s:.0f}s ago — anchor watch cannot verify position.', tags='satellite,warning')
                    _anchor_monitor['gps_notified_at'] = now
            else:
                _anchor_monitor['gps_notified_at'] = 0

            set_alarm_gpio(dragging or shallow or gps_lost)
        except Exception:
            pass  # never let one bad reading kill the monitor thread

@app.route('/api/anchor/test_alert', methods=['POST'])
def anchor_test_alert():
    ntfy_sent = send_ntfy('Test alert', '🔔 This is a test notification from the Exit Strategy anchor watch.', priority='default', tags='bell')

    def pulse():
        set_alarm_gpio(True)
        time.sleep(1)
        set_alarm_gpio(False)
    threading.Thread(target=pulse, daemon=True).start()

    return jsonify({
        'sent': ntfy_sent,
        'gpio_available': GPIO_AVAILABLE,
        'ntfy_configured': bool(get_secrets().get('NTFY_TOPIC')),
    })

# ─── AIS targets ────────────────────────────────────────────────────────────
# Unlike single-value nav topics, AIS is many independent vessels publishing
# under boat/ais/<mmsi>/<field> — this groups that flat topic dict back into
# a per-vessel list and drops any target whose position hasn't been heard
# from recently. A real vessel that's sailed out of AIS range never sends an
# explicit "gone" message, so staleness (via the timestamp dashboard_api.py
# already records on every MQTT message) is the only signal it has left —
# same reasoning as the GPS staleness check on the anchor watch page.
AIS_TARGET_STALE_S = 600  # 10 min — well past typical Class A/B position report intervals

def group_ais_topics():
    by_mmsi = {}
    with mqtt_lock:
        for topic, rec in mqtt_state['topics'].items():
            if not topic.startswith('boat/ais/'):
                continue
            parts = topic.split('/')
            if len(parts) != 4:
                continue
            _, _, mmsi, field = parts
            by_mmsi.setdefault(mmsi, {})[field] = rec
    return by_mmsi

def active_ais_targets(now):
    """Non-stale AIS targets as a list of full detail dicts (used by /api/ais/targets)."""
    targets = []
    for mmsi, fields in group_ais_topics().items():
        lat_rec = fields.get('lat')
        lon_rec = fields.get('lon')
        if not lat_rec or not lon_rec:
            continue
        try:
            age_s = (now - datetime.fromisoformat(lat_rec['time'])).total_seconds()
        except (KeyError, ValueError):
            continue
        if age_s > AIS_TARGET_STALE_S:
            continue  # not heard from recently — treat as out of range
        try:
            targets.append({
                'mmsi': mmsi,
                'lat': float(lat_rec['value']),
                'lon': float(lon_rec['value']),
                'sog': float(fields['sog']['value']) if 'sog' in fields else None,
                'cog': float(fields['cog']['value']) if 'cog' in fields else None,
                'heading': float(fields['heading']['value']) if 'heading' in fields else None,
                'nav_status': fields['nav_status']['value'] if 'nav_status' in fields else None,
                'name': fields['name']['value'] if 'name' in fields else None,
                'type': fields['type']['value'] if 'type' in fields else None,
                'class': fields['class']['value'] if 'class' in fields else None,
                'age_s': round(age_s, 1),
            })
        except (TypeError, ValueError):
            continue
    return targets

@app.route('/api/ais/targets')
def ais_targets():
    targets = active_ais_targets(datetime.now(timezone.utc))
    return jsonify({'targets': targets, 'count': len(targets)})

# ─── AIS tracks (own ship + per-target) ─────────────────────────────────────
# Recorded by a background thread every AIS_TRAIL_INTERVAL_S regardless of
# whether the AIS tab is open, same reasoning as the anchor-watch trail: without
# this, reopening the tab after it's been closed would draw a straight line from
# wherever things were last seen to wherever they are now instead of a real track.
AIS_TRAIL_WINDOW_S = 15 * 60  # matches AIS_TRAIL_WINDOW_MS on the frontend
AIS_TRAIL_INTERVAL_S = 5
AIS_TRAIL_LOCK = threading.Lock()
_ais_own_trail = []
_ais_target_trails = {}  # mmsi -> [{'lat':, 'lon':, 't':}, ...]

def _trim_trail(trail, now):
    while trail and (now - datetime.fromisoformat(trail[0]['t'])).total_seconds() > AIS_TRAIL_WINDOW_S:
        trail.pop(0)

def record_ais_trails():
    now = datetime.now(timezone.utc)
    with AIS_TRAIL_LOCK:
        pos = get_gps_position()
        if pos is not None:
            _ais_own_trail.append({'lat': pos[0], 'lon': pos[1], 't': now.isoformat()})
            _trim_trail(_ais_own_trail, now)

        active = active_ais_targets(now)
        active_mmsis = {t['mmsi'] for t in active}
        for t in active:
            trail = _ais_target_trails.setdefault(t['mmsi'], [])
            trail.append({'lat': t['lat'], 'lon': t['lon'], 't': now.isoformat()})
            _trim_trail(trail, now)

        for mmsi in list(_ais_target_trails.keys()):
            if mmsi not in active_mmsis:
                del _ais_target_trails[mmsi]  # target's gone — matches marker/vector cleanup on the frontend

def ais_trail_monitor_loop():
    while True:
        time.sleep(AIS_TRAIL_INTERVAL_S)
        try:
            record_ais_trails()
        except Exception:
            pass  # never let one bad reading kill the monitor thread

@app.route('/api/ais/own_trail')
def ais_own_trail():
    with AIS_TRAIL_LOCK:
        return jsonify(list(_ais_own_trail))

@app.route('/api/ais/trails')
def ais_trails():
    with AIS_TRAIL_LOCK:
        return jsonify({mmsi: list(trail) for mmsi, trail in _ais_target_trails.items()})

# ─── Weather forecast (National Weather Service, api.weather.gov) ──────────────
# Free, no API key, but wants a real User-Agent and shouldn't be hammered — cached
# server-side so every browser poll doesn't trigger a fresh upstream call.
WEATHER_LAT, WEATHER_LON = 27.7000, -82.6900  # home port area; swap for live GPS once cruising further afield
WEATHER_CACHE_TTL_S = 1800  # 30 min — forecast periods don't change faster than this
_weather_cache = {'data': None, 'fetched_at': 0}

@app.route('/api/weather/forecast')
def weather_forecast():
    now = time.time()
    if _weather_cache['data'] and (now - _weather_cache['fetched_at'] < WEATHER_CACHE_TTL_S):
        return jsonify(_weather_cache['data'])
    try:
        headers = {'User-Agent': 'exit-strategy-dashboard (github.com/mikemc)'}
        points = requests.get(f'https://api.weather.gov/points/{WEATHER_LAT},{WEATHER_LON}', headers=headers, timeout=8).json()
        forecast_url = points['properties']['forecast']
        forecast = requests.get(forecast_url, headers=headers, timeout=8).json()
        periods = forecast['properties']['periods'][:6]
        data = {
            'periods': [{
                'name': p['name'],
                'temperature': p['temperature'],
                'temperatureUnit': p['temperatureUnit'],
                'shortForecast': p['shortForecast'],
                'windSpeed': p['windSpeed'],
                'windDirection': p['windDirection'],
                'isDaytime': p['isDaytime'],
            } for p in periods],
            'updated': datetime.now(timezone.utc).isoformat(),
        }
        _weather_cache['data'] = data
        _weather_cache['fetched_at'] = now
        return jsonify(data)
    except Exception as e:
        if _weather_cache['data']:
            return jsonify(_weather_cache['data'])  # serve stale rather than nothing on a transient failure
        return jsonify({'error': str(e)}), 502

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    # DEBUG_MODE=True uses Flask's auto-reloader, which forks a child process —
    # WERKZEUG_RUN_MAIN is only set in that child, so gating on it avoids starting
    # every background thread twice. With DEBUG_MODE=False (production) there's no
    # reloader at all, so the threads must start unconditionally instead.
    DEBUG_MODE = True
    if not DEBUG_MODE or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_mqtt_listener()
        threading.Thread(target=anchor_monitor_loop, daemon=True).start()
        threading.Thread(target=ais_trail_monitor_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=5003, debug=DEBUG_MODE)

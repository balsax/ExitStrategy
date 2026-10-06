from flask import Flask, jsonify, request, send_from_directory, Response
import requests
import os
import glob
import io
import time
import json
import math
import shutil
import subprocess
import signal
import re
import sqlite3
from PIL import Image
from collections import Counter
import threading
import uuid
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# ─── Access log (Diagnostics tab) ───────────────────────────────────────────
# Page loads only, not every API poll -- the SPA hits a dozen+ endpoints
# every few seconds once open, which would drown out "who's actually opened
# the dashboard" in noise from a single already-open tab. In-memory only
# (same tradeoff as the anchor trail / AIS trails above -- recent activity,
# not indefinite history), capped so a long-uptime Pi doesn't grow this
# without bound.
ACCESS_LOG_MAX = 500
_access_log = []
_access_log_lock = threading.Lock()

def _client_ip():
    # Real traffic here comes through the Cloudflare Tunnel (cloudflared),
    # which sets CF-Connecting-IP to the actual visitor's IP -- request.
    # remote_addr would just be the tunnel's own local connection.
    # X-Forwarded-For is the fallback for any other reverse-proxy path;
    # remote_addr covers direct LAN access with neither header set.
    return (request.headers.get('CF-Connecting-IP')
            or (request.headers.get('X-Forwarded-For') or '').split(',')[0].strip()
            or request.remote_addr)

def record_access():
    with _access_log_lock:
        _access_log.append({
            'time': datetime.now(timezone.utc).isoformat(),
            'ip': _client_ip(),
            # Only present when the request actually came through Cloudflare
            # -- None for LAN access on the same WiFi, which is correct/
            # expected rather than a missing-data bug.
            'country': request.headers.get('CF-IPCountry'),
            'user_agent': request.headers.get('User-Agent', ''),
        })
        if len(_access_log) > ACCESS_LOG_MAX:
            del _access_log[:len(_access_log) - ACCESS_LOG_MAX]

@app.route('/api/access_log')
def access_log():
    with _access_log_lock:
        return jsonify({'entries': list(reversed(_access_log))})

@app.route('/api/access_log/clear', methods=['POST'])
def clear_access_log():
    with _access_log_lock:
        _access_log.clear()
    return jsonify({'status': 'ok'})

# ─── Simulator scripts (Diagnostics tab) ────────────────────────────────────
# Dev-only process control for the *_simulator.py stand-ins this dev copy
# uses instead of the real N2K/MQTT devices. Status is read fresh via pgrep
# each time rather than trusting our own Popen handles, since a Flask reload
# during dev editing would otherwise orphan a tracked process and make the
# UI lie about it being stopped.
SIMULATOR_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATORS = {
    'gps':        {'label': 'GPS / Depth',   'script': 'gps_simulator.py'},
    'wind':       {'label': 'Wind',          'script': 'wind_simulator.py'},
    'tank':       {'label': 'Tanks / Bilge', 'script': 'tank_simulator.py'},
    'ais':        {'label': 'AIS',           'script': 'ais_simulator.py'},
    'autopilot':  {'label': 'Autopilot',     'script': 'autopilot_simulator.py'},
    'watermaker': {'label': 'Watermaker',    'script': 'watermaker_simulator.py'},
    'garmin1243': {'label': 'Garmin 1243 (NMEA2000)', 'script': 'garmin_1243_simulator.py'},
}

def _simulator_script_path(name):
    return os.path.join(SIMULATOR_DIR, SIMULATORS[name]['script'])

def _simulator_pids(name):
    script_path = _simulator_script_path(name)
    # Anchored to the *exact* command line simulator_start below launches
    # ('python3 <script_path>', nothing else) rather than pgrep -f's default
    # substring-anywhere match -- an unanchored pattern here false-positives
    # on any unrelated process that merely mentions this path in its own
    # command line (a shell command investigating/grepping for the script,
    # an editor, etc.), which reads to simulator_start as "already running"
    # and makes it silently skip actually starting anything.
    pattern = f'^python3? {re.escape(script_path)}$'
    try:
        result = subprocess.run(['pgrep', '-f', pattern], capture_output=True, text=True, timeout=3)
        return [int(pid) for pid in result.stdout.split()]
    except (OSError, subprocess.SubprocessError, ValueError):
        return []

@app.route('/api/simulators/status')
def simulators_status():
    sims = []
    for name, info in SIMULATORS.items():
        pids = _simulator_pids(name)
        sims.append({'name': name, 'label': info['label'], 'running': len(pids) > 0, 'pid': pids[0] if pids else None})
    return jsonify({'simulators': sims})

@app.route('/api/simulators/<name>/start', methods=['POST'])
def simulator_start(name):
    if name not in SIMULATORS:
        return jsonify({'error': 'unknown simulator'}), 404
    if _simulator_pids(name):
        return jsonify({'status': 'ok', 'already_running': True})
    try:
        subprocess.Popen(['python3', _simulator_script_path(name)], cwd=SIMULATOR_DIR,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
                          start_new_session=True)
    except OSError as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'status': 'ok'})

@app.route('/api/simulators/<name>/stop', methods=['POST'])
def simulator_stop(name):
    if name not in SIMULATORS:
        return jsonify({'error': 'unknown simulator'}), 404
    pids = _simulator_pids(name)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGINT)  # simulators catch KeyboardInterrupt for a clean MQTT disconnect
        except OSError:
            pass
    # Give each script a moment to unwind (disconnect its MQTT client, flush
    # prints) before falling back to SIGKILL on anything still standing.
    deadline = time.time() + 2.0
    while time.time() < deadline and _simulator_pids(name):
        time.sleep(0.1)
    for pid in _simulator_pids(name):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    return jsonify({'status': 'ok'})

@app.route('/')
def index():
    record_access()
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

    # ─── VRM battery monitors by instance (siloed addition) ──────────────────
    # Every battery monitor (the 48V BMS and each SmartShunt) reports under
    # the same generic codes (V, I, SOC, ...), told apart only by VRM device
    # instance. The 'Bv'/'Bs'/... codes mapped above never appear, and 'Bc'
    # (Battery to consumers, kWh) / 'BT' (the 48V battery's temperature) were
    # being read as the 12V house current/temperature. Add the new diesel and
    # thruster SmartShunts here by instance once they're on the Cerbo.
    VRM_BATTERY_INSTANCES = {
        279: '12v',   # 12V house SmartShunt (150Ah)
    }
    per_instance = {'V': 'v', 'I': 'i', 'SOC': 'soc', 'CE': 'cah', 'TTG': 'ttg'}
    for suffix in VRM_BATTERY_INSTANCES.values():
        for k in ('v', 'i', 'soc', 'cah', 'ttg', 'temp'):
            result.pop(f'{k}_{suffix}', None)
    for r in data.get('records', []):
        suffix = VRM_BATTERY_INSTANCES.get(r.get('instance'))
        if suffix and r.get('Device') == 'Battery Monitor' and r.get('code') in per_instance:
            try:
                val = float(r.get('rawValue'))
            except (TypeError, ValueError):
                continue
            if r['code'] == 'TTG':
                val = round(val * 60)  # hours -> minutes, the unit the Electrical tab shows
            result[f"{per_instance[r['code']]}_{suffix}"] = val
    # 'ms' (mapped to mp_state above) is the Cerbo GX's serial number, not the
    # MultiPlus -- the VE.Bus state (Bulk / Absorption / Float / Inverting ...) is 'S'.
    # MultiPlus DC side on its own (the battery's current is the net of every
    # charger/load on the 48V bus): CI/CV from the VE.Bus device, vp = signed
    # VE.Bus charge power (negative = inverting from the battery).
    mp_dc = {('VE.Bus System', 'CI'): 'mp_dc_current', ('VE.Bus System', 'CV'): 'mp_dc_voltage'}
    for r in data.get('records', []):
        if r.get('code') == 'S' and r.get('Device') == 'VE.Bus System':
            result['mp_state'] = r.get('formattedValue', '')
        key = mp_dc.get((r.get('Device'), r.get('code')))
        if key:
            try:
                result[key] = float(r.get('rawValue'))
            except (TypeError, ValueError):
                pass

    # Read each value from the device that measures it. The "System overview"
    # totals (g1 grid, bc/bv/bs battery, vp VE.Bus charge power) can go stale in
    # the diagnostics snapshot -- e.g. still showing 724W of shore and the bank
    # charging after shore was unplugged and the MultiPlus was inverting.
    def rec(device, code, instance=None):
        for r in data.get('records', []):
            if r.get('Device') == device and r.get('code') == code and (instance is None or r.get('instance') == instance):
                return r
        return None
    def num(r):
        try:
            return float(r.get('rawValue')) if r else None
        except (TypeError, ValueError):
            return None
    if 'mp_dc_voltage' in result and 'mp_dc_current' in result:
        result['mp_dc_power'] = round(result['mp_dc_voltage'] * result['mp_dc_current'], 1)  # + charging / - inverting
    # Shore: the MultiPlus's own AC input power; 0 when its input is disconnected.
    ip1, ai = rec('VE.Bus System', 'IP1'), rec('VE.Bus System', 'AI')
    if ip1 is not None:
        disconnected = ai is not None and 'disconnect' in str(ai.get('formattedValue', '')).lower()
        result['grid'] = 0.0 if disconnected else num(ip1)
    # 48V bank: the active battery service's own monitor (the BMS), e.g.
    # 'com.victronenergy.battery/512' -> instance 512.
    abs_rec = rec('Gateway', 'abs') or next((r for r in data.get('records', []) if r.get('code') == 'abs'), None)
    try:
        bank = int(str(abs_rec.get('formattedValue', '')).rsplit('/', 1)[1]) if abs_rec else None
    except (IndexError, ValueError):
        bank = None
    if bank is not None:
        v, i, soc = (num(rec('Battery Monitor', c, bank)) for c in ('V', 'I', 'SOC'))
        if v is not None: result['voltage'] = v
        if i is not None: result['current'] = i
        if soc is not None: result['soc'] = soc
        if v is not None and i is not None: result['power'] = round(v * i, 1)
    # ─── end VRM battery monitors by instance ───────────────────────────────

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

    # Barometric TENDENCY -- the standard maritime definition is change over
    # the last 3 hours, not just whether the current absolute reading sits
    # above/below a fixed threshold. (The frontend used to derive "Rising/
    # Falling" purely from d.pressure > 1013 / < 1009, which only ever
    # describes high vs. low pressure, not which way it's moving -- a
    # steady 1015 hPa reads "Rising · fair weather" forever under that
    # logic, regardless of whether it's actually risen, fallen, or sat
    # flat.) A 1-hour-wide window centered on -3h (rather than a single
    # instant) tolerates a gap in readings landing exactly on the 3h mark.
    if 'pressure' in data:
        flux_3h = f'''from(bucket:"{s['INFLUX_BUCKET']}")
  |> range(start: -3h30m, stop: -2h30m)
  |> filter(fn: (r) => r._field == "pressure")
  |> first()'''
        past = parse_last(query_influx(flux_3h))
        if past is not None:
            delta = data['pressure'] - past
            data['pressure_delta_3h'] = round(delta, 2)
            data['pressure_trend'] = 'rising' if delta > 1.0 else 'falling' if delta < -1.0 else 'steady'

    return jsonify(data)

# ─── BME680 last-seen (siloed addition) ─────────────────────────────────────
# /api/sensor only looks back 1h, so a sensor that has dropped off just shows
# "--" forever. This reports when the BME680 last wrote anything (up to 30d
# back) so the Overview/Weather cards can say "offline, last reading ...".
# The board buffers readings while offline and uploads them late, so this
# can move backwards-in-age (newer) after a reconnect -- that's expected.
_bme_last_seen_cache = {'t': 0.0, 'data': None}
BME_LAST_SEEN_TTL_S = 30

@app.route('/api/sensor/last_seen')
def sensor_last_seen():
    now = time.time()
    if _bme_last_seen_cache['data'] is None or now - _bme_last_seen_cache['t'] > BME_LAST_SEEN_TTL_S:
        s = get_secrets()
        flux = f'''from(bucket:"{s['INFLUX_BUCKET']}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "bme680" and r._field == "temp_f")
  |> last()
  |> keep(columns: ["_time"])'''
        last = None
        try:
            header = None
            for line in query_influx(flux).strip().split('\n'):
                cols = line.strip().split(',')
                if '_time' in cols:
                    header = cols
                elif header and len(cols) == len(header):
                    last = cols[header.index('_time')]
        except Exception as e:
            return jsonify({'last_seen': None, 'error': str(e)})
        _bme_last_seen_cache['data'] = {'last_seen': last}
        _bme_last_seen_cache['t'] = now
    data = dict(_bme_last_seen_cache['data'])
    if data['last_seen']:
        ts = datetime.fromisoformat(data['last_seen'].replace('Z', '+00:00'))
        data['age_s'] = round(now - ts.timestamp())
    return jsonify(data)
# ─── end BME680 last-seen siloed addition ───────────────────────────────────

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
    # 'CODE@INSTANCE' picks one battery monitor (they all share codes like SOC/V/I)
    code, _, vrm_instance = code.partition('@')

    range_seconds = {
        '10m': 600,
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
                **({'instance': int(vrm_instance)} if vrm_instance.isdigit() else {}),
            },
            timeout=15
        )
        data = res.json()

        if not data.get('success'):
            return jsonify({'times': [], 'values': [], 'error': 'VRM API error'})

        # VRM's Graph widget nests the actual [[timestamp, value], ...] list
        # two levels deep -- records -> some category key (observed: "data",
        # not the attribute code) -> a stringified instance id -> the point
        # list -- rather than the flat records->{code: [...]} shape this was
        # originally written against. That flat assumption meant
        # `isinstance(val, list)` never matched (val was always a dict one
        # level too shallow), so this silently returned empty for every
        # request regardless of whether VRM actually had data. Recursing to
        # find the first real point list sidesteps needing to know the exact
        # intermediate key names, which appear to vary by installation/code.
        def _first_point_series(obj):
            if isinstance(obj, list):
                return obj if obj and isinstance(obj[0], list) else None
            if isinstance(obj, dict):
                for v in obj.values():
                    found = _first_point_series(v)
                    if found is not None:
                        return found
            return None

        series = _first_point_series(data.get('records', {}))
        if not series:
            return jsonify({'times': [], 'values': []})

        times = []
        values = []
        for point in series:
            if len(point) >= 2 and point[1] is not None:
                # Also seconds, not milliseconds, despite the original ts_ms
                # name -- verified against a live response (a timestamp that
                # decoded to the correct current date only as whole seconds).
                dt = datetime.fromtimestamp(point[0], tz=timezone.utc)
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
        '10m': '15s', '1h': '2m', '6h': '10m', '24h': '30m',
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

# No real autopilot is on the N2K network yet, and even once one is, there's
# no confirmed safe command path for it -- canboat's own reverse-engineering
# of PGN 126720 marks the command-direction messages "decode-only... not a
# transmit path" (see n2k_mqtt_bridge.py's PGN_MAP notes). This only ever
# reaches autopilot_simulator.py, the dev stand-in listening on the same
# boat/nav/autopilot/cmd/mode topic -- publishing here has zero effect on
# any physical CAN bus regardless of what's connected to it.
AUTOPILOT_MODES = {'engage', 'standby', 'shadow'}

@app.route('/api/autopilot/control', methods=['POST'])
def autopilot_control():
    data = request.get_json(silent=True) or {}
    mode = data.get('mode')
    if mode not in AUTOPILOT_MODES:
        return jsonify({'error': f'mode must be one of {sorted(AUTOPILOT_MODES)}'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/nav/autopilot/cmd/mode', mode)
    return jsonify({'status': 'sent', 'topic': 'boat/nav/autopilot/cmd/mode', 'mode': mode})

AUTOPILOT_COURSE_DELTAS = {-10, -1, 1, 10}  # matches the Chart page's four course-change buttons

@app.route('/api/autopilot/course_change', methods=['POST'])
def autopilot_course_change():
    data = request.get_json(silent=True) or {}
    try:
        delta = int(data.get('delta'))
    except (TypeError, ValueError):
        return jsonify({'error': 'delta must be an integer'}), 400
    if delta not in AUTOPILOT_COURSE_DELTAS:
        return jsonify({'error': f'delta must be one of {sorted(AUTOPILOT_COURSE_DELTAS)}'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/nav/autopilot/cmd/adjust_heading', str(delta))
    return jsonify({'status': 'sent', 'topic': 'boat/nav/autopilot/cmd/adjust_heading', 'delta': delta})

@app.route('/api/autopilot/set_destination', methods=['POST'])
def autopilot_set_destination():
    data = request.get_json(silent=True) or {}
    try:
        lat = float(data.get('lat'))
        lon = float(data.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'error': 'lat/lon must be numbers'}), 400
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return jsonify({'error': 'lat/lon out of range'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/nav/autopilot/cmd/set_destination', f"{lat},{lon}")
    return jsonify({'status': 'sent', 'topic': 'boat/nav/autopilot/cmd/set_destination', 'lat': lat, 'lon': lon})

WATERMAKER_MODES = {'start', 'stop', 'flush', 'auto', 'manual', 'reset'}  # 'reset' clears an active fault -- the device won't accept other manual commands until it's sent
WATERMAKER_DEVICES = {'pump', 'boost_pump', 'divert', 'flush'}
# The rest of the hardware-protection faults (HP current) can still only be
# overridden by going into full manual mode.
WATERMAKER_BYPASSABLE_FAULTS = {'product_sensor', 'postfilter', 'tank_level', 'hp_pressure_low', 'feed_starvation'}

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

@app.route('/api/watermaker/fault_bypass', methods=['POST'])
def watermaker_fault_bypass():
    data = request.get_json(silent=True) or {}
    fault = data.get('fault')
    state = data.get('state')
    if fault not in WATERMAKER_BYPASSABLE_FAULTS:
        return jsonify({'error': f'fault must be one of {sorted(WATERMAKER_BYPASSABLE_FAULTS)}'}), 400
    if str(state) not in ('0', '1'):
        return jsonify({'error': 'state must be 0 or 1'}), 400
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    payload = f'{fault}:{state}'
    mqtt_client.publish('boat/watermaker/cmd/fault_bypass', payload)
    return jsonify({'status': 'sent', 'topic': 'boat/watermaker/cmd/fault_bypass', 'payload': payload})

@app.route('/api/watermaker/production_reset', methods=['POST'])
def watermaker_production_reset():
    if not mqtt_client or not mqtt_state['connected']:
        return jsonify({'error': 'MQTT broker not connected'}), 503
    mqtt_client.publish('boat/watermaker/cmd/production_reset', '1')
    return jsonify({'status': 'sent', 'topic': 'boat/watermaker/cmd/production_reset', 'payload': '1'})

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
    'feed_rate':  'boat/watermaker/flow/feed_rate',     # same units as flow/rate -- converted to gph below
    'cond':       'boat/watermaker/flow/conductivity_comp',
    'pump_speed': 'boat/watermaker/pump/speed_pct',  # pump/rpm has no sensor installed and always reads 0 -- speed_pct (commanded duty) is what's real
    'current':    'boat/watermaker/pump/current',
    'efficiency': 'boat/watermaker/efficiency',
    'tank':       'boat/watermaker/tank/level',
    'product_temp': 'boat/watermaker/flow/temperature',  # same Digmesa flow sensor as flow/rate, reports product water temp
}
TREND_RANGE_SECONDS = {'10m': 600, '1h': 3600, '6h': 21600, '24h': 86400, '7d': 604800, '30d': 2592000}
TREND_RANGE_BUCKET = {'10m': 5, '1h': 30, '6h': 120, '24h': 600, '7d': 3600, '30d': 14400}

def get_boat_db():
    s = get_secrets()
    return mysql.connector.connect(
        host='localhost', user='mikemc', password=s.get('DB_PASS', ''),
        database='boat_monitoring', connection_timeout=5,
    )

# ─── Trend rollup read path (siloed addition) ──────────────────────────────
# Wide trend ranges (6h/24h/7d/30d) used to AVG() raw mqtt_readings per topic;
# one topic's rows are scattered across the whole table (every row a random page
# read), so 24h took ~6s, 7d ~30s and 30d minutes. mqtt_rollup.py keeps
# mqtt_readings_1m (n/sum per topic per minute); a whole-minute bucket's average
# is exactly SUM(sum_value)/SUM(n). The rollup only covers *closed* minutes, so
# the not-yet-rolled-up tail is read raw and combined. Anything the rollup can't
# answer completely (table missing, range starts before its coverage, non-numeric
# topic) returns None and the caller falls back to the original raw query.
ROLLUP_MIN_BUCKET_SECONDS = 120   # the 10m (5s) and 1h (30s) ranges are finer than a 1-minute rollup

def _query_bucketed_from_rollup(cursor, topic_id, start_dt, bucket_seconds):
    try:
        cursor.execute("SELECT covered_from, covered_to FROM mqtt_rollup_state WHERE id = 1")
        state = cursor.fetchone()
        if not state:
            return None
        covered_from, covered_to = state
        start_minute = start_dt.replace(second=0, microsecond=0)
        if covered_from > start_minute:
            return None
        cursor.execute("SELECT data_type FROM mqtt_topics WHERE id = %s", (topic_id,))
        dt_row = cursor.fetchone()
        if not dt_row or dt_row[0] not in ('int', 'float'):
            return None
        cursor.execute("""
            SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t)/%s)*%s) AS bucket_ts, SUM(s)/SUM(n) AS avg_value
            FROM (
                SELECT minute_ts AS t, sum_value AS s, n FROM mqtt_readings_1m
                WHERE topic_id = %s AND minute_ts >= %s AND minute_ts < %s
                UNION ALL
                SELECT ts, CAST(value AS DECIMAL(20,4)), 1 FROM mqtt_readings
                WHERE topic_id = %s AND ts >= %s
            ) u
            GROUP BY bucket_ts
            ORDER BY bucket_ts
        """, (bucket_seconds, bucket_seconds, topic_id, start_minute, covered_to,
              # Raw tail never starts before the requested range: with a stale
              # rollup (timer not running) covered_to can be days old, and
              # reading raw from there returned days of extra points.
              topic_id, max(covered_to, start_minute)))
        return {r[0]: float(r[1]) for r in cursor.fetchall() if r[1] is not None}
    except mysql.connector.Error:
        return None
# ─── end trend rollup siloed addition ───────────────────────────────────────

def query_bucketed_series(cursor, topic, start_dt, bucket_seconds):
    cursor.execute("SELECT id FROM mqtt_topics WHERE topic = %s", (topic,))
    row = cursor.fetchone()
    if not row:
        return {}
    topic_id = row[0]
    if bucket_seconds >= ROLLUP_MIN_BUCKET_SECONDS and bucket_seconds % 60 == 0:
        rolled = _query_bucketed_from_rollup(cursor, topic_id, start_dt, bucket_seconds)
        if rolled is not None:
            return rolled
    cursor.execute("""
        SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts)/%s)*%s) AS bucket_ts,
               AVG(CAST(value AS DECIMAL(20,4))) AS avg_value
        FROM mqtt_readings
        WHERE topic_id = %s AND ts >= %s
        GROUP BY bucket_ts
        ORDER BY bucket_ts
    """, (bucket_seconds, bucket_seconds, topic_id, start_dt))
    return {r[0]: float(r[1]) for r in cursor.fetchall() if r[1] is not None}

def query_watermaker_metric(cur, metric, start_dt, bucket):
    """Returns {times: [...], values: [...]} for one watermaker metric (or an error)."""
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
        if metric in ('flow', 'feed_rate'):
            values = [round(v * 60 / 3785.411784, 2) for v in values]  # mL/min -> gph
    return {'times': times, 'values': values}

@app.route('/api/watermaker/history')
def watermaker_history():
    # 'metrics' (comma-separated, for multi-pen trending) takes priority; 'metric'
    # (singular) is kept for older callers and just becomes a one-item list.
    metrics_param = request.args.get('metrics') or request.args.get('metric', 'membrane')
    metrics = [m.strip() for m in metrics_param.split(',') if m.strip()]
    range_val = request.args.get('range', '1h')

    bucket = TREND_RANGE_BUCKET.get(range_val, 30)
    seconds = TREND_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)  # mqtt_readings.ts is local time, not UTC

    series = {}
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        for metric in metrics:
            if metric != 'filterdp' and metric not in WATERMAKER_METRIC_TOPICS:
                series[metric] = {'times': [], 'values': [], 'error': 'unknown metric'}
                continue
            series[metric] = query_watermaker_metric(cur, metric, start_dt, bucket)
        conn.close()
    except Exception as e:
        for metric in metrics:
            series.setdefault(metric, {'times': [], 'values': [], 'error': str(e)})

    return jsonify({'series': series})

@app.route('/api/wind/history')
def wind_history():
    """Single-metric trend for the Weather tab's True Wind Speed chart --
    same bucketed-average approach as watermaker/system history above, just
    one topic. boat/nav/wind/speed gets logged into mqtt_readings for free
    by mqtt_logger.py's blanket boat/# subscription, same as everything
    else, so no new logging setup needed for this to work."""
    range_val = request.args.get('range', '1h')
    bucket = TREND_RANGE_BUCKET.get(range_val, 30)
    seconds = TREND_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)

    try:
        conn = get_boat_db()
        cur = conn.cursor()
        series = query_bucketed_series(cur, 'boat/nav/wind/speed', start_dt, bucket)
        conn.close()
        keys = sorted(series.keys())
        return jsonify({
            'times': [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys],
            'values': [round(series[k], 2) for k in keys],
        })
    except Exception as e:
        return jsonify({'times': [], 'values': [], 'error': str(e)})

@app.route('/api/wind/direction_history')
def wind_direction_history():
    """Recent apparent wind + SOG, aligned by time bucket, for the Weather
    tab's compass shading to rebuild itself from whenever that tab is opened
    -- not just picking up where an in-memory buffer left off. True wind
    angle is a client-side-derived quantity (from AWS/AWA/SOG together,
    see trueWindFromApparent() in the frontend), never logged directly, so
    there's no single topic to query the way /api/wind/history does for
    speed -- the frontend recomputes TWA per sample from these three series
    once it has them. Fixed 10-minute window/5s bucket, matching the
    shading's own rolling window (WIND_HISTORY_WINDOW_MS in the frontend);
    intersecting all three series' bucket keys so a sample only comes back
    where every input actually exists, rather than computing a bogus TWA
    against a missing SOG defaulting to something wrong."""
    seconds, bucket = 600, 5
    start_dt = datetime.now() - timedelta(seconds=seconds)

    try:
        conn = get_boat_db()
        cur = conn.cursor()
        aws_series = query_bucketed_series(cur, 'boat/nav/wind/speed', start_dt, bucket)
        awa_series = query_bucketed_series(cur, 'boat/nav/wind/angle', start_dt, bucket)
        sog_series = query_bucketed_series(cur, 'boat/nav/gps/sog', start_dt, bucket)
        conn.close()
        keys = sorted(set(aws_series) & set(awa_series) & set(sog_series))
        return jsonify({
            'times': [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys],
            'aws': [round(aws_series[k], 2) for k in keys],
            'awa': [round(awa_series[k], 1) for k in keys],
            'sog': [round(sog_series[k], 2) for k in keys],
        })
    except Exception as e:
        return jsonify({'times': [], 'aws': [], 'awa': [], 'sog': [], 'error': str(e)})

# ─── Tanks (Overview tab trend modal) ───────────────────────────────────────
# Same multi-pen bucketed-average approach as watermaker/system health above.
# boat/nav/tanks/<name>/level gets logged into mqtt_readings for free by
# mqtt_logger.py's blanket boat/# subscription -- published by
# n2k_mqtt_bridge.py's handle_fluid_level() once real senders are wired, or
# tank_simulator.py's dev stand-in until then.
TANK_METRIC_TOPICS = {
    'fresh_1': 'boat/nav/tanks/fresh_1/level',
    'fresh_2': 'boat/nav/tanks/fresh_2/level',
    'diesel':  'boat/nav/tanks/diesel/level',
    'black':   'boat/nav/tanks/black/level',
}

@app.route('/api/tanks/history')
def tanks_history():
    metrics_param = request.args.get('metrics') or request.args.get('metric', 'fresh_1')
    metrics = [m.strip() for m in metrics_param.split(',') if m.strip()]
    range_val = request.args.get('range', '1h')

    bucket = TREND_RANGE_BUCKET.get(range_val, 30)
    seconds = TREND_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)

    series = {}
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        for metric in metrics:
            if metric not in TANK_METRIC_TOPICS:
                series[metric] = {'times': [], 'values': [], 'error': 'unknown metric'}
                continue
            s = query_bucketed_series(cur, TANK_METRIC_TOPICS[metric], start_dt, bucket)
            keys = sorted(s.keys())
            series[metric] = {
                'times': [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys],
                'values': [round(s[k], 2) for k in keys],
            }
        conn.close()
    except Exception as e:
        for metric in metrics:
            series.setdefault(metric, {'times': [], 'values': [], 'error': str(e)})

    return jsonify({'series': series})

# ─── Engine compartment (Engine tab trend modal) ────────────────────────────
# Same multi-pen bucketed-average approach as tanks/watermaker above.
# boat/engine/fan/temp and /humidity are the BME680-backed compartment
# readings that already drive the live cards on the Engine tab -- logged into
# mqtt_readings for free by mqtt_logger.py's blanket boat/# subscription, same
# as everything else. Engine Telemetry (RPM/oil/coolant/etc.) isn't wired to
# real hardware yet (see the Engine tab's own warning banner), so there's
# nothing to trend there until that's live.
ENGINE_METRIC_TOPICS = {
    'temp':     'boat/engine/fan/temp',
    'humidity': 'boat/engine/fan/humidity',
}

@app.route('/api/engine/history')
def engine_history():
    metrics_param = request.args.get('metrics') or request.args.get('metric', 'temp')
    metrics = [m.strip() for m in metrics_param.split(',') if m.strip()]
    range_val = request.args.get('range', '1h')

    bucket = TREND_RANGE_BUCKET.get(range_val, 30)
    seconds = TREND_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)

    series = {}
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        for metric in metrics:
            if metric not in ENGINE_METRIC_TOPICS:
                series[metric] = {'times': [], 'values': [], 'error': 'unknown metric'}
                continue
            s = query_bucketed_series(cur, ENGINE_METRIC_TOPICS[metric], start_dt, bucket)
            keys = sorted(s.keys())
            series[metric] = {
                'times': [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys],
                'values': [round(s[k], 2) for k in keys],
            }
        conn.close()
    except Exception as e:
        for metric in metrics:
            series.setdefault(metric, {'times': [], 'values': [], 'error': str(e)})

    return jsonify({'series': series})

# ─── Server health (Pi CPU/memory/disk/temp/WiFi, logged the same way as
# watermaker telemetry) ─────────────────────────────────────────────────────
# A background thread (system_health_loop, started below) samples the Pi
# itself and publishes to boat/system/* on the same MQTT broker everything
# else uses -- the existing mqtt_logger.py service already subscribes
# broadly to boat/#, so these get logged into MariaDB for free, and trend
# queries reuse the exact same bucketed-average approach as watermaker
# history above. No new dependency (no psutil) -- CPU/memory come from
# /proc, disk from shutil, temp from the Pi's thermal zone, WiFi from `iw`.
SYSTEM_METRIC_TOPICS = {
    'cpu':        'boat/system/cpu_percent',
    'mem':        'boat/system/mem_percent',
    'disk':       'boat/system/disk_percent',
    'temp':       'boat/system/cpu_temp_c',
    'wifi':       'boat/system/wifi_signal_dbm',
    'load1':      'boat/system/load1',
    'net_rx':     'boat/system/net_rx_kbps',
    'net_tx':     'boat/system/net_tx_kbps',
    'throttled':  'boat/system/throttled_active',
    'disk_io':    'boat/system/disk_io_kbps',
    'boot_disk':  'boat/system/boot_disk_percent',
}
SYSTEM_HEALTH_INTERVAL_S = 20

@app.route('/api/system/history')
def system_history():
    metrics_param = request.args.get('metrics') or request.args.get('metric', 'cpu')
    metrics = [m.strip() for m in metrics_param.split(',') if m.strip()]
    range_val = request.args.get('range', '1h')

    bucket = TREND_RANGE_BUCKET.get(range_val, 30)
    seconds = TREND_RANGE_SECONDS.get(range_val, 3600)
    start_dt = datetime.now() - timedelta(seconds=seconds)

    series = {}
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        for metric in metrics:
            if metric not in SYSTEM_METRIC_TOPICS:
                series[metric] = {'times': [], 'values': [], 'error': 'unknown metric'}
                continue
            s = query_bucketed_series(cur, SYSTEM_METRIC_TOPICS[metric], start_dt, bucket)
            keys = sorted(s.keys())
            series[metric] = {
                'times': [k.strftime('%Y-%m-%dT%H:%M:%S') for k in keys],
                'values': [round(s[k], 2) for k in keys],
            }
        conn.close()
    except Exception as e:
        for metric in metrics:
            series.setdefault(metric, {'times': [], 'values': [], 'error': str(e)})

    return jsonify({'series': series})

# A rare/mostly-constant status like throttle state doesn't suit a trend line --
# what's actually useful is "when did this last change", not a chart of a flag
# sampled every 20s. Reuses the same logged samples (no new storage), just
# collapses consecutive identical readings into change events via LAG().
# bilge_pump is the same idea applied to boat/nav/bilge/pump_on (0/1) -- "how
# often has it run" wants a list of on/off transitions, not a trend line of
# a flag either.
SYSTEM_EVENT_TOPICS = {
    'throttled': 'boat/system/throttled_detail',
    'bilge_pump': 'boat/nav/bilge/pump_on',
}

# ─── System events: incremental change cache (siloed addition) ──────────────
# /api/system/events used to run LAG() over a topic's *entire* history on every
# poll just to return the last few value changes. mqtt_readings is tens of
# millions of rows and each row is a random lookup, so one call took over a
# minute -- and since the System tab re-polls every 30s, calls overlapped and
# stacked up until MariaDB was saturated. Instead, keep the list of value
# changes per topic in memory: the first call folds the full history once,
# later calls only read rows newer than the last one already seen.
_SYSTEM_EVENTS_CACHE = {}          # topic_id -> entry dict (see _system_events_entry)
_SYSTEM_EVENTS_CACHE_GUARD = threading.Lock()
_SYSTEM_EVENTS_MIN_REFRESH_S = 5   # concurrent pollers within this window share one refresh
_SYSTEM_EVENTS_KEEP = 200          # endpoint caps limit at 100; keep headroom, bound memory
_SYSTEM_EVENTS_UNSET = object()

def _system_events_entry(topic_id):
    with _SYSTEM_EVENTS_CACHE_GUARD:
        entry = _SYSTEM_EVENTS_CACHE.get(topic_id)
        if entry is None:
            entry = _SYSTEM_EVENTS_CACHE[topic_id] = {
                'lock': threading.Lock(),
                'events': [],                          # [(ts, value)] oldest -> newest, value changes only
                'last_ts': None,                       # newest reading folded in so far
                'last_value': _SYSTEM_EVENTS_UNSET,
                'checked_at': 0.0,
            }
        return entry

def system_events_recent(topic_id, limit):
    """Newest-first [(ts, value)] of the most recent value changes for one topic."""
    entry = _system_events_entry(topic_id)
    # Blocking lock on purpose: a second poller arriving mid-refresh just waits
    # for the same result instead of launching a second query against the DB.
    with entry['lock']:
        if time.monotonic() - entry['checked_at'] >= _SYSTEM_EVENTS_MIN_REFRESH_S:
            conn = get_boat_db()
            try:
                cur = conn.cursor()
                if entry['last_ts'] is None:
                    cur.execute("SELECT ts, value FROM mqtt_readings WHERE topic_id = %s ORDER BY ts",
                                (topic_id,))
                else:
                    cur.execute("SELECT ts, value FROM mqtt_readings WHERE topic_id = %s AND ts > %s ORDER BY ts",
                                (topic_id, entry['last_ts']))
                while True:
                    rows = cur.fetchmany(5000)
                    if not rows:
                        break
                    for ts, value in rows:
                        if entry['last_value'] is _SYSTEM_EVENTS_UNSET or value != entry['last_value']:
                            entry['events'].append((ts, value))
                        entry['last_value'] = value
                        entry['last_ts'] = ts
                    del entry['events'][:-_SYSTEM_EVENTS_KEEP]
            finally:
                conn.close()
            entry['checked_at'] = time.monotonic()
        return list(reversed(entry['events'][-limit:]))
# ─── end system events siloed addition ──────────────────────────────────────

@app.route('/api/system/events')
def system_events():
    metric = request.args.get('metric', 'throttled')
    try:
        limit = min(int(request.args.get('limit', 15)), 100)
    except ValueError:
        limit = 15
    if metric not in SYSTEM_EVENT_TOPICS:
        return jsonify({'events': [], 'error': 'unknown metric'}), 400

    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM mqtt_topics WHERE topic = %s", (SYSTEM_EVENT_TOPICS[metric],))
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({'events': []})
        conn.close()
        events = [{'time': ts.strftime('%Y-%m-%dT%H:%M:%S'), 'value': value}
                  for ts, value in system_events_recent(row[0], limit)]
        return jsonify({'events': events})
    except Exception as e:
        return jsonify({'events': [], 'error': str(e)})

_prev_cpu_times = None  # (idle, total) from the last /proc/stat sample, for the CPU% delta below

def sample_cpu_percent():
    global _prev_cpu_times
    try:
        with open('/proc/stat') as f:
            fields = f.readline().split()[1:]  # first line: "cpu  <user> <nice> <system> <idle> <iowait> ..."
        values = [int(v) for v in fields]
        idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
        total = sum(values)
    except (OSError, ValueError, IndexError):
        return None
    prev = _prev_cpu_times
    _prev_cpu_times = (idle, total)
    if prev is None:
        return None  # need two samples to compute a delta -- nothing to report yet on the very first tick
    prev_idle, prev_total = prev
    d_total = total - prev_total
    if d_total <= 0:
        return None
    return round(100.0 * (1 - (idle - prev_idle) / d_total), 1)

def sample_mem_percent():
    try:
        info = {}
        with open('/proc/meminfo') as f:
            for line in f:
                k, v = line.split(':', 1)
                info[k.strip()] = int(v.strip().split()[0])  # kB
        total = info.get('MemTotal')
        available = info.get('MemAvailable')
        if not total or available is None:
            return None
        return round(100.0 * (total - available) / total, 1)
    except (OSError, ValueError):
        return None

def sample_disk_percent(path='/'):
    try:
        usage = shutil.disk_usage(path)
        return round(100.0 * usage.used / usage.total, 1)
    except OSError:
        return None

def sample_cpu_temp_c():
    try:
        with open('/sys/class/thermal/thermal_zone0/temp') as f:
            return round(int(f.read().strip()) / 1000.0, 1)
    except (OSError, ValueError):
        return None

def sample_wifi_signal_dbm():
    # wlan0 may simply not be the boat's active connection (this Pi mainly
    # runs on eth0) -- returning None here just means the trend chart shows
    # no data for that period, same as any other not-currently-available metric.
    try:
        state = subprocess.run(['ip', '-br', 'addr', 'show', 'wlan0'], capture_output=True, text=True, timeout=3)
        if 'UP' not in state.stdout:
            return None
        link = subprocess.run(['iw', 'dev', 'wlan0', 'link'], capture_output=True, text=True, timeout=3)
        for line in link.stdout.splitlines():
            line = line.strip()
            if line.startswith('signal:'):
                return float(line.split()[1])  # "signal: -58 dBm"
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        pass
    return None

def sample_bluetooth_active():
    try:
        result = subprocess.run(['systemctl', 'is-active', 'bluetooth'], capture_output=True, text=True, timeout=3)
        return result.stdout.strip() == 'active'
    except (OSError, subprocess.SubprocessError):
        return None  # not installed/managed by systemd on this Pi -- distinct from "installed but off"

def sample_load1():
    try:
        return round(os.getloadavg()[0], 2)
    except OSError:
        return None

# vcgencmd's throttled bitmask packs both "is this happening right now" (bits
# 0-3: under-voltage / arm freq capped / throttled / soft temp limit) and
# "has this happened since boot" (bits 16-19, same order) into one value --
# only the low nibble matters for a live health flag; a one-time boot-time
# brownout showing as "currently throttled" forever would just be noise.
THROTTLE_BITS = [
    (0, 'Under-voltage'),
    (1, 'Frequency capped'),
    (2, 'Throttled'),
    (3, 'Soft temp limit'),
]  # bit+16 is the same condition's "has this happened since boot" flag

def sample_throttled_raw():
    try:
        result = subprocess.run(['vcgencmd', 'get_throttled'], capture_output=True, text=True, timeout=3)
        return int(result.stdout.strip().split('=')[1], 16)
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return None  # not a Pi, or vcgencmd unavailable

def describe_throttled(value):
    """A bare OK/THROTTLED flag throws away exactly the detail that matters --
    which condition (under-voltage/freq cap/active throttle/soft temp limit)
    and whether it happened at some point since boot even if it's clear right
    now, which is a real event worth surfacing (e.g. an intermittent power
    sag) rather than silently clearing itself from view."""
    now = [label for bit, label in THROTTLE_BITS if value & (1 << bit)]
    past = [label for bit, label in THROTTLE_BITS if (value & (1 << (bit + 16))) and label not in now]
    if now:
        return ' + '.join(now)
    if past:
        return f"OK (past: {' + '.join(past)})"
    return 'OK'

_prev_net_bytes = None  # (rx, tx, timestamp) from the last sample, for the KB/s delta below

def sample_net_rates():
    try:
        with open('/sys/class/net/eth0/statistics/rx_bytes') as f:
            rx = int(f.read().strip())
        with open('/sys/class/net/eth0/statistics/tx_bytes') as f:
            tx = int(f.read().strip())
    except (OSError, ValueError):
        return None, None
    global _prev_net_bytes
    now = time.time()
    prev = _prev_net_bytes
    _prev_net_bytes = (rx, tx, now)
    if prev is None:
        return None, None  # need two samples to compute a rate -- nothing to report on the first tick
    prev_rx, prev_tx, prev_t = prev
    dt = now - prev_t
    if dt <= 0:
        return None, None
    return round((rx - prev_rx) / dt / 1024.0, 2), round((tx - prev_tx) / dt / 1024.0, 2)

DISK_DEVICE = 'sda'  # matches df -h's `/` mount (/dev/sda2) on this Pi
_prev_disk_sectors = None  # (read, write, timestamp) from the last sample, for the KB/s delta below

def sample_disk_io_kbps():
    try:
        with open('/proc/diskstats') as f:
            for line in f:
                fields = line.split()
                if fields[2] == DISK_DEVICE:
                    read_sectors, write_sectors = int(fields[5]), int(fields[9])
                    break
            else:
                return None
    except (OSError, ValueError, IndexError):
        return None
    global _prev_disk_sectors
    now = time.time()
    prev = _prev_disk_sectors
    _prev_disk_sectors = (read_sectors, write_sectors, now)
    if prev is None:
        return None  # need two samples to compute a rate -- nothing to report on the first tick
    prev_read, prev_write, prev_t = prev
    dt = now - prev_t
    if dt <= 0:
        return None
    # /proc/diskstats sectors are always 512 bytes regardless of the drive's
    # actual physical sector size -- combined read+write, same as net rates above.
    delta_sectors = (read_sectors - prev_read) + (write_sectors - prev_write)
    return round(delta_sectors * 512 / dt / 1024.0, 2)

def system_health_loop():
    while True:
        time.sleep(SYSTEM_HEALTH_INTERVAL_S)
        try:
            if not mqtt_client or not mqtt_state['connected']:
                continue
            rx_kbps, tx_kbps = sample_net_rates()
            readings = {
                'boat/system/cpu_percent':      sample_cpu_percent(),
                'boat/system/mem_percent':      sample_mem_percent(),
                'boat/system/disk_percent':     sample_disk_percent(),
                'boat/system/cpu_temp_c':       sample_cpu_temp_c(),
                'boat/system/wifi_signal_dbm':  sample_wifi_signal_dbm(),
                'boat/system/load1':            sample_load1(),
                'boat/system/net_rx_kbps':      rx_kbps,
                'boat/system/net_tx_kbps':      tx_kbps,
                'boat/system/disk_io_kbps':     sample_disk_io_kbps(),
                'boat/system/boot_disk_percent': sample_disk_percent('/boot/firmware'),
            }
            for topic, value in readings.items():
                if value is not None:
                    mqtt_client.publish(topic, str(value), retain=False)
            bt = sample_bluetooth_active()
            mqtt_client.publish('boat/system/bluetooth_active', '1' if bt else '0' if bt is not None else '', retain=False)
            throttled_raw = sample_throttled_raw()
            if throttled_raw is not None:
                mqtt_client.publish('boat/system/throttled_active', '1' if (throttled_raw & 0xF) else '0', retain=False)
                mqtt_client.publish('boat/system/throttled_detail', describe_throttled(throttled_raw), retain=False)
            else:
                mqtt_client.publish('boat/system/throttled_active', '', retain=False)
                mqtt_client.publish('boat/system/throttled_detail', '', retain=False)
        except Exception:
            pass  # a bad sample this tick shouldn't kill the loop -- just try again next interval

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

# ─── Tank / battery low-level alarms ────────────────────────────────────────
# Same ntfy push mechanism and notify-on-transition + repeat-while-tripped
# pattern as the anchor watch alarms above (send_ntfy, a per-condition
# *_notified_at timestamp reset to 0 once clear so the next trip notifies
# immediately rather than waiting out a stale repeat window) -- just no GPIO
# buzzer, since that's specifically the anchor watch's own physical alarm
# and there's no way to tell which condition tripped it from a buzzer alone.
#
# Runs on its own, much slower cadence than anchor watch's 5s: tank/battery
# levels change over hours, not seconds, so there's no reason to poll that
# often, and it also means far fewer extra background calls to the Victron
# VRM API (SOC has no MQTT topic to read locally the way tank levels do --
# get_vrm_data() is the same live call /api/victron makes) on top of
# whatever the frontend itself is already polling.
TANK_BATTERY_MONITOR_INTERVAL_S = 120
# 30 min, not anchor watch's 180s -- these conditions can stay tripped for
# hours (e.g. low diesel until the next fuel dock), and repeating every 3
# minutes for that whole stretch would just be naggy.
TANK_BATTERY_NOTIFY_REPEAT_S = 1800

BATTERY_SOC_WARNING_PCT = 30.0
BATTERY_SOC_ALARM_PCT = 20.0
LOW_TANK_ALARM_PCT = 20.0    # diesel + fresh water
BLACK_TANK_ALARM_PCT = 75.0  # high, not low -- needs a pump-out

_tank_battery_monitor = {
    'soc_level': 'normal',  # normal | warning | alarm
    'soc_notified_at': 0,
    'diesel_notified_at': 0,
    'fresh_1_notified_at': 0,
    'fresh_2_notified_at': 0,
    'black_notified_at': 0,
}

def _check_low_tank(key, value, threshold, title, label, emoji):
    tripped = value is not None and value < threshold
    if tripped and time.time() - _tank_battery_monitor[key] > TANK_BATTERY_NOTIFY_REPEAT_S:
        send_ntfy(title, f'{emoji} {label} is at {value:.0f}% -- below the {threshold:.0f}% alarm threshold.', tags='warning')
        _tank_battery_monitor[key] = time.time()
    elif not tripped:
        _tank_battery_monitor[key] = 0

def tank_battery_monitor_loop():
    while True:
        time.sleep(TANK_BATTERY_MONITOR_INTERVAL_S)
        try:
            now = time.time()

            # Battery SOC has two severity levels, unlike the single-
            # threshold tank checks below, so it needs its own small state
            # machine: notify immediately on entering a WORSE level, then
            # keep repeating on the shared timer while still at warning or
            # alarm, and reset (so the next drop notifies right away)
            # once it's back to normal.
            soc = get_vrm_data().get('soc')
            if soc is not None:
                level = ('alarm' if soc < BATTERY_SOC_ALARM_PCT
                         else 'warning' if soc < BATTERY_SOC_WARNING_PCT else 'normal')
                prev = _tank_battery_monitor['soc_level']
                entered_worse = (level == 'alarm' and prev != 'alarm') or (level == 'warning' and prev == 'normal')
                repeat_due = now - _tank_battery_monitor['soc_notified_at'] > TANK_BATTERY_NOTIFY_REPEAT_S
                if level != 'normal' and (entered_worse or repeat_due):
                    if level == 'alarm':
                        send_ntfy('Battery critically low',
                                  f'🔋 House battery SOC is {soc:.0f}% -- below the {BATTERY_SOC_ALARM_PCT:.0f}% alarm threshold.',
                                  tags='warning')
                    else:
                        send_ntfy('Battery low',
                                  f'🔋 House battery SOC is {soc:.0f}% -- below the {BATTERY_SOC_WARNING_PCT:.0f}% warning threshold.',
                                  priority='high', tags='warning')
                    _tank_battery_monitor['soc_notified_at'] = now
                if level == 'normal':
                    _tank_battery_monitor['soc_notified_at'] = 0
                _tank_battery_monitor['soc_level'] = level

            with mqtt_lock:
                topics = mqtt_state['topics']
            def tank_pct(name):
                rec = topics.get(f'boat/nav/tanks/{name}/level')
                return float(rec['value']) if rec else None

            _check_low_tank('diesel_notified_at', tank_pct('diesel'), LOW_TANK_ALARM_PCT, 'Diesel low', 'Diesel tank', '⛽')
            _check_low_tank('fresh_1_notified_at', tank_pct('fresh_1'), LOW_TANK_ALARM_PCT, 'Fresh water low', 'Fresh Bow tank', '🚰')
            _check_low_tank('fresh_2_notified_at', tank_pct('fresh_2'), LOW_TANK_ALARM_PCT, 'Fresh water low', 'Fresh Stern tank', '🚰')

            black = tank_pct('black')
            black_full = black is not None and black >= BLACK_TANK_ALARM_PCT
            if black_full and now - _tank_battery_monitor['black_notified_at'] > TANK_BATTERY_NOTIFY_REPEAT_S:
                send_ntfy('Black water tank full', f'🚽 Black water tank is at {black:.0f}% -- consider a pump-out.', tags='warning')
                _tank_battery_monitor['black_notified_at'] = now
            elif not black_full:
                _tank_battery_monitor['black_notified_at'] = 0
        except Exception:
            pass  # never let one bad reading kill the monitor thread

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
# record_ais_trails() also doubles as the only place that purges stale AIS
# MQTT topics from mqtt_state['topics'] -- see the comment down there.
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

    # Also purge the raw boat/ais/<mmsi>/* MQTT topic entries themselves once
    # a target goes stale -- a real AIS transceiver has no way to send a
    # "this vessel is gone" message when a contact sails out of range, and
    # neither does ais_simulator.py: it just stops publishing that MMSI and
    # starts a new one (see AisTarget.expires_at there). Without this,
    # mqtt_state['topics'] -- and the MQTT Diagnostics tree that reads it
    # directly -- keeps every MMSI that has EVER existed for the life of the
    # process, growing without bound (this is what accumulated into the
    # "few hundred AIS items" seen in Diagnostics). Reuses the exact same
    # staleness signal active_ais_targets() already computed above for the
    # Chart tab, so a target disappears from Diagnostics at the same moment
    # it disappears from the map -- not a separate, only-loosely-related TTL.
    with mqtt_lock:
        for topic in list(mqtt_state['topics'].keys()):
            if not topic.startswith('boat/ais/'):
                continue
            parts = topic.split('/')
            if len(parts) == 4 and parts[2] not in active_mmsis:
                del mqtt_state['topics'][topic]

def ais_trail_monitor_loop():
    while True:
        time.sleep(AIS_TRAIL_INTERVAL_S)
        try:
            record_ais_trails()
        except Exception:
            pass  # never let one bad reading kill the monitor thread
        try:
            record_active_trip_point()
        except Exception:
            pass  # ditto -- a trip-recording DB hiccup shouldn't take down AIS trails

@app.route('/api/ais/own_trail')
def ais_own_trail():
    with AIS_TRAIL_LOCK:
        return jsonify(list(_ais_own_trail))

@app.route('/api/ais/trails')
def ais_trails():
    with AIS_TRAIL_LOCK:
        return jsonify({mmsi: list(trail) for mmsi, trail in _ais_target_trails.items()})

# ─── Trip tracks ─────────────────────────────────────────────────────────────
# Named GPS tracks a user starts/stops recording (e.g. at the start/end of a
# passage), stored in their own trips/trip_points tables rather than derived
# from mqtt_readings -- db_downsample.py collapses anything in mqtt_readings
# older than 30 days to one averaged sample per minute, which would silently
# degrade a *saved* track's resolution a month later. "Is a trip active" is
# the trips row with ended_at IS NULL -- a DB fact, not a flag file, so it
# can't drift and a backend restart just resumes sampling into the same row.
#
# Point recording piggybacks on the AIS-trail monitor's tick (above) rather
# than running a second thread on the same ~5s cadence.
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def _iso_utc(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if dt else None

def record_active_trip_point():
    pos = get_gps_position(max_age_s=GPS_STALE_THRESHOLD_S)
    if pos is None:
        return  # stale/missing fix -- skip this tick rather than logging a bad point
    conn = get_boat_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM trips WHERE ended_at IS NULL LIMIT 1")
        row = cur.fetchone()
        if row:
            cur.execute("INSERT INTO trip_points (trip_id, ts, lat, lon) VALUES (%s, %s, %s, %s)",
                        (row[0], datetime.now(timezone.utc), pos[0], pos[1]))
            conn.commit()
    finally:
        conn.close()

@app.route('/api/trips')
def trips_list():
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.name, t.started_at, t.ended_at, t.distance_m, COUNT(p.id)
            FROM trips t LEFT JOIN trip_points p ON p.trip_id = t.id
            GROUP BY t.id ORDER BY t.started_at DESC
        """)
        rows = cur.fetchall()
        conn.close()
        trips = [{
            'id': r[0], 'name': r[1],
            'started_at': _iso_utc(r[2]), 'ended_at': _iso_utc(r[3]),
            'distance_m': r[4], 'point_count': r[5], 'active': r[3] is None,
        } for r in rows]
        return jsonify({'trips': trips})
    except Exception as e:
        return jsonify({'trips': [], 'error': str(e)})

@app.route('/api/trips/<int:trip_id>/points')
def trip_points_route(trip_id):
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT ts, lat, lon FROM trip_points WHERE trip_id=%s ORDER BY ts", (trip_id,))
        rows = cur.fetchall()
        conn.close()
        return jsonify({'points': [{'t': _iso_utc(r[0]), 'lat': r[1], 'lon': r[2]} for r in rows]})
    except Exception as e:
        return jsonify({'points': [], 'error': str(e)})

# ─── Incremental trip points (siloed addition) ─────────────────────────────
# The Chart tab polls the recording trip every 5 s. Re-sending the whole
# trail each time got slower as the trip grew -- a trip left recording for
# 17 days (250k points) took ~3 minutes per fetch, the polls piled up and
# pegged the Pi. The page now fetches the trail once, then only the points
# after the last timestamp it has, via ?after=<_iso_utc timestamp>.
@app.route('/api/trips/<int:trip_id>/points_after')
def trip_points_after_route(trip_id):
    try:
        after = datetime.strptime(request.args['after'], '%Y-%m-%dT%H:%M:%S.%fZ')
    except (KeyError, ValueError):
        return jsonify({'error': 'after=<YYYY-MM-DDTHH:MM:SS.sssZ> required'}), 400
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT ts, lat, lon FROM trip_points WHERE trip_id=%s AND ts>%s ORDER BY ts",
                    (trip_id, after))
        rows = cur.fetchall()
        conn.close()
        return jsonify({'points': [{'t': _iso_utc(r[0]), 'lat': r[1], 'lon': r[2]} for r in rows]})
    except Exception as e:
        return jsonify({'points': [], 'error': str(e)})
# ─── end incremental trip points siloed addition ───────────────────────────

# ─── Thinned trip points for display (siloed addition) ─────────────────────
# Drawing a track doesn't need every 5-second point: a trip left recording
# for days is hundreds of thousands of rows, and pulling them all through
# Python took minutes. Past TRIP_DISPLAY_MAX_POINTS, MySQL keeps every Nth
# point (plus the last) so only ~that many rows ever leave the database.
# /points still returns the full-resolution track.
TRIP_DISPLAY_MAX_POINTS = 5000

@app.route('/api/trips/<int:trip_id>/points_display')
def trip_points_display_route(trip_id):
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trip_points WHERE trip_id=%s", (trip_id,))
        total = cur.fetchone()[0]
        step = -(-total // TRIP_DISPLAY_MAX_POINTS)  # ceiling division
        if step <= 1:
            cur.execute("SELECT ts, lat, lon FROM trip_points WHERE trip_id=%s ORDER BY ts", (trip_id,))
        else:
            cur.execute("""
                SELECT ts, lat, lon FROM (
                    SELECT ts, lat, lon, ROW_NUMBER() OVER (ORDER BY ts) AS rn
                    FROM trip_points WHERE trip_id=%s
                ) t WHERE MOD(rn - 1, %s) = 0 OR rn = %s ORDER BY ts""", (trip_id, step, total))
        rows = cur.fetchall()
        conn.close()
        return jsonify({'points': [{'t': _iso_utc(r[0]), 'lat': r[1], 'lon': r[2]} for r in rows],
                        'total': total, 'thinned': step > 1})
    except Exception as e:
        return jsonify({'points': [], 'error': str(e)})
# ─── end thinned trip points siloed addition ───────────────────────────────

@app.route('/api/trips/start', methods=['POST'])
def trip_start():
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400
    if len(name) > 120:
        return jsonify({'error': 'name must be 120 characters or fewer'}), 400
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM trips WHERE ended_at IS NULL LIMIT 1")
        if cur.fetchone():
            conn.close()
            return jsonify({'error': 'A trip is already recording -- stop it first'}), 409
        started_at = datetime.now(timezone.utc)
        cur.execute("INSERT INTO trips (name, started_at) VALUES (%s, %s)", (name, started_at))
        conn.commit()
        trip_id = cur.lastrowid
        conn.close()
        return jsonify({'id': trip_id, 'name': name, 'started_at': _iso_utc(started_at),
                         'ended_at': None, 'active': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trips/stop', methods=['POST'])
def trip_stop():
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM trips WHERE ended_at IS NULL LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'No trip is currently recording'}), 404
        trip_id = row[0]
        cur.execute("SELECT lat, lon FROM trip_points WHERE trip_id=%s ORDER BY ts", (trip_id,))
        pts = cur.fetchall()
        distance_m = sum(haversine_m(pts[i][0], pts[i][1], pts[i + 1][0], pts[i + 1][1])
                          for i in range(len(pts) - 1))
        ended_at = datetime.now(timezone.utc)
        cur.execute("UPDATE trips SET ended_at=%s, distance_m=%s WHERE id=%s",
                    (ended_at, distance_m, trip_id))
        conn.commit()
        conn.close()
        return jsonify({'id': trip_id, 'ended_at': _iso_utc(ended_at), 'distance_m': distance_m, 'active': False})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trips/<int:trip_id>', methods=['DELETE'])
def trip_delete(trip_id):
    try:
        conn = get_boat_db()
        cur = conn.cursor()
        cur.execute("SELECT ended_at FROM trips WHERE id=%s", (trip_id,))
        row = cur.fetchone()
        if row is None:
            conn.close()
            return jsonify({'error': 'Trip not found'}), 404
        if row[0] is None:
            conn.close()
            return jsonify({'error': 'Cannot delete a trip that is currently recording -- stop it first'}), 409
        cur.execute("DELETE FROM trips WHERE id=%s", (trip_id,))  # cascades to trip_points
        conn.commit()
        conn.close()
        return jsonify({'deleted': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Weather forecast (National Weather Service, api.weather.gov) ──────────────
# Free, no API key, but wants a real User-Agent and shouldn't be hammered — cached
# server-side so every browser poll doesn't trigger a fresh upstream call. Follows
# live GPS (falling back to the home-port default with no fix yet) — cache is
# invalidated early if the boat has moved far enough that the forecast grid box
# is probably stale, not just on a fixed timer. Note NWS only covers US waters/
# territories, so this naturally stops being useful once actually offshore —
# that's a real limitation of the data source, not something to work around here.
WEATHER_LAT, WEATHER_LON = 27.7000, -82.6900  # fallback until a GPS fix exists
WEATHER_CACHE_TTL_S = 1800  # 30 min — forecast periods don't change faster than this
WEATHER_CACHE_MOVE_THRESHOLD_NM = 10  # refresh early if the boat's moved further than this since the cached fetch
_weather_cache = {'data': None, 'fetched_at': 0, 'lat': None, 'lon': None}

def _nm_between(lat1, lon1, lat2, lon2):
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat1))
    dx = (lon2 - lon1) * m_per_deg_lon
    dy = (lat2 - lat1) * m_per_deg_lat
    return math.hypot(dx, dy) / 1852.0

def _nws_grid_url(lat, lon, headers, field):
    """Looks up NWS's /points grid info and returns the requested product
    URL (field is 'forecast' or 'forecastHourly'), or raises RuntimeError
    with a clean explanation if this location has none. Most commonly hit
    offshore: NWS's /points lookup marks open-water locations type=marine
    with gridId/forecast/forecastHourly all null (only coastal & inland
    points fall inside one of their gridded-forecast WFO areas) -- and
    their own API 404s the marine-zone-forecast equivalent product as "not
    yet supported" (confirmed live against a real Gulf marine zone), so
    there's no clean JSON fallback available to reach for instead. Letting
    requests.get(None, ...) run unchecked instead raises a raw
    "Invalid URL 'None': No scheme supplied" requests.exceptions.MissingSchema,
    which is what a caller would otherwise see verbatim -- accurate about
    *why* nothing loaded, but useless as user-facing text."""
    points = requests.get(f'https://api.weather.gov/points/{lat},{lon}', headers=headers, timeout=8).json()
    props = points.get('properties', {})
    url = props.get(field)
    if url:
        return url
    if props.get('type') == 'marine':
        raise RuntimeError('offshore / marine zone -- NWS only publishes gridded forecasts for coastal & inland points, not open water')
    raise RuntimeError('no forecast grid for this location')

@app.route('/api/weather/forecast')
def weather_forecast():
    pos = get_gps_position()
    lat, lon = pos if pos is not None else (WEATHER_LAT, WEATHER_LON)

    now = time.time()
    cache_fresh = bool(_weather_cache['data']) and (now - _weather_cache['fetched_at'] < WEATHER_CACHE_TTL_S)
    if cache_fresh and _weather_cache['lat'] is not None:
        if _nm_between(_weather_cache['lat'], _weather_cache['lon'], lat, lon) > WEATHER_CACHE_MOVE_THRESHOLD_NM:
            cache_fresh = False
    if cache_fresh:
        return jsonify(_weather_cache['data'])
    try:
        headers = {'User-Agent': 'exit-strategy-dashboard (github.com/mikemc)'}
        forecast_url = _nws_grid_url(lat, lon, headers, 'forecast')
        forecast = requests.get(forecast_url, headers=headers, timeout=8).json()
        periods = forecast['properties']['periods'][:6]
        data = {
            'periods': [{
                'name': p['name'],
                'startTime': p['startTime'],
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
        _weather_cache['lat'] = lat
        _weather_cache['lon'] = lon
        return jsonify(data)
    except Exception as e:
        if _weather_cache['data']:
            return jsonify(_weather_cache['data'])  # serve stale rather than nothing on a transient failure
        return jsonify({'error': str(e)}), 502

# ─── Hourly forecast (for the per-day drilldown when a forecast card is clicked) ─
# Same points-lookup dance as the daily forecast, just a different NWS product
# (forecastHourly instead of forecast) — ~150hrs out, one calendar-local
# startTime per entry, which the frontend buckets by day.
HOURLY_CACHE_TTL_S = 1800
_hourly_cache = {'data': None, 'fetched_at': 0, 'lat': None, 'lon': None}

@app.route('/api/weather/hourly')
def weather_hourly():
    pos = get_gps_position()
    lat, lon = pos if pos is not None else (WEATHER_LAT, WEATHER_LON)

    now = time.time()
    cache_fresh = bool(_hourly_cache['data']) and (now - _hourly_cache['fetched_at'] < HOURLY_CACHE_TTL_S)
    if cache_fresh and _hourly_cache['lat'] is not None:
        if _nm_between(_hourly_cache['lat'], _hourly_cache['lon'], lat, lon) > WEATHER_CACHE_MOVE_THRESHOLD_NM:
            cache_fresh = False
    if cache_fresh:
        return jsonify(_hourly_cache['data'])
    try:
        headers = {'User-Agent': 'exit-strategy-dashboard (github.com/mikemc)'}
        hourly_url = _nws_grid_url(lat, lon, headers, 'forecastHourly')
        hourly = requests.get(hourly_url, headers=headers, timeout=8).json()
        periods = hourly['properties']['periods']
        data = {
            'periods': [{
                'startTime': p['startTime'],
                'temperature': p['temperature'],
                'temperatureUnit': p['temperatureUnit'],
                'shortForecast': p['shortForecast'],
                'windSpeed': p['windSpeed'],
                'windDirection': p['windDirection'],
                'precipChance': (p.get('probabilityOfPrecipitation') or {}).get('value'),
                'isDaytime': p['isDaytime'],
            } for p in periods],
            'updated': datetime.now(timezone.utc).isoformat(),
        }
        _hourly_cache['data'] = data
        _hourly_cache['fetched_at'] = now
        _hourly_cache['lat'] = lat
        _hourly_cache['lon'] = lon
        return jsonify(data)
    except Exception as e:
        if _hourly_cache['data']:
            return jsonify(_hourly_cache['data'])
        return jsonify({'error': str(e)}), 502

# ─── Marine alerts (Small Craft Advisory, Gale Warning, etc.) ──────────────────
# Same api.weather.gov source as the forecast, but a much shorter cache — these
# need to show up fast, not sit behind a 30-min TTL like forecast text does.
ALERTS_CACHE_TTL_S = 300  # 5 min
_alerts_cache = {'data': None, 'fetched_at': 0, 'lat': None, 'lon': None}

@app.route('/api/weather/alerts')
def weather_alerts():
    pos = get_gps_position()
    lat, lon = pos if pos is not None else (WEATHER_LAT, WEATHER_LON)

    now = time.time()
    cache_fresh = bool(_alerts_cache['data'] is not None) and (now - _alerts_cache['fetched_at'] < ALERTS_CACHE_TTL_S)
    if cache_fresh and _alerts_cache['lat'] is not None:
        if _nm_between(_alerts_cache['lat'], _alerts_cache['lon'], lat, lon) > WEATHER_CACHE_MOVE_THRESHOLD_NM:
            cache_fresh = False
    if cache_fresh:
        return jsonify(_alerts_cache['data'])
    try:
        headers = {'User-Agent': 'exit-strategy-dashboard (github.com/mikemc)'}
        resp = requests.get('https://api.weather.gov/alerts/active', params={'point': f'{lat},{lon}'},
                             headers=headers, timeout=8).json()
        alerts = [{
            'event': f['properties']['event'],
            'severity': f['properties']['severity'],
            'headline': f['properties']['headline'],
            'description': f['properties']['description'],
            'instruction': f['properties'].get('instruction'),
            'area_desc': f['properties'].get('areaDesc'),
            'effective': f['properties']['effective'],
            'expires': f['properties']['expires'],
        } for f in resp.get('features', [])]
        data = {'alerts': alerts, 'updated': datetime.now(timezone.utc).isoformat()}
        _alerts_cache['data'] = data
        _alerts_cache['fetched_at'] = now
        _alerts_cache['lat'] = lat
        _alerts_cache['lon'] = lon
        return jsonify(data)
    except Exception as e:
        if _alerts_cache['data'] is not None:
            return jsonify(_alerts_cache['data'])
        return jsonify({'error': str(e)}), 502

# ─── Chart tab (NOAA NCDS pre-rendered base + ENC vector overlay) ──────────────
# Base chart is NOAA's own Chart Display Service (NCDS) -- a single MBTiles
# (SQLite) file downloaded via chart_tools.py, queried directly per-tile here.
# No server-side rendering for it: NOAA already rendered it. What's still
# vector (soundings, aids to navigation, hazards, bridges) is pre-converted
# to GeoJSON offline by the same tool, same as the base ENC pipeline.
CHART_DATA_DIR = '/home/mikemc/dashboard-dev/chart_data/processed'
NCDS_DIR = '/home/mikemc/dashboard-dev/chart_data/ncds'

def _ncds_files():
    if not os.path.isdir(NCDS_DIR):
        return []
    return sorted(glob.glob(os.path.join(NCDS_DIR, '*.mbtiles')))

@app.route('/api/charts/ncds/meta')
def ncds_meta():
    files = _ncds_files()
    if not files:
        return jsonify({'available': False})
    # NOAA's regions tile the coast without gaps, so a request may land in
    # any one of them -- union everything into one logical base layer rather
    # than making the frontend juggle a tile layer per region.
    bounds = None
    min_zoom = None
    max_zoom_any = None     # highest native zoom in ANY region -- informational only
    maxzoom_values = []      # native max per region -- see max_zoom_native comment below
    fmt = 'png'
    for path in files:
        conn = sqlite3.connect(path)
        try:
            meta = dict(conn.execute('SELECT name, value FROM metadata').fetchall())
        except sqlite3.DatabaseError:
            # A region file mid-download (chart_tools.py now downloads to a
            # .part name and renames atomically, so this shouldn't happen for
            # new downloads) or otherwise corrupt -- skip it rather than
            # taking the whole endpoint down.
            continue
        finally:
            conn.close()
        fmt = meta.get('format', fmt)
        if 'minzoom' in meta:
            mz = int(meta['minzoom'])
            min_zoom = mz if min_zoom is None else min(min_zoom, mz)
        if 'maxzoom' in meta:
            xz = int(meta['maxzoom'])
            max_zoom_any = xz if max_zoom_any is None else max(max_zoom_any, xz)
            maxzoom_values.append(xz)
        if 'bounds' in meta:
            west, south, east, north = (float(v) for v in meta['bounds'].split(','))
            if bounds is None:
                bounds = {'west': west, 'south': south, 'east': east, 'north': north}
            else:
                bounds['west'] = min(bounds['west'], west)
                bounds['south'] = min(bounds['south'], south)
                bounds['east'] = max(bounds['east'], east)
                bounds['north'] = max(bounds['north'], north)
    # The safe ceiling for the layer's maxNativeZoom option -- using the
    # strict minimum across every region broke as soon as coverage grew past
    # the East Coast/Gulf/Caribbean set: one remote, coarser region (a small
    # Pacific NW-adjacent area, native max 13 vs. everyone else's 16-18)
    # dragged the global ceiling down to 13, so home port and everywhere else
    # started rendering an upscaled, blurry z13 tile instead of their own
    # real z16 imagery. The most common native max across regions is a much
    # better ceiling: a lone outlier no longer holds the rest of the country
    # hostage, and it still only 404s past its own real resolution for that
    # one outlier region specifically, exactly like a normal over-zoom would.
    max_zoom_native = Counter(maxzoom_values).most_common(1)[0][0] if maxzoom_values else 16
    return jsonify({
        'available': True,
        'format': fmt,
        'minZoom': min_zoom if min_zoom is not None else 0,
        'maxZoom': max_zoom_any if max_zoom_any is not None else 16,
        'maxNativeZoom': max_zoom_native,
        'bounds': bounds,
        'regionCount': len(files),
    })

# _ncds_lookup_tile used to open a brand-new sqlite3 connection against every
# single region .mbtiles file (47 of them, 22GB total -- well past this Pi's
# 3.7GB RAM, so most of that is cold on disk, not page-cache-resident) for
# EVERY tile request, then throw the connection away. A zoomed-in viewport
# needing a few dozen tiles meant 1000+ fresh SQLite file-opens hitting disk.
#
# _ncds_region_bounds() below caches each file's declared lon/lat bbox (read
# once, from the metadata table also used by ncds_meta()), so a tile whose
# bbox can't possibly overlap a region skips that file's query entirely.
# Bboxes still overlap at low zoom (see _ncds_lookup_tile's docstring), so
# this doesn't shrink the candidate list to one there -- but at the
# city/harbor zoom levels people actually navigate at, a tile's bbox is tiny
# and this cuts dozens of irrelevant multi-hundred-MB files out of every
# single request, which is where nearly all of the real win is.
#
# An earlier version of this also cached one shared, reused sqlite3
# connection per file (check_same_thread=False) to skip the file-open cost
# too. That caused a real production incident: Python's own docs are clear
# that check_same_thread=False only disables sqlite3's OWN safety check, it
# does not add any actual cross-thread locking -- and Flask's threaded=True
# dev server hands every concurrent request its own thread, so a normal
# chart pan (dozens of simultaneous tile requests) meant many threads
# hitting the SAME connection object at once. Observed result: threads
# piling up (16+ stuck at once) and one worker process pegging 2+ CPU cores
# for hours. Reverted to a plain, independent open-query-close per call
# below -- exactly the original's connection lifecycle, just gated by the
# bbox filter so far fewer files ever need to be opened.
_ncds_bounds_cache = {}

def _ncds_region_bounds(path):
    """(west, south, east, north) from this file's metadata, or None if
    unavailable -- cached after the first read since these files are static
    once downloaded. Independent short-lived connection, same as any other
    one-off metadata read (see ncds_meta()) -- deliberately NOT a shared
    connection reused across threads (see the incident note above)."""
    if path in _ncds_bounds_cache:
        return _ncds_bounds_cache[path]
    bounds = None
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        meta = dict(conn.execute('SELECT name, value FROM metadata').fetchall())
        if 'bounds' in meta:
            west, south, east, north = (float(v) for v in meta['bounds'].split(','))
            bounds = (west, south, east, north)
    except sqlite3.DatabaseError:
        bounds = None  # mid-download or otherwise corrupt -- treat as "can't tell, don't skip"
    finally:
        conn.close()
    _ncds_bounds_cache[path] = bounds
    return bounds

def _tile_bounds_lonlat(z, x, y):
    """Standard XYZ slippy-map tile -> (west, south, east, north) in degrees."""
    n = 2 ** z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (west, south, east, north)

def _ncds_lookup_tile(files, z, x, y):
    """Best real tile at exactly this z/x/y across every region file, or
    None if nothing has a row there.
    Regions' rectangular bounding boxes overlap at low zoom even though
    their real detailed coverage doesn't -- NOAA's own MBTiles fill that
    whole rectangle with tiles, including a blank placeholder (~190 bytes)
    for the parts outside real coverage. So more than one region file can
    have a row at the same z/x/y, and taking the first hit (alphabetical
    file order) can return a neighboring region's blank placeholder instead
    of the real content sitting in the correct one. Checking every match
    and keeping the largest reliably picks the real tile: actual chart
    imagery compresses to KB, blank placeholders don't."""
    tms_row = (2 ** z - 1) - y  # MBTiles stores rows TMS-style; Leaflet requests XYZ
    tile_w, tile_s, tile_e, tile_n = _tile_bounds_lonlat(z, x, y)
    best = None
    for path in files:
        bounds = _ncds_region_bounds(path)
        if bounds is not None:
            west, south, east, north = bounds
            if tile_e < west or tile_w > east or tile_n < south or tile_s > north:
                continue  # this region's own bbox can't contain this tile -- skip the query
        conn = sqlite3.connect(path)
        try:
            row = conn.execute(
                'SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?',
                (z, x, tms_row)
            ).fetchone()
        except sqlite3.DatabaseError:
            continue  # mid-download or otherwise corrupt file -- skip, don't 500 the tile request
        finally:
            conn.close()
        if row is not None and (best is None or len(row[0]) > len(best)):
            best = row[0]
    return best

NCDS_OVERZOOM_MAX_LEVELS = 8  # how far up the ancestor chain to search before giving up on a missing tile

@app.route('/api/charts/ncds/tiles/<int:z>/<int:x>/<int:y>.png')
def ncds_tile(z, x, y):
    files = _ncds_files()
    if not files:
        return '', 404

    data = _ncds_lookup_tile(files, z, x, y)
    if data is not None:
        return Response(data, mimetype='image/png')

    # No tile at this exact z/x/y -- NOAA's own detailed rendering doesn't
    # cover every square inch of a region even within its own declared
    # maxzoom (spot-checked once: a region with 200k+ real z16 tiles still
    # had none within 150+ tiles of a specific Gulf-coast point that DOES
    # have real z15 coverage). Leaflet's maxNativeZoom already gives
    # "zoom past the edge, see an upscaled blurry tile" for free once a
    # whole REGION tops out at some zoom -- this is the same idea applied
    # per-tile instead of globally, for small gaps inside an otherwise-
    # detailed region. Walk up the pyramid to the nearest ancestor zoom
    # that DOES have a real tile, crop out the sub-square this tile
    # corresponds to, and scale it back up to 256x256.
    for k in range(1, NCDS_OVERZOOM_MAX_LEVELS + 1):
        pz = z - k
        if pz < 0:
            break
        tile_px = 256 >> k
        if tile_px < 1:
            break
        ancestor = _ncds_lookup_tile(files, pz, x >> k, y >> k)
        if ancestor is None:
            continue
        sub_x, sub_y = x & ((1 << k) - 1), y & ((1 << k) - 1)
        left, top = sub_x * tile_px, sub_y * tile_px
        img = Image.open(io.BytesIO(ancestor)).convert('RGBA')
        crop = img.resize((256, 256), Image.LANCZOS, box=(left, top, left + tile_px, top + tile_px))
        buf = io.BytesIO()
        crop.save(buf, format='PNG')
        return Response(buf.getvalue(), mimetype='image/png')

    return '', 404

@app.route('/api/charts/cells')
def chart_cells():
    cells = []
    if os.path.isdir(CHART_DATA_DIR):
        for name in sorted(os.listdir(CHART_DATA_DIR)):
            meta_path = os.path.join(CHART_DATA_DIR, name, 'meta.json')
            if os.path.isfile(meta_path):
                with open(meta_path) as f:
                    cells.append(json.load(f))
    return jsonify({'cells': cells})

@app.route('/api/charts/<cell>/<layer>')
def chart_layer(cell, layer):
    # cell/layer come straight off the URL -- restrict to exactly what
    # chart_tools.py can produce (send_from_directory itself blocks path
    # traversal; this just avoids serving anything that isn't a chart file).
    if not cell.isalnum() or not (layer == 'meta.json' or layer.endswith('.geojson')):
        return jsonify({'error': 'not found'}), 404
    cell_dir = os.path.join(CHART_DATA_DIR, cell)
    if not os.path.isdir(cell_dir):
        return jsonify({'error': 'unknown cell'}), 404
    return send_from_directory(cell_dir, layer)

# ─── O-Charts base layer (siloed addition) ─────────────────────────────────
# Third base source for the Chart tab, drawn on demand from the decrypted
# O-charts files in ~/ocharts/exported by ochart_tiles.py (and cached on disk
# under chart_data/ocharts_cache). Nothing here touches the NCDS code above;
# to back it out, delete this block, ochart_tiles.py and osenc_parse.py.
import ochart_tiles

@app.route('/api/charts/ocharts/meta')
def ocharts_meta():
    return jsonify(ochart_tiles.index_summary())

@app.route('/api/charts/ocharts/tiles/<int:z>/<int:x>/<int:y>.png')
def ocharts_tile(z, x, y):
    data = ochart_tiles.get_tile(z, x, y)
    if data is ochart_tiles.BUSY:
        return '', 503   # waited too long behind other tiles; the page retries it
    if data is None:
        return '', 404
    resp = Response(data, mimetype='image/png')
    resp.headers['Cache-Control'] = 'public, max-age=86400'
    return resp

# Aids/lights/bridges/hazards/areas/soundings for the current view -- fed into
# the Chart tab's existing overlay checkboxes while O-Charts is the base.
@app.route('/api/charts/ocharts/features')
def ocharts_features():
    try:
        z = int(request.args['z'])
        west, south, east, north = (float(request.args[k]) for k in ('west', 'south', 'east', 'north'))
    except (KeyError, ValueError):
        return jsonify({'error': 'z, west, south, east, north required'}), 400
    data = ochart_tiles.features_for_view(z, west, south, east, north)
    if data is None:
        return jsonify({'error': 'view too large'}), 400
    return jsonify(data)
# ─── end O-Charts siloed addition ──────────────────────────────────────────

# ─── Merged NOAA + O-Charts base (siloed addition) ─────────────────────────
# One base layer for the Chart tab: NOAA NCDS drawn on top of O-Charts. NCDS
# tiles are transparent outside NOAA coverage (e.g. BVI), so wherever NOAA has
# the area it wins and O-Charts only shows through the gaps. Reuses the NCDS
# helpers above and ochart_tiles without changing either; to back it out,
# delete this block and point loadNcdsBase() back at /api/charts/ncds/tiles.
#
# NOAA past its native zoom is only trusted a couple of levels deep: in the
# Bahamas NCDS has nothing but small-scale charts, and a z8 tile blown up to
# z14 is giant lettering -- there the real O-Charts detail is used instead,
# and the deep upscale is only a last resort when O-Charts has nothing.
MERGED_NOAA_OVERZOOM = 2

def _merged_ncds_image(files, z, x, y, max_levels):
    """NCDS tile (RGBA Image) at z/x/y, upscaled from an ancestor at most
    max_levels up -- same crop-and-scale as ncds_tile() -- or None."""
    for k in range(0, max_levels + 1):
        if z - k < 0:
            break
        data = _ncds_lookup_tile(files, z - k, x >> k, y >> k)
        if data is None:
            continue
        img = Image.open(io.BytesIO(data)).convert('RGBA')
        if k == 0:
            return img
        tile_px = 256 >> k
        left, top = (x & ((1 << k) - 1)) * tile_px, (y & ((1 << k) - 1)) * tile_px
        return img.resize((256, 256), Image.LANCZOS, box=(left, top, left + tile_px, top + tile_px))
    return None

def _merged_png(img):
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    resp = Response(buf.getvalue(), mimetype='image/png')
    resp.headers['Cache-Control'] = 'public, max-age=86400'
    return resp

@app.route('/api/charts/merged/tiles/<int:z>/<int:x>/<int:y>.png')
def merged_tile(z, x, y):
    files = _ncds_files()
    noaa = _merged_ncds_image(files, z, x, y, MERGED_NOAA_OVERZOOM) if files else None
    if noaa is not None and noaa.getchannel('A').getextrema()[0] == 255:
        return _merged_png(noaa)   # NOAA covers the whole tile -- no need to draw O-Charts

    oc = ochart_tiles.get_tile(z, x, y) if ochart_tiles.MIN_ZOOM <= z <= ochart_tiles.MAX_ZOOM else None
    if oc is ochart_tiles.BUSY:
        return '', 503   # the page retries it
    if oc is not None:
        base = Image.open(io.BytesIO(oc)).convert('RGBA')
        if noaa is not None:
            base.alpha_composite(noaa)
        return _merged_png(base)
    if noaa is not None:
        return _merged_png(noaa)
    if files:
        deep = _merged_ncds_image(files, z, x, y, NCDS_OVERZOOM_MAX_LEVELS)
        if deep is not None:
            return _merged_png(deep)
    return '', 404

# O-Charts aids/soundings/etc. for the view, minus anything inside a NOAA ENC
# cell -- the page still loads those cells itself, so NOAA wins the overlap.
_merged_cell_bounds = None

def _merged_noaa_cell_bounds():
    global _merged_cell_bounds
    if _merged_cell_bounds is None:
        boxes = []
        if os.path.isdir(CHART_DATA_DIR):
            for name in os.listdir(CHART_DATA_DIR):
                try:
                    with open(os.path.join(CHART_DATA_DIR, name, 'meta.json')) as f:
                        b = json.load(f).get('bounds')
                except (OSError, ValueError):
                    continue
                if b:
                    boxes.append((b['west'], b['south'], b['east'], b['north']))
        _merged_cell_bounds = boxes
    return _merged_cell_bounds

def _merged_feature_point(geom):
    """A representative lon/lat for a feature: the point itself, or the middle
    of a polygon's bounding box."""
    coords = geom.get('coordinates')
    if geom.get('type') == 'Point':
        return coords[0], coords[1]
    pts = []
    def walk(c):
        if c and isinstance(c[0], (int, float)):
            pts.append(c)
        else:
            for sub in c:
                walk(sub)
    walk(coords)
    if not pts:
        return None
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return (min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2

@app.route('/api/charts/merged/features')
def merged_features():
    try:
        z = int(request.args['z'])
        west, south, east, north = (float(request.args[k]) for k in ('west', 'south', 'east', 'north'))
    except (KeyError, ValueError):
        return jsonify({'error': 'z, west, south, east, north required'}), 400
    data = ochart_tiles.features_for_view(z, west, south, east, north)
    if data is None:
        return jsonify({'error': 'view too large'}), 400
    noaa = [b for b in _merged_noaa_cell_bounds()
            if not (b[0] > east or b[2] < west or b[1] > north or b[3] < south)]
    if noaa:
        for fc in data['layers'].values():
            kept = []
            for feat in fc['features']:
                p = _merged_feature_point(feat['geometry'])
                if p is None or not any(b[0] <= p[0] <= b[2] and b[1] <= p[1] <= b[3] for b in noaa):
                    kept.append(feat)
            fc['features'] = kept
    return jsonify(data)
# ─── end merged NOAA + O-Charts siloed addition ─────────────────────────────

# ─── MOB / man overboard marks (siloed addition) ───────────────────────────
MOB_MARKS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mob_marks.json')
MOB_MARKS_LOCK = threading.Lock()

def load_mob_marks():
    with MOB_MARKS_LOCK:
        if os.path.exists(MOB_MARKS_FILE):
            with open(MOB_MARKS_FILE) as f:
                return json.load(f)
    return []

def save_mob_marks(marks):
    with MOB_MARKS_LOCK:
        with open(MOB_MARKS_FILE, 'w') as f:
            json.dump(marks, f)

@app.route('/api/mob/marks')
def mob_marks_route():
    return jsonify(load_mob_marks())

@app.route('/api/mob/mark', methods=['POST'])
def mob_mark():
    # Always the boat's own live GPS fix, not a client-supplied position --
    # a MOB mark is only meaningful if it's the vessel's actual position at
    # the moment of the button press, same reasoning as anchor_drop()'s
    # default (dashboard_api.py's get_gps_position/GPS_STALE_THRESHOLD_S).
    pos = get_gps_position(max_age_s=GPS_STALE_THRESHOLD_S)
    if pos is None:
        return jsonify({'error': 'No live GPS position available (missing or stale)'}), 503
    lat, lon = pos
    mark = {'id': uuid.uuid4().hex, 'lat': lat, 'lon': lon, 't': datetime.now(timezone.utc).isoformat()}
    marks = load_mob_marks()
    marks.append(mark)
    save_mob_marks(marks)
    return jsonify(mark)

@app.route('/api/mob/marks/<mark_id>', methods=['DELETE'])
def mob_clear(mark_id):
    marks = [m for m in load_mob_marks() if m['id'] != mark_id]
    save_mob_marks(marks)
    return jsonify({'ok': True})
# ─── end MOB siloed addition ────────────────────────────────────────────────

# ─── Wind velocity overlay (siloed addition) ────────────────────────────────
# Hardcoded to the dashboard-dev tree, same reasoning as CHART_DATA_DIR/
# NCDS_DIR above -- this file also runs as a deployed copy at
# /home/mikemc/dashboard_api.py, where a path derived from __file__ would
# resolve to the wrong directory (wind_grid.py only exists in dashboard-dev).
# Both the dev and prod copies of this file end up sharing the same cache
# file/script this way, so only one of them actually needs to do the fetch.
WIND_DIR = '/home/mikemc/dashboard-dev'
WIND_DATA_DIR = os.path.join(WIND_DIR, 'chart_data', 'wind')
WIND_MANIFEST_PATH = os.path.join(WIND_DATA_DIR, 'manifest.json')
WIND_SCRIPT_PATH = os.path.join(WIND_DIR, 'wind_grid.py')
WIND_CHECK_INTERVAL_S = 1800  # GFS only updates every 6h -- this just polls for a new run

def wind_monitor_loop():
    while True:
        try:
            subprocess.run(['python3', WIND_SCRIPT_PATH, 'fetch-if-stale', WIND_DATA_DIR],
                            capture_output=True, timeout=180, check=False)
        except Exception:
            pass
        time.sleep(WIND_CHECK_INTERVAL_S)

threading.Thread(target=wind_monitor_loop, daemon=True).start()

def _load_wind_manifest():
    if not os.path.exists(WIND_MANIFEST_PATH):
        return None
    with open(WIND_MANIFEST_PATH) as f:
        return json.load(f)

@app.route('/api/wind/manifest')
def wind_manifest():
    manifest = _load_wind_manifest()
    if manifest is None:
        return jsonify({'error': 'wind data not yet available'}), 503
    return jsonify(manifest)

@app.route('/api/wind/velocity')
def wind_velocity():
    manifest = _load_wind_manifest()
    if manifest is None:
        return jsonify({'error': 'wind data not yet available'}), 503
    try:
        hour = int(request.args.get('hour', 0))
    except (TypeError, ValueError):
        return jsonify({'error': 'hour must be an integer'}), 400
    if hour not in {f['hour'] for f in manifest['forecasts']}:
        return jsonify({'error': f'hour {hour} not in this cycle -- see /api/wind/manifest'}), 404
    filename = f"wind_{manifest['cycle']}_f{hour:03d}.json"
    return send_from_directory(WIND_DATA_DIR, filename)

# On-demand single-hour/single-region fetch for wherever the Chart tab is
# panned to outside wind_grid.py's fixed BBOX (Gulf/Caribbean) -- GFS itself
# is a global model, that box is just what's pre-fetched on a schedule for
# instant loading. Live NOMADS round-trip + GDAL conversion in the request
# path (a few seconds), so this is deliberately single-hour rather than the
# full 5-day sweep fetch() does -- fetching 21 hours live every time someone
# pans somewhere new would feel like a real stall; the frontend re-fetches
# just the currently-selected hour again if the slider moves while away from
# the home region (see setWindHourByIndex() in static-src/index.html).
WIND_REGION_CACHE_DIR = os.path.join(WIND_DATA_DIR, 'regions')
WIND_REGION_ROUND_DEG = 10  # snaps requested bboxes to a coarser grid so nearby pans reuse the same cache entry

def _round_region_bbox(west, east, north, south):
    r = lambda v: round(v / WIND_REGION_ROUND_DEG) * WIND_REGION_ROUND_DEG
    return r(west), r(east), r(north), r(south)

@app.route('/api/wind/velocity_region')
def wind_velocity_region():
    manifest = _load_wind_manifest()
    if manifest is None:
        return jsonify({'error': 'wind data not yet available'}), 503
    try:
        hour = int(request.args.get('hour', 0))
        west = float(request.args.get('west'))
        east = float(request.args.get('east'))
        north = float(request.args.get('north'))
        south = float(request.args.get('south'))
    except (TypeError, ValueError):
        return jsonify({'error': 'hour/west/east/north/south must be numbers'}), 400
    if not (-179 <= west < east <= 179 and -85 <= south < north <= 85):
        return jsonify({'error': 'bbox out of range or invalid'}), 400

    west, east, north, south = _round_region_bbox(west, east, north, south)
    os.makedirs(WIND_REGION_CACHE_DIR, exist_ok=True)
    filename = f"wind_{manifest['cycle']}_f{hour:03d}_{west}_{east}_{north}_{south}.json"
    out_path = os.path.join(WIND_REGION_CACHE_DIR, filename)
    if not os.path.exists(out_path):
        # Cheap self-cleaning -- drop any cached region from an older cycle
        # whenever a new one gets requested, rather than growing forever.
        for path in glob.glob(os.path.join(WIND_REGION_CACHE_DIR, 'wind_*.json')):
            if not os.path.basename(path).startswith(f"wind_{manifest['cycle']}_"):
                os.remove(path)
        result = subprocess.run(
            ['python3', WIND_SCRIPT_PATH, 'fetch-region', out_path, manifest['cycle'], str(hour),
             str(west), str(east), str(north), str(south)],
            capture_output=True, timeout=60, check=False,
        )
        if result.returncode != 0 or not os.path.exists(out_path):
            return jsonify({'error': 'failed to fetch wind data for this region'}), 502
    return send_from_directory(WIND_REGION_CACHE_DIR, filename)
# ─── end wind velocity siloed addition ──────────────────────────────────────

# ─── Reference library (siloed addition) ────────────────────────────────────
# Static reference PDFs (NGA Atlas of Pilot Charts, Pub. 249 Sight Reduction
# Tables) for the Reference tab. Fetched once by hand into ~/reference_docs
# (outside dashboard-dev/chart_data -- these aren't generated/gitignored data,
# just a small fixed set of downloaded documents), not periodically refreshed
# the way live weather data is.
REFERENCE_DOCS_DIR = '/home/mikemc/reference_docs'
REFERENCE_CATEGORIES = {'pilot_charts', 'celestial_navigation', 'sailing_directions', 'navigation_rules', 'list_of_lights', 'chart_no1'}

@app.route('/api/reference/<category>/<filename>')
def reference_file(category, filename):
    if category not in REFERENCE_CATEGORIES or not filename.lower().endswith('.pdf') or '/' in filename or '..' in filename:
        return jsonify({'error': 'not found'}), 404
    category_dir = os.path.join(REFERENCE_DOCS_DIR, category)
    if not os.path.isfile(os.path.join(category_dir, filename)):
        return jsonify({'error': 'not found'}), 404
    return send_from_directory(category_dir, filename)
# ─── end reference library siloed addition ──────────────────────────────────

# ─── Tides & Currents (siloed addition) ─────────────────────────────────────
# Station list + predictions are pre-downloaded by tide_tools.py (harmonic,
# computed in advance -- same "cache once, read locally forever" reasoning as
# the NCDS/ENC chart data above), so /stations and /predictions never touch
# the network themselves. /live is the one exception: a direct proxy to NOAA
# CO-OPS's real-time observation endpoint, deliberately with no stale-data
# fallback (unlike _weather_cache above) -- on any failure (no connection, or
# this particular station just has no real-time sensor) it 502s, and the
# frontend takes that as "hide this," not "show old data."
TIDES_DATA_DIR = '/home/mikemc/dashboard-dev/chart_data/tides'
TIDES_PRED_DIR = os.path.join(TIDES_DATA_DIR, 'predictions')
NOAA_DATAGETTER = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter'
TIDES_LIVE_CACHE_TTL_S = 300
_tides_live_cache = {}  # station id -> {'data': {...}, 'fetched_at': epoch seconds}

# tides_stations()/tides_current_directions() used to re-read and JSON-parse
# every one of the ~3,850 prediction files (177MB+) on every single request --
# 40-55s to check the Tides & Currents box. tide_tools.py syncs these in bulk,
# offline, not live, so an in-memory cache is safe: rebuilt only if the
# predictions dir's mtime changes (a resync) or TIDES_STATION_CACHE_TTL_S
# elapses (belt-and-suspenders for a resync that overwrites files in place
# without touching the dir's own mtime).
TIDES_STATION_CACHE_TTL_S = 6 * 3600
_tides_station_cache = {'built_at': 0, 'dir_mtime': None, 'stations': [], 'current_events': {}}
# A cold rebuild reads and JSON-parses all ~3,850 prediction files -- measured
# ~10s on this Pi. Without a lock, every request that lands while the cache is
# stale (at the 6-hour mark, or right after a resync) would independently
# redo that same 10s scan concurrently under Flask's threaded=True -- bounded
# and self-resolving, not the same failure mode as the earlier tile-cache
# incident, but the same class of risk on the same box, so guarding against
# it here too. Double-checked: the cheap freshness check runs lock-free on
# every call (the common case), only the actual rebuild is serialized.
_tides_station_cache_lock = threading.Lock()

def _load_tides_station_cache():
    try:
        dir_mtime = os.path.getmtime(TIDES_PRED_DIR)
    except OSError:
        dir_mtime = None
    now = time.time()
    if (_tides_station_cache['dir_mtime'] == dir_mtime
            and now - _tides_station_cache['built_at'] < TIDES_STATION_CACHE_TTL_S):
        return
    with _tides_station_cache_lock:
        # Re-check: another thread may have already rebuilt while this one
        # was waiting for the lock.
        if (_tides_station_cache['dir_mtime'] == dir_mtime
                and now - _tides_station_cache['built_at'] < TIDES_STATION_CACHE_TTL_S):
            return
        _rebuild_tides_station_cache(dir_mtime, now)

def _rebuild_tides_station_cache(dir_mtime, now):
    stations = []
    current_events = {}
    if dir_mtime is not None:
        for name in os.listdir(TIDES_PRED_DIR):
            if not name.endswith('.json'):
                continue
            try:
                with open(os.path.join(TIDES_PRED_DIR, name)) as f:
                    d = json.load(f)
                station = {'id': d['id'], 'name': d['name'], 'lat': d['lat'], 'lon': d['lon'], 'type': d['type']}
                events = d.get('events')
                # Every event in a station's cp list carries the same
                # meanFloodDir/meanEbbDir (it's a property of the station, not
                # the individual event) -- the first one is as good as any.
                # isinstance guard: tide_tools.py normalizes NOAA's occasional
                # "Currents are weak and variable" string response to [] now, but
                # this stays as a second line of defense against any cache file
                # written before that fix, or any other future shape surprise --
                # one bad file must never take the whole endpoint down again.
                if d['type'] == 'current' and isinstance(events, list) and events and isinstance(events[0], dict):
                    station['flood_dir'] = events[0].get('meanFloodDir')
                    # Keeping the raw event dicts here held ~1GB+ in memory
                    # across ~1,600 current stations (734,827 events total,
                    # each event was a 7-key dict -- Type/meanFloodDir/Bin/
                    # meanEbbDir/Time/Depth/Velocity_Major -- but
                    # _current_phase_direction() below only ever reads Time
                    # and Type per event, plus meanFloodDir/meanEbbDir once
                    # per station, not per event. Down to a (time, type)
                    # tuple per event -- discards Bin/Depth/Velocity_Major
                    # entirely and the per-event flood/ebb dir duplication --
                    # and pre-sorted once here instead of on every request
                    # (_current_phase_direction() used to re-sort on every
                    # single call). NOTE: flood_dir/ebb_dir here deliberately
                    # come from the chronologically-FIRST event (post-sort),
                    # matching _current_phase_direction()'s old `evs[0]` --
                    # NOT the same source as station['flood_dir'] above
                    # (which reads the file's raw, unsorted events[0]). A
                    # station with multiple depth bins can genuinely have
                    # different flood/ebb readings per bin, so those two
                    # fields have always picked different "first" events for
                    # two different features (the static map marker vs. this
                    # live current-direction lookup) -- preserved exactly as
                    # it was, not something this trim should change.
                    events_by_time = sorted(events, key=lambda e: e['Time'])
                    current_events[d['id']] = {
                        'flood_dir': events_by_time[0].get('meanFloodDir'),
                        'ebb_dir': events_by_time[0].get('meanEbbDir'),
                        'events': [(e['Time'], e['Type']) for e in events_by_time],
                    }
                stations.append(station)
            except (json.JSONDecodeError, KeyError, OSError):
                continue
    _tides_station_cache['stations'] = stations
    _tides_station_cache['current_events'] = current_events
    _tides_station_cache['dir_mtime'] = dir_mtime
    _tides_station_cache['built_at'] = now

@app.route('/api/tides/stations')
def tides_stations():
    # Only ever the subset tide_tools.py sync has actually cached predictions
    # for -- a station in NOAA's full index with nothing synced would just be
    # a marker that 404s the moment it's clicked.
    _load_tides_station_cache()
    return jsonify(_tides_station_cache['stations'])

@app.route('/api/tides/predictions')
def tides_predictions():
    station = request.args.get('station', '')
    if not station.isalnum():
        return jsonify({'error': 'invalid station id'}), 400
    path = os.path.join(TIDES_PRED_DIR, f'{station}.json')
    if not os.path.isfile(path):
        return jsonify({'error': 'no cached predictions for this station -- run tide_tools.py sync'}), 404
    with open(path) as f:
        return jsonify(json.load(f))

@app.route('/api/tides/live')
def tides_live():
    station = request.args.get('station', '')
    if not station.isalnum():
        return jsonify({'error': 'invalid station id'}), 400
    cached = _tides_live_cache.get(station)
    if cached and (time.time() - cached['fetched_at'] < TIDES_LIVE_CACHE_TTL_S):
        return jsonify(cached['data'])

    station_type = 'tide'
    pred_path = os.path.join(TIDES_PRED_DIR, f'{station}.json')
    if os.path.isfile(pred_path):
        with open(pred_path) as f:
            station_type = json.load(f).get('type', 'tide')

    params = dict(station=station, application='exit-strategy-dashboard', date='latest',
                  units='english', time_zone='lst_ldt', format='json')
    params.update({'product': 'water_level', 'datum': 'MLLW'} if station_type == 'tide' else {'product': 'currents'})
    try:
        d = requests.get(NOAA_DATAGETTER, params=params, timeout=6).json()
        if 'error' in d or not d.get('data'):
            raise RuntimeError((d.get('error') or {}).get('message', 'no live data'))
        latest = d['data'][-1]
        data = ({'time': latest['t'], 'value': latest.get('v')} if station_type == 'tide'
                else {'time': latest['t'], 'speed': latest.get('s'), 'direction': latest.get('d')})
        _tides_live_cache[station] = {'data': data, 'fetched_at': time.time()}
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 502

def _current_phase_direction(station_events, now_str):
    """Which way a current station is running right now, derived from its
    cached flood/ebb/slack event list -- NOAA only gives event TIMES (not a
    continuous direction feed), so "now" always falls between two events.
    The direction is whichever of flood/ebb the *most recent past* event
    started, since the current keeps running that way (just decelerating)
    until the next slack; if the most recent event was itself a slack, the
    current has already begun turning into whatever direction comes *next*.
    Timestamps are plain 'YYYY-MM-DD HH:MM' strings (zero-padded, same
    time_zone=lst_ldt convention as everywhere else in this feature), so
    lexical comparison is chronological comparison -- no datetime parsing
    needed.

    station_events is the {'flood_dir', 'ebb_dir', 'events': [(time, type), ...]}
    shape _rebuild_tides_station_cache() builds -- events pre-sorted and
    trimmed to just (time, type) there, since this runs on every request
    for every current station and the full per-event dicts (Bin/Depth/
    Velocity_Major/meanFloodDir/meanEbbDir on every single event, when only
    one station-level flood/ebb dir is ever used) cost ~1GB across ~1,600
    stations' worth of cached predictions for data never read here."""
    if not station_events or not station_events.get('events'):
        return None
    evs = station_events['events']
    prev, nxt = None, None
    for t, typ in evs:
        if t <= now_str:
            prev = typ
        elif nxt is None:
            nxt = typ
    phase = prev if prev in ('flood', 'ebb') else (nxt if nxt in ('flood', 'ebb') else None)
    if phase == 'flood':
        return station_events.get('flood_dir')
    if phase == 'ebb':
        return station_events.get('ebb_dir')
    return None

@app.route('/api/tides/current_directions')
def tides_current_directions():
    # Deliberately not the same lazy-per-popup pattern as predictions/live --
    # a marker's rotation has to be right without opening its popup, so this
    # computes phase for every synced current station in one request. All
    # from the in-memory station cache (see _load_tides_station_cache above),
    # so this never touches the network or disk beyond an occasional resync.
    _load_tides_station_cache()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    directions = {}
    for station_id, station_events in _tides_station_cache['current_events'].items():
        direction = _current_phase_direction(station_events, now_str)
        if direction is not None:
            directions[station_id] = direction
    return jsonify(directions)
# ─── end tides & currents siloed addition ───────────────────────────────────

# ─── Victron simulator siloed addition ──────────────────────────────────────
# Serves victron_simulator.py's boat/sim/victron/<device>/<field> topics
# (diesel + thruster SmartShunts, MPPT 150/35, Orion 12|48, engine/alternator)
# to the Electrical tab as flat <device>_<field> keys, e.g. mppt_pv_power.
# Stale topics (simulator stopped) are dropped so the nodes fall back to "--".
VICTRON_SIM_PREFIX = 'boat/sim/victron/'
VICTRON_SIM_STALE_S = 30

@app.route('/api/victron/sim')
def victron_sim():
    cutoff = datetime.now(timezone.utc).timestamp() - VICTRON_SIM_STALE_S
    out = {}
    with mqtt_lock:
        items = [(t, v) for t, v in mqtt_state['topics'].items() if t.startswith(VICTRON_SIM_PREFIX)]
    for topic, entry in items:
        try:
            if datetime.fromisoformat(entry['time']).timestamp() < cutoff:
                continue
        except (KeyError, ValueError):
            continue
        key = topic[len(VICTRON_SIM_PREFIX):].replace('/', '_')
        try:
            out[key] = float(entry['value'])
        except ValueError:
            out[key] = entry['value']
    return jsonify(out)
# ─── end Victron simulator siloed addition ──────────────────────────────────

# ─── Critical battery push alerts (12V / diesel / thruster) siloed addition ─
# tank_battery_monitor_loop above already pushes for the 48V house bank
# (VRM 'soc'); this covers the other three batteries, critical level only,
# with the same ntfy notify-on-trip + repeat-every-30-min pattern. Thresholds
# match the header chips in static-src/index.html (BATTERY_ALERTS).
# Diesel/thruster only exist as victron_simulator.py topics until the real
# SmartShunts are installed; simulated readings push only from the dev
# server (DEBUG_MODE) and are titled "SIM:", so prod never sends fake alarms.
CRIT_BATTERY_MONITOR_INTERVAL_S = 60
CRIT_BATTERIES = [
    # key, label, crit %, source, soc field, alarm field
    ('bat12',     '12V house battery',    30.0, 'vrm', 'soc_12v',            None),  # assumed AGM
    ('batdiesel', 'Diesel start battery', 30.0, 'sim', 'shunt_diesel/soc',   'shunt_diesel/alarm'),
    ('batthrust', 'Bow thruster battery', 30.0, 'sim', 'shunt_thruster/soc', 'shunt_thruster/alarm'),
]
_crit_battery_notified_at = {k[0]: 0 for k in CRIT_BATTERIES}

def _sim_value(topics, field, cutoff):
    rec = topics.get(VICTRON_SIM_PREFIX + field)
    try:
        if rec and datetime.fromisoformat(rec['time']).timestamp() >= cutoff:
            return rec['value']
    except (KeyError, ValueError):
        pass
    return None

def crit_battery_monitor_loop():
    while True:
        time.sleep(CRIT_BATTERY_MONITOR_INTERVAL_S)
        try:
            now = time.time()
            vrm = vrm_cached()
            with mqtt_lock:
                topics = dict(mqtt_state['topics'])
            cutoff = now - VICTRON_SIM_STALE_S
            for key, label, crit, source, soc_field, alarm_field in CRIT_BATTERIES:
                if source == 'sim' and not globals().get('DEBUG_MODE'):
                    continue
                if source == 'vrm':
                    soc, alarm = vrm.get(soc_field), None
                else:
                    soc = _sim_value(topics, soc_field, cutoff)
                    alarm = _sim_value(topics, alarm_field, cutoff) if alarm_field else None
                try:
                    soc = float(soc) if soc is not None else None
                except ValueError:
                    soc = None
                alarm = alarm if alarm and alarm != 'None' else None
                tripped = alarm is not None or (soc is not None and soc <= crit)
                if not tripped:
                    _crit_battery_notified_at[key] = 0
                    continue
                if now - _crit_battery_notified_at[key] <= TANK_BATTERY_NOTIFY_REPEAT_S:
                    continue
                prefix = 'SIM: ' if source == 'sim' else ''
                if alarm:
                    msg = f'🔋 {label}: {alarm}' + (f' ({soc:.0f}% SOC).' if soc is not None else '.')
                else:
                    msg = f'🔋 {label} is at {soc:.0f}% -- at or below the {crit:.0f}% critical threshold.'
                send_ntfy(f'{prefix}{label} critically low', msg, tags='warning')
                _crit_battery_notified_at[key] = now
        except Exception as e:
            print(f'crit_battery_monitor_loop: {e}', flush=True)  # never let one bad reading kill the thread
# ─── end Critical battery push alerts siloed addition ───────────────────────

# ─── Bluetooth device list (Diagnostics > Server Health) siloed addition ───
# Paired/connected devices as bluetoothd knows them, via `bluetoothctl`
# (runs fine as this user, no sudo). Devices that were only seen in a scan
# (never paired, not connected) are counted but not listed.
def _bluetoothctl(*args):
    result = subprocess.run(['bluetoothctl', *args], capture_output=True, text=True, timeout=5)
    return result.stdout

@app.route('/api/system/bluetooth')
def system_bluetooth():
    try:
        known = re.findall(r'^Device ([0-9A-F:]{17}) ?(.*)$', _bluetoothctl('devices'), re.M)
    except (OSError, subprocess.SubprocessError) as e:
        return jsonify({'available': False, 'error': str(e), 'devices': [], 'unpaired_seen': 0})
    devices, unpaired = [], 0
    for mac, name in known:
        try:
            info = _bluetoothctl('info', mac)
        except (OSError, subprocess.SubprocessError):
            continue
        field = lambda k: (re.search(rf'^\s*{k}: (.*)$', info, re.M) or [None, None])[1]
        paired, connected = field('Paired') == 'yes', field('Connected') == 'yes'
        if not (paired or connected):
            unpaired += 1
            continue
        battery = re.search(r'Battery Percentage: 0x[0-9a-f]+ \((\d+)\)', info)
        rssi = re.search(r'RSSI: (?:0x[0-9a-f]+ \()?(-?\d+)', info)  # "-62" or "0xffffffc2 (-62)"
        devices.append({
            'mac': mac,
            'name': field('Alias') or field('Name') or name or mac,
            'type': field('Icon'),
            'paired': paired,
            'connected': connected,
            'trusted': field('Trusted') == 'yes',
            'rssi': int(rssi.group(1)) if rssi else None,
            'battery': int(battery.group(1)) if battery else None,
        })
    devices.sort(key=lambda d: (not d['connected'], d['name'].lower()))
    return jsonify({'available': True, 'devices': devices, 'unpaired_seen': unpaired})
# ─── end Bluetooth device list siloed addition ──────────────────────────────

# ─── Network status: Ethernet + recovery hotspot (siloed addition) ──────────
# The Pi's only uplink is eth0 to the boat router. (A Wi-Fi backup client on
# wlan0 used to live here; removed 2026-10-05 because it joined the same router
# as eth0 and protected nothing. wlan0 is now only the recovery hotspot and the
# scanner for the router's auto-rejoin.) Read-only: `ip`, sysfs, resolvectl.
NETPATH_CACHE_TTL_S = 5
_netpath_cache = {'t': 0.0, 'data': None}

def _netpath_status():
    def run(args, timeout=3):
        try:
            return subprocess.run(args, capture_output=True, text=True, timeout=timeout).stdout
        except Exception:
            return ''
    try:
        routes = json.loads(run(['ip', '-j', '-4', 'route', 'show', 'default']) or '[]')
    except ValueError:
        routes = []
    def sysfs(rel):
        try:
            with open(f'/sys/class/net/eth0/{rel}') as f:
                return f.read().strip()
        except OSError:
            return None
    def num(rel):
        try:
            return int(sysfs(rel))
        except (TypeError, ValueError):
            return None
    try:
        addrs = json.loads(run(['ip', '-j', 'addr', 'show', 'dev', 'eth0']) or '[]')
    except ValueError:
        addrs = []
    default = next((r for r in routes if r.get('dev') == 'eth0'), None)
    gateway = default.get('gateway') if default else None
    carrier = sysfs('carrier') == '1'
    gateway_ok = bool(carrier and gateway) and subprocess.run(
        ['ping', '-n', '-q', '-c', '1', '-W', '1', '-I', 'eth0', gateway], capture_output=True, timeout=3).returncode == 0
    speed = num('speed')
    dns = run(['resolvectl', 'dns', 'eth0']).split(':', 1)
    eth0 = {
        'mac': sysfs('address'), 'operstate': sysfs('operstate'), 'carrier': carrier,
        'ipv4': [f"{a['local']}/{a['prefixlen']}" for x in addrs for a in x.get('addr_info', []) if a.get('family') == 'inet'],
        'ipv6': [f"{a['local']}/{a['prefixlen']}" for x in addrs for a in x.get('addr_info', [])
                 if a.get('family') == 'inet6' and a.get('scope') == 'global'],
        'mtu': num('mtu'), 'speed_mbps': speed if speed and speed > 0 else None, 'duplex': sysfs('duplex'),
        'gateway': gateway, 'gateway_ok': gateway_ok,
        'dns': dns[1].split() if len(dns) == 2 else [],
        'rx_bytes': num('statistics/rx_bytes'), 'tx_bytes': num('statistics/tx_bytes'),
        'rx_errors': num('statistics/rx_errors'), 'tx_errors': num('statistics/tx_errors'),
        'rx_dropped': num('statistics/rx_dropped'), 'tx_dropped': num('statistics/tx_dropped'),
    }
    return {'eth0': eth0,
            # an eth0-config change waiting for Keep (its revert timer is armed)
            'eth0_change_pending': run(['systemctl', 'is-active', 'eth0-config-revert.timer']).strip() == 'active',
            'recovery': _recovery_state()}

# Wi-Fi recovery hotspot (/usr/local/sbin/wifi-recovery, wifi-recovery.service):
# wlan0 becomes an access point (10.42.0.1) for 10 min after boot or after 5 min
# with no reachable network. Its controller writes a world-readable state file.
RECOVERY_HELPER = '/usr/local/sbin/wifi-recovery'
RECOVERY_STATE_FILE = '/run/wifi-recovery/state.json'

def _recovery_state():
    if not os.path.exists(RECOVERY_HELPER):
        return {'installed': False}
    try:
        st = json.load(open(RECOVERY_STATE_FILE))
    except (OSError, ValueError):
        st = {'active': False}
    st['installed'] = True
    st['stale'] = time.time() - st.get('updated', 0) > 120     # controller not running?
    return st

@app.route('/api/system/recovery', methods=['POST'])
def system_recovery():
    action = (request.get_json(silent=True) or {}).get('action')
    if action not in ('on', 'off'):
        return jsonify({'ok': False, 'error': 'action must be on or off'}), 400
    _netpath_cache['data'] = None
    return jsonify(_root_helper(action, timeout=60, helper=RECOVERY_HELPER))

@app.route('/api/system/netpath')
def system_netpath():
    now = time.time()
    if _netpath_cache['data'] is None or now - _netpath_cache['t'] > NETPATH_CACHE_TTL_S:
        _netpath_cache['data'] = _netpath_status()
        _netpath_cache['t'] = now
    return jsonify(_netpath_cache['data'])

# Root helpers (eth0-config, wifi-recovery) are called through narrow sudoers
# rules; JSON goes over stdin, the reply is the helper's last stdout line.
WIFI_SCAN_HELPER = '/usr/local/sbin/wifi-scan'      # scan-only, used by the router auto-rejoin

def _root_helper(cmd, payload=None, timeout=20, helper=None):
    if not os.path.exists(helper):
        return {'ok': False, 'error': f'{helper} is not installed'}
    try:
        r = subprocess.run(['sudo', '-n', helper, cmd], capture_output=True, text=True, timeout=timeout,
                           input=json.dumps(payload) if payload is not None else '')
    except subprocess.TimeoutExpired:
        return {'ok': False, 'error': 'timed out'}
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        err = r.stderr.strip()
        if 'password is required' in err or 'not allowed' in err:
            err = f'sudo rule for {os.path.basename(helper)} is not installed (/etc/sudoers.d/{os.path.basename(helper)})'
        return {'ok': False, 'error': err[:300] or f'helper exited {r.returncode}'}

# Ethernet (eth0) addressing (Network pop-up). Same pattern: a root helper
# (/usr/local/sbin/eth0-config) via a narrow sudoers rule. `set` applies the
# change ~3 s after replying and reverts it after 3 min unless `confirm`
# arrives -- open the dashboard at the new address and press Keep.
ETH0_HELPER = '/usr/local/sbin/eth0-config'

@app.route('/api/system/eth0/current')
def system_eth0_current():
    return jsonify(_root_helper('current', helper=ETH0_HELPER))

@app.route('/api/system/eth0/config', methods=['POST'])
def system_eth0_config():
    body = request.get_json(silent=True) or {}
    payload = {k: body.get(k) for k in ('mode', 'address', 'gateway', 'dns')}
    _netpath_cache['data'] = None
    return jsonify(_root_helper('set', payload, timeout=60, helper=ETH0_HELPER))

@app.route('/api/system/eth0/confirm', methods=['POST'])
def system_eth0_confirm():
    _netpath_cache['data'] = None
    return jsonify(_root_helper('confirm', helper=ETH0_HELPER))

@app.route('/api/system/eth0/revert', methods=['POST'])
def system_eth0_revert():
    _netpath_cache['data'] = None
    return jsonify(_root_helper('revert', timeout=150, helper=ETH0_HELPER))
# ─── end Internet path siloed addition ──────────────────────────────────────

# ─── Starlink dish status (siloed addition) ─────────────────────────────────
# Reads the Starlink Mini's local API (192.168.100.1) over normal routing (it
# used to go through the Wi-Fi backup, removed 2026-10-05). The boat LAN was
# moved off 192.168.100.0/24 (to 192.168.10.0/24) so the dish address is free.
# Uses the dish's gRPC-web port (9201, plain HTTP) with a tiny protobuf
# decoder instead of adding grpcio. Field numbers were read from the dish via
# gRPC server reflection (api_version 43, software 2026.09.24); a firmware
# change could renumber fields, in which case values just come back missing.
import http.client
import socket
import struct

STARLINK_DISH_HOST = '192.168.100.1'
STARLINK_GRPC_WEB_PORT = 9201
STARLINK_CACHE_TTL_S = 10
STARLINK_HISTORY_WINDOW_S = 900       # averages over the last 15 min (history is 1 sample/s)
_starlink_cache = {'t': 0.0, 'data': None}

# Request{get_status} = field 1004, Request{get_history} = field 1007, each an empty message.
_SL_REQ_STATUS = b'\xe2\x3e\x00'
_SL_REQ_HISTORY = b'\xfa\x3e\x00'

STARLINK_ENUMS = {
    'disablement_code': {0: 'UNKNOWN_STATE', 1: 'OKAY', 2: 'NO_ACTIVE_ACCOUNT', 3: 'TOO_FAR_FROM_SERVICE_ADDRESS',
                         4: 'IN_OCEAN', 6: 'BLOCKED_COUNTRY', 7: 'DATA_OVERAGE_SANDBOX_POLICY', 8: 'CELL_IS_DISABLED',
                         10: 'ROAM_RESTRICTED', 11: 'UNKNOWN_LOCATION', 12: 'ACCOUNT_DISABLED',
                         13: 'UNSUPPORTED_VERSION', 14: 'MOVING_TOO_FAST_FOR_POLICY',
                         15: 'UNDER_AVIATION_FLYOVER_LIMITS', 16: 'BLOCKED_AREA', 17: 'OUTSIDE_HOME_REGION'},
    'rate_limit': {0: 'UNKNOWN', 1: 'NO_LIMIT', 2: 'POLICY_LIMIT', 3: 'USER_CUSTOM_LIMIT',
                   5: 'OVERAGE_LIMIT', 6: 'LOW_SPEED_POLICY_LIMIT'},
    'mobility_class': {0: 'STATIONARY', 1: 'NOMADIC', 2: 'MOBILE'},
    'software_update_state': {0: 'UNKNOWN', 1: 'IDLE', 2: 'FETCHING', 3: 'PRE_CHECK', 4: 'WRITING',
                              5: 'POST_CHECK', 6: 'REBOOT_REQUIRED', 7: 'DISABLED', 8: 'FAULTED'},
}
# DishAlerts field number -> name (all bools)
STARLINK_ALERTS = {1: 'motors_stuck', 2: 'thermal_shutdown', 3: 'thermal_throttle', 4: 'unexpected_location',
                   5: 'mast_not_near_vertical', 6: 'slow_ethernet_speeds', 8: 'install_pending', 9: 'is_heating',
                   10: 'power_supply_thermal_throttle', 11: 'is_power_save_idle', 14: 'dbf_telem_stale',
                   16: 'low_motor_current', 17: 'lower_signal_than_predicted', 18: 'slow_ethernet_speeds_100',
                   19: 'obstruction_map_reset', 20: 'dish_water_detected', 21: 'router_water_detected',
                   22: 'upsu_router_port_slow', 23: 'no_ethernet_link'}

def _pb_decode(buf):
    """Minimal protobuf wire decoder: {field_number: [raw values]} (ints for
    varints, bytes for length-delimited / fixed32 / fixed64)."""
    out, i, n = {}, 0, len(buf)
    def varint():
        nonlocal i
        shift = result = 0
        while True:
            b = buf[i]; i += 1
            result |= (b & 0x7f) << shift
            if not b & 0x80:
                return result
            shift += 7
    while i < n:
        key = varint()
        field, wt = key >> 3, key & 7
        if wt == 0:
            val = varint()
        elif wt == 1:
            val = buf[i:i + 8]; i += 8
        elif wt == 2:
            ln = varint(); val = buf[i:i + ln]; i += ln
        elif wt == 5:
            val = buf[i:i + 4]; i += 4
        else:
            break   # groups (3/4) aren't used by this API
        out.setdefault(field, []).append(val)
    return out

def _pb_float(d, f):
    v = d.get(f)
    if v and isinstance(v[-1], bytes) and len(v[-1]) == 4:
        return struct.unpack('<f', v[-1])[0]
    # proto3 omits fields equal to 0, so a missing float in a message that
    # did arrive means 0.0 (e.g. downlink_bps while idle).
    return 0.0 if d else None

def _pb_int(d, f, signed=False):
    v = d.get(f)
    if not v or not isinstance(v[-1], int):
        return None
    x = v[-1]
    return x - (1 << 64) if signed and x >= 1 << 63 else x

def _pb_str(d, f):
    v = d.get(f)
    return v[-1].decode('utf-8', 'replace') if v and isinstance(v[-1], bytes) else None

def _pb_msg(d, f):
    v = d.get(f)
    return _pb_decode(v[-1]) if v and isinstance(v[-1], bytes) else {}

def _pb_packed_floats(d, f):
    vals = []
    for chunk in d.get(f, []):
        if isinstance(chunk, bytes):
            vals.extend(struct.unpack(f'<{len(chunk) // 4}f', chunk[:len(chunk) // 4 * 4]))
    return vals

def _starlink_call(request_bytes):
    """One gRPC-web Device/Handle call; returns the decoded Response message."""
    conn = http.client.HTTPConnection(STARLINK_DISH_HOST, STARLINK_GRPC_WEB_PORT, timeout=5)
    try:
        body = b'\x00' + struct.pack('>I', len(request_bytes)) + request_bytes
        conn.request('POST', '/SpaceX.API.Device.Device/Handle', body,
                     {'Content-Type': 'application/grpc-web+proto', 'X-Grpc-Web': '1'})
        resp = conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise RuntimeError(f'dish HTTP {resp.status}')
    finally:
        conn.close()
    # grpc-web body: frames of [flag:1][len:4][payload]; flag 0x80 = trailers
    i, msg = 0, None
    while i + 5 <= len(data):
        flag, ln = data[i], struct.unpack('>I', data[i + 1:i + 5])[0]
        payload = data[i + 5:i + 5 + ln]; i += 5 + ln
        if flag & 0x80:
            m = re.search(rb'grpc-status:\s*(\d+)', payload)
            if m and m.group(1) != b'0':
                raise RuntimeError('dish grpc-status ' + m.group(1).decode())
        elif msg is None:
            msg = payload
    if msg is None:
        raise RuntimeError('empty reply from dish')
    return _pb_decode(msg)

def _starlink_status():
    st = _pb_msg(_starlink_call(_SL_REQ_STATUS), 2004)          # Response.dish_get_status
    info, state = _pb_msg(st, 1), _pb_msg(st, 2)
    obs, alerts_m, align = _pb_msg(st, 1004), _pb_msg(st, 1005), _pb_msg(st, 1027)
    ready_m, gps = _pb_msg(st, 1019), _pb_msg(st, 1015)
    def enum(kind, f):
        v = _pb_int(st, f)
        return STARLINK_ENUMS[kind].get(v, str(v)) if v is not None else None
    data = {
        'online': True,
        'hardware': _pb_str(info, 2), 'software': _pb_str(info, 3), 'bootcount': _pb_int(info, 8),
        'uptime_s': _pb_int(state, 1),
        'pop_ping_latency_ms': _pb_float(st, 1009),
        'pop_ping_drop_rate': _pb_float(st, 1003),
        'downlink_bps': _pb_float(st, 1007), 'uplink_bps': _pb_float(st, 1008),
        'signal_quality': _pb_float(st, 1057),
        'snr_above_noise_floor': bool(_pb_int(st, 1018)),
        'snr_persistently_low': bool(_pb_int(st, 1022)),
        'fraction_obstructed': _pb_float(obs, 1),
        'currently_obstructed': bool(_pb_int(obs, 5)),
        'tilt_deg': _pb_float(align, 3),
        'azimuth_deg': _pb_float(align, 4), 'elevation_deg': _pb_float(align, 5),
        'desired_azimuth_deg': _pb_float(align, 8), 'desired_elevation_deg': _pb_float(align, 9),
        'gps_valid': bool(_pb_int(gps, 1)), 'gps_sats': _pb_int(gps, 2),
        'disablement': enum('disablement_code', 1024),
        'dl_limit': enum('rate_limit', 1044), 'ul_limit': enum('rate_limit', 1045),
        'mobility': enum('mobility_class', 1017),
        'software_update': enum('software_update_state', 1021),
        'stow_requested': bool(_pb_int(st, 1010)),
        'treat_as_metered': bool(_pb_int(st, 1056)),
        'alerts': sorted(name for f, name in STARLINK_ALERTS.items() if _pb_int(alerts_m, f)),
        'not_ready': sorted(n for f, n in {1: 'cady', 2: 'scp', 3: 'l1l2', 4: 'xphy', 5: 'aap', 6: 'rf'}.items()
                            if f in ready_m and not _pb_int(ready_m, f)),
    }
    try:
        h = _pb_msg(_starlink_call(_SL_REQ_HISTORY), 2006)       # Response.dish_get_history
        current = _pb_int(h, 1) or 0
        def recent(f):
            ring = _pb_packed_floats(h, f)
            if not ring:
                return []
            L = len(ring)
            k = min(STARLINK_HISTORY_WINDOW_S, L, current)
            return [ring[(current - 1 - j) % L] for j in range(k)]
        lat, drop = recent(1002), recent(1001)
        dl, ul, pw = recent(1003), recent(1004), recent(1010)
        avg = lambda a: sum(a) / len(a) if a else None
        lat_ok = [x for x in lat if x > 0]
        data['history_15m'] = {
            'latency_ms_avg': avg(lat_ok),
            'drop_rate_avg': avg(drop),
            'downlink_bps_max': max(dl) if dl else None, 'uplink_bps_max': max(ul) if ul else None,
            'power_w_avg': avg(pw),
            'samples': len(drop),
        }
    except Exception as e:
        data['history_15m'] = {'error': str(e)}
    return data

@app.route('/api/starlink/status')
def starlink_status():
    now = time.time()
    if _starlink_cache['data'] is None or now - _starlink_cache['t'] > STARLINK_CACHE_TTL_S:
        try:
            data = _starlink_status()
        except Exception as e:
            data = {'online': False, 'error': str(e)}
        data['fetched_at'] = datetime.now(timezone.utc).isoformat()
        _starlink_cache['data'] = data
        _starlink_cache['t'] = now
    return jsonify(_starlink_cache['data'])
# ─── end Starlink dish status siloed addition ───────────────────────────────

# ─── Boat router (GL.iNet GL-MT3000) internet source (siloed addition) ──────
# Which uplink the boat router is using right now (Ethernet WAN / Wi-Fi
# repeater / USB tethering), from its firmware-4.x JSON-RPC API at /rpc.
# Login is challenge/response: crypt(password, $alg$salt$), then
# <hash-method>("root:<crypt>:<nonce>") -- the password itself is never sent.
# The admin password is ROUTER_PASS in /etc/dashboard/secrets.env. Read-only:
# this only calls get_* methods. Response shapes checked against firmware 4.11.0.
import hashlib

GLINET_FALLBACK_HOST = '192.168.10.1'

def _glinet_host():
    # the boat router is the Pi's eth0 gateway (follows subnet changes)
    try:
        r = subprocess.run(['ip', '-4', 'route', 'show', 'default', 'dev', 'eth0'], capture_output=True, text=True, timeout=3).stdout.split()
        if 'via' in r:
            return r[r.index('via') + 1]
    except Exception:
        pass
    return GLINET_FALLBACK_HOST
GLINET_CACHE_TTL_S = 15
GLINET_IFACE_LABELS = {'wan': 'Ethernet WAN', 'wwan': 'Wi-Fi repeater', 'tethering': 'USB tethering',
                       'modem': 'Cellular', 'secondwan': 'Second WAN'}
_glinet = {'sid': None, 't': 0.0, 'data': None, 'lock': threading.Lock()}

def _glinet_rpc(method, params, timeout=6):
    r = requests.post(f'http://{_glinet_host()}/rpc', json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}, timeout=timeout)
    return r.json()

def _glinet_crypt(password, alg, salt):
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            import crypt as _crypt                 # stdlib until Python 3.13
        return _crypt.crypt(password, f'${alg}${salt}')
    except ImportError:                            # fall back to openssl; password via stdin, not argv
        flag = {1: '-1', 5: '-5', 6: '-6'}[int(alg)]
        r = subprocess.run(['openssl', 'passwd', flag, '-salt', salt, '-stdin'], input=password,
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip()

def _glinet_login():
    s = get_secrets()
    password = s.get('ROUTER_PASS') or s.get('GLINET_PASSWORD')
    if not password:
        raise RuntimeError('router password not set (ROUTER_PASS in /etc/dashboard/secrets.env)')
    ch = _glinet_rpc('challenge', {'username': 'root'})['result']
    cipher = _glinet_crypt(password, ch['alg'], ch['salt'])
    algo = {'md5': hashlib.md5, 'sha256': hashlib.sha256, 'sha512': hashlib.sha512}.get(ch.get('hash-method', 'md5'), hashlib.md5)
    res = _glinet_rpc('login', {'username': 'root', 'hash': algo(f"root:{cipher}:{ch['nonce']}".encode()).hexdigest()})
    if 'result' not in res:
        raise RuntimeError('router login failed (check ROUTER_PASS)')
    return res['result']['sid']

def _glinet_call(module, method):
    """call with the cached session, logging in again once if it has expired."""
    for attempt in (0, 1):
        if not _glinet['sid']:
            _glinet['sid'] = _glinet_login()
        res = _glinet_rpc('call', [_glinet['sid'], module, method, {}])
        if 'result' in res:
            return res['result']
        if res.get('error', {}).get('code') == -32000 and attempt == 0:      # Access denied: session expired
            _glinet['sid'] = None
            continue
        raise RuntimeError(f"{module}.{method}: {res.get('error', {}).get('message', 'error')}")

def _glinet_status():
    status = _glinet_call('kmwan', 'get_status')
    config = _glinet_call('kmwan', 'get_config')
    metric = {c['interface']: c.get('metric', 99) for c in config.get('interfaces', [])}
    ifaces = []
    for i in status.get('interfaces', []):
        name = i.get('interface')
        ifaces.append({'interface': name, 'label': GLINET_IFACE_LABELS.get(name, name),
                       'online': i.get('status_v4') == 0,      # 0 = online (verified against a working WAN)
                       'status_v4': i.get('status_v4'), 'priority': metric.get(name, 99)})
    ifaces.sort(key=lambda x: x['priority'])
    mode = 'load_balance' if config.get('mode') == 1 else 'failover'
    online = [x for x in ifaces if x['online']]
    active = online[0]['interface'] if online and mode == 'failover' else None
    # Starlink in bypass mode hands the router a CGNAT address (100.64.0.0/10),
    # so label the WAN "Starlink" when that's what is plugged in.
    try:
        import ipaddress as _ip
        _wan_ip = ((_glinet_call('cable', 'get_status') or {}).get('ipv4') or {}).get('ip') or ''
        if _wan_ip and _ip.ip_interface(_wan_ip).ip in _ip.ip_network('100.64.0.0/10'):
            for x in ifaces:
                if x['interface'] == 'wan':
                    x['label'] = 'Starlink'
    except Exception:
        pass
    active_label = next((x['label'] for x in ifaces if x['interface'] == active), None)
    data = {'ok': True, 'mode': mode, 'interfaces': ifaces, 'active': active,
            'active_label': active_label,
            'load_balance_over': [x['label'] for x in online] if mode == 'load_balance' else None}
    try:
        cable = _glinet_call('cable', 'get_status')
        v4 = cable.get('ipv4') or {}
        data['wan'] = {'connected': cable.get('status') == 1, 'protocol': cable.get('protocol'),
                       'ip': v4.get('ip'), 'gateway': v4.get('gateway'), 'dns': v4.get('dns') or []}
    except Exception as e:
        data['wan'] = {'error': str(e)}
    try:
        rep = _glinet_call('repeater', 'get_status')
        rep.pop('portal_info', None)                                       # may hold captive-portal credentials
        cfg = rep.get('config') or {}                                      # target network (key is never passed on)
        # firmware 4.11: 0 idle, 3 failed, 4 retrying (observed); state_s isn't always present
        state = rep.get('state_s') or {0: 'idle', 1: 'connecting', 2: 'connected', 3: 'failed', 4: 'retrying'}.get(rep.get('state'), f"state {rep.get('state')}")
        data['repeater'] = {'state': state, 'running': rep.get('running'),
                            'ssid': rep.get('ssid') or cfg.get('ssid'), 'fail_type': rep.get('fail_type') or None}
    except Exception as e:
        data['repeater'] = {'error': str(e)}
    try:
        teth = _glinet_call('tethering', 'get_status')
        data['tethering'] = {'status': teth.get('status'), 'devices': len(teth.get('devices') or [])}
    except Exception as e:
        data['tethering'] = {'error': str(e)}
    try:
        info = _glinet_call('system', 'get_info')
        data['router'] = {'model': (info.get('board_info') or {}).get('model'), 'firmware': info.get('firmware_version')}
    except Exception:
        data['router'] = {}
    return data

@app.route('/api/router/status')
def router_status():
    now = time.time()
    with _glinet['lock']:
        if _glinet['data'] is None or now - _glinet['t'] > GLINET_CACHE_TTL_S:
            try:
                data = _glinet_status()
            except Exception as e:
                _glinet['sid'] = None
                data = {'ok': False, 'error': str(e)}
            data['fetched_at'] = datetime.now(timezone.utc).isoformat()
            data['autorejoin'] = _rejoin_public_state()
            _glinet['data'], _glinet['t'] = data, now
        return jsonify(_glinet['data'])

# Repeater reconnect + automatic rejoin. GL.iNet firmware stops scanning for
# repeater networks while another uplink (Ethernet/Starlink) has internet, so
# once a hotspot/marina Wi-Fi drops it never comes back on its own. These send
# repeater.connect (WRITE) for a network already saved on the router, using the
# password the router itself returns for it -- it never reaches the browser.
# Auto-rejoin scans with the Pi's own Wi-Fi (wifi-fallback-config scan), not the
# router's radio, so the boat Wi-Fi isn't disturbed just to look. A connect
# attempt does briefly move the router's radio onto the upstream channel.
REJOIN_FIRST_CHECK_S = 30         # first check soon after (re)start
REJOIN_INTERVAL_S = 60             # then check every minute
REJOIN_STUCK_S = 60                # repeater must be down this long before acting
REJOIN_BACKOFF_S = [120, 300, 900] # after 1st/2nd/3rd+ consecutive failure on an SSID
REJOIN_BLIND_RETRY_S = 300         # if the Pi can't see any saved network (other band/channel), let the
                                   # router scan + connect on its own at most this often
REJOIN_STABLE_S = 300              # a reconnect only counts as a success once it has stayed up this long
                                   # (a link that drops sooner counts as a failure, so a flaky
                                   # hotspot doesn't make the router retune its radio every minute)
# Network-feature state is shared by the dev (5003) and prod (5001) dashboards:
# only one of them runs the usage meter / auto-rejoin (flock), so per-copy files
# would leave the other showing empty usage and default device names.
NETWORK_STATE_DIR = os.path.expanduser('~/.local/share/exit-strategy')
os.makedirs(NETWORK_STATE_DIR, exist_ok=True)
REJOIN_STATE_FILE = os.path.join(NETWORK_STATE_DIR, 'router_autorejoin.json')
REJOIN_LOCK_FILE = os.path.expanduser('~/.cache/router-autorejoin.lock')   # dev + prod both run this loop; one wins
_rejoin = {'enabled': True, 'events': [], 'last_attempt': {}, 'failures': {}, 'last_ok': {}, 'down_since': None, 'runner': False, 'last_blind': 0.0}

def _rejoin_load():
    try:
        _rejoin['enabled'] = bool(json.load(open(REJOIN_STATE_FILE)).get('enabled', True))
    except (OSError, ValueError):
        pass

def _rejoin_public_state():
    return {'enabled': _rejoin['enabled'], 'runner': _rejoin['runner'], 'events': _rejoin['events'][-8:][::-1]}

def _rejoin_event(kind, text):
    _rejoin['events'].append({'time': datetime.now(timezone.utc).isoformat(), 'kind': kind, 'text': text})
    del _rejoin['events'][:-30]
    print(f'[router-rejoin] {kind}: {text}', flush=True)

def _repeater_connect(ssid=None):
    """Reconnect the router's repeater to a saved network (default: its configured target).
    Waits up to ~30 s for it to come up. Returns a result dict."""
    saved = (_glinet_call('repeater', 'get_saved_ap_list') or {}).get('res') or []
    if not saved:
        return {'ok': False, 'error': 'no saved repeater networks on the router'}
    if ssid is None:
        target = ((_glinet_call('repeater', 'get_status') or {}).get('config') or {}).get('ssid')
        ssid = target if any(s.get('ssid') == target for s in saved) else saved[0].get('ssid')
    ap = next((s for s in saved if s.get('ssid') == ssid), None)
    if not ap:
        return {'ok': False, 'error': f'"{ssid}" is not saved on the router'}
    # Same payload the router's own UI sends when a saved network is clicked
    # (gl-sdk4-ui-internet, handleConnetSavedAp): the saved entry as returned by
    # get_saved_ap_list, minus UI-only fields, plus remember=true. Firmware 4.11
    # rejects extra empty fields like bssid/band/channel with "Invalid params".
    params = {k: v for k, v in ap.items() if k not in ('hasDfs', 'signal', 'bssidList')}
    params['remember'] = True
    with _glinet['lock']:
        _glinet['data'] = None                          # force fresh status afterwards
    # Like the router's own UI: refresh the router's scan first (it stops
    # scanning on its own while another uplink has internet, so a bare
    # connect can aim at stale data and fail), then connect.
    try:
        _glinet_call_params('repeater', 'scan', {'refresh': True}, timeout=60)
    except Exception:
        pass                                   # a failed scan shouldn't block the attempt
    try:
        _glinet_call_params('repeater', 'connect', params, timeout=30)
    except Exception as e:
        return {'ok': False, 'ssid': ssid, 'error': str(e)}
    # The router's API can stop answering for a few seconds while its radio
    # retunes, so tolerate errors here and keep polling.
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(3)
        try:
            st = _glinet_call('repeater', 'get_status') or {}
        except Exception:
            continue
        if st.get('state_s') == 'connected' or (st.get('running') and st.get('state') == 2):
            return {'ok': True, 'ssid': ssid}
        if st.get('state') in (3,) and time.time() > deadline - 30:
            break
    return {'ok': False, 'ssid': ssid, 'error': 'the router could not join the network within 60 s'}

def _glinet_call_params(module, method, params, timeout=20):
    for attempt in (0, 1):
        if not _glinet['sid']:
            _glinet['sid'] = _glinet_login()
        res = _glinet_rpc('call', [_glinet['sid'], module, method, params], timeout=timeout)
        if 'result' in res:
            return res['result']
        if res.get('error', {}).get('code') == -32000 and attempt == 0:
            _glinet['sid'] = None
            continue
        raise RuntimeError(f"{module}.{method}: {res.get('error', {}).get('message', 'error')}")

def router_autorejoin_loop():
    import fcntl
    os.makedirs(os.path.dirname(REJOIN_LOCK_FILE), exist_ok=True)
    lock = open(REJOIN_LOCK_FILE, 'w')
    while True:                                          # wait until this process holds the lock
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            time.sleep(60)
    _rejoin['runner'] = True
    _rejoin_load()
    first = True
    while True:
        time.sleep(REJOIN_FIRST_CHECK_S if first else REJOIN_INTERVAL_S)
        first = False
        try:
            _usage_sample()                      # data-usage ledger (same single runner as auto-rejoin)
        except Exception as e:
            print(f'[router-usage] {e}', flush=True)
        if not _rejoin['enabled']:
            continue
        try:
            status = _glinet_call('kmwan', 'get_status')
            config = _glinet_call('kmwan', 'get_config')
            if config.get('mode') != 0:
                continue                                 # load balance: nothing to prefer
            prio = sorted(config.get('interfaces', []), key=lambda c: c.get('metric', 99))
            if not prio or prio[0].get('interface') != 'wwan':
                continue                                 # repeater isn't the preferred uplink
            # judge the link by the repeater's own state: the router's internet check
            # (kmwan) takes a while to mark a fresh connection online
            rep_st = _glinet_call('repeater', 'get_status') or {}
            if rep_st.get('running') and rep_st.get('state') == 2:
                _rejoin['down_since'] = None
                for s_, t_ in list(_rejoin['last_ok'].items()):
                    if time.time() - t_ >= REJOIN_STABLE_S:          # held long enough: a real success
                        _rejoin['failures'].pop(s_, None)
                        _rejoin['last_ok'].pop(s_, None)
                continue                                 # repeater already online
            now = time.time()
            for s_, t_ in list(_rejoin['last_ok'].items()):       # reconnected but dropped again quickly
                if now - t_ < REJOIN_STABLE_S:
                    _rejoin['failures'][s_] = _rejoin['failures'].get(s_, 0) + 1
                    _rejoin_event('unstable', f'"{s_}" dropped {int(now - t_)} s after reconnecting; backing off')
                _rejoin['last_ok'].pop(s_, None)
            _rejoin['down_since'] = _rejoin['down_since'] or now
            if now - _rejoin['down_since'] < REJOIN_STUCK_S:
                continue
            saved = [s.get('ssid') for s in (_glinet_call('repeater', 'get_saved_ap_list') or {}).get('res') or []]
            r = subprocess.run(['sudo', '-n', WIFI_SCAN_HELPER], capture_output=True, text=True, timeout=45)
            visible = {n['ssid']: n.get('signal_dbm') or -999 for n in (json.loads(r.stdout or '{}').get('networks') or [])}
            def backoff(s):
                n = _rejoin['failures'].get(s, 0)
                return 0 if n == 0 else REJOIN_BACKOFF_S[min(n, len(REJOIN_BACKOFF_S)) - 1]
            candidates = sorted((s for s in saved if s in visible and visible[s] > -80
                                 and now - _rejoin['last_attempt'].get(s, 0) > backoff(s)),
                                key=lambda s: -visible[s])
            if not candidates:
                # The Pi's Wi-Fi may not hear the network (e.g. a hotspot that came back on
                # 5/6 GHz or a DFS channel). Let the router look for its saved network itself,
                # but not too often: each attempt briefly retunes the boat's Wi-Fi.
                target = (rep_st.get('config') or {}).get('ssid')
                if target in saved and not any(s in visible for s in saved) \
                        and now - _rejoin['last_blind'] > REJOIN_BLIND_RETRY_S \
                        and now - _rejoin['last_attempt'].get(target, 0) > backoff(target):
                    _rejoin['last_blind'] = now
                    _rejoin['last_attempt'][target] = now
                    _rejoin_event('attempt', f'"{target}" not visible to the Pi; asking the router to look for it')
                    res = _repeater_connect(target)
                    _rejoin_event('connected' if res.get('ok') else 'failed',
                                  f'"{target}" connected' if res.get('ok') else f'"{target}": {res.get("error")}')
                    if res.get('ok'):
                        _rejoin['down_since'] = None
                        _rejoin['last_ok'][target] = time.time()
                continue
            ssid = candidates[0]
            _rejoin['last_attempt'][ssid] = now
            _rejoin_event('attempt', f'"{ssid}" is in range ({visible[ssid]} dBm) but the repeater is down; reconnecting')
            res = _repeater_connect(ssid)
            _rejoin_event('connected' if res.get('ok') else 'failed',
                          f'"{ssid}" connected' if res.get('ok') else f'"{ssid}": {res.get("error")}')
            if res.get('ok'):
                _rejoin['down_since'] = None
                _rejoin['last_ok'][ssid] = time.time()   # success is confirmed after REJOIN_STABLE_S
            else:
                _rejoin['failures'][ssid] = _rejoin['failures'].get(ssid, 0) + 1
        except Exception as e:
            _glinet['sid'] = None
            _rejoin_event('error', str(e)[:200])

# Network map: every device on the boat router, identified by MAC vendor
# (nmap's IEEE OUI table) plus hostname, with live up/down rates. Rates come
# from the router's per-client byte totals between polls -- its own rx/tx
# rate fields update too rarely. Verified 2026-10-05: total_rx is what the
# DEVICE received (download), total_tx what it sent (upload). Read-only.
# Password/key fields from the router are never passed on.
OUI_FILE = '/usr/share/nmap/nmap-mac-prefixes'
_oui = {'map': None}
_map_prev = {}          # mac -> (time, total_rx, total_tx)
_map_cache = {'t': 0.0, 'data': None}
MAP_CACHE_TTL_S = 8

def _oui_vendor(mac):
    if _oui['map'] is None:
        m = {}
        try:
            for line in open(OUI_FILE, encoding='utf-8', errors='replace'):
                if line[:1] != '#' and len(line) > 7:
                    m[line[:6].upper()] = line[7:].strip()
        except OSError:
            pass
        _oui['map'] = m
    hexmac = mac.replace(':', '').replace('-', '').upper()
    if len(hexmac) >= 2 and int(hexmac[1], 16) & 0x2:
        return None, True                     # locally administered = randomized "private" MAC
    return _oui['map'].get(hexmac[:6]), False

_KIND_BY_NAME = [(r'iphone|android|pixel|phone|galaxy-s|sm-[sga]', 'phone'), (r'ipad|tab', 'tablet'),
                 (r'macbook|laptop|desktop|-pc\b|^pc-|windows|thinkpad|surface|imac', 'computer'),
                 (r'cam|doorbell', 'camera'), (r'\btv\b|roku|firetv|chromecast|appletv|shield', 'tv'),
                 (r'echo|alexa|sonos|homepod|speaker', 'speaker'),
                 (r'relay|plug|switch|light|bulb|shelly|tasmota|esp|sonoff|tuya|sensor', 'iot'),
                 (r'raspberry|\bpi\b|rpi', 'pi'), (r'cerbo|victron|venus', 'victron'),
                 (r'garmin|raymarine|axiom|navico|b&g|simrad|furuno', 'marine')]
_KIND_BY_VENDOR = [(r'raspberry', 'pi'), (r'victron', 'victron'), (r'garmin|raymarine|navico|furuno', 'marine'),
                   (r'espressif|tuya|shelly|itead|sonoff|allterco|lumi|signify|philips lighting', 'iot'),
                   (r'hikvision|dahua|reolink|amcrest|axis comm|ezviz|wyze', 'camera'),
                   (r'sonos|bose', 'speaker'), (r'roku|vizio|tcl', 'tv'),
                   (r'gl technologies|tp-link|netgear|ubiquiti|mikrotik|cisco|aruba|starlink|spacex', 'network'),
                   (r'intel|dell|hewlett|hp inc|lenovo|asustek|micro-star|liteon|azurewave|realtek|gigabyte', 'computer')]

def _device_kind(name, vendor, private):
    n = (name or '').lower()
    for pat, kind in _KIND_BY_NAME:
        if re.search(pat, n):
            return kind
    v = (vendor or '').lower()
    for pat, kind in _KIND_BY_VENDOR:
        if re.search(pat, v):
            return kind
    if 'apple' in v or 'samsung' in v or 'google' in v or 'oneplus' in v or 'xiaomi' in v or 'motorola' in v:
        return 'phone'
    return 'phone' if private else 'unknown'      # randomized MACs are almost always phones/tablets

# User-assigned names/types per MAC (Network Map > click a device). Plain JSON
# next to the app; phones with randomized MACs may need re-naming if they rotate.
NETMAP_DEVICES_FILE = os.path.join(NETWORK_STATE_DIR, 'network_devices.json')
NETMAP_KINDS = ['phone', 'tablet', 'computer', 'pi', 'iot', 'camera', 'tv', 'speaker', 'network', 'victron', 'marine', 'unknown']

def _netmap_overrides():
    try:
        d = json.load(open(NETMAP_DEVICES_FILE))
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}

@app.route('/api/router/device', methods=['POST'])
def router_device_set():
    body = request.get_json(silent=True) or {}
    mac = str(body.get('mac') or '').upper()
    if not re.fullmatch(r'([0-9A-F]{2}:){5}[0-9A-F]{2}', mac):
        return jsonify({'ok': False, 'error': 'bad MAC'}), 400
    name = str(body.get('name') or '').strip()[:40]
    kind = body.get('kind') or ''
    if kind and kind not in NETMAP_KINDS:
        return jsonify({'ok': False, 'error': 'unknown type'}), 400
    ov = _netmap_overrides()
    if name or kind:
        ov[mac] = {k: v for k, v in (('name', name), ('kind', kind)) if v}
    else:
        ov.pop(mac, None)                       # both blank = back to automatic
    tmp = NETMAP_DEVICES_FILE + '.new'
    with open(tmp, 'w') as f:
        json.dump(ov, f, indent=1, sort_keys=True)
    os.replace(tmp, NETMAP_DEVICES_FILE)
    with _glinet['lock']:
        _map_cache['data'] = None
    return jsonify({'ok': True})

def _router_map():
    st = _glinet_status()
    overrides = _netmap_overrides()
    clients = (_glinet_call('clients', 'get_list') or {}).get('clients') or []
    try:
        with open('/sys/class/net/eth0/address') as f:
            my_mac = f.read().strip().upper()
    except OSError:
        my_mac = ''
    now = time.time()
    devices = []
    for c in clients:
        mac = (c.get('mac') or '').upper()
        if not mac:
            continue
        vendor, private = _oui_vendor(mac)
        name = c.get('name') or ''
        this_pi = mac == my_mac
        try:
            trx, ttx = int(c.get('total_rx') or 0), int(c.get('total_tx') or 0)
        except ValueError:
            trx = ttx = 0
        down = up = None
        prev = _map_prev.get(mac)
        if prev and now > prev[0] and c.get('online'):
            dt = now - prev[0]
            down = max(0.0, (trx - prev[1]) / dt)
            up = max(0.0, (ttx - prev[2]) / dt)
        _map_prev[mac] = (now, trx, ttx)
        devices.append({
            'mac': mac, 'ip': c.get('ip'), 'name': 'Server' if this_pi else (name or None),
            'hostname': name or None, 'iface': c.get('iface'), 'online': bool(c.get('online')),
            'vendor': vendor, 'private_mac': private, 'this_pi': this_pi,
            'kind': 'pi' if this_pi else _device_kind(name, vendor, private),
            'down_Bps': down, 'up_Bps': up, 'total_down': trx, 'total_up': ttx,
        })
        ov = overrides.get(mac)
        if ov:                                  # user-assigned name / type win over guesses
            devices[-1]['auto_name'], devices[-1]['auto_kind'] = devices[-1]['name'], devices[-1]['kind']
            devices[-1]['name'] = ov.get('name') or devices[-1]['name']
            devices[-1]['kind'] = ov.get('kind') or devices[-1]['kind']
            devices[-1]['custom'] = True
    devices.sort(key=lambda d: (not d['online'], {'cable': 0, '5G': 1, '2.4G': 2}.get(d['iface'], 3),
                                not d['this_pi'], (d['name'] or d['ip'] or '').lower()))
    online = [d for d in devices if d['online']]
    st['devices'] = devices
    st['totals'] = {'down_Bps': sum(d['down_Bps'] or 0 for d in online), 'up_Bps': sum(d['up_Bps'] or 0 for d in online),
                    'online': len(online)}
    return st

@app.route('/api/router/map')
def router_map():
    now = time.time()
    with _glinet['lock']:
        if _map_cache['data'] is None or now - _map_cache['t'] > MAP_CACHE_TTL_S:
            try:
                data = _router_map()
                data['ok'] = True
            except Exception as e:
                _glinet['sid'] = None
                data = {'ok': False, 'error': str(e)}
            data['autorejoin'] = _rejoin_public_state()
            _map_cache['data'], _map_cache['t'] = data, now
        return jsonify(_map_cache['data'])

# Data usage per uplink. The router keeps no WAN counters (only for cellular
# modems), so the Pi meters it: every minute (from the auto-rejoin loop, which
# only one dashboard process runs) it sums the growth of every client's byte
# totals and books it to the uplink that was active. Approximate: traffic
# between boat devices is counted too, and a sample is skipped across counter
# resets. Kept per day (62 days) and per month in network_usage.json.
NETMAP_USAGE_FILE = os.path.join(NETWORK_STATE_DIR, 'network_usage.json')
_usage = {'prev': {}, 'lock': threading.Lock()}

def _usage_load():
    try:
        d = json.load(open(NETMAP_USAGE_FILE))
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}

def _usage_sample():
    clients = (_glinet_call('clients', 'get_list') or {}).get('clients') or []
    st = _glinet_call('kmwan', 'get_status') or {}
    cfg = _glinet_call('kmwan', 'get_config') or {}
    metric = {c['interface']: c.get('metric', 99) for c in cfg.get('interfaces', [])}
    online = sorted((i['interface'] for i in st.get('interfaces', []) if i.get('status_v4') == 0), key=lambda k: metric.get(k, 99))
    active = online[0] if online else None
    down = up = 0
    cur = {}
    for c in clients:
        mac = (c.get('mac') or '').upper()
        try:
            trx, ttx = int(c.get('total_rx') or 0), int(c.get('total_tx') or 0)
        except ValueError:
            continue
        cur[mac] = (trx, ttx)
        prev = _usage['prev'].get(mac)
        if prev and trx >= prev[0] and ttx >= prev[1]:          # skip counter resets
            down += trx - prev[0]
            up += ttx - prev[1]
    _usage['prev'] = cur
    if not active or (down == 0 and up == 0):
        return
    now = datetime.now()
    with _usage['lock']:
        d = _usage_load()
        for bucket, key in (('months', now.strftime('%Y-%m')), ('days', now.strftime('%Y-%m-%d'))):
            e = d.setdefault(bucket, {}).setdefault(key, {}).setdefault(active, {'down': 0, 'up': 0})
            e['down'] += down
            e['up'] += up
        for old in sorted(d.get('days', {}))[:-62]:
            d['days'].pop(old, None)
        d['since'] = d.get('since') or now.isoformat(timespec='seconds')
        tmp = NETMAP_USAGE_FILE + '.new'
        with open(tmp, 'w') as f:
            json.dump(d, f)
        os.replace(tmp, NETMAP_USAGE_FILE)

@app.route('/api/router/uplink/<iface>')
def router_uplink(iface):
    if iface not in ('wan', 'wwan'):
        return jsonify({'ok': False, 'error': 'unknown uplink'}), 404
    try:
        st = _glinet_status()
    except Exception as e:
        _glinet['sid'] = None
        return jsonify({'ok': False, 'error': str(e)})
    up = next((i for i in st.get('interfaces', []) if i['interface'] == iface), {})
    d = _usage_load()
    now = datetime.now()
    month, last_month = now.strftime('%Y-%m'), (now.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    pick = lambda bucket, key: (d.get(bucket, {}).get(key, {}) or {}).get(iface, {'down': 0, 'up': 0})
    days = d.get('days', {})
    last30 = {'down': 0, 'up': 0}
    for k in sorted(days)[-30:]:
        e = (days[k] or {}).get(iface) or {}
        last30['down'] += e.get('down', 0); last30['up'] += e.get('up', 0)
    out = {'ok': True, 'iface': iface, 'label': up.get('label'), 'online': up.get('online'),
           'active': st.get('active') == iface, 'priority': [i['interface'] for i in st.get('interfaces', [])].index(iface) + 1 if up else None,
           'usage': {'today': pick('days', now.strftime('%Y-%m-%d')), 'month': pick('months', month),
                     'last_month': pick('months', last_month), 'last_30_days': last30,
                     'month_name': now.strftime('%B'), 'last_month_name': (now.replace(day=1) - timedelta(days=1)).strftime('%B'),
                     'metered_since': d.get('since')},
           'rate': None}
    m = _map_cache.get('data') or {}
    if out['active'] and m.get('totals'):
        out['rate'] = {'down_Bps': m['totals'].get('down_Bps'), 'up_Bps': m['totals'].get('up_Bps')}
    if iface == 'wan':
        out['wan'] = st.get('wan')
        sl = _starlink_cache.get('data')
        if sl is None or time.time() - _starlink_cache['t'] > STARLINK_CACHE_TTL_S:
            try:
                sl = _starlink_status()
            except Exception as e:
                sl = {'online': False, 'error': str(e)}
            sl['fetched_at'] = datetime.now(timezone.utc).isoformat()
            _starlink_cache['data'], _starlink_cache['t'] = sl, time.time()
        out['starlink'] = sl
    else:
        rep_st = _glinet_call('repeater', 'get_status') or {}
        rep_st.pop('portal_info', None)
        cfg = rep_st.pop('config', None) or {}
        out['repeater'] = {k: v for k, v in rep_st.items() if k not in ('key', 'password', 'passwd')}
        out['repeater']['ssid'] = cfg.get('ssid')
        try:   # signal as heard by the Pi's own Wi-Fi (the router doesn't report it)
            r = subprocess.run(['sudo', '-n', WIFI_SCAN_HELPER], capture_output=True, text=True, timeout=45)
            vis = {n['ssid']: n for n in (json.loads(r.stdout or '{}').get('networks') or [])}
            out['pi_sees'] = vis.get(cfg.get('ssid'))
        except Exception:
            out['pi_sees'] = None
        out['autorejoin'] = _rejoin_public_state()
    return jsonify(out)

@app.route('/api/router/repeater/reconnect', methods=['POST'])
def router_repeater_reconnect():
    body = request.get_json(silent=True) or {}
    try:
        res = _repeater_connect(body.get('ssid'))
        if res.get('ok'):
            _rejoin['failures'].pop(res.get('ssid'), None)
    except Exception as e:
        res = {'ok': False, 'error': str(e)}
    _rejoin_event('manual', (f'"{res.get("ssid")}" connected' if res.get('ok') else f'reconnect failed: {res.get("error")}'))
    return jsonify(res)

# Repeater networks saved on the router (Network Map > Repeater networks).
# Same router calls as its own admin page; Wi-Fi passwords go browser ->
# Flask -> router only and are never sent back (saved list strips `key`).
# A router scan or a connect briefly retunes the boat Wi-Fi radio.
def _random_repeater_mac():
    import random
    first = random.choice('0123456789ABCDEF') + random.choice('26AE')      # locally administered, like the router UI
    return ':'.join([first] + [f'{random.randrange(256):02X}' for _ in range(5)])

@app.route('/api/router/repeater/saved')
def router_repeater_saved():
    try:
        saved = (_glinet_call('repeater', 'get_saved_ap_list') or {}).get('res') or []
        st = _glinet_call('repeater', 'get_status') or {}
    except Exception as e:
        _glinet['sid'] = None
        return jsonify({'ok': False, 'error': str(e)})
    target = (st.get('config') or {}).get('ssid')
    return jsonify({'ok': True, 'connected': bool(st.get('running') and st.get('state') == 2), 'target': target,
                    'networks': [{'ssid': a.get('ssid'), 'has_password': bool(a.get('key')), 'protocol': a.get('protocol'),
                                  'current': a.get('ssid') == target} for a in saved]})

@app.route('/api/router/repeater/scan', methods=['POST'])
def router_repeater_scan():
    try:
        res = _glinet_call_params('repeater', 'scan', {'all_band': True, 'refresh': True}, timeout=60)
        saved = {a.get('ssid') for a in ((_glinet_call('repeater', 'get_saved_ap_list') or {}).get('res') or [])}
    except Exception as e:
        _glinet['sid'] = None
        return jsonify({'ok': False, 'error': str(e)})
    best = {}
    for ap in (res or {}).get('res') or []:
        ssid = ap.get('ssid')
        if not ssid or not ssid.strip():
            continue                                    # hidden network
        if ssid not in best or (ap.get('signal') or -999) > (best[ssid].get('signal') or -999):
            best[ssid] = ap
    nets = [{'ssid': s, 'signal': a.get('signal'), 'band': a.get('band'), 'channel': a.get('channel'),
             'secure': bool((a.get('encryption') or {}).get('enabled')), 'saved': s in saved}
            for s, a in best.items()]
    nets.sort(key=lambda n: -(n['signal'] or -999))
    return jsonify({'ok': True, 'networks': nets})

@app.route('/api/router/repeater/add', methods=['POST'])
def router_repeater_add():
    body = request.get_json(silent=True) or {}
    ssid = str(body.get('ssid') or '')
    password = body.get('password') or ''
    if not ssid.strip() or len(ssid.encode('utf-8')) > 32:
        return jsonify({'ok': False, 'error': 'network name must be 1-32 bytes'}), 400
    if password and not (8 <= len(password) <= 63):
        return jsonify({'ok': False, 'error': 'password must be 8-63 characters (or empty for an open network)'}), 400
    # Payload as the router UI builds it for a new network (repeaterFn): DHCP,
    # randomized repeater MAC, remember=true. Joining is how the router saves it.
    params = {'ssid': ssid, 'remember': True, 'protocol': 'dhcp', 'disguise': False, 'manual': False,
              'auto_portal': False, 'macaddr': {'mode': 'random', 'macaddr': _random_repeater_mac(), 'update': 'none'}}
    if password:
        params['key'] = password
    with _glinet['lock']:
        _glinet['data'] = None
        _map_cache['data'] = None
    try:
        _glinet_call_params('repeater', 'scan', {'refresh': True}, timeout=60)
    except Exception:
        pass
    try:
        _glinet_call_params('repeater', 'connect', params, timeout=30)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(3)
        try:
            st = _glinet_call('repeater', 'get_status') or {}
        except Exception:
            continue
        if st.get('running') and st.get('state') == 2:
            _rejoin_event('manual', f'"{ssid}" added and connected')
            return jsonify({'ok': True, 'ssid': ssid})
    _rejoin_event('manual', f'"{ssid}" added but did not connect within 60 s')
    return jsonify({'ok': False, 'ssid': ssid, 'saved': True,
                    'error': 'saved on the router, but it did not connect within 60 s (wrong password or out of range?)'})

@app.route('/api/router/repeater/forget', methods=['POST'])
def router_repeater_forget():
    ssid = (request.get_json(silent=True) or {}).get('ssid')
    if not ssid:
        return jsonify({'ok': False, 'error': 'ssid required'}), 400
    disconnected = False
    try:
        # remove_saved_ap only deletes the entry; a live connection to that network
        # keeps running, so disconnect first when it's the network in use.
        st = _glinet_call('repeater', 'get_status') or {}
        if (st.get('config') or {}).get('ssid') == ssid and st.get('running'):
            _glinet_call_params('repeater', 'disconnect', {}, timeout=30)
            disconnected = True
        _glinet_call_params('repeater', 'remove_saved_ap', {'ssid': ssid})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
    _rejoin['failures'].pop(ssid, None)
    _rejoin['last_ok'].pop(ssid, None)
    with _glinet['lock']:
        _glinet['data'] = None
        _map_cache['data'] = None
    _rejoin_event('manual', f'"{ssid}" forgotten' + (' (disconnected)' if disconnected else ''))
    return jsonify({'ok': True, 'disconnected': disconnected})

@app.route('/api/router/autorejoin', methods=['POST'])
def router_autorejoin_toggle():
    body = request.get_json(silent=True) or {}
    _rejoin['enabled'] = bool(body.get('enabled'))
    try:
        json.dump({'enabled': _rejoin['enabled']}, open(REJOIN_STATE_FILE, 'w'))
    except OSError:
        pass
    with _glinet['lock']:
        _glinet['data'] = None
    return jsonify({'ok': True, 'enabled': _rejoin['enabled'], 'runner': _rejoin['runner']})
# ─── end boat router siloed addition ────────────────────────────────────────

# ─── Webcams siloed addition ────────────────────────────────────────────────
# Two Dahua-style IP cameras on the boat LAN. Their /cgi-bin/snapshot.cgi
# returns a 640x480 JPEG with no login, so the Webcams tab just polls this
# proxy (works through the Cloudflare tunnel too, since the browser never
# talks to the cameras directly). Delete this block plus the "Webcams" blocks
# in static-src/index.html to remove the feature.
WEBCAMS = {
    '1': {'name': 'Front Yard', 'host': '192.168.200.200'},
    '2': {'name': 'Backyard',   'host': '192.168.200.201'},
}

@app.route('/api/webcams')
def webcams_list():
    return jsonify([{'id': k, 'name': v['name'], 'host': v['host']} for k, v in WEBCAMS.items()])

@app.route('/api/webcams/<cam_id>/snapshot')
def webcam_snapshot(cam_id):
    cam = WEBCAMS.get(cam_id)
    if not cam:
        return jsonify({'error': 'unknown camera'}), 404
    try:
        r = requests.get(f"http://{cam['host']}/cgi-bin/snapshot.cgi", timeout=5)
        r.raise_for_status()
    except requests.RequestException as e:
        return jsonify({'error': f"{cam['name']} unreachable: {e}"}), 502
    return Response(r.content, mimetype=r.headers.get('Content-Type', 'image/jpeg'),
                    headers={'Cache-Control': 'no-store'})

# Playback of webcam_recorder.py's rolling 24 h recordings. It writes 5-minute
# MP4s named by UTC start time to <recordings_dir>/<cam id>/; the recordings
# dir comes from its config (~/.config/webcams/cameras.json, which also holds
# the camera login and so stays out of this repo).
WEBCAM_REC_NAME = re.compile(r'^\d{8}-\d{6}\.mp4$')

def _webcam_recordings_dir():
    try:
        with open(os.path.expanduser('~/.config/webcams/cameras.json')) as f:
            return json.load(f)['recordings_dir']
    except (OSError, ValueError, KeyError):
        return os.path.expanduser('~/webcam_recordings')

@app.route('/api/webcams/<cam_id>/recordings')
def webcam_recordings(cam_id):
    if cam_id not in WEBCAMS:
        return jsonify({'error': 'unknown camera'}), 404
    cam_dir = os.path.join(_webcam_recordings_dir(), cam_id)
    try:
        names = sorted(f for f in os.listdir(cam_dir) if WEBCAM_REC_NAME.match(f))
    except FileNotFoundError:
        names = []
    out = []
    for i, name in enumerate(names):
        start = datetime.strptime(name[:15], '%Y%m%d-%H%M%S').replace(tzinfo=timezone.utc).timestamp()
        try:
            st = os.stat(os.path.join(cam_dir, name))
        except FileNotFoundError:
            continue    # pruned between listdir and stat
        # a segment ends where the next begins; the one being written ends at its last write
        end = min(st.st_mtime, datetime.strptime(names[i + 1][:15], '%Y%m%d-%H%M%S')
                  .replace(tzinfo=timezone.utc).timestamp()) if i + 1 < len(names) else st.st_mtime
        out.append({'file': name, 'start': start, 'end': max(end, start), 'bytes': st.st_size,
                    'recording': i == len(names) - 1 and time.time() - st.st_mtime < 30})
    return jsonify({'camera': WEBCAMS[cam_id]['name'], 'segments': out})

@app.route('/api/webcams/<cam_id>/recordings/<name>')
def webcam_recording_file(cam_id, name):
    if cam_id not in WEBCAMS or not WEBCAM_REC_NAME.match(name):
        return jsonify({'error': 'not found'}), 404
    # conditional=True gives Range support, which the video player needs to seek
    resp = send_from_directory(os.path.join(_webcam_recordings_dir(), cam_id), name,
                               mimetype='video/mp4', conditional=True)
    resp.headers['Cache-Control'] = 'no-cache'
    return resp

# Full-resolution Live view: the recorder also keeps a short rolling HLS
# playlist per camera in RAM (live_dir in its config); serve it as-is.
WEBCAM_LIVE_NAME = re.compile(r'^(index\.m3u8|seg\d+\.ts)$')

@app.route('/api/webcams/<cam_id>/live/<name>')
def webcam_live_file(cam_id, name):
    if cam_id not in WEBCAMS or not WEBCAM_LIVE_NAME.match(name):
        return jsonify({'error': 'not found'}), 404
    try:
        with open(os.path.expanduser('~/.config/webcams/cameras.json')) as f:
            live_dir = json.load(f)['live_dir']
    except (OSError, ValueError, KeyError):
        return jsonify({'error': 'live feed not configured'}), 404
    playlist = name.endswith('.m3u8')
    resp = send_from_directory(os.path.join(live_dir, cam_id), name,
                               mimetype='application/vnd.apple.mpegurl' if playlist else 'video/mp2t')
    resp.headers['Cache-Control'] = 'no-store' if playlist else 'max-age=60'
    return resp
# ─── end Webcams siloed addition ────────────────────────────────────────────

# ─── Victron Bluetooth (Orion-Tr Smart) siloed addition ─────────────────────
# victron_ble_bridge.py reads the Orions' Bluetooth "Instant Readout"
# broadcasts (they have no VE.Direct port, so VRM never sees them) and
# publishes boat/victron/<name>/<field>. Served flat as <name>_<field>, e.g.
# orion_house_state. Broadcasts are irregular (the bridge heartbeats every
# 30s), hence the longer staleness window than the simulator route above.
VICTRON_BLE_PREFIX = 'boat/victron/'
VICTRON_BLE_STALE_S = 120

@app.route('/api/victron/ble')
def victron_ble():
    cutoff = datetime.now(timezone.utc).timestamp() - VICTRON_BLE_STALE_S
    out = {}
    with mqtt_lock:
        items = [(t, v) for t, v in mqtt_state['topics'].items() if t.startswith(VICTRON_BLE_PREFIX)]
    for topic, entry in items:
        try:
            if datetime.fromisoformat(entry['time']).timestamp() < cutoff:
                continue
        except (KeyError, ValueError):
            continue
        key = topic[len(VICTRON_BLE_PREFIX):].replace('/', '_')
        try:
            out[key] = float(entry['value'])
        except ValueError:
            out[key] = entry['value']
    return jsonify(out)
# ─── end Victron Bluetooth siloed addition ──────────────────────────────────

# ─── Electrical history siloed addition ─────────────────────────────────────
# History for the Electrical tab devices VRM doesn't have (simulated shunts /
# MPPTs / alternator, Bluetooth Orions, derived DC loads), recorded by
# electrical_history.py into its own SQLite file. Same response shape as the
# other pen-chart endpoints: {series: {key: {times, values}}}.
import electrical_history

# VRM snapshot for the recorder's Energy balance, fetched at most once a minute
# (the diagnostics call is shared VRM API quota with /api/victron and the
# tank/battery monitor).
_vrm_cache = {'t': 0.0, 'd': {}}
def vrm_cached(max_age_s=60):
    if time.time() - _vrm_cache['t'] > max_age_s:
        try:
            _vrm_cache['d'] = get_vrm_data() or {}
        except Exception as e:
            print(f'vrm_cached: {e}')
        _vrm_cache['t'] = time.time()
    return _vrm_cache['d']

@app.route('/api/electrical/history')
def electrical_history_route():
    keys = [k.strip() for k in (request.args.get('metrics') or '').split(',') if k.strip()]
    try:
        return jsonify({'series': electrical_history.series(keys, request.args.get('range', '1h'))})
    except Exception as e:
        return jsonify({'series': {k: {'times': [], 'values': [], 'error': str(e)} for k in keys}})
# ─── end Electrical history siloed addition ─────────────────────────────────

# ─── Victron simulator as a user service — siloed addition ───────────────────
# victron_simulator.py runs as the systemd *user* service victron-simulator
# (unit file victron-simulator.service in this dir) so it survives reboots.
# This adds it to Diagnostics -> Simulators by wrapping simulators_status()
# and giving it its own start/stop routes (a literal path segment outranks
# the <name> rule). ON = enable --now, OFF = disable --now, so the choice
# also sticks across reboots. disable removes the linked unit's symlink too,
# which is why start enables it by full path. The unit always lives in the
# dev copy (it runs the dev script), so the path is fixed rather than based on
# SIMULATOR_DIR, which is ~ in production. Delete this block to remove.
VICTRON_SIM_UNIT_PATH = os.path.expanduser('~/dashboard-dev/victron-simulator.service')

def _victron_sim_systemctl(*args):
    uid = os.getuid()
    env = dict(os.environ, XDG_RUNTIME_DIR=f'/run/user/{uid}',
               DBUS_SESSION_BUS_ADDRESS=f'unix:path=/run/user/{uid}/bus')
    return subprocess.run(['systemctl', '--user', *args], capture_output=True, text=True, timeout=15, env=env)

def _victron_sim_entry():
    try:
        out = _victron_sim_systemctl('show', 'victron-simulator', '-p', 'ActiveState', '-p', 'MainPID').stdout
        props = dict(line.split('=', 1) for line in out.splitlines() if '=' in line)
    except (OSError, subprocess.SubprocessError):
        props = {}
    running = props.get('ActiveState') == 'active'
    pid = int(props.get('MainPID') or 0)
    return {'name': 'victron', 'label': 'Victron (Electrical)', 'running': running, 'pid': pid if running and pid else None}

_simulators_status_before_victron = app.view_functions['simulators_status']

def _simulators_status_with_victron():
    data = _simulators_status_before_victron().get_json()
    data['simulators'].append(_victron_sim_entry())
    return jsonify(data)

app.view_functions['simulators_status'] = _simulators_status_with_victron

@app.route('/api/simulators/victron/start', methods=['POST'])
def victron_sim_service_start():
    r = _victron_sim_systemctl('enable', '--now', VICTRON_SIM_UNIT_PATH)
    if r.returncode != 0:
        return jsonify({'error': r.stderr.strip() or 'systemctl enable failed'}), 500
    return jsonify({'status': 'ok'})

@app.route('/api/simulators/victron/stop', methods=['POST'])
def victron_sim_service_stop():
    r = _victron_sim_systemctl('disable', '--now', 'victron-simulator')
    if r.returncode != 0:
        return jsonify({'error': r.stderr.strip() or 'systemctl disable failed'}), 500
    return jsonify({'status': 'ok'})
# ─── end Victron simulator service ───────────────────────────────────────────

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
        threading.Thread(target=system_health_loop, daemon=True).start()
        threading.Thread(target=tank_battery_monitor_loop, daemon=True).start()
        threading.Thread(target=crit_battery_monitor_loop, daemon=True).start()  # 12V/diesel/thruster critical push (siloed)
        threading.Thread(target=router_autorejoin_loop, daemon=True).start()     # boat router repeater auto-rejoin (siloed)
        threading.Thread(target=_load_tides_station_cache, daemon=True).start()
        electrical_history.start(mqtt_state, mqtt_lock, vrm_cached)  # Electrical history (siloed)
    # threaded=True matters a lot for the Chart tab specifically: a browser
    # loads a viewport's worth of tile <img> requests in parallel, and
    # without this the dev server handles them one at a time -- fine for a
    # tightly zoomed-in view needing a handful of tiles, but a zoomed-out
    # view needing dozens queues up behind itself and can look like tiles
    # just aren't rendering, even though every individual request is fast.
    # reloader_interval: the reloader re-lists the project tree (chart_data/raw,
    # chart_data/processed, .venv-ble) on every check, ~0.18 s of CPU each; the
    # default 1 s cost ~18% of a core. Edits now take up to 5 s to reload.
    # Ignored when DEBUG_MODE is False (no reloader).
    app.run(host='0.0.0.0', port=5003, debug=DEBUG_MODE, threaded=True, reloader_interval=5)

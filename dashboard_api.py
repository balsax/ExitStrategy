from flask import Flask, jsonify, request, send_from_directory
import requests
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

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)

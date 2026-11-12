"""Victron data straight from the Cerbo GX on the boat LAN (siloed addition, 2026-11-12).

The Cerbo (Venus OS v3.72, 192.168.10.15, set manually on the Cerbo) runs its own
MQTT broker ("MQTT on LAN", port 1883, no login). Every D-Bus value is
published as N/<portal id>/<service>/<instance>/<path> = {"value": ...}, but
only while a client keeps sending R/<portal id>/keepalive (Venus stops after
~60 s without one). Values are published when they change, so a steady value
is not re-sent: freshness is judged from the Cerbo's N/<id>/heartbeat topic,
not per value.

snapshot() returns {service/instance/path: value} plus 'age_s' (seconds since
the Cerbo was last heard), or None when the Cerbo hasn't been heard for
STALE_S -- dashboard_api.py then falls back to the VRM cloud API.

Read-only: nothing is ever published to W/ (write) topics.

Env: CERBO_HOST (default 192.168.10.15), CERBO_LOCAL=0 disables this.
To back out: delete this file and the marked "Cerbo local" block in
dashboard_api.py.
"""
import json
import os
import threading
import time

import paho.mqtt.client as mqtt

CERBO_HOST = os.environ.get('CERBO_HOST', '192.168.10.15')
CERBO_PORT = 1883
KEEPALIVE_EVERY_S = 30
STALE_S = 30
# Only the services the dashboard reads; settings/ alone is ~850 retained topics.
SERVICES = ('system/0', 'battery', 'vebus', 'solarcharger', 'dcdc', 'gps/0')

_lock = threading.Lock()
_values = {}              # 'battery/512/Soc' -> 99.0
_state = {'portal': None, 'last_rx': 0.0, 'connected': False, 'error': None, 'started': False}


def enabled():
    return os.environ.get('CERBO_LOCAL', '1') != '0'


def _keepalive(client, full=False):
    portal = _state['portal']
    if not portal:
        return
    # An empty payload asks for a full republish (needed after every
    # (re)connect); suppress-republish just keeps the stream alive.
    payload = '' if full else json.dumps({'keepalive-options': ['suppress-republish']})
    client.publish(f'R/{portal}/keepalive', payload)


def _subscribe_portal(client, portal):
    _state['portal'] = portal
    for svc in SERVICES:
        client.subscribe(f'N/{portal}/{svc}/#')
    client.subscribe(f'N/{portal}/heartbeat')
    _keepalive(client, full=True)


def _on_connect(client, userdata, flags, reason_code, properties=None):
    _state['connected'] = not reason_code.is_failure
    if reason_code.is_failure:
        _state['error'] = f'connect: {reason_code}'
        return
    _state['error'] = None
    with _lock:
        _values.clear()
    if _state['portal']:
        _subscribe_portal(client, _state['portal'])
    else:
        client.subscribe('N/+/system/0/Serial')     # retained; gives the portal id


def _on_disconnect(client, userdata, flags, reason_code, properties=None):
    _state['connected'] = False


def _on_message(client, userdata, msg):
    parts = msg.topic.split('/', 2)
    if len(parts) < 3:
        return
    portal, path = parts[1], parts[2]
    if _state['portal'] is None:
        if path == 'system/0/Serial':
            client.unsubscribe('N/+/system/0/Serial')
            _subscribe_portal(client, portal)
        return
    if portal != _state['portal']:
        return
    try:
        value = json.loads(msg.payload)['value'] if msg.payload else None
    except (ValueError, KeyError, TypeError):
        return
    with _lock:
        _state['last_rx'] = time.time()
        if value is None:
            _values.pop(path, None)       # device gone / value not available
        else:
            _values[path] = value


def _run():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'boat-dashboard-{os.getpid()}')
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=2, max_delay=30)
    while True:
        try:
            client.connect(CERBO_HOST, CERBO_PORT, keepalive=30)
            break
        except OSError as e:
            _state['error'] = f'{CERBO_HOST}: {e}'
            time.sleep(15)
    client.loop_start()                   # paho reconnects on its own from here
    while True:
        time.sleep(KEEPALIVE_EVERY_S)
        if _state['connected']:
            try:
                _keepalive(client)
            except Exception as e:        # never let the keepalive thread die
                _state['error'] = f'keepalive: {e}'


def start():
    """Idempotent; started lazily from the first snapshot() call."""
    if _state['started'] or not enabled():
        return
    _state['started'] = True
    threading.Thread(target=_run, name='cerbo-local', daemon=True).start()


def snapshot():
    start()
    with _lock:
        age = time.time() - _state['last_rx']
        if not _state['last_rx'] or age > STALE_S or not _values:
            return None
        d = dict(_values)
    d['age_s'] = round(age, 1)
    return d


def status():
    """For diagnostics: whether local data is in use and why not."""
    with _lock:
        last = _state['last_rx']
        n = len(_values)
    return {'host': CERBO_HOST, 'portal': _state['portal'], 'connected': _state['connected'],
            'enabled': enabled(), 'values': n, 'age_s': round(time.time() - last, 1) if last else None,
            'error': _state['error']}

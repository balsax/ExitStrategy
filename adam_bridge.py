#!/usr/bin/env python3
"""
ADAM-6017 analog input module -> MQTT bridge (shunt currents).

The ADAM-6017 at 192.168.10.250 (static IP set in the module itself) reads
8 differential inputs, all on the +/-150 mV range. Shunts are wired across
them; this polls the module over Modbus/TCP (input registers 0-7, raw
0..65535 = -150..+150 mV) and publishes:

  boat/adam/status               online / offline (retained, last will)
  boat/adam/<name>/mv            shunt voltage, mV
  boat/adam/<name>/current       A, for channels with a shunt rating

The Electrical tab uses alternator (ch0) and orion_house (ch4).

Channel names and shunt ratings are in CHANNELS below. If a shunt is wired
backwards (charging reads negative), flip its sign there rather than the wires.

Usage:
  python3 adam_bridge.py            # run the bridge
  python3 adam_bridge.py --print    # read + print, no MQTT

Firmware 2.03 notes: no DHCP, and network settings / input ranges can only be
changed with Advantech's Windows utility. UDP 1025 also answers ASCII
commands (#01 = all channels in mV, $01M = model) if Modbus ever acts up.
"""
import argparse
import socket
import struct
import time

import paho.mqtt.client as mqtt

ADAM_HOST = "192.168.10.250"
ADAM_PORT = 502
RANGE_MV = 150.0          # all channels are on +/-150 mV (checked 2026-11-03)
POLL_S = 2                # mqtt_logger caps fast streams at one write per 2 s anyway

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "adam_bridge"
TOPIC_PREFIX = "boat/adam"

# channel -> (topic name, shunt amps, shunt mV, sign); amps None = publish mV only
CHANNELS = {
    0: ('alternator', 200, 75, 1),
    1: ('ch1', None, None, 1),        # not connected yet (mV only); 50A/75mV shunt planned
    2: ('ch2', None, None, 1),        # not connected yet (mV only); 50A/75mV shunt planned
    3: ('ch3', None, None, 1),        # not connected yet (mV only); 50A/75mV shunt planned
    4: ('orion_house', 50, 75, 1),    # Orion 48|12 output to the house batteries, ground side
    5: ('orion_thruster', 50, 75, 1), # Orion output to the thruster/diesel batteries, ground side
    6: ('ch6', None, None, 1),
    7: ('ch7', None, None, 1),
}

# Zero correction: the module reads ~1 mV off zero with no current through a
# shunt (about 2.6 A on the 200 A alternator shunt). ZERO_MV[ch] is that resting
# reading, measured with no current flowing, and is subtracted before the amps
# conversion; anything left within DEADBAND_MV of zero reads 0 A. The published
# mv stays the raw reading, so the offset can be re-measured from it later.
# ch0 measured 2026-11-08, engine off: 0.96 mV (sd 0.09). ch4/ch5 not yet
# measured (each Orion was running) -- measure with that Orion off.
ZERO_MV = {0: 0.96}
DEADBAND_MV = 0.3


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip().strip('"\'')
    return secrets


class Adam:
    """Minimal Modbus/TCP client: one persistent connection, FC4 reads only."""
    def __init__(self, host, port):
        self.host, self.port, self.sock, self.tid = host, port, None, 0

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None

    def read_inputs(self, addr=0, count=8):
        if self.sock is None:
            self.sock = socket.create_connection((self.host, self.port), timeout=3)
        self.tid = (self.tid + 1) & 0xFFFF
        self.sock.sendall(struct.pack(">HHHBBHH", self.tid, 0, 6, 1, 4, addr, count))
        resp = b''
        while len(resp) < 9 or len(resp) < 9 + resp[8]:
            chunk = self.sock.recv(256)
            if not chunk:
                raise ConnectionError('ADAM closed the connection')
            resp += chunk
            if len(resp) >= 8 and resp[7] & 0x80:
                raise IOError(f'Modbus exception {resp[8] if len(resp) > 8 else "?"}')
        tid, = struct.unpack(">H", resp[:2])
        if tid != self.tid:
            raise IOError('Modbus transaction id mismatch')
        n = resp[8]
        return struct.unpack(">" + "H" * (n // 2), resp[9:9 + n])


def to_mv(raw):
    return raw / 65535.0 * (2 * RANGE_MV) - RANGE_MV


def readings(raw_regs):
    """{name: {'mv': ..., 'current': ...}} from the 8 raw registers."""
    out = {}
    for ch, raw in enumerate(raw_regs):
        name, amps, shunt_mv, sign = CHANNELS[ch]
        mv = sign * to_mv(raw)
        r = {'mv': round(mv, 2)}
        if amps:
            net = mv - ZERO_MV.get(ch, 0.0)
            if abs(net) < DEADBAND_MV:
                net = 0.0
            r['current'] = round(net * amps / shunt_mv, 1) + 0.0  # + 0.0 turns -0.0 into 0.0
        out[name] = r
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--print', dest='print_only', action='store_true', help='Print readings instead of publishing')
    args = ap.parse_args()

    adam = Adam(ADAM_HOST, ADAM_PORT)
    client = None
    if not args.print_only:
        s = get_secrets()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
        client.username_pw_set(s['MQTT_USER'], s['MQTT_PASS'])
        client.will_set(f"{TOPIC_PREFIX}/status", 'offline', retain=True)
        client.on_connect = lambda c, u, f, rc, p: c.publish(f"{TOPIC_PREFIX}/status", 'online', retain=True)
        client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=30)
        client.loop_start()

    online = None
    while True:
        t0 = time.monotonic()
        try:
            data = readings(adam.read_inputs())
            if args.print_only:
                print('  '.join(f"{n}={v['mv']:+.2f}mV" + (f"/{v['current']:+.1f}A" if 'current' in v else '')
                                for n, v in data.items()), flush=True)
            else:
                for name, vals in data.items():
                    for field, value in vals.items():
                        client.publish(f"{TOPIC_PREFIX}/{name}/{field}", str(value))
                if online is not True:
                    client.publish(f"{TOPIC_PREFIX}/status", 'online', retain=True)
            if online is not True:
                print(f'ADAM {ADAM_HOST} online', flush=True)
            online = True
        except (OSError, ConnectionError) as e:
            adam.close()
            if online is not False:
                print(f'ADAM {ADAM_HOST} unreachable: {e}', flush=True)
                if client:
                    client.publish(f"{TOPIC_PREFIX}/status", 'offline', retain=True)
            online = False
        time.sleep(max(0.0, POLL_S - (time.monotonic() - t0)))


if __name__ == '__main__':
    main()

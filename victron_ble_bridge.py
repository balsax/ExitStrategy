#!/usr/bin/env python3
"""
Victron Bluetooth "Instant Readout" -> MQTT bridge.

The Orion-Tr Smart 48/12-30A chargers have no VE.Direct port, so the Cerbo
(and therefore VRM) never sees them. They do broadcast an encrypted status
advertisement over Bluetooth, which this reads with the victron-ble library
and publishes to:

  boat/victron/<name>/state            Off / Bulk / Absorption / Float / ...
  boat/victron/<name>/state_code       numeric (0 off, 3 bulk, 4 abs, 5 float)
  boat/victron/<name>/input_voltage    V
  boat/victron/<name>/output_voltage   V
  boat/victron/<name>/off_reason       e.g. "None", "Engine shutdown", "Remote input"
  boat/victron/<name>/charger_error    "None" or the error name
  boat/victron/<name>/rssi             dBm, to judge the radio link

The broadcast has no current reading -- the Orion-Tr Smart doesn't send one.

Setup (once per device):
  1. VictronConnect -> the Orion -> gear icon -> "..." -> Product info ->
     Instant readout via Bluetooth: Enable, then "Show" the encryption data
     (MAC address + key).
  2. Put them in ~/.config/victron-ble/devices.json (chmod 600):
       {
         "orion_house":  {"mac": "AA:BB:CC:DD:EE:01", "key": "0123...cdef"},
         "orion_thruster": {"mac": "AA:BB:CC:DD:EE:02", "key": "4567...89ab"}
       }
     The names become the MQTT topic segment. orion_house / orion_thruster are
     what the Electrical tab looks for.

Usage:
  .venv-ble/bin/python victron_ble_bridge.py --discover   # list Victron devices in range
  .venv-ble/bin/python victron_ble_bridge.py              # run the bridge
  .venv-ble/bin/python victron_ble_bridge.py --print      # decode + print, no MQTT

Needs BlueZ (sudo apt install bluez) and the .venv-ble venv (pip install victron-ble).
"""
import argparse
import asyncio
import json
import os
import time

import paho.mqtt.client as mqtt
from bleak import BleakScanner
from victron_ble.devices import detect_device_type
from victron_ble.devices.base import BitReader, ChargerError, OperationMode

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "victron_ble_bridge"
TOPIC_PREFIX = "boat/victron"
CONFIG_PATH = os.path.expanduser("~/.config/victron-ble/devices.json")
VICTRON_MFR_ID = 0x02E1
HEARTBEAT_S = 30        # republish unchanged values this often
MIN_INTERVAL_S = 2      # but never more often than this per device

# Off-reason is a bitmask (several can be set at once), unlike the enum
# victron-ble decodes it into, which raises on combined bits.
OFF_REASON_BITS = [
    (0x001, 'No input power'),
    (0x002, 'Switched off (switch)'),
    (0x004, 'Switched off (register)'),
    (0x008, 'Remote input'),
    (0x010, 'Protection active'),
    (0x020, 'Pay-as-you-go'),
    (0x040, 'BMS'),
    (0x080, 'Engine shutdown'),
    (0x100, 'Analysing input voltage'),
]


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def load_devices():
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    return {d['mac'].lower(): {'name': name, 'key': d['key']} for name, d in cfg.items()}


def enum_name(enum_cls, value):
    try:
        return enum_cls(value).name.replace('_', ' ').capitalize()
    except ValueError:
        return f'Unknown ({value})'


def decode_dcdc(decrypted):
    r = BitReader(decrypted)
    state = r.read_unsigned_int(8)
    err = r.read_unsigned_int(8)
    vin = r.read_unsigned_int(16)
    vout = r.read_signed_int(16)
    off = r.read_unsigned_int(32)
    out = {'state_code': state}
    out['state'] = 'Unavailable' if state == 0xFF else enum_name(OperationMode, state)
    out['charger_error'] = 'None' if err in (0, 0xFF) else enum_name(ChargerError, err)
    if vin != 0xFFFF:
        out['input_voltage'] = round(vin / 100, 2)
    if vout != 0x7FFF:
        out['output_voltage'] = round(vout / 100, 2)
    reasons = [label for bit, label in OFF_REASON_BITS if off & bit]
    out['off_reason'] = ', '.join(reasons) if reasons else 'None'
    return out


async def discover(seconds):
    seen = {}

    def cb(device, adv):
        data = adv.manufacturer_data.get(VICTRON_MFR_ID)
        if not data or device.address in seen:
            return
        readout = data.startswith(b'\x10')
        kind = detect_device_type(data) if readout else None
        seen[device.address] = True
        print(f"{device.address}  rssi {adv.rssi:>4}  {device.name or '(no name)':<28} "
              f"{kind.__name__ if kind else ('instant readout OFF' if not readout else 'unknown type')}")

    print(f"Listening {seconds}s for Victron devices...")
    async with BleakScanner(detection_callback=cb):
        await asyncio.sleep(seconds)
    if not seen:
        print("None found. Check the Pi is in range and Bluetooth is on (systemctl status bluetooth).")


async def run(print_only):
    devices = load_devices()
    client = None
    if not print_only:
        s = get_secrets()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
        client.username_pw_set(s['MQTT_USER'], s['MQTT_PASS'])
        client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=30)
        client.loop_start()

    parsers = {}
    last = {}  # mac -> (time, payload)

    def cb(device, adv):
        mac = device.address.lower()
        data = adv.manufacturer_data.get(VICTRON_MFR_ID)
        if mac not in devices or not data or not data.startswith(b'\x10'):
            return
        cfg = devices[mac]
        try:
            if mac not in parsers:
                klass = detect_device_type(data)
                if klass is None or klass.__name__ != 'DcDcConverter':
                    print(f"{cfg['name']}: not a DC-DC converter ({klass}), skipping")
                    devices.pop(mac)
                    return
                parsers[mac] = klass(cfg['key'])
            payload = decode_dcdc(parsers[mac].decrypt(data))
        except Exception as e:  # wrong key, truncated packet, ...
            print(f"{cfg['name']}: could not decode ({e})")
            return
        now = time.time()
        prev_t, prev_p = last.get(mac, (0, None))
        if now - prev_t < MIN_INTERVAL_S or (payload == prev_p and now - prev_t < HEARTBEAT_S):
            return
        last[mac] = (now, payload)
        payload['rssi'] = adv.rssi
        if print_only:
            print(cfg['name'], json.dumps(payload))
            return
        for field, value in payload.items():
            client.publish(f"{TOPIC_PREFIX}/{cfg['name']}/{field}", str(value))

    print(f"Reading {', '.join(d['name'] for d in devices.values())} -- Ctrl+C to stop")
    async with BleakScanner(detection_callback=cb):
        while True:
            await asyncio.sleep(3600)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--discover', action='store_true', help='List Victron devices in range and exit')
    ap.add_argument('--seconds', type=int, default=20, help='How long --discover listens (default 20)')
    ap.add_argument('--print', dest='print_only', action='store_true', help='Print decoded data instead of publishing')
    args = ap.parse_args()
    try:
        asyncio.run(discover(args.seconds) if args.discover else run(args.print_only))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()

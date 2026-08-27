#!/usr/bin/env python3
"""
Tank level + bilge pump simulator for dashboard development.

Publishes fake boat/nav/tanks/<tank>/level MQTT topics (PGN 127505, Fluid
Level, one message per physical tank keyed by its Instance field) for the
boat's 4 tanks -- 2 fresh water, 1 diesel, 1 black water -- so the Overview
tab's tank gauges can be exercised before real tank senders are wired to the
N2K bus. Tank names (fresh_1/fresh_2/diesel/black) match
n2k_mqtt_bridge.py's TANK_INSTANCE_TOPICS, so this is a drop-in stand-in for
those real PGN 127505 messages, not just a look-alike.

Consumables (fresh water, diesel) drain steadily with light sensor jitter
and get "topped off" back near full once they run low -- like actually
using the boat and re-watering/refueling at the dock, not an unbounded
drift. Black water does the reverse: fills steadily with use, "pumped out"
back to near-empty once it gets full. Real tank levels don't wander back
and forth the way wind direction does -- they trend steadily one way based
on consumption -- so this is a plain drift-with-jitter model, not the
velocity-smoothed random walk wind_simulator.py/gps_simulator.py use.

Also publishes boat/nav/bilge/pump_on and .../last_run -- a rare on/off
event (a float switch triggering briefly), not a level, and not tied to any
standard NMEA2000 PGN the way the tanks are (bilge pumps are typically just
wired to a panel breaker, not on the N2K bus), so this stays a dev-only
topic with no corresponding n2k_mqtt_bridge.py mapping to build.

Usage:
  python3 tank_simulator.py
  python3 tank_simulator.py --rate 2

Ctrl+C to stop.
"""
import argparse
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "tank_simulator"

# name -> (starting level %, per-tick drift range %, refill/pump-out
# trigger level %, level it jumps to when triggered). Consumables drift
# negative and trigger a refill once LOW; black water drifts positive and
# triggers a pump-out once HIGH -- same fields, just read in the direction
# that matches how each tank actually behaves.
#
# Trigger levels are kept a healthy margin clear of dashboard_api.py's own
# alarm thresholds (LOW_TANK_ALARM_PCT=20 for fresh/diesel, BLACK_TANK_-
# ALARM_PCT=75 for black water) on purpose -- this simulator is meant to
# demo normal level movement, not spam ntfy every few minutes. Push a tank
# past its trigger by hand (or just lower these) if you actually want to
# exercise the alarms.
TANKS = {
    'fresh_1': {'level': 82.0, 'drift': (-0.9, -0.3), 'trigger': 35.0, 'reset_to': 98.0},
    'fresh_2': {'level': 65.0, 'drift': (-0.8, -0.25), 'trigger': 35.0, 'reset_to': 98.0},
    'diesel':  {'level': 90.0, 'drift': (-0.15, -0.03), 'trigger': 35.0, 'reset_to': 100.0},
    'black':   {'level': 25.0, 'drift': (0.15, 0.6), 'trigger': 60.0, 'reset_to': 5.0},
}

# Bilge: mostly off, briefly on every few minutes (a float switch tripping
# once enough water's collected), not a continuous level like the tanks
# above -- state machine, not a drift model.
BILGE_START_CHANCE = 0.015   # per tick while idle -- ~5.5min average gap at the default 5s rate
BILGE_RUN_TICKS = (2, 4)     # how many ticks a run lasts once triggered -- 10-20s at the default rate


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--rate', type=float, default=5.0, help='Publish interval in seconds (default 5, matches n2k_mqtt_bridge.py\'s "slow" PGN class)')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    bilge_on = False
    bilge_ticks_left = 0
    bilge_last_run = None  # ISO string, set the moment a run starts

    print("Simulating 4 tanks: fresh_1, fresh_2, diesel (drain + refill), black (fill + pump-out)")
    print("Plus a bilge pump that runs briefly every few minutes")
    print("Publishing to boat/nav/tanks/<tank>/level, boat/nav/bilge/* -- Ctrl+C to stop\n")

    try:
        while True:
            for name, t in TANKS.items():
                t['level'] += random.uniform(*t['drift'])
                # Draining tanks trigger below the threshold, filling tanks
                # (black water) trigger above it -- same comparison works for
                # both since 'trigger' is already read in the right sense.
                draining = t['drift'][0] < 0
                if (draining and t['level'] <= t['trigger']) or (not draining and t['level'] >= t['trigger']):
                    t['level'] = t['reset_to']
                t['level'] = max(0.0, min(100.0, t['level']))
                # A little sensor-like jitter on top of the underlying drift,
                # published only (not fed back into state) -- same convention
                # wind_simulator.py uses for its own apparent-wind output.
                level_out = max(0.0, min(100.0, t['level'] + random.uniform(-0.3, 0.3)))
                client.publish(f"boat/nav/tanks/{name}/level", f"{level_out:.1f}", retain=False)

            if bilge_on:
                bilge_ticks_left -= 1
                if bilge_ticks_left <= 0:
                    bilge_on = False
            elif random.random() < BILGE_START_CHANCE:
                bilge_on = True
                bilge_ticks_left = random.randint(*BILGE_RUN_TICKS)
                bilge_last_run = datetime.now().isoformat()
            client.publish("boat/nav/bilge/pump_on", "1" if bilge_on else "0", retain=False)
            if bilge_last_run is not None:
                client.publish("boat/nav/bilge/last_run", bilge_last_run, retain=False)

            status = "  ".join(f"{name}={t['level']:.0f}%" for name, t in TANKS.items())
            bilge_status = "bilge=ON" if bilge_on else "bilge=off"
            print(f"\r{status}  {bilge_status}   ", end='', flush=True)
            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

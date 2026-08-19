#!/usr/bin/env python3
"""
Simulates a Garmin GPSMAP 1243 chartplotter's NMEA2000 output, in the same
canboat `analyzer -json` line format n2k_mqtt_bridge.py already expects on
stdin -- one JSON object per line: {"pgn": <PGN>, "fields": {...}}.

The real chain is candump can0 | analyzer -json | python3 n2k_mqtt_bridge.py.
This stands in for candump+analyzer specifically, so the bridge's PGN_MAP can
be exercised and verified end-to-end without a CAN HAT wired into the boat's
N2K backbone yet -- same purpose as gps_simulator.py, but one layer lower
(canboat JSON instead of publishing MQTT directly), so it's testing the
bridge itself rather than standing in for it.

Emits the core position/nav PGNs a chartplotter puts on the bus:
  129025  Position, Rapid Update      (lat/lon, ~1Hz)
  129026  COG & SOG, Rapid Update     (~1Hz)
  127250  Vessel Heading              (~1Hz)
  129029  GNSS Position Data          (fix quality detail, ~5s)

Heading and COG are deliberately made to drift apart a little (not locked
together) -- a boat's bow doesn't always point exactly where it's actually
tracking over the ground under leeway or current, and that gap is real,
useful information the Chart tab's "My Vessel" heading/COG lines are meant
to show.

Usage:
    python3 garmin_1243_simulator.py | python3 n2k_mqtt_bridge.py
    python3 garmin_1243_simulator.py --swing-radius 25 | python3 n2k_mqtt_bridge.py
    python3 garmin_1243_simulator.py --sog 5.5 --heading 90 | python3 n2k_mqtt_bridge.py
"""
import sys
import json
import time
import math
import random
import argparse


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--lat', type=float, default=27.7000, help='starting latitude')
    p.add_argument('--lon', type=float, default=-82.6900, help='starting longitude')
    p.add_argument('--swing-radius', type=float, default=0, help='meters -- simulate swinging at anchor instead of holding position')
    p.add_argument('--heading', type=float, default=45.0, help='base heading, degrees true')
    p.add_argument('--sog', type=float, default=0.0, help='knots, base speed over ground')
    p.add_argument('--rate', type=float, default=1.0, help='seconds between fast-PGN updates (129025/129026/127250)')
    args = p.parse_args()

    lat0, lon0 = args.lat, args.lon
    t0 = time.time()
    tick = 0

    def emit(pgn, fields):
        print(json.dumps({"pgn": pgn, "fields": fields}), flush=True)

    while True:
        t = time.time() - t0

        if args.swing_radius > 0:
            angle = (t / 60.0) * 2 * math.pi  # ~1 revolution/minute, like a boat swinging at anchor
            m_per_deg_lat = 111320.0
            m_per_deg_lon = 111320.0 * math.cos(math.radians(lat0))
            lat = lat0 + (args.swing_radius * math.sin(angle)) / m_per_deg_lat
            lon = lon0 + (args.swing_radius * math.cos(angle)) / m_per_deg_lon
        else:
            lat, lon = lat0, lon0

        heading_now = (args.heading + 8 * math.sin(t / 20.0)) % 360
        cog_now = (heading_now + 12 * math.sin(t / 35.0 + 1.0)) % 360
        sog_now = max(0.0, args.sog + random.uniform(-0.1, 0.1))

        emit(129025, {"Latitude": round(lat, 6), "Longitude": round(lon, 6)})
        emit(129026, {"COG": round(cog_now, 1), "SOG": round(sog_now, 2)})
        emit(127250, {"Heading": round(heading_now, 1)})

        if tick % 5 == 0:  # matches n2k_mqtt_bridge.py's SLOW_INTERVAL_S cadence
            emit(129029, {
                "Latitude": round(lat, 6),
                "Longitude": round(lon, 6),
                "Altitude": 0.5,
                "GNSS type": "GPS+SBAS",
                "Method": "GNSS fix",
                "Number of SVs": random.randint(8, 12),
                "HDOP": round(random.uniform(0.6, 1.2), 2),
                "PDOP": round(random.uniform(1.0, 1.8), 2),
                "Geoidal Separation": -24.5,
            })

        tick += 1
        time.sleep(args.rate)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        sys.exit(0)

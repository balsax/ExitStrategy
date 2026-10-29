#!/usr/bin/env python3
"""
AIS traffic simulator for AIS-tab development.

Publishes fake nearby-vessel AIS targets to MQTT, shaped the way the real
Garmin AIS 800 will eventually look once it's wired into the boat's N2K
backbone (see n2k_mqtt_bridge.py (same repo)) and decoded via canboat. Lets the AIS
dashboard page be built and tested before that hardware is connected —
same idea as gps_simulator.py (same repo) for GPS.

── Topic schema ──────────────────────────────────────────────────────────
Unlike single-value nav topics (one boat = one lat/lon), AIS involves many
targets that appear and disappear as vessels come in and out of range. There
is no single "boat/ais/lat" — each target gets its own MMSI-keyed subtree:

    boat/ais/<mmsi>/lat          float, degrees
    boat/ais/<mmsi>/lon          float, degrees
    boat/ais/<mmsi>/sog          float, knots
    boat/ais/<mmsi>/cog          float, degrees true
    boat/ais/<mmsi>/heading      float, degrees true
    boat/ais/<mmsi>/nav_status   string  (e.g. "Under way using engine", "At anchor")
    boat/ais/<mmsi>/name         string  (static/voyage data — published less often)
    boat/ais/<mmsi>/type         string  (static/voyage data — published less often)
    boat/ais/<mmsi>/class        string  "A" or "B" (static)

This mirrors real AIS message structure (Class A position reports carry
MMSI/status/SOG/COG/heading/position; static & voyage data — name, type —
comes in a separate, much less frequent message) and matches how PGNs
129038/129039 (position) vs 129794/129809/129810 (static/voyage) split on
the wire. dashboard_api.py already timestamps every MQTT message it sees,
so per-target staleness (a vessel that's sailed out of range) is derived
from that existing timestamp rather than needing an explicit "target gone"
message — same pattern as the GPS staleness check already built for the
anchor watch. See /api/ais/targets in dashboard_api.py, which groups these
flat topics back into a per-vessel list and drops anything not heard from
recently.

MMSIs here are synthetic, in the 990000000+ range (outside any real vessel's
actual MMSI range) so simulated targets are never mistakable for real ones.

Usage:
  python3 ais_simulator.py
  python3 ais_simulator.py --num-targets 8 --radius-nm 3
  python3 ais_simulator.py --center-lat 27.7000 --center-lon -82.6900

Ctrl+C to stop.
"""
import argparse
import math
import random
import time
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "ais_simulator"

METERS_PER_DEG_LAT = 111_320.0
NM_TO_METERS = 1852.0

STATIC_REPUBLISH_S = 30  # name/type/class rarely change — publish far less often than position

VESSEL_NAMES = [
    "Sea Breeze", "Windward Bound", "Salty Dog", "Osprey", "Blue Horizon",
    "Second Wind", "Tradewinds", "Mystic Voyager", "Reel Time", "Southern Cross",
    "Serenity Now", "Knot At Work", "Pura Vida", "Aegean Spirit", "Wind Dancer",
    "Cape Runner", "Gulf Stream", "Manatee Express", "Night Heron", "Compass Rose",
]
VESSEL_TYPES = ["Sailing", "Pleasure Craft", "Fishing", "Tug", "Cargo", "Tanker", "Passenger"]


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def meters_to_latlon_offset(center_lat, dx_m, dy_m):
    """dx_m = east offset (m), dy_m = north offset (m) -> (dlat, dlon) in degrees."""
    dlat = dy_m / METERS_PER_DEG_LAT
    meters_per_deg_lon = METERS_PER_DEG_LAT * math.cos(math.radians(center_lat))
    dlon = dx_m / meters_per_deg_lon if meters_per_deg_lon > 0 else 0
    return dlat, dlon


class AisTarget:
    """One simulated vessel: dead-reckons forward from SOG/COG, with a slow
    random course drift (like a real vessel correcting/tacking) and a finite
    lifetime so it eventually 'sails out of range' and stops publishing —
    exercising the same staleness-expiry path a real disappearing contact would."""

    _next_mmsi = 990000001

    def __init__(self, center_lat, center_lon, radius_m):
        self.mmsi = AisTarget._next_mmsi
        AisTarget._next_mmsi += 1

        self.name = random.choice(VESSEL_NAMES)
        self.vessel_type = random.choice(VESSEL_TYPES)
        self.ais_class = random.choice(["A", "A", "B"])  # Class A (larger/commercial) more common in this mix

        # spawn at a random point within radius_m of the center
        angle = random.uniform(0, 2 * math.pi)
        dist = random.uniform(radius_m * 0.2, radius_m)
        dx = dist * math.cos(angle)
        dy = dist * math.sin(angle)
        dlat, dlon = meters_to_latlon_offset(center_lat, dx, dy)
        self.lat = center_lat + dlat
        self.lon = center_lon + dlon

        at_anchor = random.random() < 0.15
        self.nav_status = "At anchor" if at_anchor else "Under way using engine"
        self.sog_kn = 0.0 if at_anchor else random.uniform(3.0, 14.0)
        self.cog_deg = random.uniform(0, 360)
        self.cog_drift_rate = random.uniform(-2.0, 2.0)  # deg/s, slow heading correction
        self.heading_deg = self.cog_deg

        self.last_static_publish = 0.0
        self.expires_at = time.time() + random.uniform(5 * 60, 20 * 60)  # 5-20 min simulated on-scope time

    def expired(self):
        return time.time() > self.expires_at

    def step(self, dt):
        if self.sog_kn > 0:
            self.cog_deg = (self.cog_deg + self.cog_drift_rate * dt) % 360
            self.heading_deg = self.cog_deg
            dist_m = self.sog_kn * 0.514444 * dt  # knots -> m/s
            dx = dist_m * math.sin(math.radians(self.cog_deg))
            dy = dist_m * math.cos(math.radians(self.cog_deg))
            dlat, dlon = meters_to_latlon_offset(self.lat, dx, dy)
            self.lat += dlat
            self.lon += dlon

    def publish(self, client):
        base = f"boat/ais/{self.mmsi}"
        client.publish(f"{base}/lat", f"{self.lat:.6f}", retain=False)
        client.publish(f"{base}/lon", f"{self.lon:.6f}", retain=False)
        client.publish(f"{base}/sog", f"{self.sog_kn:.1f}", retain=False)
        client.publish(f"{base}/cog", f"{self.cog_deg:.1f}", retain=False)
        client.publish(f"{base}/heading", f"{self.heading_deg:.1f}", retain=False)
        client.publish(f"{base}/nav_status", self.nav_status, retain=False)

        now = time.time()
        if now - self.last_static_publish > STATIC_REPUBLISH_S:
            client.publish(f"{base}/name", self.name, retain=False)
            client.publish(f"{base}/type", self.vessel_type, retain=False)
            client.publish(f"{base}/class", self.ais_class, retain=False)
            self.last_static_publish = now


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--center-lat', type=float, default=27.7000, help='Center latitude to scatter targets around (default matches gps_simulator.py)')
    ap.add_argument('--center-lon', type=float, default=-82.6900, help='Center longitude')
    ap.add_argument('--num-targets', type=int, default=5, help='Number of simulated vessels to keep on scope at once (default 5)')
    ap.add_argument('--radius-nm', type=float, default=2.0, help='Spawn radius in nautical miles around the center (default 2.0)')
    ap.add_argument('--rate', type=float, default=3.0, help='Publish interval in seconds (default 3.0)')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    radius_m = args.radius_nm * NM_TO_METERS
    targets = [AisTarget(args.center_lat, args.center_lon, radius_m) for _ in range(args.num_targets)]

    print(f"Simulating {args.num_targets} AIS targets within {args.radius_nm}nm of "
          f"{args.center_lat:.4f}, {args.center_lon:.4f}")
    print("Publishing to boat/ais/<mmsi>/* — Ctrl+C to stop\n")

    last_t = time.time()
    try:
        while True:
            now = time.time()
            dt = now - last_t
            last_t = now

            for tgt in targets:
                tgt.step(dt)
                tgt.publish(client)

            expired = [t for t in targets if t.expired()]
            for t in expired:
                targets.remove(t)
                print(f"\n[{t.name} / MMSI {t.mmsi}] left range — no longer publishing")
                targets.append(AisTarget(args.center_lat, args.center_lon, radius_m))

            names = ", ".join(f"{t.name}({t.sog_kn:.0f}kn)" for t in targets)
            print(f"\r[{len(targets)} targets] {names}" + " " * 10, end='', flush=True)

            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
GPS simulator for dashboard development.

Publishes fake boat/nav/gps/* MQTT topics that look like they came from the
canboat NMEA2000 bridge (see n2k_mqtt_bridge.py (same repo) / ~/claude.md), so
dashboard pages that need a moving/anchored vessel can be built and tested
before the CAN bus is actually wired into the boat's N2K backbone.

Two scenarios, pick with --mode:

  underway (default) -- boat moves in a straight line at a steady SOG, out
  in open water (no anchor point, no swing). If autopilot_simulator.py (or
  real autopilot hardware) is Engaged, it follows boat/nav/autopilot/mode
  and .../heading_to_steer live and turns to match -- a real vessel's own
  turn rate is modeled (--turn-rate, capped deg/s, proportional easing into
  it rather than snapping), not an instant heading jump. Not engaged, it
  just holds its current heading, same as a real boat with the autopilot
  in Standby.

  anchor -- the original scenario this script started as: wanders around a
  fixed anchor point within a swing radius like a boat on rode does as
  wind/current shift, with small GPS jitter. Optionally, after a delay,
  switches into a steady outward "drag" so you can watch the drag alarm
  trigger in the dashboard.

Usage:
  python3 gps_simulator.py
  python3 gps_simulator.py --heading 090 --sog 6
  python3 gps_simulator.py --mode anchor --swing-radius 25 --drag-after 60 --drag-rate 0.3
  python3 gps_simulator.py --mode anchor --anchor-lat 27.7000 --anchor-lon -82.6900

Ctrl+C to stop.
"""
import argparse
import math
import random
import time
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "gps_simulator"

# Same topic names the real n2k_mqtt_bridge.py will eventually publish,
# so the frontend doesn't need to know or care whether the data is real.
TOPIC_LAT = "boat/nav/gps/latitude"
TOPIC_LON = "boat/nav/gps/longitude"
TOPIC_COG = "boat/nav/gps/cog"
TOPIC_SOG = "boat/nav/gps/sog"
TOPIC_ALT = "boat/nav/gps/altitude"
TOPIC_FIX_TYPE = "boat/nav/gps/fix_type"
TOPIC_SATS = "boat/nav/gps/satellites"
TOPIC_HDOP = "boat/nav/gps/hdop"
TOPIC_HEADING = "boat/nav/heading"
TOPIC_DEPTH = "boat/nav/depth"

# Read-only from this script's point of view -- autopilot_simulator.py (or
# real hardware once wired) is the source of truth for these.
TOPIC_AP_MODE = "boat/nav/autopilot/mode"
TOPIC_AP_HTS = "boat/nav/autopilot/heading_to_steer"

METERS_PER_DEG_LAT = 111_320.0


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def meters_to_latlon_offset(anchor_lat, dx_m, dy_m):
    """dx_m = east offset (m), dy_m = north offset (m) -> (dlat, dlon) in degrees."""
    dlat = dy_m / METERS_PER_DEG_LAT
    meters_per_deg_lon = METERS_PER_DEG_LAT * math.cos(math.radians(anchor_lat))
    dlon = dx_m / meters_per_deg_lon if meters_per_deg_lon > 0 else 0
    return dlat, dlon


class PassageSimulator:
    """Straight-line passage, optionally following a live autopilot heading."""

    def __init__(self, start_lat, start_lon, heading_deg, sog_kn, turn_rate_deg_s):
        self.lat = start_lat
        self.lon = start_lon
        self.heading = heading_deg % 360
        self.sog_kn = sog_kn
        self.turn_rate_deg_s = turn_rate_deg_s

        # Updated live from outside (MQTT callbacks in main()) -- this is
        # the only channel between the autopilot and this simulator, same
        # as a real boat: the autopilot steers the rudder, the GPS just
        # reports wherever that ends up pointing.
        self.ap_mode = None
        self.ap_hts = None

        self.last_t = time.time()

    def step(self):
        now = time.time()
        dt = now - self.last_t
        self.last_t = now

        if self.ap_mode == 'Engaged' and self.ap_hts is not None:
            # Proportional turn toward the commanded heading, capped at the
            # vessel's own turn rate -- fast when well off course, easing in
            # as it approaches rather than snapping straight to it. Not
            # engaged: heading just holds, same as a real boat with the
            # autopilot in Standby (nobody's turning the wheel).
            error = ((self.ap_hts - self.heading + 540) % 360) - 180
            max_turn = self.turn_rate_deg_s * dt
            turn = max(-max_turn, min(max_turn, error * 0.3))
            self.heading = (self.heading + turn) % 360

        sog_mps = self.sog_kn * 0.514444
        dist_m = sog_mps * dt
        heading_rad = math.radians(self.heading)
        dx = dist_m * math.sin(heading_rad)
        dy = dist_m * math.cos(heading_rad)
        dlat, dlon = meters_to_latlon_offset(self.lat, dx, dy)
        self.lat += dlat
        self.lon += dlon

        # Reported position gets small GPS jitter, +/- ~1.5m, same as real
        # consumer/marine fix noise -- the true (self.lat/self.lon) position
        # driving the next tick stays clean, only what's published wobbles.
        jdx, jdy = random.uniform(-1.5, 1.5), random.uniform(-1.5, 1.5)
        jdlat, jdlon = meters_to_latlon_offset(self.lat, jdx, jdy)

        return {
            'lat': self.lat + jdlat,
            'lon': self.lon + jdlon,
            'sog_kn': self.sog_kn,
            'cog_deg': self.heading,
            'heading_deg': self.heading,
            'following_ap': self.ap_mode == 'Engaged' and self.ap_hts is not None,
        }


class AnchorSimulator:
    def __init__(self, anchor_lat, anchor_lon, swing_radius_m, drag_after_s, drag_rate_mps):
        self.anchor_lat = anchor_lat
        self.anchor_lon = anchor_lon
        self.swing_radius_m = swing_radius_m
        self.drag_after_s = drag_after_s
        self.drag_rate_mps = drag_rate_mps

        self.start_time = time.time()
        self.swing_angle = random.uniform(0, 2 * math.pi)
        self.swing_angle_rate = random.uniform(0.02, 0.05)  # rad/s, slow wind-driven swing
        self.dragging = False
        self.drag_distance_m = 0.0
        self.drag_heading_rad = 0.0  # set for real when dragging actually begins, see step()
        self.last_clean_dx = 0.0
        self.last_clean_dy = 0.0
        self.last_t = time.time()
        # SOG/COG are derived from a position delta / dt, which is meaningless on the
        # very first tick (near-zero elapsed time since __init__). Skip that derived
        # calculation once rather than trying to pre-compute a matching starting delta.
        self._first_step = True

    def step(self):
        now = time.time()
        elapsed = now - self.start_time
        dt = now - self.last_t
        self.last_t = now

        if self.drag_after_s is not None and elapsed >= self.drag_after_s:
            if not self.dragging:
                # Continue from wherever the swing was heading, rather than jumping
                # to an independently-random direction — avoids a one-tick artificial
                # speed spike in the derived SOG at the moment dragging begins.
                self.drag_heading_rad = self.swing_angle
            self.dragging = True

        if self.dragging:
            self.drag_distance_m += self.drag_rate_mps * dt
            radius = self.swing_radius_m + self.drag_distance_m
            angle = self.drag_heading_rad
        else:
            self.swing_angle += self.swing_angle_rate * dt
            # radius wanders between ~55% and ~95% of max swing radius, like a boat
            # tacking back and forth on its rode rather than sitting at a fixed point
            radius = self.swing_radius_m * (0.75 + 0.20 * math.sin(elapsed * 0.15))
            angle = self.swing_angle

        dx = radius * math.cos(angle)
        dy = radius * math.sin(angle)

        # Derive COG/SOG from the *clean* (non-jittered) commanded position, the way
        # real hardware does (GPS receivers compute SOG from Doppler velocity, not by
        # differentiating noisy position fixes) — this keeps speed/heading realistic
        # and stable instead of amplifying position jitter into apparent knots.
        if self._first_step:
            sog_mps = 0.0
            cog_deg = None
            self._first_step = False
        else:
            d_dist_m = math.hypot(dx - self.last_clean_dx, dy - self.last_clean_dy)
            sog_mps = d_dist_m / dt if dt > 0 else 0.0
            cog_deg = (math.degrees(math.atan2(dx - self.last_clean_dx, dy - self.last_clean_dy)) + 360) % 360 \
                if d_dist_m > 0.05 else None
        self.last_clean_dx, self.last_clean_dy = dx, dy

        # small GPS jitter, +/- ~1.5m, applied only to the *reported* position —
        # like real consumer/marine GPS fix noise, independent of the SOG/COG above
        dx += random.uniform(-1.5, 1.5)
        dy += random.uniform(-1.5, 1.5)

        dlat, dlon = meters_to_latlon_offset(self.anchor_lat, dx, dy)
        lat = self.anchor_lat + dlat
        lon = self.anchor_lon + dlon

        return {
            'lat': lat, 'lon': lon,
            'sog_kn': sog_mps * 1.94384,
            'cog_deg': cog_deg,
            'heading_deg': cog_deg,
            'radius_m': radius,
            'dragging': self.dragging,
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--mode', choices=['underway', 'anchor'], default='underway', help='Scenario to simulate (default: underway)')

    # underway mode
    ap.add_argument('--start-lat', type=float, default=27.7000, help='[underway] Starting latitude (default: Tampa Bay area)')
    ap.add_argument('--start-lon', type=float, default=-82.6900, help='[underway] Starting longitude')
    ap.add_argument('--heading', type=float, default=90.0, help='[underway] Starting heading, degrees true (default 090)')
    ap.add_argument('--sog', type=float, default=6.0, help='[underway] Speed over ground, knots (default 6)')
    ap.add_argument('--turn-rate', type=float, default=3.0, help="[underway] Vessel's own max turn rate when following the autopilot, deg/s (default 3)")

    # anchor mode
    ap.add_argument('--anchor-lat', type=float, default=27.7000, help='[anchor] Anchor latitude (default: Tampa Bay area)')
    ap.add_argument('--anchor-lon', type=float, default=-82.6900, help='[anchor] Anchor longitude')
    ap.add_argument('--swing-radius', type=float, default=25.0, help='[anchor] Normal swing radius in meters (default 25m ~ 82ft)')
    ap.add_argument('--drag-after', type=float, default=None, help='[anchor] Seconds after start to begin simulated dragging (default: never drag)')
    ap.add_argument('--drag-rate', type=float, default=0.3, help='[anchor] Drag speed in m/s once dragging starts (default 0.3 m/s ~ 0.6kn)')

    ap.add_argument('--rate', type=float, default=1.0, help='Publish interval in seconds (default 1.0, matches real GPS PGN rate)')
    ap.add_argument('--depth-ft', type=float, default=12.0, help='Simulated water depth in feet, +/- small jitter (default 12ft, above the default 6ft alarm)')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])

    if args.mode == 'underway':
        sim = PassageSimulator(args.start_lat, args.start_lon, args.heading, args.sog, args.turn_rate)

        def on_connect(c, userdata, flags, reason_code, properties):
            c.subscribe(TOPIC_AP_MODE)
            c.subscribe(TOPIC_AP_HTS)

        def on_message(c, userdata, msg):
            payload = msg.payload.decode('utf-8', errors='ignore').strip()
            if msg.topic == TOPIC_AP_MODE:
                sim.ap_mode = payload
            elif msg.topic == TOPIC_AP_HTS:
                try:
                    sim.ap_hts = float(payload)
                except ValueError:
                    pass

        client.on_connect = on_connect
        client.on_message = on_message
    else:
        sim = AnchorSimulator(args.anchor_lat, args.anchor_lon, args.swing_radius, args.drag_after, args.drag_rate)

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    if args.mode == 'underway':
        print(f"Simulating underway at {args.start_lat:.6f}, {args.start_lon:.6f}, "
              f"heading {args.heading:.0f}° at {args.sog:.1f}kn, turn rate {args.turn_rate:.1f}deg/s")
        print(f"Following {TOPIC_AP_MODE}/{TOPIC_AP_HTS} live when Engaged, otherwise holding current heading")
    else:
        print(f"Simulating anchor at {args.anchor_lat:.6f}, {args.anchor_lon:.6f}, "
              f"swing radius {args.swing_radius}m" +
              (f", dragging starts at t+{args.drag_after:.0f}s (rate {args.drag_rate} m/s)" if args.drag_after else ", no drag scheduled"))
    print("Publishing to boat/nav/gps/* — Ctrl+C to stop\n")

    tick = 0
    try:
        while True:
            s = sim.step()

            client.publish(TOPIC_LAT, f"{s['lat']:.6f}", retain=False)
            client.publish(TOPIC_LON, f"{s['lon']:.6f}", retain=False)
            client.publish(TOPIC_SOG, f"{s['sog_kn']:.2f}", retain=False)
            if s['cog_deg'] is not None:
                client.publish(TOPIC_COG, f"{s['cog_deg']:.1f}", retain=False)
            if s['heading_deg'] is not None:
                client.publish(TOPIC_HEADING, f"{s['heading_deg']:.1f}", retain=False)

            depth_m = args.depth_ft * 0.3048 + random.uniform(-0.05, 0.05)
            client.publish(TOPIC_DEPTH, f"{depth_m:.2f}", retain=False)

            # slower "fix quality" telemetry, every ~5s like the real bridge script
            if tick % 5 == 0:
                client.publish(TOPIC_ALT, "1.2", retain=False)
                client.publish(TOPIC_FIX_TYPE, "GNSS fix", retain=False)
                client.publish(TOPIC_SATS, str(random.randint(8, 12)), retain=False)
                client.publish(TOPIC_HDOP, f"{random.uniform(0.7, 1.3):.2f}", retain=False)

            if args.mode == 'underway':
                status = "AP" if s['following_ap'] else "manual"
                print(f"\r[{status:<6}] lat={s['lat']:.6f} lon={s['lon']:.6f} "
                      f"hdg={s['heading_deg']:.1f}° sog={s['sog_kn']:.2f}kn depth={depth_m:.2f}m   ", end='', flush=True)
            else:
                status = "DRAGGING" if s['dragging'] else "swinging"
                print(f"\r[{status}] lat={s['lat']:.6f} lon={s['lon']:.6f} "
                      f"radius={s['radius_m']:.1f}m sog={s['sog_kn']:.2f}kn depth={depth_m:.2f}m   ", end='', flush=True)

            tick += 1
            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Autopilot simulator for dashboard development.

Publishes fake boat/nav/autopilot/* MQTT topics -- the same prospective
topic names n2k_mqtt_bridge.py's PGN_MAP would publish once a Garmin
GHP 12 / GHC 10 (PGN 126720) is actually wired to the N2K backbone -- so
the Chart page's "My Vessel & Autopilot" panel can be exercised before
that hardware exists. Also drives boat/nav/destination/* (PGN 129284)
once Engaged, since the Destination & ETA panel only shows itself while
autopilot mode reads Engaged -- this is the only way to test that link
without a chartplotter on the bus either.

Simulates Standby -> (optional) Shadow Drive -> Engaged, with small rudder
jitter like a real autopilot correcting for wind/wave. Once Engaged: if a
destination is set (the default; see --no-destination), this is "Go To"
behavior -- it continuously steers toward the destination's live bearing
from gps_simulator.py's actual reported position (subscribes to
boat/nav/gps/latitude|longitude), same as a real chartplotter/autopilot
integration, with distance/bearing/VMG/ETA all derived from that real
position rather than assumed. No destination: plain heading-hold, driven
by the target heading and the course-change (+/-1/+/-10) commands.

Runs the timed schedule below by default, but also accepts live commands
at any point, from either stdin (type one and press Enter) or MQTT on the
matching boat/nav/autopilot/cmd/* topic (matches dashboard_api.py's
/api/autopilot/* endpoints, so the Chart page's buttons/pin drag drive
this too):
  engage | standby | shadow | quit  (quit is stdin-only)
  a signed number, e.g. -10 or 1    (heading delta, the course-change buttons)
  dest:lat,lon                      (new destination, dragging/clicking the pin)
The first manual command takes over from the timer permanently for that
run (so you can just let it auto-engage and watch, or drive it by hand
from a terminal or the dashboard).

Usage:
  python3 autopilot_simulator.py
  python3 autopilot_simulator.py --engage-after 5 --target-heading 090
  python3 autopilot_simulator.py --shadow-drive-for 10
  python3 autopilot_simulator.py --no-destination

Ctrl+C (or 'quit') to stop.
"""
import argparse
import math
import queue
import random
import threading
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "autopilot_simulator"

TOPIC_MODE = "boat/nav/autopilot/mode"
TOPIC_HTS = "boat/nav/autopilot/heading_to_steer"
TOPIC_RUDDER = "boat/nav/rudder"
# Matches dashboard_api.py's /api/autopilot/control -- lets the Chart page's
# Engage/Standby buttons drive this simulator the same way they'll eventually
# drive real hardware, without needing a terminal open. Payload vocabulary
# (engage/standby/shadow) matches what's typed on stdin.
CMD_TOPIC_MODE = "boat/nav/autopilot/cmd/mode"
# Matches dashboard_api.py's /api/autopilot/course_change -- payload is a
# signed degree delta (e.g. "-10", "1") applied to the held target heading.
CMD_TOPIC_ADJUST = "boat/nav/autopilot/cmd/adjust_heading"
# Matches dashboard_api.py's /api/autopilot/set_destination -- payload is
# "lat,lon", from either dragging the destination pin or clicking the chart
# in Set Destination mode.
CMD_TOPIC_SET_DEST = "boat/nav/autopilot/cmd/set_destination"

TOPIC_DEST_LAT = "boat/nav/destination/latitude"
TOPIC_DEST_LON = "boat/nav/destination/longitude"
TOPIC_DEST_DIST = "boat/nav/destination/distance"
TOPIC_DEST_BEARING = "boat/nav/destination/bearing"
TOPIC_DEST_VMG = "boat/nav/destination/vmg"
TOPIC_DEST_ETA_TIME = "boat/nav/destination/eta_time"
TOPIC_DEST_ETA_DATE = "boat/nav/destination/eta_date"

# Read-only from this script's point of view -- gps_simulator.py (or real GPS
# hardware once wired) is the source of truth for actual position. Needed to
# steer toward a destination for real: without it, "Engaged + a destination"
# could only ever hold whatever bearing was true back when the destination
# was first set, not track it as the boat actually moves.
TOPIC_GPS_LAT = "boat/nav/gps/latitude"
TOPIC_GPS_LON = "boat/nav/gps/longitude"
# SOG/COG, also read-only from here -- used for VMG, see the comment where
# vmg_kn is computed below for why (gps_simulator.py derives these from its
# clean internal position, unlike the jittered lat/lon above).
TOPIC_GPS_SOG = "boat/nav/gps/sog"
TOPIC_GPS_COG = "boat/nav/gps/cog"

METERS_PER_DEG_LAT = 111_320.0
NM_TO_M = 1852.0


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


def distance_bearing(lat1, lon1, lat2, lon2):
    """Inverse of meters_to_latlon_offset -- flat-earth approximation, same as
    the dashboard's own aisDistanceBearing() JS helper, consistent for the
    small distances a coastal/inland sim scenario covers."""
    meters_per_deg_lon = METERS_PER_DEG_LAT * math.cos(math.radians(lat1))
    dx = (lon2 - lon1) * meters_per_deg_lon
    dy = (lat2 - lat1) * METERS_PER_DEG_LAT
    dist_m = math.hypot(dx, dy)
    bearing = (math.degrees(math.atan2(dx, dy)) + 360) % 360
    return dist_m, bearing


def input_reader(cmd_queue):
    """Runs on a daemon thread -- blocking input() would otherwise stall the
    publish loop between lines typed."""
    while True:
        try:
            line = input()
        except EOFError:
            break
        cmd_queue.put(line.strip().lower())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--start-lat', type=float, default=27.7000, help='Starting latitude (default: Tampa Bay area, matches gps_simulator.py default)')
    ap.add_argument('--start-lon', type=float, default=-82.6900, help='Starting longitude')
    ap.add_argument('--target-heading', type=float, default=90.0, help='Heading to steer once engaged, degrees true (default 090)')
    ap.add_argument('--engage-after', type=float, default=8.0, help='Seconds in Standby before Shadow Drive/Engaged begins (default 8)')
    ap.add_argument('--shadow-drive-for', type=float, default=0.0, help='Seconds to hold Shadow Drive before Engaged (default 0, skip straight to Engaged)')
    ap.add_argument('--dest-distance-nm', type=float, default=5.0, help='Initial distance to the simulated destination waypoint once engaged, nm (default 5)')
    ap.add_argument('--no-destination', action='store_true', help="Don't simulate boat/nav/destination/* -- autopilot-only")
    ap.add_argument('--rate', type=float, default=1.0, help='Publish interval in seconds (default 1.0)')
    args = ap.parse_args()

    cmd_queue = queue.Queue()  # created before the MQTT client so on_message below can feed it too
    live_pos = {'lat': args.start_lat, 'lon': args.start_lon, 'sog': 0.0, 'cog': None}  # updated from gps_simulator.py; see on_message

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])

    def on_connect(c, userdata, flags, reason_code, properties):
        c.subscribe(CMD_TOPIC_MODE)
        c.subscribe(CMD_TOPIC_ADJUST)
        c.subscribe(CMD_TOPIC_SET_DEST)
        c.subscribe(TOPIC_GPS_LAT)
        c.subscribe(TOPIC_GPS_LON)
        c.subscribe(TOPIC_GPS_SOG)
        c.subscribe(TOPIC_GPS_COG)

    def on_message(c, userdata, msg):
        # GPS position/SOG/COG update live_pos directly -- frequent (1Hz)
        # plain floats, no reason to route them through cmd_queue's command
        # parsing like the mode/adjust/dest topics below.
        if msg.topic in (TOPIC_GPS_LAT, TOPIC_GPS_LON, TOPIC_GPS_SOG, TOPIC_GPS_COG):
            try:
                value = float(msg.payload.decode('utf-8', errors='ignore'))
            except ValueError:
                return
            key = {TOPIC_GPS_LAT: 'lat', TOPIC_GPS_LON: 'lon', TOPIC_GPS_SOG: 'sog', TOPIC_GPS_COG: 'cog'}[msg.topic]
            live_pos[key] = value
            return

        payload = msg.payload.decode('utf-8', errors='ignore').strip().lower()
        # Tagged by topic rather than sniffing payload shape -- set_destination's
        # "lat,lon" payload is otherwise indistinguishable from other formats.
        # (Also typeable at stdin as "dest:lat,lon" for the same effect.)
        if msg.topic == CMD_TOPIC_SET_DEST:
            payload = f"dest:{payload}"
        cmd_queue.put(payload)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    # The destination waypoint is fixed once at start, straight ahead on the
    # target heading -- a real waypoint doesn't move, only distance/ETA to it
    # do. Keeps this a straightforward "on track" scenario rather than also
    # simulating cross-track error.
    dest_distance_m = args.dest_distance_nm * NM_TO_M
    dlat, dlon = meters_to_latlon_offset(
        args.start_lat,
        dest_distance_m * math.sin(math.radians(args.target_heading)),
        dest_distance_m * math.cos(math.radians(args.target_heading)),
    )
    dest_lat, dest_lon = args.start_lat + dlat, args.start_lon + dlon
    # Bearing/distance are recomputed live every Engaged tick below from
    # gps_simulator.py's actual reported position (live_pos) once a
    # destination exists -- this initial value is just what's true before
    # the first tick runs.
    dest_bearing = args.target_heading

    print(f"Simulating autopilot: Standby for {args.engage_after:.0f}s" +
          (f", then Shadow Drive for {args.shadow_drive_for:.0f}s" if args.shadow_drive_for > 0 else "") +
          f", then Engaged holding {args.target_heading:.0f}°T" +
          ("" if args.no_destination else f", destination {args.dest_distance_nm:.1f}nm ahead"))
    print(f"Publishing to {TOPIC_MODE} etc., listening on {CMD_TOPIC_MODE} and {CMD_TOPIC_ADJUST}")
    print("Type a command + Enter to take over manually: engage | standby | shadow | quit")
    print("Or a signed number (e.g. -10, 1) to adjust the held heading by that many degrees.")
    print("(same commands also accepted from the dashboard's buttons, or otherwise the")
    print(" timed schedule above runs on its own) -- Ctrl+C also stops\n")

    threading.Thread(target=input_reader, args=(cmd_queue,), daemon=True).start()
    manual_mode = None  # once set by a command (stdin or MQTT), overrides the timed schedule for good
    target_heading = args.target_heading  # mutable now -- course-change commands adjust this directly

    start_time = time.time()
    heading_now = target_heading  # drifts toward target_heading once engaged, like a real AP correcting

    try:
        while True:
            while not cmd_queue.empty():
                cmd = cmd_queue.get_nowait()
                if not cmd:
                    continue
                if cmd.startswith('dest:'):
                    try:
                        lat_s, lon_s = cmd[5:].split(',')
                        dest_lat, dest_lon = float(lat_s), float(lon_s)
                        dest_distance_m, dest_bearing = distance_bearing(live_pos['lat'], live_pos['lon'], dest_lat, dest_lon)
                        print(f"\nNew destination: {dest_lat:.6f}, {dest_lon:.6f} "
                              f"({dest_distance_m / NM_TO_M:.2f}nm @ {dest_bearing:.0f}° from current position)")
                    except (ValueError, IndexError):
                        print(f"\nBad destination payload: '{cmd}' -- expected dest:lat,lon")
                    continue
                try:
                    # A signed number is a heading-delta command (the course-change
                    # buttons), not a mode word -- tried first since "engage" etc.
                    # never parse as floats, so there's no ambiguity either way.
                    target_heading = (target_heading + float(cmd)) % 360
                    continue
                except ValueError:
                    pass
                if cmd in ('e', 'engage'):
                    manual_mode = 'Engaged'
                elif cmd in ('sh', 'shadow'):
                    manual_mode = 'Shadow Drive'
                elif cmd in ('s', 'standby'):
                    manual_mode = 'Standby'
                elif cmd in ('q', 'quit'):
                    raise KeyboardInterrupt
                else:
                    print(f"\nUnrecognized command '{cmd}' -- try: engage | standby | shadow | quit | a heading delta like -10")

            now = time.time()
            elapsed = now - start_time

            if manual_mode is not None:
                mode = manual_mode
            elif elapsed < args.engage_after:
                mode = 'Standby'
            elif elapsed < args.engage_after + args.shadow_drive_for:
                mode = 'Shadow Drive'
            else:
                mode = 'Engaged'

            if mode == 'Engaged':
                if not args.no_destination:
                    # "Go To" behavior: continuously steer toward the destination's
                    # live bearing from the boat's actual reported position
                    # (live_pos, from gps_simulator.py), not a fixed heading set
                    # once at the start. This overrides any pending course-change
                    # command while a destination is active -- same as a real
                    # chartplotter/autopilot integration, where Nav mode drives
                    # the heading, not the +/-1/+/-10 buttons.
                    dest_distance_m, dest_bearing = distance_bearing(live_pos['lat'], live_pos['lon'], dest_lat, dest_lon)
                    target_heading = dest_bearing

                # Small heading-hold jitter/correction around the target --
                # rudder is derived from the correction, like a real autopilot
                # nudging against wind/wave rather than holding a dead-flat lock.
                heading_now += random.uniform(-0.3, 0.3)
                error = ((target_heading - heading_now + 540) % 360) - 180
                heading_now += error * 0.1
                rudder = max(-15.0, min(15.0, error * 0.8 + random.uniform(-1, 1)))
                hts = target_heading
            else:
                rudder, hts = 0.0, None

            client.publish(TOPIC_MODE, mode, retain=False)
            if hts is not None:
                client.publish(TOPIC_HTS, f"{hts:.1f}", retain=False)
            client.publish(TOPIC_RUDDER, f"{rudder:.1f}", retain=False)

            dest_status = ""
            if not args.no_destination and mode == 'Engaged':
                dist_nm = dest_distance_m / NM_TO_M
                # VMG to the destination = the component of the boat's actual
                # speed/course-over-ground that points along the bearing to
                # the destination -- the standard "VMG to waypoint" formula,
                # using gps_simulator.py's SOG/COG (which it derives from its
                # clean internal position -- see its own comment on that).
                # This replaced an earlier version that differenced
                # dest_distance_m -- computed from live_pos, the REPORTED
                # position, which gps_simulator.py deliberately jitters by
                # +/-1.5m to mimic real GPS fix noise -- over a single ~1s
                # tick. Differentiating jittered position over a short window
                # amplifies that noise directly into speed: two consecutive
                # +/-1.5m fixes can differ by up to 3m, i.e. +/-~5.8kn of
                # pure jitter on a single tick, on top of the real VMG --
                # easily enough on its own to swing an apparent 1.5kn down to
                # under 0 or up past 10kn between ticks. SOG/COG don't have
                # that problem since gps_simulator.py already derives THEM
                # from the clean, unjittered position for the same reason.
                cog = live_pos['cog'] if live_pos['cog'] is not None else target_heading
                vmg_kn = max(0.0, live_pos['sog'] * math.cos(math.radians(dest_bearing - cog)))
                eta_s = (dist_nm / vmg_kn) * 3600 if vmg_kn > 0.05 else 0
                eta_dt = datetime.now() + timedelta(seconds=eta_s)

                client.publish(TOPIC_DEST_LAT, f"{dest_lat:.6f}", retain=False)
                client.publish(TOPIC_DEST_LON, f"{dest_lon:.6f}", retain=False)
                client.publish(TOPIC_DEST_DIST, f"{dist_nm:.2f}", retain=False)
                client.publish(TOPIC_DEST_BEARING, f"{dest_bearing:.1f}", retain=False)
                client.publish(TOPIC_DEST_VMG, f"{vmg_kn:.1f}", retain=False)
                client.publish(TOPIC_DEST_ETA_TIME, eta_dt.strftime('%H:%M:%S'), retain=False)
                client.publish(TOPIC_DEST_ETA_DATE, eta_dt.strftime('%Y-%m-%d'), retain=False)
                dest_status = f" dest={dist_nm:.2f}nm brg={dest_bearing:.0f}° vmg={vmg_kn:.1f}kn"

            print(f"\r[{mode:<12}] hdg={heading_now:.1f}° hts={target_heading if mode=='Engaged' else 0:.0f}° "
                  f"rudder={rudder:+.1f}°{dest_status}   ", end='', flush=True)

            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

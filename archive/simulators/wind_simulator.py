#!/usr/bin/env python3
"""
Wind simulator for dashboard development.

Publishes fake boat/nav/wind/* MQTT topics (PGN 130306, Wind Data) --
apparent wind speed/angle plus the Reference field -- the same schema
n2k_mqtt_bridge.py's PGN_MAP already expects, so the Weather tab's wind
instrument (overhead boat + apparent/true arrows) can be exercised before
a real transducer is wired.

Simulates a true wind that wanders slowly in speed and direction (like
real weather, not flat/constant) but stays generally from one direction --
--true-wind-dir is a "general direction" the wind shifts around within
+/-90°, not a starting point for an unbounded drift -- then derives the
APPARENT wind felt by a moving boat from that true wind plus the boat's
own speed/heading, the exact inverse of the trueWindFromApparent()
calculation already built into the dashboard's own frontend, so round-
tripping through it should recover something close to the simulated true
wind.

Boat speed/heading: subscribes to boat/nav/gps/sog and boat/nav/heading
(the same topics gps_simulator.py publishes) and uses whatever's live
there, falling back to --boat-speed/--boat-heading until something's
been seen -- so this can either ride along with gps_simulator.py (or
real GPS/heading hardware) for a fully consistent scenario, or run
standalone with its own fixed boat motion.

On top of that slow drift, a GUST fires every couple of minutes --
true wind speed ramps smoothly up to 1.5x whatever it happened to be
when the gust started, holds briefly, then eases back down, direction
untouched -- then a new gust is scheduled a few minutes out. See
--gust-* flags to tune or disable.

Usage:
  python3 wind_simulator.py
  python3 wind_simulator.py --true-wind-speed 18 --true-wind-dir 045
  python3 wind_simulator.py --boat-speed 6 --boat-heading 090   # no GPS source running

Ctrl+C to stop.
"""
import argparse
import math
import random
import time

import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "wind_simulator"

TOPIC_SPEED = "boat/nav/wind/speed"
TOPIC_ANGLE = "boat/nav/wind/angle"
TOPIC_REFERENCE = "boat/nav/wind/reference"

TOPIC_BOAT_SOG = "boat/nav/gps/sog"
TOPIC_BOAT_HEADING = "boat/nav/heading"


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def apparent_from_true(tws, twd, boat_speed, boat_heading):
    """Inverse of the dashboard's own trueWindFromApparent() JS helper --
    true wind (speed, compass direction it's blowing FROM) plus the boat's
    own speed/heading -> apparent wind (speed, angle from the bow, 0-360
    clockwise -- same PGN 130306 convention the frontend already expects)."""
    twa_boat_rel = math.radians((twd - boat_heading) % 360)
    x = tws * math.cos(twa_boat_rel) + boat_speed
    y = tws * math.sin(twa_boat_rel)
    aws = math.hypot(x, y)
    awa = math.degrees(math.atan2(y, x)) % 360
    return aws, awa


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--true-wind-speed', type=float, default=12.0, help='Base true wind speed, knots (default 12)')
    ap.add_argument('--true-wind-dir', type=float, default=115.0, help='General true wind direction, compass bearing it blows FROM -- wanders within +/-wind-dir-spread of this, does not drift past it (default 115)')
    ap.add_argument('--wind-dir-spread', type=float, default=45.0, help='How far either side of --true-wind-dir the direction can wander, degrees (default 45, i.e. a 70-160 range at the default center)')
    ap.add_argument('--boat-speed', type=float, default=5.0, help='Boat speed to use until boat/nav/gps/sog is seen live (knots, default 5)')
    ap.add_argument('--boat-heading', type=float, default=0.0, help='Boat heading to use until boat/nav/heading is seen live (degrees, default 0)')
    ap.add_argument('--rate', type=float, default=0.3, help='Publish interval in seconds (default 0.3 -- frequent, small-step updates)')
    ap.add_argument('--gust-multiplier', type=float, default=1.5, help='Peak gust speed as a multiple of the true wind speed at gust onset (default 1.5, i.e. 50%% over)')
    ap.add_argument('--gust-interval-min', type=float, default=90.0, help='Shortest gap between gusts, seconds (default 90)')
    ap.add_argument('--gust-interval-max', type=float, default=240.0, help='Longest gap between gusts, seconds (default 240, so gusts land every couple-few minutes on average)')
    ap.add_argument('--gust-duration-min', type=float, default=8.0, help='Shortest gust length, seconds (default 8)')
    ap.add_argument('--gust-duration-max', type=float, default=18.0, help='Longest gust length, seconds (default 18)')
    ap.add_argument('--no-gusts', action='store_true', help='Disable gusts entirely')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])

    live = {'sog': args.boat_speed, 'heading': args.boat_heading}

    def on_connect(c, userdata, flags, reason_code, properties):
        c.subscribe(TOPIC_BOAT_SOG)
        c.subscribe(TOPIC_BOAT_HEADING)

    def on_message(c, userdata, msg):
        try:
            value = float(msg.payload.decode('utf-8', errors='ignore'))
        except ValueError:
            return
        if msg.topic == TOPIC_BOAT_SOG:
            live['sog'] = value
        elif msg.topic == TOPIC_BOAT_HEADING:
            live['heading'] = value

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    tws = args.true_wind_speed
    twd_base = args.true_wind_dir  # the "general direction" -- twd wanders around this, doesn't drift past it
    twd_offset = 0.0
    tws_velocity = 0.0  # kn/tick -- smoothed, not the raw random draw itself, see below
    twd_velocity = 0.0  # deg/tick -- same

    # Gust scheduling -- next_gust_at/gust_active track a single upcoming or
    # in-progress gust; wall-clock time.time() is used (not tick count) so
    # gust cadence stays correct in real seconds regardless of --rate.
    gust_active = False
    gust_start_time = 0.0
    gust_duration = 0.0
    gust_peak_bonus = 0.0  # kn -- fixed at gust onset from that moment's tws, not re-derived mid-gust
    next_gust_at = None if args.no_gusts else time.time() + random.uniform(args.gust_interval_min, args.gust_interval_max)

    print(f"Simulating true wind {tws:.0f}kn generally from {twd_base:.0f}° "
          f"(wandering within +/-{args.wind_dir_spread:.0f}°, i.e. {(twd_base - args.wind_dir_spread) % 360:.0f}-{(twd_base + args.wind_dir_spread) % 360:.0f}°), "
          f"boat speed/heading from live GPS topics if present, else {args.boat_speed:.1f}kn/{args.boat_heading:.0f}° until then")
    print(f"Publishing to {TOPIC_SPEED} etc. -- Ctrl+C to stop\n")

    tick = 0
    try:
        while True:
            # True wind wanders slowly and smoothly, like real weather --
            # random noise is applied to VELOCITY and low-pass filtered
            # (each tick keeps 97.5% of its previous velocity, blends in
            # only 2.5% of a fresh random nudge) before being integrated
            # into position, rather than adding raw random noise straight
            # to speed/direction each tick. That's what actually reads as
            # smooth continuous drift instead of jittering back and forth
            # tick-to-tick -- inertia, not just a smaller random range.
            # Retain factor and noise magnitude are both tuned for the
            # default --rate=0.3s: ticking ~3.3x more often than the
            # original 1s design needs a correspondingly higher retain
            # (0.92**0.3 ~ 0.975) and smaller per-tick noise to land on the
            # same real-time drift speed, just sampled more finely.
            tws_velocity = tws_velocity * 0.975 + random.uniform(-0.025, 0.025) * 0.08
            tws += tws_velocity
            tws = max(2.0, min(35.0, tws))

            # More energetic than tws's wander on purpose -- direction should
            # actually roam across most of the +/-wind-dir-spread range over
            # a several-minute span (weak reversion pull), not just huddle
            # near twd_base. Lower retain than tws's too (0.96 vs 0.975) --
            # less inertia, so it responds to a fresh nudge faster. Noise
            # magnitude doubled twice more from the original +/-0.8 (now
            # +/-3.2) -- ~1.5deg/tick max, enough to reach both ends of the
            # spread rather than just wander the middle.
            twd_velocity = twd_velocity * 0.96 + random.uniform(-3.2, 3.2) * 0.08
            twd_offset += twd_velocity
            twd_offset -= twd_offset * 0.001  # weak pull back toward twd_base -- bounded by the hard clamp below, not this
            twd_offset = max(-args.wind_dir_spread, min(args.wind_dir_spread, twd_offset))
            twd = (twd_base + twd_offset) % 360

            # Gusts: a temporary boost on top of tws, direction untouched.
            # Speed rises and falls over the gust smoothly (a single sine
            # hump across gust_duration -- 0 at both ends, full strength at
            # the midpoint) rather than snapping to the peak, so it reads as
            # a puff of wind rather than a step change. gust_peak_bonus is
            # fixed at onset (args.gust_multiplier x whatever tws was right
            # then) so the gust doesn't chase tws's own slow drift mid-gust.
            now = time.time()
            gust_bonus = 0.0
            if not args.no_gusts:
                if not gust_active and next_gust_at is not None and now >= next_gust_at:
                    gust_active = True
                    gust_start_time = now
                    gust_duration = random.uniform(args.gust_duration_min, args.gust_duration_max)
                    gust_peak_bonus = tws * (args.gust_multiplier - 1.0)
                if gust_active:
                    elapsed = now - gust_start_time
                    if elapsed >= gust_duration:
                        gust_active = False
                        next_gust_at = now + random.uniform(args.gust_interval_min, args.gust_interval_max)
                    else:
                        gust_bonus = gust_peak_bonus * math.sin(math.pi * elapsed / gust_duration)

            tws_gusted = max(0.0, tws + gust_bonus)
            aws, awa = apparent_from_true(tws_gusted, twd, live['sog'], live['heading'])
            # A little sensor-like jitter on top of the smooth underlying
            # drift, applied only to what's published (not fed back into
            # twd/tws state) -- a real transducer doesn't read perfectly
            # steady even when the wind itself genuinely isn't shifting.
            aws_out = max(0.0, aws + random.uniform(-0.2, 0.2))
            awa_out = (awa + random.uniform(-1.5, 1.5)) % 360

            client.publish(TOPIC_SPEED, f"{aws_out:.2f}", retain=False)
            client.publish(TOPIC_ANGLE, f"{awa_out:.1f}", retain=False)
            if tick % 5 == 0:
                client.publish(TOPIC_REFERENCE, "Apparent", retain=False)

            gust_tag = f" GUST +{gust_bonus:.1f}kn" if gust_active else ""
            print(f"\rtrue={tws:.1f}kn@{twd:.0f}°  boat={live['sog']:.1f}kn@{live['heading']:.0f}°  "
                  f"apparent={aws_out:.1f}kn@{awa_out:.0f}°{gust_tag}   ", end='', flush=True)

            tick += 1
            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

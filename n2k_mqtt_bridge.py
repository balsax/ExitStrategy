#!/usr/bin/env python3
"""
NMEA2000 (CAN bus) -> MQTT bridge.

Reads canboat's decoded JSON (one PGN per line) from stdin and republishes it
under the boat/nav/* and boat/ais/* MQTT topic namespace, the same schema the
dev simulators (gps_simulator.py, ais_simulator.py, garmin_1243_simulator.py)
use -- so the dashboard never needs to know whether it's talking to real
hardware or a simulator.

Real invocation (verified 2026-08-21 against the actual CAN bus once the CAN
HAT was wired in and the Garmin AIS800 was plugged in -- candump's live `-L`
format is NOT something `analyzer` understands directly, despite what
~/claude.md's setup notes say; it needs converting first):

  candump -L can0 | candump2analyzer | analyzer -json -debugdata | python3 n2k_mqtt_bridge.py

(`candump2analyzer` is a canboat helper binary, already built at
/usr/local/bin/candump2analyzer -- with no file argument it reads and
converts the candump stream live from stdin. `-debugdata` adds a "data" hex
field to each JSON line -- the CAN Bus diagnostics tab's raw-bytes view reads
that; the rest of this script ignores it.)
"""
import sys
import json
import time
import subprocess
import paho.mqtt.client as mqtt

MQTT_BROKER    = "localhost"
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "n2k_mqtt_bridge"

CAN_INTERFACE = "can0"
# Fixed pseudo source address this bridge uses only as the sender of its own
# outgoing ISO Requests below -- not a real claimed N2K node, just enough to
# put a plausible SA in the CAN ID, same as any one-off PC diagnostic tool.
OUR_SOURCE_ADDRESS = 0xF9

FAST_INTERVAL_S = 1.0   # position, speed, heading, wind, depth, engine RPM
SLOW_INTERVAL_S = 5.0   # fix quality, tank levels, battery, temperatures


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


# PGN -> { canboat field name: (mqtt topic, rate class, decimal places or None) }
# Field names for 126992/129026/129029/129539/129540/129039 were verified
# 2026-08-21 against real traffic once the CAN HAT was wired in and the AIS800
# was plugged in. Everything else here (engine/battery/fluid/wind/depth/
# speed-through-water) is still an unverified best-effort guess -- no engine,
# depth, wind, or battery-monitor hardware is on the bus yet to check against.
PGN_MAP = {
    129025: {  # Position, Rapid Update
        "Latitude":  ("boat/nav/gps/latitude",  "fast", 6),
        "Longitude": ("boat/nav/gps/longitude", "fast", 6),
    },
    129026: {  # COG & SOG, Rapid Update
        "COG": ("boat/nav/gps/cog", "fast", 1),
        "SOG": ("boat/nav/gps/sog", "fast", 2),
    },
    129029: {  # GNSS Position Data
        "Latitude":           ("boat/nav/gps/latitude",           "fast", 6),
        "Longitude":          ("boat/nav/gps/longitude",          "fast", 6),
        "Altitude":           ("boat/nav/gps/altitude",           "slow", 1),
        "GNSS type":          ("boat/nav/gps/fix_type",           "slow", None),
        "Method":             ("boat/nav/gps/fix_method",         "slow", None),
        "Geoidal Separation": ("boat/nav/gps/geoidal_separation", "slow", 1),
        # Number of SVs/HDOP/PDOP were originally (wrongly) guessed to live
        # here -- real traffic shows this PGN never carries them; satellite
        # count and DOP values come from the separate 129540/129539 PGNs below.
    },
    129539: {  # GNSS DOPs -- HDOP/VDOP live here, not on 129029
        "HDOP":        ("boat/nav/gps/hdop", "slow", 2),
        "VDOP":        ("boat/nav/gps/vdop", "slow", 2),  # canboat calls it VDOP, not PDOP as originally guessed
        "Actual Mode": ("boat/nav/gps/fix_mode", "slow", None),
    },
    129540: {  # GNSS Sats in View -- satellite count lives here, not on 129029
        "Sats in View": ("boat/nav/gps/satellites", "slow", 0),
    },
    127250: {  # Vessel Heading
        "Heading":   ("boat/nav/heading",            "fast", 1),
        "Deviation": ("boat/nav/heading_deviation",   "slow", 1),
        "Variation": ("boat/nav/heading_variation",   "slow", 1),
    },
    128259: {  # Speed, Water Referenced
        "Speed Water Referenced": ("boat/nav/speed_through_water", "fast", 2),
    },
    128267: {  # Water Depth
        "Depth":  ("boat/nav/depth",        "fast", 2),
        "Offset": ("boat/nav/depth_offset", "slow", 2),
    },
    130306: {  # Wind Data
        "Wind Speed": ("boat/nav/wind/speed",     "fast", 2),
        "Wind Angle": ("boat/nav/wind/angle",     "fast", 1),
        "Reference":  ("boat/nav/wind/reference", "slow", None),
    },
    127488: {  # Engine Parameters, Rapid Update
        "Engine Speed": ("boat/nav/engine/rpm", "fast", 0),
    },
    127489: {  # Engine Parameters, Dynamic
        "Oil pressure":         ("boat/nav/engine/oil_pressure",      "slow", 1),
        "Oil temperature":      ("boat/nav/engine/oil_temp",          "slow", 1),
        "Temperature":          ("boat/nav/engine/coolant_temp",      "slow", 1),
        "Alternator Potential": ("boat/nav/engine/alternator_voltage","slow", 2),
        "Fuel Rate":            ("boat/nav/engine/fuel_rate",         "slow", 2),
    },
    # 127505 (Fluid Level) is handled separately below, not through this flat
    # PGN_MAP -- it carries an Instance field distinguishing which of several
    # physical tanks a message is about, and a flat mapping (one fixed topic
    # per canboat field name) can't represent that; every tank would collide
    # on the same boat/nav/fluid/level topic. See handle_fluid_level().
    127508: {  # Battery Status
        "Voltage":     ("boat/nav/battery/voltage",     "slow", 2),
        "Current":     ("boat/nav/battery/current",     "slow", 2),
        "Temperature": ("boat/nav/battery/temperature", "slow", 1),
    },
    126992: {  # System Time
        "Date": ("boat/nav/system/date", "slow", None),
        "Time": ("boat/nav/system/time", "slow", None),
    },
    129284: {  # Navigation Data -- standard PGN (not proprietary like the autopilot),
               # confirmed transmitted by the 1243 itself per Garmin's own PGN table,
               # but still unverified here -- no chartplotter is on the bus yet to
               # check field names/units against, same caveat as the rest below.
               # Only ever populated by the source device while it has an active
               # Go To/route; absent otherwise.
        "Destination Latitude":                      ("boat/nav/destination/latitude",  "slow", 6),
        "Destination Longitude":                     ("boat/nav/destination/longitude", "slow", 6),
        "Distance to Waypoint":                      ("boat/nav/destination/distance",  "slow", 1),
        "Bearing, Position to Destination Waypoint": ("boat/nav/destination/bearing",   "slow", 1),
        "Waypoint Closing Velocity":                 ("boat/nav/destination/vmg",       "slow", 2),
        "ETA Time":                                  ("boat/nav/destination/eta_time",  "slow", None),
        "ETA Date":                                  ("boat/nav/destination/eta_date",  "slow", None),
        # YES_NO lookup -- canboat's analyzer resolves this to the literal
        # string "Yes"/"No", not a number (confirmed against canboat.json's
        # LookupEnumerations, same unverified-against-real-hardware caveat as
        # the rest of this PGN). Firmware clears this back to "No" once a new
        # leg starts, so watching for its lat/lon-keyed transitions back to
        # "Yes" (see checkDestinationArrival() in static-src/index.html) is
        # enough to catch every waypoint in a chartplotter-built route, not
        # just a single Go To -- no need to also decode PGN 129285's full
        # route/waypoint list just to know a leg was completed.
        "Arrival Circle Entered":                    ("boat/nav/destination/arrived",   "slow", None),
    },
}

# PGN 127505 (Fluid Level) reports one physical tank per message, identified
# by its Instance field -- there are 4 tanks on this boat (2 fresh water, 1
# diesel, 1 black water), each its own sender on the bus, each publishing its
# own Instance number. This maps that Instance to which physical tank it is
# -- installer-assigned at commissioning time, not something this bridge can
# infer from the PGN itself, so it's a hardcoded guess (0/1/2/3 in the most
# natural order) until real senders are wired and the actual assignment can
# be read off the live bus and corrected here. dashboard-dev's
# tank_simulator.py publishes under these same names for the dev stand-in.
TANK_INSTANCE_TOPICS = {
    0: 'fresh_1',
    1: 'fresh_2',
    2: 'diesel',
    3: 'black',
}

def handle_fluid_level(client, fields):
    tank = TANK_INSTANCE_TOPICS.get(fields.get("Instance"))
    if tank is None:
        return  # unrecognized instance -- don't publish under a guessed tank name
    if "Level" in fields:
        topic = f"boat/nav/tanks/{tank}/level"
        if should_publish(topic, "slow"):
            client.publish(topic, format_value(fields["Level"], 1), retain=False)
    if "Capacity" in fields:
        topic = f"boat/nav/tanks/{tank}/capacity"
        if should_publish(topic, "slow"):
            client.publish(topic, format_value(fields["Capacity"], 1), retain=False)

# AIS PGNs can't use the flat PGN_MAP above because the MQTT topic itself is
# data-dependent — it's keyed by the target vessel's MMSI (canboat's "User ID"
# field), not a fixed field name. See the boat/ais/<mmsi>/<field> topic schema
# documented in ais_simulator.py (same repo).
# 'User ID' as the MMSI field name is confirmed against a real AIS800 capture
# 2026-08-21 (129039, MMSI 368492150). The rest of the field names below
# (Latitude/Longitude/SOG/COG/True Heading/Nav Status) are still unconfirmed —
# that same real target's position reports never carried any of those fields
# (only the non-numeric ones: transceiver/unit-type/comm-state info), so
# there's no real sample yet to check the numeric field names against. Handled
# gracefully either way since every field is optional here (`if 'X' in
# fields`), so a differently-named/missing field just means that one value
# doesn't get published rather than a crash.
AIS_POSITION_PGNS = {129038, 129039}  # Class A / Class B position reports
AIS_STATIC_PGNS = {129794, 129809, 129810}  # Class A / Class B static & voyage data

AIS_NAV_STATUS = {
    0: "Under way using engine", 1: "At anchor", 2: "Not under command",
    3: "Restricted maneuverability", 4: "Constrained by draft", 5: "Moored",
    6: "Aground", 7: "Engaged in fishing", 8: "Under way sailing",
}

last_publish = {}  # topic -> last publish timestamp


def should_publish(topic, rate_class):
    interval = FAST_INTERVAL_S if rate_class == "fast" else SLOW_INTERVAL_S
    now = time.time()
    if now - last_publish.get(topic, 0) >= interval:
        last_publish[topic] = now
        return True
    return False


def format_value(value, decimals):
    if decimals is None:
        return str(value)
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


# Real capture 2026-08-21 (129029, no GPS fix yet) showed canboat pass a "no
# fix" sentinel through as a literal out-of-range value (~91.0000010,
# ~180.9999961) instead of omitting the field the way it does for other N/A
# values -- publishing that as if it were a real position would feed garbage
# into anchor watch / the chart's own-vessel marker. Guard any topic carrying
# a latitude or longitude against that before it goes out.
def valid_latlon(topic, value):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return True  # not a coordinate we can range-check, let it through
    if topic.endswith('latitude') or topic.endswith('/lat'):
        return -90.0 <= v <= 90.0
    if topic.endswith('longitude') or topic.endswith('/lon'):
        return -180.0 <= v <= 180.0
    return True


def handle_ais_pgn(client, pgn, fields):
    mmsi = fields.get('User ID')
    if mmsi is None:
        return
    base = f"boat/ais/{mmsi}"

    if pgn in AIS_POSITION_PGNS:
        if not should_publish(f"_ais_pos/{mmsi}", "fast"):
            return
        if 'Latitude' in fields and 'Longitude' in fields:
            if valid_latlon(f"{base}/lat", fields['Latitude']) and valid_latlon(f"{base}/lon", fields['Longitude']):
                client.publish(f"{base}/lat", format_value(fields['Latitude'], 6), retain=False)
                client.publish(f"{base}/lon", format_value(fields['Longitude'], 6), retain=False)
        if 'SOG' in fields:
            client.publish(f"{base}/sog", format_value(fields['SOG'], 1), retain=False)
        if 'COG' in fields:
            client.publish(f"{base}/cog", format_value(fields['COG'], 1), retain=False)
        if 'True Heading' in fields:
            client.publish(f"{base}/heading", format_value(fields['True Heading'], 1), retain=False)
        status = fields.get('Nav Status')
        if status is not None:
            label = AIS_NAV_STATUS.get(status, str(status)) if isinstance(status, int) else str(status)
            client.publish(f"{base}/nav_status", label, retain=False)
        client.publish(f"{base}/class", "A" if pgn == 129038 else "B", retain=False)

    elif pgn in AIS_STATIC_PGNS:
        if not should_publish(f"_ais_static/{mmsi}", "slow"):
            return
        if 'Name' in fields:
            client.publish(f"{base}/name", str(fields['Name']).strip(), retain=False)
        if 'Type of Ship' in fields:
            client.publish(f"{base}/type", str(fields['Type of Ship']), retain=False)


def handle_unmapped(client, pgn, fields):
    # Generic fallback so nothing decoded on the bus is silently dropped,
    # even if it's not one of the PGNs explicitly mapped above.
    for field_name, value in fields.items():
        topic = f"boat/nav/pgn/{pgn}/{sanitize_field_name(field_name)}"
        if should_publish(topic, "slow"):
            client.publish(topic, str(value), retain=False)


def sanitize_field_name(field_name):
    return str(field_name).lower().replace(" ", "_").replace("/", "_")


# Feeds the dashboard's CAN Bus diagnostics tab -- every decoded PGN gets a
# live entry here regardless of whether it's also separately mapped to a
# semantic boat/nav/* or boat/ais/* topic above, so the diagnostics view shows
# everything actually on the bus, not just what this bridge knows how to
# interpret. canboat's own `description` field (from its PGN database) is
# "known" -- basically every standard/proprietary PGN it recognizes gets one;
# a PGN that comes through with no description (or canboat's own "Unknown ..."
# placeholder) is genuinely unrecognized, so the dashboard just shows its raw
# number instead of a translated label. Rate-limited like everything else
# here -- this is a diagnostics view, not a control input, so it doesn't need
# sub-second freshness even for PGNs that broadcast at 10Hz.
def publish_canbus_diag(client, pgn, description, src, fields, raw_hex):
    base = f"boat/canbus/pgn/{pgn}"
    is_known = bool(description) and "unknown" not in description.lower()
    if should_publish(f"{base}/_description", "slow"):
        client.publish(f"{base}/_description", description if is_known else "", retain=False)
    if src is not None and should_publish(f"{base}/_src", "slow"):
        client.publish(f"{base}/_src", str(src), retain=False)
    # Raw bytes as they actually appeared on the bus -- the only information
    # available at all for a PGN canboat can't decode (no fields, no
    # description), and a way to cross-check canboat's own decode for a known
    # one if it's ever in doubt. Space it out for readability in the UI.
    if raw_hex and should_publish(f"{base}/_raw", "slow"):
        spaced = " ".join(raw_hex[i:i + 2] for i in range(0, len(raw_hex), 2))
        client.publish(f"{base}/_raw", spaced, retain=False)
    for field_name, value in fields.items():
        topic = f"{base}/field/{sanitize_field_name(field_name)}"
        if should_publish(topic, "slow"):
            # Some fields (e.g. per-satellite lists) are nested structures --
            # flatten to a single readable string rather than raw Python repr.
            display = json.dumps(value) if isinstance(value, (list, dict)) else str(value)
            client.publish(topic, display, retain=False)

    # Inverted view for the "which device sent this" side of the diagnostics
    # tab -- same (pgn -> description) fact as above, just indexed by source
    # address instead, so the dashboard can list every PGN seen from a given
    # device without having to scan every boat/canbus/pgn/* branch itself.
    # Excludes OUR_SOURCE_ADDRESS -- that's this bridge's own outgoing ISO
    # Requests looping back on the bus, not a real boat instrument, and would
    # otherwise show up as a bogus extra "device" in that list.
    if src is not None and src != OUR_SOURCE_ADDRESS:
        dev_topic = f"boat/canbus/device/{src}/pgn/{pgn}"
        if should_publish(dev_topic, "slow"):
            client.publish(dev_topic, description if is_known else "", retain=False)


# PGN 126996 (Product Information) and 60928 (ISO Address Claim) are how a
# real NMEA2000 device identifies itself on the bus. Field names below are
# canboat's standard/stable names for these two PGNs -- verified 2026-08-21
# against the real AIS800 (Model ID came back "AIS 800", confirming the
# device on source address 43). Builds a best-effort friendly device name
# once either PGN is seen; until then the dashboard just shows "Device <src>".
device_identity = {}  # src -> {"model": str|None, "manufacturer": str|None}

def handle_device_identity(client, pgn, src, fields):
    if src is None:
        return
    ident = device_identity.setdefault(src, {"model": None, "manufacturer": None})
    changed = False
    if pgn == 126996:
        model = fields.get("Model ID")
        if model and ident["model"] != model:
            ident["model"] = str(model).strip()
            changed = True
    elif pgn == 60928:
        mfg = fields.get("Manufacturer Code")
        if mfg and ident["manufacturer"] != mfg:
            ident["manufacturer"] = str(mfg).strip()
            changed = True
    if changed:
        name = ident["model"] or ident["manufacturer"]
        client.publish(f"boat/canbus/device/{src}/_name", name, retain=False)


# Devices don't reliably volunteer Product Information on their own (many
# only send it once at boot, which has usually already happened by the time
# this bridge starts) -- so rather than wait, ask. Same standard ISO Request
# (PGN 59904 asking for PGN 126996) verified manually against the real AIS800
# on 2026-08-21 via `cansend can0 18EA2BF9#14F001`, just built dynamically
# here for whatever source address shows up. One-shot per source address per
# run -- a device that answers gets named via handle_device_identity() above;
# one that doesn't (or isn't a real NMEA2000 node, or is offline) just stays
# "Device <src>" without retrying forever.
requested_product_info = set()

def request_product_information(src):
    if src in requested_product_info:
        return
    requested_product_info.add(src)
    priority, pf, requested_pgn = 6, 0xEA, 126996
    can_id = (priority << 26) | (pf << 16) | (int(src) << 8) | OUR_SOURCE_ADDRESS
    frame = f"{can_id:08X}#{requested_pgn.to_bytes(3, 'little').hex().upper()}"
    try:
        subprocess.run(["cansend", CAN_INTERFACE, frame], check=False, timeout=2)
    except (OSError, subprocess.SubprocessError):
        pass  # best-effort diagnostics nicety -- never worth crashing the bridge over


def handle_line(client, line):
    line = line.strip()
    if not line:
        return
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return

    pgn = data.get("pgn")
    fields = data.get("fields")
    if pgn is None or not fields:
        return

    src = data.get("src")
    publish_canbus_diag(client, pgn, data.get("description", ""), src, fields, data.get("data"))
    if pgn in (126996, 60928):
        handle_device_identity(client, pgn, src, fields)
    elif src is not None and src != OUR_SOURCE_ADDRESS:
        ident = device_identity.get(src)
        if ident is None or (ident["model"] is None and ident["manufacturer"] is None):
            request_product_information(src)

    if pgn in AIS_POSITION_PGNS or pgn in AIS_STATIC_PGNS:
        handle_ais_pgn(client, pgn, fields)
        return

    if pgn == 127505:
        handle_fluid_level(client, fields)
        return

    mapping = PGN_MAP.get(pgn)
    if mapping:
        for field_name, value in fields.items():
            if field_name in mapping:
                topic, rate_class, decimals = mapping[field_name]
                if not valid_latlon(topic, value):
                    continue
                if should_publish(topic, rate_class):
                    client.publish(topic, format_value(value, decimals), retain=False)
    else:
        handle_unmapped(client, pgn, fields)


def main():
    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    for line in sys.stdin:
        handle_line(client, line)


if __name__ == "__main__":
    main()

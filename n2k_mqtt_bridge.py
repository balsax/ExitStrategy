#!/usr/bin/env python3
import sys
import json
import time
import paho.mqtt.client as mqtt

MQTT_BROKER    = "localhost"
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "n2k_mqtt_bridge"

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
# NOTE: field names below are best-effort guesses based on canboat's typical PGN
# definitions. VERIFY against real `analyzer -json` output before trusting this map.
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
        "Number of SVs":      ("boat/nav/gps/satellites",         "slow", 0),
        "HDOP":               ("boat/nav/gps/hdop",               "slow", 2),
        "PDOP":               ("boat/nav/gps/pdop",               "slow", 2),
        "Geoidal Separation": ("boat/nav/gps/geoidal_separation", "slow", 1),
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
    127505: {  # Fluid Level
        "Level":    ("boat/nav/fluid/level",    "slow", 1),
        "Capacity": ("boat/nav/fluid/capacity", "slow", 1),
    },
    127508: {  # Battery Status
        "Voltage":     ("boat/nav/battery/voltage",     "slow", 2),
        "Current":     ("boat/nav/battery/current",     "slow", 2),
        "Temperature": ("boat/nav/battery/temperature", "slow", 1),
    },
    126992: {  # System Time
        "Date": ("boat/nav/system/date", "slow", None),
        "Time": ("boat/nav/system/time", "slow", None),
    },
}

# AIS PGNs can't use the flat PGN_MAP above because the MQTT topic itself is
# data-dependent — it's keyed by the target vessel's MMSI (canboat's "User ID"
# field), not a fixed field name. See the boat/ais/<mmsi>/<field> topic schema
# documented in ais_simulator.py (same repo); this mapping is best-effort and, like the
# rest of this file, UNVERIFIED against real AIS 800 `analyzer -json` output.
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


def handle_ais_pgn(client, pgn, fields):
    mmsi = fields.get('User ID')
    if mmsi is None:
        return
    base = f"boat/ais/{mmsi}"

    if pgn in AIS_POSITION_PGNS:
        if not should_publish(f"_ais_pos/{mmsi}", "fast"):
            return
        if 'Latitude' in fields and 'Longitude' in fields:
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
        safe_field = str(field_name).lower().replace(" ", "_").replace("/", "_")
        topic = f"boat/nav/pgn/{pgn}/{safe_field}"
        if should_publish(topic, "slow"):
            client.publish(topic, str(value), retain=False)


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

    if pgn in AIS_POSITION_PGNS or pgn in AIS_STATIC_PGNS:
        handle_ais_pgn(client, pgn, fields)
        return

    mapping = PGN_MAP.get(pgn)
    if mapping:
        for field_name, value in fields.items():
            if field_name in mapping:
                topic, rate_class, decimals = mapping[field_name]
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

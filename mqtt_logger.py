#!/usr/bin/env python3
"""
mqtt_logger.py
Autodiscovering MQTT to MariaDB logger for Exit Strategy boat monitoring.

- Subscribes to all topics under boat/# and N/#
- Auto-registers new devices and topics as they appear
- Logs time series data to mqtt_readings
- Maintains last_value and last_update in mqtt_topics
- Updates device status and last_seen in mqtt_devices
- Logs to systemd journal and rotating log file
"""

import os
import sys
import time
import logging
import logging.handlers
import mysql.connector
import paho.mqtt.client as mqtt
from datetime import datetime
from logger_policy import StreamGate

# ── Configuration ─────────────────────────────────────────────────────────────
def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets

_secrets = get_secrets()

MQTT_BROKER   = "localhost"
MQTT_PORT     = 1883
MQTT_USER     = _secrets['MQTT_USER']
MQTT_PASS     = _secrets['MQTT_PASS']
MQTT_CLIENT_ID = "mqtt_logger"
MQTT_TOPICS   = [
    ("boat/#", 0),
    ("N/#",    0),
]

DB_HOST = "localhost"
DB_USER = "mikemc"
DB_PASS = _secrets['DB_PASS']
DB_NAME = "boat_monitoring"

LOG_DIR  = "/var/log/mqtt_logger"
LOG_FILE = os.path.join(LOG_DIR, "mqtt_logger.log")

# Topics to exclude from time series logging (still registered, log_enabled=0)
EXCLUDE_FROM_LOG = {
    "host", "build", "mac", "ssid", "datetime"
}

# Topic prefixes to ignore completely — not registered as a device/topic, not
# logged. boat/ais/* is the dev AIS simulator's synthetic-MMSI traffic (see
# dashboard-dev/ais_simulator.py); its own track history is served entirely
# from an in-memory store in dashboard_api.py and never touches this database,
# so there's nothing lost by skipping it here. Each fake MMSI would otherwise
# register as a brand-new permanent "device" that never gets reused.
IGNORE_PREFIXES = (
    "boat/ais/",
)

# Topics that are still registered and still get last_value/last_seen updates,
# but whose *readings* are not stored in mqtt_readings. While the NMEA2000 side
# is simulated (gps_simulator.py, autopilot_simulator.py, ... publish the same
# boat/nav/* topics real hardware will), these were ~55% of all rows in the
# table and nothing reads them back from the database -- the dashboard uses
# them live over MQTT only (anchor watch, chart, autopilot panel; trips have
# their own trip_points table). Anything dashboard_api.py *does* read from
# mqtt_readings (wind speed/angle, gps/sog, tanks, engine, system, bilge) is
# deliberately NOT listed here or is re-allowed in NAV_KEEP_LOGGING.
# Set NAV_IS_SIMULATED = False once the CAN HAT is wired into the real N2K
# backbone to resume logging real position/depth/heading history.
NAV_IS_SIMULATED = True
NAV_NO_READINGS_PREFIXES = (
    "boat/nav/destination/",
    "boat/nav/autopilot/",
    "boat/nav/rudder",
    "boat/nav/heading",
    "boat/nav/depth",
    "boat/nav/gps/",
)
NAV_KEEP_LOGGING = {
    "boat/nav/gps/sog",   # Weather tab wind-direction chart plots SOG (dashboard_api.py /api/wind/direction_history)
}

# Write-reduction gates (see logger_policy.py for the exact rules).
# Readings: skip unchanged values (heartbeat row every 5s, which is the smallest
# trend bucket so charts never get holes) and cap fast streams to one row per 2s.
# Topic last_value / device last_seen: at most ~1 update/second per key.
READING_MIN_INTERVAL_S = 2.0
READING_HEARTBEAT_S    = 5.0
TOUCH_INTERVAL_S       = 1.0

# Topics that indicate device status
STATUS_TOPIC_SUFFIXES = {"status", "connected", "LWT"}
STATUS_ONLINE_VALUES  = {"online", "ONLINE", "Online"}
STATUS_OFFLINE_VALUES = {"offline", "OFFLINE", "Offline"}
# ─────────────────────────────────────────────────────────────────────────────

# ── Logging setup ─────────────────────────────────────────────────────────────
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("mqtt_logger")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Rotating file handler — 5MB per file, keep 5 files
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    # Stream handler — systemd journal captures stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

log = setup_logging()

# ── Database ──────────────────────────────────────────────────────────────────
class Database:
    def __init__(self):
        self.conn   = None
        self.cursor = None
        self._device_cache = {}   # device_id -> topic_prefix
        self._topic_cache  = {}   # topic -> topic_id
        self._warned_topics = set()
        self._log_enabled = {}            # topic_id -> bool, refreshed by log_enabled_cached()
        self._log_enabled_loaded = 0.0    # time.monotonic() of the last refresh

    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                unix_socket="/run/mysqld/mysqld.sock",
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                autocommit=False
            )
            self.cursor = self.conn.cursor(dictionary=True)
            log.info("Database connected")
            self._load_caches()
        except mysql.connector.Error as e:
            log.error(f"Database connection failed: {e}")
            raise

    def _load_caches(self):
        self.cursor.execute(
            "SELECT device_id, topic_prefix FROM mqtt_devices WHERE topic_prefix IS NOT NULL")
        for row in self.cursor.fetchall():
            self._device_cache[row["device_id"]] = row["topic_prefix"]

        self.cursor.execute("SELECT id, topic FROM mqtt_topics")
        for row in self.cursor.fetchall():
            self._topic_cache[row["topic"]] = row["id"]

        log.info(f"Cache loaded: {len(self._device_cache)} devices, "
                 f"{len(self._topic_cache)} topics")

    def reconnect(self):
        log.warning("Reconnecting to database...")
        try:
            self.connect()
        except Exception as e:
            log.error(f"Reconnect failed: {e}")

    def execute(self, sql, params=None, commit=False):
        try:
            self.cursor.execute(sql, params or ())
            if commit:
                self.conn.commit()
            return True
        except mysql.connector.Error as e:
            log.error(f"DB execute error: {e}  SQL: {sql[:80]}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False

    # ── Device prefix matching ─────────────────────────────────────────────────
    def match_device(self, topic):
        """Return device_id for the longest matching prefix, or None."""
        best_device = None
        best_len    = 0
        for device_id, prefix in self._device_cache.items():
            if topic.startswith(prefix + "/") or topic == prefix:
                if len(prefix) > best_len:
                    best_device = device_id
                    best_len    = len(prefix)
        return best_device

    # ── Auto-discover device ───────────────────────────────────────────────────
    def infer_device(self, topic):
        """Infer device_id and prefix from topic structure."""
        parts = topic.split("/")
        if parts[0] == "boat" and len(parts) >= 3:
            device_id = f"{parts[1]}_{parts[2]}"
            prefix    = f"boat/{parts[1]}/{parts[2]}"
        elif parts[0] == "N" and len(parts) >= 2:
            device_id = f"N_{parts[1]}"
            prefix    = f"N/{parts[1]}"
        else:
            device_id = parts[0]
            prefix    = parts[0]
        return device_id, prefix

    def ensure_device(self, topic):
        """Return device_id, auto-registering if needed."""
        device_id = self.match_device(topic)
        if device_id:
            return device_id

        device_id, prefix = self.infer_device(topic)

        if device_id not in self._device_cache:
            log.info(f"Auto-registering device: {device_id}  prefix={prefix}")
            ok = self.execute("""
                INSERT IGNORE INTO mqtt_devices
                    (device_id, topic_prefix, description, status)
                VALUES (%s, %s, %s, %s)
            """, (device_id, prefix, "Auto-discovered", "UNKNOWN"), commit=True)
            if ok:
                self._device_cache[device_id] = prefix
                log.info(f"Device registered: {device_id}")

        return device_id

    # ── Auto-discover topic ────────────────────────────────────────────────────
    def ensure_topic(self, topic, device_id, value):
        """Return topic_id, auto-registering if needed."""
        if topic in self._topic_cache:
            return self._topic_cache[topic]

        # Detect data type
        data_type = detect_type(value)

        # Detect if this should be excluded from logging
        leaf = topic.split("/")[-1]
        log_enabled = 0 if leaf in EXCLUDE_FROM_LOG else 1

        # Detect if this is a status topic
        is_status = leaf in STATUS_TOPIC_SUFFIXES

        log.info(f"Auto-registering topic: {topic}  "
                 f"type={data_type}  log={log_enabled}")

        ok = self.execute("""
            INSERT IGNORE INTO mqtt_topics
                (device_id, topic, direction, data_type, log_enabled)
            VALUES (%s, %s, 'pub', %s, %s)
        """, (device_id, topic, data_type, log_enabled), commit=True)

        if ok:
            self.cursor.execute(
                "SELECT id FROM mqtt_topics WHERE topic = %s", (topic,))
            row = self.cursor.fetchone()
            if row:
                topic_id = row["id"]
                self._topic_cache[topic] = topic_id

                # If this looks like a status topic, update the device record
                if is_status:
                    self.execute("""
                        UPDATE mqtt_devices SET status_topic = %s
                        WHERE device_id = %s AND status_topic IS NULL
                    """, (topic, device_id), commit=True)

                return topic_id

        # Fallback — try fetching if INSERT IGNORE hit a duplicate
        self.cursor.execute(
            "SELECT id FROM mqtt_topics WHERE topic = %s", (topic,))
        row = self.cursor.fetchone()
        if row:
            self._topic_cache[topic] = row["id"]
            return row["id"]

        return None

    # ── log_enabled lookup (cached) ────────────────────────────────────────────
    # on_message used to SELECT log_enabled from mqtt_topics for every message.
    # It only changes when someone edits the row by hand, so re-read the whole
    # (~300 row) table once a minute -- or immediately for a topic_id we haven't
    # seen yet, e.g. one that was just auto-registered.
    def log_enabled_cached(self, topic_id):
        now = time.monotonic()
        if topic_id not in self._log_enabled or now - self._log_enabled_loaded >= 60:
            self.cursor.execute("SELECT id, log_enabled FROM mqtt_topics")
            self._log_enabled = {r["id"]: bool(r["log_enabled"]) for r in self.cursor.fetchall()}
            self._log_enabled_loaded = now
            # autocommit is off: end the read snapshot now, otherwise a message
            # that writes nothing would leave it open and the next refresh
            # would keep seeing this same (stale) view of the table.
            self.conn.commit()
        return self._log_enabled.get(topic_id, False)

    # ── Update last_value on topic ─────────────────────────────────────────────
    def update_topic_last(self, topic_id, value, commit=True):
        return self.execute("""
            UPDATE mqtt_topics
            SET last_value = %s, last_update = %s
            WHERE id = %s
        """, (value, now_ms(), topic_id), commit=commit)

    # ── Insert reading ─────────────────────────────────────────────────────────
    def insert_reading(self, topic_id, value, commit=True):
        return self.execute("""
            INSERT INTO mqtt_readings (ts, topic_id, value)
            VALUES (%s, %s, %s)
        """, (now_ms(), topic_id, value), commit=commit)

    # ── Update device status ───────────────────────────────────────────────────
    def update_device_status(self, device_id, status, ts):
        self.execute("""
            UPDATE mqtt_devices
            SET status = %s, last_seen = %s
            WHERE device_id = %s
        """, (status, ts, device_id), commit=True)

    # ── Update device last_seen ────────────────────────────────────────────────
    def update_device_seen(self, device_id, ts, commit=True):
        return self.execute("""
            UPDATE mqtt_devices SET last_seen = %s
            WHERE device_id = %s
        """, (ts, device_id), commit=commit)

    def check_connection(self):
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=2)
        except Exception:
            self.reconnect()

# ── Helpers ───────────────────────────────────────────────────────────────────
def now_ms():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def detect_type(value):
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        pass
    return "string"

# ── MQTT callbacks ────────────────────────────────────────────────────────────
db = Database()

reading_gate      = StreamGate(READING_MIN_INTERVAL_S, READING_HEARTBEAT_S)
topic_touch_gate  = StreamGate(TOUCH_INTERVAL_S, TOUCH_INTERVAL_S)
device_touch_gate = StreamGate(TOUCH_INTERVAL_S, TOUCH_INTERVAL_S)

def readings_excluded(topic):
    """True if this topic's readings should not be written to mqtt_readings (see NAV_* above)."""
    return (NAV_IS_SIMULATED
            and topic.startswith(NAV_NO_READINGS_PREFIXES)
            and topic not in NAV_KEEP_LOGGING)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT connected")
        for topic, qos in MQTT_TOPICS:
            client.subscribe(topic, qos)
            log.info(f"Subscribed: {topic}")
    else:
        log.error(f"MQTT connection failed rc={rc}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning(f"MQTT disconnected unexpectedly rc={rc} — will reconnect")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic

        if topic.startswith(IGNORE_PREFIXES):
            return

        value = msg.payload.decode("utf-8", errors="replace").strip()
        ts    = now_ms()

        if not value:
            return

        db.check_connection()

        # Ensure device exists
        device_id = db.ensure_device(topic)
        if not device_id:
            log.warning(f"Could not determine device for topic: {topic}")
            return

        # Ensure topic exists
        topic_id = db.ensure_topic(topic, device_id, value)
        if not topic_id:
            log.warning(f"Could not register topic: {topic}")
            return

        # Writes below are batched into one commit at the end of the message
        # (they used to commit individually -- ~3 fsyncs per message).
        dirty = False

        # Update topic last value (throttled to ~1/s per topic, see logger_policy.py)
        if topic_touch_gate.allow(topic_id, value):
            dirty |= bool(db.update_topic_last(topic_id, value, commit=False))

        # Update device last_seen (~1/s per device)
        if device_touch_gate.allow(device_id, None):
            dirty |= bool(db.update_device_seen(device_id, ts, commit=False))

        # Handle status topics (never throttled -- these are state transitions)
        leaf = topic.split("/")[-1]
        if leaf in STATUS_TOPIC_SUFFIXES:
            if value in STATUS_ONLINE_VALUES:
                db.update_device_status(device_id, "ONLINE", ts)
                log.info(f"Device ONLINE: {device_id}")
            elif value in STATUS_OFFLINE_VALUES:
                db.update_device_status(device_id, "OFFLINE", ts)
                log.warning(f"Device OFFLINE: {device_id}")

        # Log to readings table if enabled, not a simulated-nav topic that nothing
        # reads back, and worth writing (changed / heartbeat / rate cap).
        if (db.log_enabled_cached(topic_id)
                and not readings_excluded(topic)
                and reading_gate.allow(topic_id, value)):
            dirty |= bool(db.insert_reading(topic_id, value, commit=False))

        if dirty:
            db.conn.commit()

    except Exception as e:
        log.error(f"Error processing message topic={msg.topic}: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== MQTT Logger starting ===")

    try:
        db.connect()
    except Exception:
        log.error("Could not connect to database — exiting")
        sys.exit(1)

    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    client.reconnect_delay_set(min_delay=1, max_delay=30)

    while True:
        try:
            log.info(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            log.error(f"MQTT connection error: {e} — retrying in 10s")
            time.sleep(10)

if __name__ == "__main__":
    main()

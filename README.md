# Exit Strategy Dashboard

A self-hosted monitoring and control dashboard for the sailboat **Exit Strategy**, running on a Raspberry Pi aboard. Flask backend, single-file vanilla JS frontend (no build step), reading/writing live boat systems over MQTT.

This repo is the **dev copy**. Production runs from `/home/mikemc/dashboard_api.py` (systemd service `dashboard-api.service`, port 5001) and `/var/www/dashboard/index.html` (served by Nginx on port 8080) — separate files, deployed manually from this repo when ready. See [`CLAUDE.md`](./CLAUDE.md) for the exact rules around that boundary.

## Architecture

- **Backend** (`dashboard_api.py`) — Flask app. Maintains a live in-memory snapshot of every MQTT topic (`boat/#`, `N/#`) via a background paho-mqtt subscriber thread, exposed at `/api/mqtt/topics` for the frontend to poll. Also runs a couple of always-on background threads independent of any browser tab (anchor drag/depth/GPS-staleness alarms with push+GPIO dispatch, AIS track recording) — see `DEBUG_MODE` in `dashboard_api.py` for why that gating matters if you ever run this outside `python3 dashboard_api.py`.
- **Frontend** (`static-src/index.html`) — everything (HTML/CSS/JS) in one file, served directly by Flask via `send_from_directory`. No build step, no bundler.
- **MQTT** (Mosquitto, `localhost:1883`) — the real-time bus. Real hardware and the dev simulators both publish to the same topic namespace, so the frontend never needs to know which it's talking to.
- **MariaDB** (`boat_monitoring` db) — a separate, pre-existing logging pipeline (`mqtt_logger.py`, this repo, symlinked from `~/python/mqtt_logger.py` where its systemd service expects it) subscribes to everything under `boat/#`/`N/#` and logs time-series readings. The dashboard only *reads* from it for trend charts; it never writes here directly, and `mqtt_logger.py` explicitly ignores `boat/ais/*` (synthetic simulator MMSIs would otherwise register as permanent fake "devices").
- **NMEA2000 → MQTT bridge** (`n2k_mqtt_bridge.py`) — decodes canboat's `analyzer -json` output off the CAN bus into the same `boat/nav/*` / `boat/ais/*` topics the simulators use. Not yet deployed as a service — blocked on the CAN HAT being physically wired into the boat's N2K backbone.

## Running it

```
python3 dashboard_api.py   # binds 0.0.0.0:5003, debug=True (auto-reload)
```

Secrets (MQTT/DB credentials, VRM/Influx tokens, ntfy topic) are read from `/etc/dashboard/secrets.env` — never hardcoded. See `get_secrets()` in `dashboard_api.py` for the expected key names.

## Pages

| Tab | What it does | Data source |
|---|---|---|
| Overview | Cabin temp/humidity/pressure/air quality, system status, smart relay control | BME680 sensor, InfluxDB, `boat/power/relay1/*` |
| Electrical | Live power flow diagram (shore/solar/battery/loads) | Victron VRM |
| Watermaker | RO system gauges, start/stop/flush, manual device control, trend history | `boat/watermaker/*`, MariaDB |
| Engine | Compartment temp/humidity, cooling fan control (mode/setpoints/timeout) in a modal, engine telemetry placeholders | `boat/engine/fan/*` (real), `boat/nav/engine/*` (not yet wired) |
| Anchor Watch | Click-to-place drop point, live distance/bearing, drag/depth/GPS-staleness alarms (ntfy push + GPIO buzzer, works with no tab open), 15-min server-recorded trail | `boat/nav/gps/*`, `boat/nav/depth` |
| Chart | NOAA nautical chart — pre-rendered NCDS base tiles (real paper-chart symbology, served straight out of NOAA's own MBTiles, switchable to Streets/Dark) + a clickable ENC vector overlay (soundings, buoys/beacons/lights, bridges, wrecks/obstructions), zoom-gated layer toggles, AIS traffic (target plotting, heading vectors, server-recorded tracks — replaces the old standalone AIS tab), and a "My Vessel & Autopilot" info panel (heading/COG/SOG; autopilot not wired) | `chart_data/ncds/` (MBTiles), `chart_data/processed/` (GeoJSON), `boat/ais/<mmsi>/*` |
| Weather | Local conditions (BME680) + NWS forecast + active marine alert banner (Small Craft Advisory, Gale Warning, etc.) sub-tab; live Windy.com wind map sub-tab (re-centers on GPS on open) | BME680, api.weather.gov, embed.windy.com |
| MQTT Diagnostics | Collapsible tree of every live MQTT topic, search, connection status | `boat/#`, `N/#` |

## Dev/test tools (not deployed as services)

- **`gps_simulator.py`** — simulates a boat swinging at anchor (optionally dragging) on `boat/nav/gps/*`, `boat/nav/heading`, `boat/nav/depth`. Unblocks anchor watch / AIS / weather dev without real GPS hardware.
  ```
  python3 gps_simulator.py --swing-radius 25
  python3 gps_simulator.py --drag-after 30 --drag-rate 0.3   # trigger the drag alarm
  python3 gps_simulator.py --depth-ft 4                       # trigger the depth alarm
  ```
- **`ais_simulator.py`** — publishes N synthetic AIS targets (`boat/ais/<mmsi>/*`, MMSIs ≥ 990000001 so they can never collide with a real vessel) that wander, drift out of range, and get replaced.
  ```
  python3 ais_simulator.py --num-targets 5 --radius-nm 2
  ```

Both publish to the exact same topics the real hardware (NMEA2000 GPS/depth, AIS receiver) will eventually use via `n2k_mqtt_bridge.py`, so no frontend code needs to change once that's wired up.

- **`garmin_1243_simulator.py`** — simulates a Garmin GPSMAP 1243 chartplotter's NMEA2000 output one layer lower than the two simulators above: canboat `analyzer -json` lines (position/COG/SOG/heading/GNSS-fix PGNs) on stdout, piped into the real, unmodified `n2k_mqtt_bridge.py` instead of publishing MQTT directly. Stands in for `candump can0 | analyzer -json`, so it exercises (and helped verify) the bridge's actual PGN-to-MQTT mapping, not just the dashboard. Heading and COG deliberately drift apart a little, matching real leeway/current behavior — visible as two distinct lines on the Chart tab's "My Vessel" overlay.
  ```
  python3 garmin_1243_simulator.py | python3 n2k_mqtt_bridge.py
  python3 garmin_1243_simulator.py --swing-radius 25 | python3 n2k_mqtt_bridge.py
  ```

- **`chart_tools.py`** — downloads chart data for the Chart tab. The base layer is NOAA's own pre-rendered Chart Display Service (NCDS) tiles, one MBTiles file per coastal region — no local rendering needed, dashboard_api.py scans every downloaded region and queries the SQLite file that actually has the requested tile. The clickable overlay (soundings, aids to navigation, bridges, hazards) is ENC vector data converted to GeoJSON via `ogr2ogr`, same as any ENC pipeline requires. Requires `gdal-bin` (`ogr2ogr`).
  ```
  python3 chart_tools.py download-ncds ncds_10                        # one region (large — several hundred MB each)
  python3 chart_tools.py sync US5TPAEF US5TPAFG US4FL1PQ US3FL1EE     # vector overlay cells (home port area)
  ```
  Currently downloaded: US East Coast (Maine–South Florida), the full Gulf of Mexico, and the Caribbean/Puerto Rico/USVI region (`ncds_01a` through `ncds_14`, `ncds_09`) — 17 regions, ~8.1GB in `chart_data/ncds/` (gitignored, not in version control). See the docstring at the top of the file for how to look up the NCDS region or ENC cell(s) covering a different location.

## Hardware status

| System | Status |
|---|---|
| Watermaker, smart relay, engine fan controller | **Real**, live on MQTT |
| BME680 (cabin sensor), Victron VRM | **Real** |
| GPS, depth, heading, AIS | **Simulated** — CAN bus has zero physical traffic; `n2k_mqtt_bridge.py`'s nav PGN mapping (position/COG/SOG/heading) is verified against simulated canboat JSON via `garmin_1243_simulator.py`, but not yet deployed against the real bus, pending the CAN HAT being wired into the N2K backbone |
| Engine telemetry (RPM, oil, coolant, alternator, fuel) | Not wired — PGN mapping exists in `n2k_mqtt_bridge.py`, unverified against real hardware |
| Wind transducer | Not wired — Weather tab's live "Wind" card is a placeholder; the Windy map is an independent forecast source, not this instrument |

## Deploying to production

Production (`/home/mikemc/dashboard_api.py`, `/var/www/dashboard/index.html`, `dashboard-api.service`) is deployed manually from this repo — never automatically, and never without explicit instruction (see `CLAUDE.md`). The backend needs one deliberate config change before it can run there: `DEBUG_MODE = False` and the prod bind (`127.0.0.1:5001`) instead of the dev defaults (`0.0.0.0:5003`, `DEBUG_MODE = True`).

#!/usr/bin/env python3
"""
Download NOAA CO-OPS tide/current station data for the Chart tab's
"Tides & Currents" layer.

Tide and current predictions are harmonic (computed from known constituents,
not measured) -- NOAA publishes them for weeks/months in advance, so unlike
live observations they can be downloaded once and read back fully offline,
same as the NCDS/ENC chart data chart_tools.py already handles that way.

Two steps, same shape as chart_tools.py's download/sync split:

  1. Station metadata (small, global, one-time):
       python3 tide_tools.py list-stations
     Fetches every NOAA tide-prediction and current-prediction station
     (id/name/lat/lon) into chart_data/tides/stations.json. No filtering --
     the index itself is only a few hundred KB.

  2. Predictions for the stations you actually care about (the heavy part):
       python3 tide_tools.py sync --near 27.70 -82.69 --radius-nm 50
       python3 tide_tools.py sync 8726520 ACT0091 ...
     For each station in range (or named explicitly), downloads 60 days of
     predictions and writes one JSON file to chart_data/tides/predictions/
     <station_id>.json. Re-running is safe -- each station's file is just
     overwritten. Re-sync manually as you cruise into new areas, same as
     re-running chart_tools.py sync for a new chart region.

dashboard_api.py's /api/tides/* routes only ever read these cached files --
no network access at request time. Live observations (when there's actually
a connection) are proxied separately by /api/tides/live, straight through to
CO-OPS, not handled by this script.
"""
import argparse
import json
import math
import os
import sys
import time
from datetime import datetime

import requests

MDAPI = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
DATAGETTER = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APP_NAME = "exit-strategy-dashboard"

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chart_data', 'tides')
STATIONS_FILE = os.path.join(OUT_DIR, 'stations.json')
PRED_DIR = os.path.join(OUT_DIR, 'predictions')
DEFAULT_SYNC_DAYS = 60


def haversine_nm(lat1, lon1, lat2, lon2):
    r_nm = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * r_nm * math.asin(math.sqrt(a))


def cmd_list_stations(args):
    os.makedirs(OUT_DIR, exist_ok=True)
    stations = []
    tide = requests.get(f"{MDAPI}/stations.json", params={'type': 'tidepredictions'}, timeout=20).json()
    for s in tide['stations']:
        stations.append({'id': s['id'], 'name': s['name'], 'lat': s['lat'], 'lon': s['lng'], 'type': 'tide'})
    # Current stations list one row per depth bin at the same location (e.g. a
    # channel with predictions at both a shallow and a deeper bin) -- keep only
    # the shallowest bin per station id, since that's the one relevant to this
    # boat and downloading/caching the others would just be wasted duplicate
    # fetches of a station id we'd overwrite anyway.
    curr_by_id = {}
    curr = requests.get(f"{MDAPI}/stations.json", params={'type': 'currentpredictions'}, timeout=20).json()
    for s in curr['stations']:
        entry = {'id': s['id'], 'name': s['name'], 'lat': s['lat'], 'lon': s['lng'], 'type': 'current', 'bin': s.get('currbin') or 1}
        existing = curr_by_id.get(s['id'])
        if existing is None or entry['bin'] < existing['bin']:
            curr_by_id[s['id']] = entry
    stations.extend(curr_by_id.values())
    with open(STATIONS_FILE, 'w') as f:
        json.dump(stations, f)
    n_tide = sum(1 for s in stations if s['type'] == 'tide')
    n_curr = sum(1 for s in stations if s['type'] == 'current')
    print(f"Wrote {len(stations)} stations ({n_tide} tide, {n_curr} current) to {STATIONS_FILE}")


def _load_stations():
    if not os.path.exists(STATIONS_FILE):
        sys.exit("No station list yet -- run 'python3 tide_tools.py list-stations' first.")
    return json.load(open(STATIONS_FILE))


def _get_json_retrying(params, tries=3, backoff=1.0):
    """NOAA's CO-OPS API is documented elsewhere in this project as
    "intermittently flaky" (see chart_tools.py's NCDS/ENC metadata-lookup
    docstring) and that turns out to be true here too, on both failure
    shapes this hits:
      - A 200 with an empty predictions list and no 'error' key, seen under
        rapid-fire sync load.
      - An explicit 'error' key ("No Predictions data was found") that looks
        like "this station has no harmonic constituents at all" but isn't
        always -- confirmed directly: a station that failed with exactly
        this error via this function returned complete, valid predictions
        on a plain curl half a minute later, no request change at all.
    So there's no reliable way to tell "permanently unsupported station"
    apart from "NOAA blipped" by the error shape alone -- both get the same
    short, flat retry (not exponential -- keeps the cost bounded for the
    ~40% of nearby stations that really are permanently unsupported)."""
    last_err = None
    for attempt in range(tries):
        try:
            r = requests.get(DATAGETTER, params=params, timeout=15)
            d = r.json()
            if 'error' in d:
                last_err = RuntimeError(d['error'])
            elif d.get('predictions') or d.get('current_predictions'):
                return d
            else:
                last_err = RuntimeError('empty response (possibly rate-limited)')
        except Exception as e:
            last_err = e
        if attempt < tries - 1:
            time.sleep(backoff)
    raise last_err


def _sync_one(station, days):
    begin = datetime.now().strftime('%Y%m%d')
    common = dict(
        application=APP_NAME, station=station['id'], begin_date=begin,
        range=days * 24, units='english', time_zone='lst_ldt', format='json',
    )
    out = {
        'id': station['id'], 'name': station['name'], 'lat': station['lat'], 'lon': station['lon'],
        'type': station['type'], 'synced_at': time.time(),
    }
    try:
        if station['type'] == 'tide':
            # 'events' (hi/lo) drives the popup's "Next High/Low" text; 'hourly'
            # (a smooth height curve) drives the mini chart. Current stations
            # don't get an hourly curve -- see the docstring note above, their
            # predictions are inherently event-based (flood/ebb/slack), not a
            # continuous height, so there's nothing equivalent to plot.
            #
            # hourly is fetched best-effort, separately from hilo: NOAA's
            # weaker "subordinate" stations (offset-only from a reference
            # station, no full harmonic model) support hilo but genuinely,
            # permanently 404-shaped-error on interval=h -- confirmed
            # directly against TEC4271 (Pass-a-Grille Beach), not a transient
            # NOAA blip. That's a real per-station capability gap, not
            # something a retry fixes, so it must not block caching the hilo
            # events, which every tide station does support.
            hilo = _get_json_retrying({**common, 'product': 'predictions', 'datum': 'MLLW', 'interval': 'hilo'})
            out['events'] = hilo.get('predictions', [])
            try:
                hourly = _get_json_retrying({**common, 'product': 'predictions', 'datum': 'MLLW', 'interval': 'h'})
                out['hourly'] = hourly.get('predictions', [])
            except Exception:
                out['hourly'] = []
        else:
            cp = _get_json_retrying({**common, 'product': 'currents_predictions', 'bin': station.get('bin', 1), 'interval': 'MAX_SLACK'})
            out['events'] = cp.get('current_predictions', {}).get('cp', [])
    except Exception as e:
        print(f"  ! {station['id']} ({station['name']}): {e}")
        return False
    with open(os.path.join(PRED_DIR, f"{station['id']}.json"), 'w') as f:
        json.dump(out, f)
    return True


def cmd_sync(args):
    os.makedirs(PRED_DIR, exist_ok=True)
    stations = _load_stations()
    if args.bbox:
        lat0, lon0, lat1, lon1 = args.bbox
        targets = [s for s in stations if lat0 <= s['lat'] <= lat1 and lon0 <= s['lon'] <= lon1]
        print(f"{len(targets)} station(s) within [{lat0},{lon0}] - [{lat1},{lon1}]")
    elif args.near:
        lat, lon = args.near
        targets = [s for s in stations if haversine_nm(lat, lon, s['lat'], s['lon']) <= args.radius_nm]
        print(f"{len(targets)} station(s) within {args.radius_nm:.0f}nm of {lat},{lon}")
    else:
        ids = set(args.station_ids)
        if not ids:
            sys.exit("Pass station ids, --near <lat> <lon> [--radius-nm N], or --bbox <min_lat> <min_lon> <max_lat> <max_lon>.")
        targets = [s for s in stations if s['id'] in ids]
        missing = ids - {s['id'] for s in targets}
        if missing:
            print(f"Unknown station id(s), skipping: {', '.join(sorted(missing))}")
    ok = 0
    for i, s in enumerate(targets):
        if i > 0:
            time.sleep(0.3)  # be a courteous bulk caller -- this is what triggered the empty-response retries above
        if _sync_one(s, args.days):
            ok += 1
            print(f"  synced {s['id']} ({s['type']}) {s['name']}")
    print(f"Synced {ok}/{len(targets)} station(s), {args.days} days -> {PRED_DIR}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('list-stations', help='Fetch the full tide/current station index (run this first)')
    sp = sub.add_parser('sync', help='Download predictions for specific stations or everything within a radius')
    sp.add_argument('station_ids', nargs='*', help='Explicit station IDs, e.g. 8726520 (tide) or ACT0091 (current)')
    sp.add_argument('--near', nargs=2, type=float, metavar=('LAT', 'LON'), help='Sync every station within --radius-nm of this point')
    sp.add_argument('--radius-nm', type=float, default=50.0, help='Radius in nautical miles for --near (default 50)')
    sp.add_argument('--bbox', nargs=4, type=float, metavar=('MIN_LAT', 'MIN_LON', 'MAX_LAT', 'MAX_LON'), help='Sync every station within this box -- for covering a whole cruising area in one run rather than one --near circle at a time')
    sp.add_argument('--days', type=int, default=DEFAULT_SYNC_DAYS, help=f'Days of predictions to cache (default {DEFAULT_SYNC_DAYS})')
    args = ap.parse_args()
    {'list-stations': cmd_list_stations, 'sync': cmd_sync}[args.cmd](args)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""wind_grid.py -- Fetch NOAA GFS 10m wind (UGRD/VGRD) for a fixed Gulf/Caribbean
bounding box, across a 5-day forecast window, and convert each forecast hour
to the JSON format leaflet-velocity expects -- so the Chart tab can render an
animated wind-particle overlay with a time control for "wind a few days out",
not just right now (see the "Wind velocity overlay (siloed addition)" block
in dashboard_api.py).

Kept deliberately standalone -- invoked via subprocess with an absolute path,
never imported -- so it works identically regardless of whether dashboard_api.py
is running from dashboard-dev (port 5003) or its deployed copy at
/home/mikemc/dashboard_api.py (port 5001). Same reasoning ochart_tools.py and
chart_tools.py are standalone CLI tools rather than imported modules.

GFS is NOAA's *global* forecast model (unlike the NWS point-forecast/alerts
API dashboard_api.py already uses for the Weather tab, which is US-only) --
free, no API key, no signup, covers international waters fine. New runs
every 6h (00/06/12/18 UTC), published roughly 3-5h after the nominal cycle
time, hence the lookback across several candidate cycles below.

Output layout, all in <out_dir>:
    manifest.json           {cycle, generatedAt, forecasts: [{hour, validTime}, ...]}
    wind_<cycle>_f<hhh>.json   one leaflet-velocity JSON per forecast hour

Usage:
    python3 wind_grid.py fetch <out_dir>             -- fetch whatever the
                                                         latest available GFS
                                                         cycle is, all forecast
                                                         hours in FORECAST_HOURS
    python3 wind_grid.py fetch-if-stale <out_dir>    -- skip entirely (no
                                                         network calls at all)
                                                         if <out_dir> already
                                                         holds the latest
                                                         available cycle
"""
import sys
import os
import json
import glob
import tempfile
from datetime import datetime, timedelta, timezone

import requests
from osgeo import gdal

gdal.UseExceptions()

# Gulf of Mexico through the Lesser Antilles/BVI -- covers the boat's home
# waters (Tampa) and its Caribbean cruising ground with room to spare.
BBOX = {'west': -95, 'east': -55, 'north': 33, 'south': 5}

# 5 days out at 6h steps -- GFS 0.25 publishes hourly to f120 and beyond, but
# 6h is plenty of resolution for passage planning and keeps a full refresh to
# ~21 requests rather than 121.
FORECAST_HOURS = list(range(0, 121, 6))

NOMADS_URL = (
    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    "?file=gfs.t{cycle:02d}z.pgrb2.0p25.f{hour:03d}"
    "&lev_10_m_above_ground=on&var_UGRD=on&var_VGRD=on"
    "&subregion=&leftlon={west}&rightlon={east}&toplat={north}&bottomlat={south}"
    "&dir=%2Fgfs.{date}%2F{cycle:02d}%2Fatmos"
)


def _candidate_cycles(n=6):
    """(date_str, cycle, run_time) tuples, most recent nominal cycle first --
    GFS runs at 00/06/12/18 UTC but isn't published until ~3-5h later, so
    "most recent nominal cycle" often isn't actually ready yet; the caller
    tries these in order and takes the first whose f000 responds with real
    data (a full forecast run either exists or it doesn't -- f000 is the
    cheapest hour to probe with)."""
    now = datetime.now(timezone.utc)
    out = []
    t = now
    while len(out) < n:
        cycle = (t.hour // 6) * 6
        run_time = t.replace(hour=cycle, minute=0, second=0, microsecond=0)
        out.append((run_time.strftime('%Y%m%d'), cycle, run_time))
        t = run_time - timedelta(minutes=1)  # step back to just before this cycle's boundary
    return out


def _fetch_grib(date_str, cycle, hour, bbox=BBOX):
    url = NOMADS_URL.format(cycle=cycle, hour=hour, date=date_str, **bbox)
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200 or len(resp.content) < 1000 or not resp.content.startswith(b'GRIB'):
        return None
    return resp.content


def _grib_to_velocity_json(grib_bytes, valid_time, forecast_hour):
    with tempfile.NamedTemporaryFile(suffix='.grib2', delete=False) as f:
        f.write(grib_bytes)
        grib_path = f.name
    try:
        ds = gdal.Open(grib_path)
        gt = ds.GetGeoTransform()
        nx, ny = ds.RasterXSize, ds.RasterYSize
        # windy.js reads la1/lo1 as the grid's NW corner and dy as a positive
        # magnitude (it walks south by subtracting row*dy itself) -- GDAL's
        # geotransform gives dy negative (pixel height, since row 0 is north),
        # hence abs() here rather than passing gt[5] straight through.
        header_base = {
            'la1': gt[3], 'lo1': gt[0], 'dx': abs(gt[1]), 'dy': abs(gt[5]),
            'nx': nx, 'ny': ny,
            'la2': gt[3] + gt[5] * ny, 'lo2': gt[0] + gt[1] * nx,
            # refTime + forecastTime (windy.js adds forecastTime hours to
            # refTime itself), rather than passing valid_time directly, so
            # windy.js's own date math is what's authoritative -- but
            # valid_time is what fetch()/manifest.json hand back to the
            # frontend for the time-picker labels.
            'refTime': valid_time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'forecastTime': 0,
        }
        result = []
        # (GRIB_ELEMENT, GRIB2 parameterCategory, parameterNumber) -- standard
        # WMO GRIB2 Table 4.2 codes for u/v-component of wind, category 2
        # ("Momentum"). leaflet-velocity only uses these two for its internal
        # cache key / color-recipe lookup, not for locating the band itself.
        for element, cat, num in [('UGRD', 2, 2), ('VGRD', 2, 3)]:
            band = None
            for i in range(1, ds.RasterCount + 1):
                b = ds.GetRasterBand(i)
                if b.GetMetadataItem('GRIB_ELEMENT') == element:
                    band = b
                    break
            if band is None:
                raise RuntimeError(f'{element} band not found in GRIB response')
            data = band.ReadAsArray().astype(float).flatten().tolist()
            header = dict(header_base, parameterCategory=cat, parameterNumber=num)
            result.append({'header': header, 'data': data})
        return result
    finally:
        os.unlink(grib_path)


def fetch(out_dir, only_if_stale=False):
    os.makedirs(out_dir, exist_ok=True)
    manifest_path = os.path.join(out_dir, 'manifest.json')
    have_cycle = None
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            have_cycle = json.load(f).get('cycle')

    for date_str, cycle, run_time in _candidate_cycles():
        tag = f'{date_str}{cycle:02d}'
        if only_if_stale and have_cycle is not None and have_cycle >= tag:
            print(f'Already have {have_cycle} (>= {tag}) -- nothing newer to fetch')
            return False

        # Probe f000 first -- cheap way to know whether this cycle exists at
        # all before spending 20 more requests on hours that won't either.
        probe = _fetch_grib(date_str, cycle, 0)
        if probe is None:
            print(f'GFS {tag}z not available yet, trying earlier cycle...')
            continue

        forecasts = []
        for hour in FORECAST_HOURS:
            grib = probe if hour == 0 else _fetch_grib(date_str, cycle, hour)
            if grib is None:
                print(f'  f{hour:03d} not available, skipping (later hours may still be missing too)')
                continue
            valid_time = run_time + timedelta(hours=hour)
            velocity_json = _grib_to_velocity_json(grib, valid_time, hour)
            out_path = os.path.join(out_dir, f'wind_{tag}_f{hour:03d}.json')
            tmp_out = out_path + '.tmp'
            with open(tmp_out, 'w') as f:
                json.dump(velocity_json, f)
            os.replace(tmp_out, out_path)  # atomic -- readers never see a partial write
            forecasts.append({'hour': hour, 'validTime': valid_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
            print(f'  wrote f{hour:03d} ({valid_time.strftime("%a %H:%MZ")})')

        if not forecasts:
            print(f'GFS {tag}z probed OK but no forecast hours actually converted -- trying earlier cycle...')
            continue

        manifest = {
            'cycle': tag,
            'generatedAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'forecasts': forecasts,
        }
        tmp_manifest = manifest_path + '.tmp'
        with open(tmp_manifest, 'w') as f:
            json.dump(manifest, f)
        os.replace(tmp_manifest, manifest_path)

        # Drop the previous cycle's files now that the new one is fully
        # written -- otherwise this grows by ~15MB every 6h forever.
        for path in glob.glob(os.path.join(out_dir, 'wind_*.json')):
            if os.path.basename(path) not in {f'wind_{tag}_f{h:03d}.json' for h in FORECAST_HOURS}:
                os.remove(path)

        print(f'Wrote manifest + {len(forecasts)} forecast hours to {out_dir} from GFS {tag}z')
        return True

    print('No GFS cycle available in the lookback window')
    return False


def fetch_region(out_path, cycle, hour, bbox):
    """On-demand single-hour, single-region fetch -- for whatever part of the
    world the Chart tab is currently panned to, outside BBOX above (see the
    "Wind velocity overlay" block in dashboard_api.py's /api/wind/velocity_region).
    Unlike fetch(), this doesn't probe for the latest cycle -- <cycle> is
    expected to be one dashboard_api.py already knows is good, from
    manifest.json (written by a fetch()/fetch-if-stale call elsewhere)."""
    date_str, cyc = cycle[:8], int(cycle[8:])
    run_time = datetime.strptime(cycle, '%Y%m%d%H').replace(tzinfo=timezone.utc)
    grib = _fetch_grib(date_str, cyc, hour, bbox=bbox)
    if grib is None:
        print(f'GFS {cycle}z f{hour:03d} not available for this region')
        return False
    valid_time = run_time + timedelta(hours=hour)
    velocity_json = _grib_to_velocity_json(grib, valid_time, hour)
    tmp_out = out_path + '.tmp'
    with open(tmp_out, 'w') as f:
        json.dump(velocity_json, f)
    os.replace(tmp_out, out_path)
    print(f'Wrote {out_path} from GFS {cycle}z f{hour:03d}, bbox={bbox}')
    return True


if __name__ == '__main__':
    if len(sys.argv) >= 3 and sys.argv[1] in ('fetch', 'fetch-if-stale'):
        ok = fetch(sys.argv[2], only_if_stale=(sys.argv[1] == 'fetch-if-stale'))
        sys.exit(0 if ok else 1)
    if len(sys.argv) == 9 and sys.argv[1] == 'fetch-region':
        _, _, out_path, cycle, hour, west, east, north, south = sys.argv
        region_bbox = {'west': float(west), 'east': float(east), 'north': float(north), 'south': float(south)}
        ok = fetch_region(out_path, cycle, int(hour), region_bbox)
        sys.exit(0 if ok else 1)
    print(__doc__)
    print("    python3 wind_grid.py fetch-region <out.json> <cycle> <hour> <west> <east> <north> <south>")
    sys.exit(1)

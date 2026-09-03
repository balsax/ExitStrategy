#!/usr/bin/env python3
"""ochart_tools.py - Turn decrypted O-charts OSENC (.oesu) files into the same
kind of raster-tile-base + clickable-vector-overlay data chart_tools.py already
produces for NOAA charts, so they can be served by the siloed /api/charts/ocharts/*
routes in dashboard_api.py (see the "O-Charts (siloed addition)" block there) and
the existing /api/charts/cells + /api/charts/<cell>/<layer> routes (unchanged,
reused as-is for the vector side).

Kept deliberately separate from chart_tools.py / dashboard_api.py's existing NCDS
code -- see the project's plan file for why (fancy-finding-reef.md): easy revert,
zero risk to the working NOAA layer.

Pipeline, per chart:
    render          .oesu -> georeferenced GeoTIFF (PIL raster + gdal_translate)
    extract-vectors .oesu -> chart_data/processed/OC<id>/*.geojson + meta.json

Then, across a set of rendered GeoTIFFs covering one area:
    mosaic  <name> <tif...>   -> chart_data/ocharts_src/<name>.vrt (coarsest-scale
                                  chart first, most-detailed last -- last source
                                  wins on overlapping pixels)
    tile    <vrt> <mbtiles>   -> gdal2tiles.py + pack into an MBTiles matching
                                  the exact schema dashboard_api.py's NCDS reader
                                  expects (TMS row numbering)

Convenience wrapper:
    batch <out_dir> <chart.oesu>...   -> render + extract-vectors for each chart

Requires gdal-bin (gdal_translate, gdalbuildvrt, gdal2tiles.py) and PIL/numpy,
all already present in this environment.

Usage:
    python3 ochart_tools.py render <chart.oesu> <out_dir>
    python3 ochart_tools.py extract-vectors <chart.oesu> <processed_dir>
    python3 ochart_tools.py batch <out_dir> <chart.oesu> [chart.oesu ...]
    python3 ochart_tools.py mosaic <name> <out_dir> <tif> [tif ...]
    python3 ochart_tools.py tile <vrt_path> <mbtiles_path> <minzoom> <maxzoom>
"""

import io
import json
import math
import os
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile

import numpy as np
from PIL import Image, ImageDraw

import osenc_parse as op

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# render: OSENC -> georeferenced GeoTIFF raster
# ---------------------------------------------------------------------------

def _hex_rgba(hex_color, alpha=255):
    hex_color = hex_color.lstrip('#')
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
    return (r, g, b, alpha)


def _target_long_side(native_scale):
    """Heuristic pixel budget for a chart's longer geographic dimension,
    scaled down for coarser (larger native_scale number) charts so a
    wide-area overview chart doesn't render a huge mostly-empty canvas.
    Tunable -- revisit after visually checking a few charts in the browser."""
    if not native_scale:
        native_scale = 25000
    return max(600, min(6000, round(20_000_000 / native_scale)))


def render_chart_raster(chart):
    """Rasterize a parsed osenc_parse.Chart's polygons/lines to an RGBA
    image using osenc_parse.STYLE, clipped to its coverage polygon when
    decode_coverage() found one (falls back to the full bounding rectangle
    otherwise). Returns (PIL.Image RGBA, (west, north, east, south))."""
    if not chart.extent:
        raise ValueError(f"{chart.name!r}: no CELL_EXTENT_RECORD, can't rasterize")
    nw_lat, nw_lon, se_lat, se_lon = chart.extent
    west, east = nw_lon, se_lon
    north, south = nw_lat, se_lat
    lon_span, lat_span = east - west, north - south
    if lon_span <= 0 or lat_span <= 0:
        raise ValueError(f"{chart.name!r}: degenerate extent {chart.extent}")

    lat_mid = (north + south) / 2.0
    aspect = (lon_span * math.cos(math.radians(lat_mid))) / lat_span
    long_side = _target_long_side(chart.native_scale)
    if aspect >= 1:
        width, height = long_side, max(1, round(long_side / aspect))
    else:
        width, height = max(1, round(long_side * aspect)), long_side

    def to_px(lat, lon):
        return ((lon - west) / lon_span * width, (north - lat) / lat_span * height)

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Coverage-polygon fill first (the "sea" backdrop), same color chartViewer.py
    # uses for its axes background -- only within real coverage, so uncharted
    # parts of the bounding rectangle stay transparent for the mosaic step.
    sea_rgba = _hex_rgba('#dff1f7')
    if chart.coverage_polygons:
        for ring in chart.coverage_polygons:
            pts = [to_px(lat, lon) for lat, lon in ring]
            if len(pts) >= 3:
                draw.polygon(pts, fill=sea_rgba)
    else:
        draw.rectangle([0, 0, width, height], fill=sea_rgba)

    by_z = {}
    for feat in chart.features:
        style = op.STYLE.get(feat.type_name)
        if not style:
            continue
        if feat.polygons and style["kind"] in ("poly", "poly_outline"):
            for ring in feat.polygons:
                if len(ring) < 3:
                    continue
                by_z.setdefault(style["z"], []).append(
                    ("poly", [to_px(lat, lon) for lat, lon in ring], style))
        if feat.lines and style["kind"] == "line":
            for line in feat.lines:
                if len(line) < 2:
                    continue
                by_z.setdefault(style["z"], []).append(
                    ("line", [to_px(lat, lon) for lat, lon in line], style))

    for z in sorted(by_z):
        for kind, pts, style in by_z[z]:
            if kind == "poly":
                if style["kind"] == "poly" and style.get("face"):
                    draw.polygon(pts, fill=_hex_rgba(style["face"]))
                if style.get("edge"):
                    draw.line(pts + [pts[0]], fill=_hex_rgba(style["edge"]), width=1)
            elif kind == "line":
                draw.line(pts, fill=_hex_rgba(style["color"]),
                          width=max(1, round(style["width"])))

    # Clip everything to the coverage mask (belt-and-suspenders: a feature
    # polygon could technically straddle the coverage edge).
    if chart.coverage_polygons:
        mask = Image.new("L", (width, height), 0)
        mdraw = ImageDraw.Draw(mask)
        for ring in chart.coverage_polygons:
            pts = [to_px(lat, lon) for lat, lon in ring]
            if len(pts) >= 3:
                mdraw.polygon(pts, fill=255)
        arr = np.array(img)
        mask_arr = np.array(mask).astype(np.uint16)
        arr[..., 3] = (arr[..., 3].astype(np.uint16) * mask_arr // 255).astype(np.uint8)
        img = Image.fromarray(arr, "RGBA")

    return img, (west, north, east, south)


def render(oesu_path, out_dir):
    chart = op.parse_chart(oesu_path)
    op.build_all_geometry(chart)
    op.decode_coverage(chart)

    img, (west, north, east, south) = render_chart_raster(chart)

    chart_id = os.path.splitext(os.path.basename(oesu_path))[0]
    os.makedirs(out_dir, exist_ok=True)
    png_path = os.path.join(out_dir, f'{chart_id}.png')
    tif_path = os.path.join(out_dir, f'{chart_id}.tif')
    img.save(png_path)

    result = subprocess.run(
        ['gdal_translate', '-of', 'GTiff', '-a_srs', 'EPSG:4326',
         '-a_ullr', str(west), str(north), str(east), str(south),
         '-co', 'ALPHA=YES',
         '-mo', f'NATIVE_SCALE={chart.native_scale}',
         '-mo', f'CHART_NAME={chart.name}',
         png_path, tif_path],
        capture_output=True, text=True,
    )
    os.remove(png_path)
    if result.returncode != 0:
        raise RuntimeError(f'gdal_translate failed for {oesu_path}:\n{result.stderr}')

    print(f'{chart_id}: {chart.name!r} scale=1:{chart.native_scale} '
          f'{img.width}x{img.height}px coverage_rings={len(chart.coverage_polygons)} '
          f'-> {tif_path}')
    return tif_path


# ---------------------------------------------------------------------------
# extract-vectors: OSENC -> GeoJSON, same layout the NOAA ENC pipeline uses
# ---------------------------------------------------------------------------

def _geojson_feature(feat, layer):
    if layer == 'SOUNDG':
        features = []
        for lat, lon, depth in feat.multipoint:
            features.append({
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [lon, lat, depth]},
                'properties': {'DEPTH': depth},
            })
        return features
    if feat.point:
        lat, lon = feat.point
        geom = {'type': 'Point', 'coordinates': [lon, lat]}
    elif feat.polygons:
        rings = [[[lon, lat] for lat, lon in ring] for ring in feat.polygons if len(ring) >= 3]
        if not rings:
            return []
        geom = {'type': 'Polygon', 'coordinates': rings} if len(rings) == 1 \
            else {'type': 'MultiPolygon', 'coordinates': [[r] for r in rings]}
    elif feat.lines:
        lines = [[[lon, lat] for lat, lon in line] for line in feat.lines if len(line) >= 2]
        if not lines:
            return []
        geom = {'type': 'LineString', 'coordinates': lines[0]} if len(lines) == 1 \
            else {'type': 'MultiLineString', 'coordinates': lines}
    else:
        return []
    props = dict(feat.attributes)
    if 'Colour' in props:  # match NOAA/S-57's raw COLOUR field name -- see CHART_LAYER_STYLERS's chartMarkColor()
        props['COLOUR'] = props.pop('Colour')
    return [{'type': 'Feature', 'geometry': geom, 'properties': props}]


def extract_vectors(oesu_path, processed_dir):
    chart = op.parse_chart(oesu_path)
    op.build_all_geometry(chart)

    chart_id = os.path.splitext(os.path.basename(oesu_path))[0]
    cell = ''.join(ch for ch in chart_id if ch.isalnum())  # chart_id already starts with "OC"
    out_dir = os.path.join(processed_dir, cell)
    os.makedirs(out_dir, exist_ok=True)

    by_layer = {}
    for feat in chart.features:
        layer = op.TYPE_NAME_TO_S57_LAYER.get(feat.type_name)
        if not layer:
            continue
        by_layer.setdefault(layer, []).extend(_geojson_feature(feat, layer))

    written = []
    for layer, features in by_layer.items():
        if not features:
            continue
        out_path = os.path.join(out_dir, f'{layer}.geojson')
        with open(out_path, 'w') as f:
            json.dump({'type': 'FeatureCollection', 'features': features}, f)
        written.append((layer, len(features)))

    bounds = None
    if chart.extent:
        nw_lat, nw_lon, se_lat, se_lon = chart.extent
        bounds = {'south': se_lat, 'north': nw_lat, 'west': nw_lon, 'east': se_lon}
    meta = {'cell': cell, 'layers': dict(written), 'bounds': bounds,
            'source': 'ochart', 'name': chart.name, 'native_scale': chart.native_scale}
    with open(os.path.join(out_dir, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)

    print(f'{chart_id} -> {cell}: {len(written)} layers -> {out_dir}')
    for layer, count in written:
        print(f'   {layer}: {count} features')
    return meta


# ---------------------------------------------------------------------------
# batch: render + extract-vectors for a list of charts
# ---------------------------------------------------------------------------

def batch(raster_out_dir, processed_dir, oesu_paths):
    tifs = []
    for path in oesu_paths:
        try:
            tifs.append(render(path, raster_out_dir))
        except Exception as e:
            print(f'  RENDER FAILED {path}: {e}')
        try:
            extract_vectors(path, processed_dir)
        except Exception as e:
            print(f'  VECTORS FAILED {path}: {e}')
    return tifs


# ---------------------------------------------------------------------------
# mosaic: coarsest-scale-first / finest-scale-last VRT, most detail wins
# ---------------------------------------------------------------------------

def _native_scale_of(tif_path):
    result = subprocess.run(['gdalinfo', '-json', tif_path], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    info = json.loads(result.stdout)
    val = info.get('metadata', {}).get('', {}).get('NATIVE_SCALE')
    return int(val) if val else None


def mosaic(name, out_dir, tif_paths):
    scored = [(_native_scale_of(p) or 0, p) for p in tif_paths]
    scored.sort(key=lambda t: t[0], reverse=True)  # coarsest (largest scale number) first
    ordered = [p for _, p in scored]

    os.makedirs(out_dir, exist_ok=True)
    vrt_path = os.path.join(out_dir, f'{name}.vrt')
    result = subprocess.run(
        ['gdalbuildvrt', '-resolution', 'highest', vrt_path, *ordered],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f'gdalbuildvrt failed:\n{result.stderr}')
    print(f'{name}: {len(ordered)} charts -> {vrt_path}')
    for scale, p in scored:
        print(f'   1:{scale or "?"}  {os.path.basename(p)}')
    return vrt_path


# ---------------------------------------------------------------------------
# tile: VRT -> gdal2tiles.py XYZ/TMS directory -> packed MBTiles
# ---------------------------------------------------------------------------

MBTILES_SCHEMA = """
CREATE TABLE metadata (name text, value text);
CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);
CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);
"""


def _pack_mbtiles(tiles_dir, mbtiles_path, bounds, minzoom, maxzoom):
    if os.path.exists(mbtiles_path):
        os.remove(mbtiles_path)
    conn = sqlite3.connect(mbtiles_path)
    conn.executescript(MBTILES_SCHEMA)
    meta = {
        'name': os.path.splitext(os.path.basename(mbtiles_path))[0],
        'format': 'png',
        'minzoom': str(minzoom),
        'maxzoom': str(maxzoom),
        'bounds': f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
        'type': 'baselayer',
    }
    conn.executemany('INSERT INTO metadata (name, value) VALUES (?, ?)', meta.items())

    count = 0
    # gdal2tiles.py's default (non-XYZ) output directory is already TMS-numbered
    # -- dashboard_api.py's NCDS reader assumes MBTiles rows are TMS-style and
    # does its own XYZ<->TMS flip on read, so this is a direct copy, no flip here.
    for z in sorted(os.listdir(tiles_dir)):
        z_dir = os.path.join(tiles_dir, z)
        if not z.isdigit() or not os.path.isdir(z_dir):
            continue
        for x in sorted(os.listdir(z_dir)):
            x_dir = os.path.join(z_dir, x)
            if not x.isdigit() or not os.path.isdir(x_dir):
                continue
            for fname in sorted(os.listdir(x_dir)):
                if not fname.endswith('.png'):
                    continue
                y = fname[:-4]
                if not y.isdigit():
                    continue
                with open(os.path.join(x_dir, fname), 'rb') as f:
                    data = f.read()
                conn.execute(
                    'INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)',
                    (int(z), int(x), int(y), data))
                count += 1
    conn.commit()
    conn.close()
    print(f'  packed {count} tiles -> {mbtiles_path}')


def _vrt_bounds(vrt_path):
    result = subprocess.run(['gdalinfo', '-json', vrt_path], capture_output=True, text=True)
    info = json.loads(result.stdout)
    w, s, e, n = info['cornerCoordinates']['lowerLeft'][0], info['cornerCoordinates']['lowerLeft'][1], \
                 info['cornerCoordinates']['upperRight'][0], info['cornerCoordinates']['upperRight'][1]
    return (w, s, e, n)


def tile(vrt_path, mbtiles_path, minzoom, maxzoom):
    bounds = _vrt_bounds(vrt_path)
    with tempfile.TemporaryDirectory(prefix='ochart_tiles_') as tmp_dir:
        result = subprocess.run(
            ['gdal2tiles.py', '-p', 'mercator', '-z', f'{minzoom}-{maxzoom}',
             '-r', 'average', '-w', 'none', '--processes=1',
             vrt_path, tmp_dir],
        )
        if result.returncode != 0:
            raise RuntimeError(f'gdal2tiles.py failed with exit code {result.returncode}')
        _pack_mbtiles(tmp_dir, mbtiles_path, bounds, minzoom, maxzoom)
    print(f'{vrt_path} -> {mbtiles_path}  bounds={bounds} z{minzoom}-{maxzoom}')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    for tool in ('gdal_translate', 'gdalbuildvrt', 'gdal2tiles.py', 'gdalinfo'):
        if shutil.which(tool) is None:
            print(f'{tool} not found -- install it first: sudo apt install -y gdal-bin')
            sys.exit(1)

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd, args = sys.argv[1], sys.argv[2:]

    if cmd == 'render':
        render(args[0], args[1])
    elif cmd == 'extract-vectors':
        extract_vectors(args[0], args[1])
    elif cmd == 'batch':
        batch(os.path.join(args[0], 'ocharts_src'), os.path.join(args[0], 'processed'), args[1:])
    elif cmd == 'mosaic':
        mosaic(args[0], args[1], args[2:])
    elif cmd == 'tile':
        tile(args[0], args[1], int(args[2]), int(args[3]))
    else:
        print(f'Unknown command: {cmd}')
        print(__doc__)
        sys.exit(1)

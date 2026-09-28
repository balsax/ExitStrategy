#!/usr/bin/env python3
"""
charts_to_mbtiles.py - Render decrypted OpenCPN .oesu/.oesenc charts into a
standard MBTiles file (the same SQLite tile-pyramid container format NOAA's
Chart Display Service ships as downloadable MBTiles).

This does NOT replicate NOAA's actual chart symbology/cartography -- that's
a proprietary rendering pipeline. It draws chart features (land, depth
areas, coastlines, soundings, aids to navigation) in a simplified
paper-chart-like style, using the OSENC binary parser ported from
hornang's oesenc C++ library, and packages the result as real MBTiles
(same file format, own rendering).

Usage:
    python charts_to_mbtiles.py <chart_dir> <output.mbtiles>
    python charts_to_mbtiles.py <chart_dir> <output.mbtiles> --min-zoom 12 --max-zoom 16
    python charts_to_mbtiles.py <chart_dir> <output.mbtiles> --limit 10   (test run)

Requires: matplotlib
"""

import argparse
import glob
import io
import math
import os
import sqlite3
import struct
import sys
import time
from collections import namedtuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon

# ---------------------------------------------------------------------------
# OSENC record parsing (ported from hornang/oesenc C++ library)
# ---------------------------------------------------------------------------

HEADER_SENC_VERSION = 1
HEADER_CELL_NAME = 2
HEADER_CELL_PUBLISHDATE = 3
HEADER_CELL_EDITION = 4
HEADER_CELL_UPDATEDATE = 5
HEADER_CELL_UPDATE = 6
HEADER_CELL_NATIVESCALE = 7
HEADER_CELL_SENCCREATEDATE = 8
HEADER_CELL_SOUNDINGDATUM = 9

FEATURE_ID_RECORD = 64
FEATURE_ATTRIBUTE_RECORD = 65

FEATURE_GEOMETRY_RECORD_POINT = 80
FEATURE_GEOMETRY_RECORD_LINE = 81
FEATURE_GEOMETRY_RECORD_AREA = 82
FEATURE_GEOMETRY_RECORD_MULTIPOINT = 83
FEATURE_GEOMETRY_RECORD_AREA_EXT = 84

VECTOR_EDGE_NODE_TABLE_EXT_RECORD = 85
VECTOR_CONNECTED_NODE_TABLE_EXT_RECORD = 86

VECTOR_EDGE_NODE_TABLE_RECORD = 96
VECTOR_CONNECTED_NODE_TABLE_RECORD = 97

CELL_COVR_RECORD = 98
CELL_NOCOVR_RECORD = 99
CELL_EXTENT_RECORD = 100
CELL_TXTDSC_INFO_FILE_RECORD = 101

SERVER_STATUS_RECORD = 200

TYPE_CODES = {
    1: "AdministrationArea", 4: "AnchorageArea", 6: "BeaconIsolatedDanger",
    9: "Beacon", 7: "BeaconLateral", 11: "Bridge", 13: "BuiltUpArea",
    17: "BuoyLateral", 21: "CableOverhead", 22: "Canal", 27: "CautionArea",
    30: "CoastLine", 33: "ControlPoint", 42: "DepthArea", 43: "DepthContour",
    50: "CartographicLine", 69: "Lake", 71: "LandArea", 73: "LandRegion",
    74: "Landmark", 75: "Light", 85: "NavigationLine", 86: "Obstruction",
    90: "Pile", 91: "PilotBoardingPlace", 94: "Pipeline", 95: "Pontoon",
    106: "Railway", 109: "RecommendedTrack", 112: "RestrictedArea",
    114: "River", 116: "Road", 119: "SeaArea", 121: "SeabedArea",
    122: "ShorelineConstruction", 129: "Sounding",
    132: "StraightLineTerritorialSeaBaseline", 135: "TerritorialSeaArea",
    153: "UnderwaterRock", 154: "UnsurveyedArea", 159: "Wreck",
    302: "Coverage", 306: "NavigationalSystemOfMarks", 308: "QualityOfData",
}

ATTRIBUTE_CODES = {
    2: "BeaconShape", 4: "BuoyShape", 18: "CategoryOfCoverage",
    36: "CategoryOfLateralMark", 57: "CategoryOfRoad",
    66: "CategoryOfSpecialPurposeMark", 75: "Colour", 87: "DepthValue1",
    95: "Height", 107: "LightCharacteristic", 109: "MarkNavigationalSystem",
    113: "NatureOfSurface", 116: "ObjectName", 133: "ScaleMin",
    141: "SignalGroup", 142: "SignalPeriod", 147: "SourceDate",
    148: "SourceIndication", 149: "Status", 178: "ValueOfNominalRange",
    179: "ValueOfSounding", 187: "WaterLevelEffect",
}

LineElement = namedtuple("LineElement", ["start_node", "edge_vector", "end_node", "direction"])


class Cursor:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def remaining(self):
        return len(self.data) - self.pos

    def read(self, n):
        b = self.data[self.pos:self.pos + n]
        self.pos += n
        return b

    def u8(self):
        v = struct.unpack_from("<B", self.data, self.pos)[0]
        self.pos += 1
        return v

    def u16(self):
        v = struct.unpack_from("<H", self.data, self.pos)[0]
        self.pos += 2
        return v

    def u32(self):
        v = struct.unpack_from("<I", self.data, self.pos)[0]
        self.pos += 4
        return v

    def i32(self):
        v = struct.unpack_from("<i", self.data, self.pos)[0]
        self.pos += 4
        return v

    def f32(self):
        v = struct.unpack_from("<f", self.data, self.pos)[0]
        self.pos += 4
        return v

    def f64(self):
        v = struct.unpack_from("<d", self.data, self.pos)[0]
        self.pos += 8
        return v

    def cstr(self, n):
        raw = self.read(n)
        return raw.split(b"\x00", 1)[0].decode("latin-1", errors="replace")


WGS84_SEMIMAJOR_AXIS_M = 6378137.0
MERCATOR_K0 = 0.9996
DEG = math.pi / 180.0


def from_simple_mercator(x, y, ref_lat, ref_lon):
    z = WGS84_SEMIMAJOR_AXIS_M * MERCATOR_K0
    s0 = math.sin(ref_lat * DEG)
    y0 = 0.5 * math.log((1 + s0) / (1 - s0)) * z
    lat = (2.0 * math.atan(math.exp((y0 + y) / z)) - math.pi / 2.0) / DEG
    lon = ref_lon + (x / (DEG * z))
    return lat, lon


class Feature:
    def __init__(self, type_code):
        self.type_code = type_code
        self.type_name = TYPE_CODES.get(type_code, "Unknown")
        self.attributes = {}
        self.point = None
        self.multipoint = []
        self.line_elements = []
        self.polygon_elements = []
        self.lines = []
        self.polygons = []


class Chart:
    def __init__(self, path):
        self.path = path
        self.name = ""
        self.version = 0
        self.native_scale = 0
        self.extent = None  # (nw_lat, nw_lon, se_lat, se_lon)
        self.features = []
        self.vector_edges = {}
        self.connected_nodes = {}
        self.min_zoom = None
        self.max_zoom = None

    def center(self):
        if not self.extent:
            return 0.0, 0.0
        nw_lat, nw_lon, se_lat, se_lon = self.extent
        return (nw_lat + se_lat) / 2.0, (nw_lon + se_lon) / 2.0


def parse_chart(path):
    """Parse a decrypted .oesu/.oesenc OSENC v201 file into a Chart object."""
    with open(path, "rb") as f:
        data = f.read()

    cur = Cursor(data)
    chart = Chart(path)
    current_feature = None

    while cur.remaining() >= 6:
        record_type = cur.u16()
        record_length = cur.u32()
        if record_type == 0:
            break
        payload_len = record_length - 6
        if payload_len < 0 or cur.remaining() < payload_len:
            break
        payload = Cursor(cur.read(payload_len))

        if record_type == HEADER_SENC_VERSION and payload_len == 2:
            chart.version = payload.u16()
        elif record_type == HEADER_CELL_NAME:
            chart.name = payload.cstr(payload_len)
        elif record_type == HEADER_CELL_NATIVESCALE and payload_len == 4:
            chart.native_scale = payload.u32()
        elif record_type == CELL_EXTENT_RECORD and payload_len == 64:
            sw_lat, sw_lon = payload.f64(), payload.f64()
            nw_lat, nw_lon = payload.f64(), payload.f64()
            ne_lat, ne_lon = payload.f64(), payload.f64()
            se_lat, se_lon = payload.f64(), payload.f64()
            chart.extent = (nw_lat, nw_lon, se_lat, se_lon)
        elif record_type == FEATURE_ID_RECORD:
            type_code = payload.u16()
            payload.u16()
            payload.u8()
            current_feature = Feature(type_code)
            chart.features.append(current_feature)
        elif record_type == FEATURE_ATTRIBUTE_RECORD and current_feature is not None and payload_len >= 3:
            attr_code = payload.u16()
            value_type = payload.u8()
            name = ATTRIBUTE_CODES.get(attr_code)
            val = None
            if value_type == 0 and payload.remaining() >= 4:
                val = payload.u32()
            elif value_type == 2 and payload.remaining() >= 8:
                val = payload.f64()
            elif value_type == 4:
                val = payload.cstr(payload.remaining())
            if name and val is not None:
                current_feature.attributes[name] = val
        elif record_type == FEATURE_GEOMETRY_RECORD_POINT and current_feature is not None and payload_len == 16:
            current_feature.point = (payload.f64(), payload.f64())
        elif record_type == FEATURE_GEOMETRY_RECORD_MULTIPOINT and current_feature is not None and payload_len >= 36:
            for _ in range(4):
                payload.f64()
            point_count = payload.u32()
            ref_lat, ref_lon = chart.center()
            pts = []
            for _ in range(point_count):
                easting, northing, depth = payload.f32(), payload.f32(), payload.f32()
                lat, lon = from_simple_mercator(easting, northing, ref_lat, ref_lon)
                pts.append((lat, lon, depth))
            current_feature.multipoint = pts
        elif record_type == FEATURE_GEOMETRY_RECORD_LINE and current_feature is not None and payload_len >= 36:
            for _ in range(4):
                payload.f64()
            edge_count = payload.u32()
            elements = []
            for _ in range(edge_count):
                elements.append(LineElement(payload.i32(), payload.i32(), payload.i32(), payload.i32()))
            current_feature.line_elements = elements
        elif record_type == FEATURE_GEOMETRY_RECORD_AREA and current_feature is not None and payload_len >= 44:
            for _ in range(4):
                payload.f64()
            contour_count = payload.u32()
            triprim_count = payload.u32()
            edge_count = payload.u32()
            for _ in range(contour_count):
                payload.i32()
            for _ in range(triprim_count):
                payload.u8()
                nvert = payload.u32()
                for _ in range(4):
                    payload.f64()
                payload.read(nvert * 2 * 4)
            elements = []
            for _ in range(edge_count):
                elements.append(LineElement(payload.i32(), payload.i32(), payload.i32(), payload.i32()))
            current_feature.polygon_elements = elements
        elif record_type == VECTOR_EDGE_NODE_TABLE_RECORD:
            n_count = payload.i32()
            ref_lat, ref_lon = chart.center()
            for _ in range(n_count):
                feature_index = payload.i32()
                point_count = payload.i32()
                positions = []
                for _ in range(point_count):
                    x, y = payload.f32(), payload.f32()
                    positions.append(from_simple_mercator(x, y, ref_lat, ref_lon))
                chart.vector_edges[feature_index] = positions
        elif record_type == VECTOR_CONNECTED_NODE_TABLE_RECORD:
            n_count = payload.i32()
            ref_lat, ref_lon = chart.center()
            for _ in range(n_count):
                feature_index = payload.i32()
                x, y = payload.f32(), payload.f32()
                chart.connected_nodes[feature_index] = from_simple_mercator(x, y, ref_lat, ref_lon)
        elif record_type in (CELL_COVR_RECORD, CELL_NOCOVR_RECORD, CELL_TXTDSC_INFO_FILE_RECORD,
                              FEATURE_GEOMETRY_RECORD_AREA_EXT, VECTOR_EDGE_NODE_TABLE_EXT_RECORD,
                              VECTOR_CONNECTED_NODE_TABLE_EXT_RECORD, SERVER_STATUS_RECORD,
                              HEADER_CELL_PUBLISHDATE, HEADER_CELL_UPDATEDATE, HEADER_CELL_SOUNDINGDATUM,
                              HEADER_CELL_EDITION, HEADER_CELL_UPDATE, HEADER_CELL_SENCCREATEDATE):
            pass
        else:
            break

    return chart


def build_geometries(line_elements, vector_edges, connected_nodes):
    line_strings = []
    for le in line_elements:
        placed = False
        for ls in line_strings:
            if le.start_node == ls[-1].end_node:
                ls.append(le)
                placed = True
                break
            elif le.end_node == ls[0].start_node:
                ls.insert(0, le)
                placed = True
                break
        if not placed:
            line_strings.append([le])

    geometries = []
    for ls in line_strings:
        geom = []
        for le in ls:
            node = connected_nodes.get(le.start_node)
            if node:
                geom.append(node)
            if le.edge_vector != 0:
                edge = vector_edges.get(le.edge_vector)
                if edge:
                    geom.extend(reversed(edge) if le.direction == 1 else edge)
        end_node = connected_nodes.get(ls[-1].end_node)
        if end_node:
            geom.append(end_node)
        if geom:
            geometries.append(geom)
    return geometries


def build_all_geometry(chart):
    for feat in chart.features:
        if feat.line_elements:
            feat.lines = build_geometries(feat.line_elements, chart.vector_edges, chart.connected_nodes)
        if feat.polygon_elements:
            feat.polygons = build_geometries(feat.polygon_elements, chart.vector_edges, chart.connected_nodes)


# ---------------------------------------------------------------------------
# Rendering styles (same as view_chart.py)
# ---------------------------------------------------------------------------

STYLE = {
    "LandArea": dict(kind="poly", face="#e8dcb5", edge="#a89968", z=2),
    "LandRegion": dict(kind="poly", face="#e8dcb5", edge="#a89968", z=2),
    "BuiltUpArea": dict(kind="poly", face="#d9c9a0", edge="#a89968", z=2),
    "Lake": dict(kind="poly", face="#bcd9e8", edge="#7fa8c9", z=2),
    "DepthArea": dict(kind="poly", face="#cfe7f5", edge="#9dc6dd", z=1),
    "SeabedArea": dict(kind="poly", face="#dcefee", edge="#a9d3d1", z=1),
    "SeaArea": dict(kind="poly", face="#dff1f7", edge=None, z=0),
    "UnsurveyedArea": dict(kind="poly", face="#eeeeee", edge="#bbbbbb", z=0),
    "CoastLine": dict(kind="line", color="#5a4a2a", width=1.0, z=4),
    "ShorelineConstruction": dict(kind="line", color="#555555", width=0.8, z=4),
    "DepthContour": dict(kind="line", color="#7fa8c9", width=0.4, z=3),
    "Road": dict(kind="line", color="#cc6633", width=0.6, z=4),
    "Railway": dict(kind="line", color="#333333", width=0.6, z=4),
    "Canal": dict(kind="line", color="#4488cc", width=0.8, z=4),
    "River": dict(kind="line", color="#4488cc", width=0.8, z=4),
    "Pipeline": dict(kind="line", color="#996633", width=0.5, z=4),
    "CableOverhead": dict(kind="line", color="#996633", width=0.5, z=4),
    "RecommendedTrack": dict(kind="line", color="#cc00cc", width=0.6, z=5),
    "NavigationLine": dict(kind="line", color="#cc00cc", width=0.5, z=5),
    "RestrictedArea": dict(kind="poly_outline", edge="#cc3333", z=5),
    "AnchorageArea": dict(kind="poly_outline", edge="#3333cc", z=5),
    "CautionArea": dict(kind="poly_outline", edge="#cc9933", z=5),
    "TerritorialSeaArea": dict(kind="poly_outline", edge="#888888", z=5),
    "AdministrationArea": dict(kind="poly_outline", edge="#888888", z=5),
}

POINT_STYLE = {
    "BuoyLateral": dict(marker="^", color="red", size=18),
    "BeaconLateral": dict(marker="^", color="green", size=18),
    "Beacon": dict(marker="^", color="black", size=14),
    "BeaconIsolatedDanger": dict(marker="x", color="black", size=18),
    "Light": dict(marker="*", color="orange", size=30),
    "Pile": dict(marker="s", color="gray", size=12),
    "Pontoon": dict(marker="s", color="dimgray", size=12),
    "Obstruction": dict(marker="P", color="black", size=14),
    "UnderwaterRock": dict(marker="+", color="black", size=18),
    "Wreck": dict(marker="x", color="darkred", size=20),
    "Landmark": dict(marker="^", color="purple", size=14),
    "PilotBoardingPlace": dict(marker="o", color="blue", size=14),
}


# ---------------------------------------------------------------------------
# Web Mercator tiling math (standard OSM/Google/NOAA slippy-map scheme)
# ---------------------------------------------------------------------------

WEBMERC_R = 6378137.0


def lonlat_to_webmerc(lon, lat):
    x = WEBMERC_R * math.radians(lon)
    lat = max(min(lat, 85.05112878), -85.05112878)
    y = WEBMERC_R * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))
    return x, y


def deg2tilenum(lon, lat, zoom):
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = (lon + 180.0) / 360.0 * n
    ytile = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n
    return xtile, ytile


def tile_merc_bounds(z, x, y):
    n = 2 ** z
    world = 2 * math.pi * WEBMERC_R
    x_min = -math.pi * WEBMERC_R + x * (world / n)
    x_max = -math.pi * WEBMERC_R + (x + 1) * (world / n)
    y_max = math.pi * WEBMERC_R - y * (world / n)
    y_min = math.pi * WEBMERC_R - (y + 1) * (world / n)
    return x_min, y_min, x_max, y_max


def resolution_m_per_px(z, tile_size=256):
    return (2 * math.pi * WEBMERC_R) / (tile_size * (2 ** z))


def zoom_band_for_scale(native_scale, tile_size=256):
    """Rough heuristic mapping a chart's native scale denominator to a
    reasonable zoom band. Not a precise cartographic standard -- tune with
    --min-zoom/--max-zoom if the results look off for your charts."""
    if not native_scale:
        return 10, 14
    target_res = native_scale / 3000.0  # tunable factor
    z = math.log2((2 * math.pi * WEBMERC_R) / (tile_size * target_res))
    z = max(0, min(18, round(z)))
    return max(0, z - 3), min(18, z + 1)


# ---------------------------------------------------------------------------
# Tile rendering
# ---------------------------------------------------------------------------

def render_tile_png(charts, z, x, y, tile_size=256):
    x_min, y_min, x_max, y_max = tile_merc_bounds(z, x, y)

    sea_color = STYLE["SeaArea"]["face"]
    fig = plt.figure(figsize=(1, 1), dpi=tile_size)
    fig.patch.set_facecolor(sea_color)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)
    ax.axis("off")
    ax.set_facecolor(sea_color)

    for chart in charts:
        for feat in chart.features:
            style = STYLE.get(feat.type_name)

            if feat.polygons and style and style["kind"] in ("poly", "poly_outline"):
                for ring in feat.polygons:
                    if len(ring) < 3:
                        continue
                    pts = [lonlat_to_webmerc(lon, lat) for lat, lon in ring]
                    xs = [p[0] for p in pts]
                    ys = [p[1] for p in pts]
                    if max(xs) < x_min or min(xs) > x_max or max(ys) < y_min or min(ys) > y_max:
                        continue
                    face = style.get("face", "none") if style["kind"] == "poly" else "none"
                    edge = style.get("edge")
                    patch = MplPolygon(pts, closed=True, facecolor=face,
                                        edgecolor=edge if edge else "none",
                                        linewidth=0.5 if edge else 0)
                    ax.add_patch(patch)

            if feat.lines and style and style["kind"] == "line":
                for line in feat.lines:
                    if len(line) < 2:
                        continue
                    pts = [lonlat_to_webmerc(lon, lat) for lat, lon in line]
                    xs = [p[0] for p in pts]
                    ys = [p[1] for p in pts]
                    if max(xs) < x_min or min(xs) > x_max or max(ys) < y_min or min(ys) > y_max:
                        continue
                    ax.plot(xs, ys, color=style["color"], linewidth=style["width"])

            if feat.type_name == "Sounding" and feat.multipoint:
                pts = [lonlat_to_webmerc(lon, lat) for lat, lon, d in feat.multipoint
                       if x_min <= lonlat_to_webmerc(lon, lat)[0] <= x_max
                       and y_min <= lonlat_to_webmerc(lon, lat)[1] <= y_max]
                if pts:
                    ax.scatter([p[0] for p in pts], [p[1] for p in pts], s=1, color="#336688")

            if feat.point and feat.type_name in POINT_STYLE:
                lat, lon = feat.point
                px, py = lonlat_to_webmerc(lon, lat)
                if x_min <= px <= x_max and y_min <= py <= y_max:
                    pstyle = POINT_STYLE[feat.type_name]
                    ax.scatter([px], [py], marker=pstyle["marker"], color=pstyle["color"],
                               s=pstyle["size"])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=sea_color, edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read(), True  # always keep the tile if a chart covers it, even if just open water


# ---------------------------------------------------------------------------
# MBTiles writer
# ---------------------------------------------------------------------------

def create_mbtiles(path):
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE metadata (name text, value text)")
    conn.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, "
                 "tile_row integer, tile_data blob)")
    conn.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)")
    conn.commit()
    return conn


def write_metadata(conn, name, bounds, min_zoom, max_zoom, description):
    rows = [
        ("name", name),
        ("format", "png"),
        ("bounds", "{:.6f},{:.6f},{:.6f},{:.6f}".format(*bounds)),
        ("minzoom", str(min_zoom)),
        ("maxzoom", str(max_zoom)),
        ("type", "baselayer"),
        ("version", "1.1"),
        ("description", description),
    ]
    conn.executemany("INSERT INTO metadata (name, value) VALUES (?, ?)", rows)
    conn.commit()


def insert_tile(conn, z, x, y_xyz, png_bytes):
    tms_row = (2 ** z - 1 - y_xyz)  # MBTiles uses TMS row numbering
    conn.execute(
        "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) "
        "VALUES (?, ?, ?, ?)", (z, x, tms_row, sqlite3.Binary(png_bytes)))


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def find_chart_files(input_dir):
    patterns = ["**/*.oesu", "**/*.oesenc"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(input_dir, pat), recursive=True))
    return sorted(set(files))


def _chart_intersects_merc(chart, tile_bounds):
    x_min, y_min, x_max, y_max = tile_bounds
    nw_lat, nw_lon, se_lat, se_lon = chart.extent
    cx1, cy1 = lonlat_to_webmerc(nw_lon, nw_lat)
    cx2, cy2 = lonlat_to_webmerc(se_lon, se_lat)
    c_x_min, c_x_max = min(cx1, cx2), max(cx1, cx2)
    c_y_min, c_y_max = min(cy1, cy2), max(cy1, cy2)
    return not (c_x_max < x_min or c_x_min > x_max or c_y_max < y_min or c_y_min > y_max)


def main():
    ap = argparse.ArgumentParser(description="Render decrypted OpenCPN charts into an MBTiles file")
    ap.add_argument("chart_dir", help="Directory of decrypted .oesu/.oesenc files (searched recursively)")
    ap.add_argument("output", help="Output .mbtiles path")
    ap.add_argument("--min-zoom", type=int, default=None, help="Force a fixed min zoom (overrides auto)")
    ap.add_argument("--max-zoom", type=int, default=None, help="Force a fixed max zoom (overrides auto)")
    ap.add_argument("--tile-size", type=int, default=256)
    ap.add_argument("--limit", type=int, default=None, help="Only process the first N chart files (for testing)")
    ap.add_argument("--name", default="Custom Charts")
    args = ap.parse_args()

    files = find_chart_files(args.chart_dir)
    if args.limit:
        files = files[:args.limit]

    if not files:
        print(f"No .oesu/.oesenc files found under {args.chart_dir}")
        sys.exit(1)

    print(f"Found {len(files)} chart file(s). Parsing...")
    charts = []
    for i, path in enumerate(files, 1):
        try:
            chart = parse_chart(path)
        except Exception as e:
            print(f"  [skip] {os.path.basename(path)}: parse error: {e}")
            continue
        if not chart.extent:
            print(f"  [skip] {os.path.basename(path)}: no cell extent found")
            continue
        build_all_geometry(chart)
        min_z, max_z = zoom_band_for_scale(chart.native_scale, args.tile_size)
        if args.min_zoom is not None:
            min_z = args.min_zoom
        if args.max_zoom is not None:
            max_z = args.max_zoom
        chart.min_zoom, chart.max_zoom = min_z, max_z
        charts.append(chart)
        if i % 25 == 0 or i == len(files):
            print(f"  parsed {i}/{len(files)}")

    if not charts:
        print("No charts parsed successfully.")
        sys.exit(1)

    overall_min_z = min(c.min_zoom for c in charts)
    overall_max_z = max(c.max_zoom for c in charts)
    all_west = min(c.extent[1] for c in charts)
    all_east = max(c.extent[3] for c in charts)
    all_north = max(c.extent[0] for c in charts)
    all_south = min(c.extent[2] for c in charts)

    print(f"Zoom range: {overall_min_z}-{overall_max_z}")
    print(f"Bounds: west={all_west:.4f} south={all_south:.4f} east={all_east:.4f} north={all_north:.4f}")

    conn = create_mbtiles(args.output)
    write_metadata(conn, args.name, (all_west, all_south, all_east, all_north),
                    overall_min_z, overall_max_z, "Rendered from decrypted OpenCPN charts")

    total_tiles = 0
    start_time = time.time()

    for z in range(overall_min_z, overall_max_z + 1):
        applicable = [c for c in charts if c.min_zoom <= z <= c.max_zoom]
        if not applicable:
            continue
        applicable.sort(key=lambda c: -(c.native_scale or 0))  # coarse first, detailed last (drawn on top)

        west = min(c.extent[1] for c in applicable)
        east = max(c.extent[3] for c in applicable)
        north = max(c.extent[0] for c in applicable)
        south = min(c.extent[2] for c in applicable)

        x_min_f, y_min_f = deg2tilenum(west, north, z)
        x_max_f, y_max_f = deg2tilenum(east, south, z)
        x_start, x_end = int(math.floor(x_min_f)), int(math.floor(x_max_f))
        y_start, y_end = int(math.floor(y_min_f)), int(math.floor(y_max_f))

        n_tiles_zoom = (x_end - x_start + 1) * (y_end - y_start + 1)
        print(f"Zoom {z}: {len(applicable)} chart(s) applicable, "
              f"rendering {n_tiles_zoom} tile(s) "
              f"(x {x_start}-{x_end}, y {y_start}-{y_end})")

        for x in range(x_start, x_end + 1):
            for y in range(y_start, y_end + 1):
                tile_bounds = tile_merc_bounds(z, x, y)
                relevant = [c for c in applicable if _chart_intersects_merc(c, tile_bounds)]
                if not relevant:
                    continue
                png_bytes, drew = render_tile_png(relevant, z, x, y, args.tile_size)
                if drew:
                    insert_tile(conn, z, x, y, png_bytes)
                    total_tiles += 1

        conn.commit()

    conn.close()
    elapsed = time.time() - start_time
    print(f"Done. Wrote {total_tiles} tiles to {args.output} in {elapsed:.1f}s")


if __name__ == "__main__":
    main()

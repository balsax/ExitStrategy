#!/usr/bin/env python3
"""
view_chart.py - Render a decrypted OpenCPN .oesu / .oesenc (OSENC v201) chart
file as a matplotlib map.

This parses the OSENC binary record format directly (record types, payload
layouts, and vector-edge/connected-node assembly) based on hornang's
open-source C++ parsing library (github.com/hornang/oesenc), which is the
same format hornang's oesenc-export decrypts to. It draws land, depth
areas, coastlines, soundings and aids to navigation -- not full S-52 ECDIS
symbology, just enough to actually see the chart.

Usage:
    python view_chart.py path\\to\\chart.oesu
    python view_chart.py path\\to\\chart.oesu --save chart.png
    python view_chart.py path\\to\\chart.oesu --no-show --save chart.png
    python view_chart.py path\\to\\chart.oesu --depths
"""

import argparse
import math
import struct
import sys
from collections import namedtuple

# ---------------------------------------------------------------------------
# Record type constants (from chartfile.cpp)
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

# S-57 object type codes -> readable names (from s57.cpp fromTypeCode)
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

# S-57 attribute codes -> readable names (from s57.cpp attributeFromTypeCode)
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

LineElement = namedtuple(
    "LineElement", ["start_node", "edge_vector", "end_node", "direction"]
)


# ---------------------------------------------------------------------------
# Byte cursor helper
# ---------------------------------------------------------------------------

class Cursor:
    """Simple forward-only reader over a bytes buffer."""

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
        (v,) = struct.unpack_from("<B", self.data, self.pos)
        self.pos += 1
        return v

    def u16(self):
        (v,) = struct.unpack_from("<H", self.data, self.pos)
        self.pos += 2
        return v

    def u32(self):
        (v,) = struct.unpack_from("<I", self.data, self.pos)
        self.pos += 4
        return v

    def i32(self):
        (v,) = struct.unpack_from("<i", self.data, self.pos)
        self.pos += 4
        return v

    def f32(self):
        (v,) = struct.unpack_from("<f", self.data, self.pos)
        self.pos += 4
        return v

    def f64(self):
        (v,) = struct.unpack_from("<d", self.data, self.pos)
        self.pos += 8
        return v

    def cstr(self, n):
        raw = self.read(n)
        return raw.split(b"\x00", 1)[0].decode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# Mercator conversion (OpenCPN "simple mercator", from opencpn.cpp)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Feature container
# ---------------------------------------------------------------------------

class Feature:
    def __init__(self, type_code):
        self.type_code = type_code
        self.type_name = TYPE_CODES.get(type_code, "Unknown")
        self.attributes = {}
        self.point = None            # (lat, lon)
        self.multipoint = []         # list of (lat, lon, depth)
        self.line_elements = []      # list[LineElement]
        self.polygon_elements = []   # list[LineElement]
        self.lines = []              # resolved: list of [(lat,lon), ...]
        self.polygons = []           # resolved: list of [(lat,lon), ...]


# ---------------------------------------------------------------------------
# Main chart parser
# ---------------------------------------------------------------------------

class Chart:
    def __init__(self):
        self.name = ""
        self.version = 0
        self.publish_date = ""
        self.update_date = ""
        self.native_scale = 0
        self.sounding_datum = ""
        self.extent = None  # (nw_lat, nw_lon, se_lat, se_lon)
        self.features = []
        self.vector_edges = {}      # id -> list[(lat,lon)]
        self.connected_nodes = {}   # id -> (lat,lon)

    def center(self):
        if not self.extent:
            return 0.0, 0.0
        nw_lat, nw_lon, se_lat, se_lon = self.extent
        return (nw_lat + se_lat) / 2.0, (nw_lon + se_lon) / 2.0


def parse_chart(path):
    with open(path, "rb") as f:
        data = f.read()

    cur = Cursor(data)
    chart = Chart()
    current_feature = None

    while cur.remaining() >= 6:
        record_type = cur.u16()
        record_length = cur.u32()

        if record_type == 0:
            break  # end-of-stream marker

        payload_len = record_length - 6

        if payload_len < 0 or cur.remaining() < payload_len:
            print(f"  [warn] truncated record type {record_type}, stopping")
            break

        payload = Cursor(cur.read(payload_len))

        if record_type == SERVER_STATUS_RECORD:
            pass  # server status - not needed once already decrypted to disk

        elif record_type == HEADER_SENC_VERSION:
            if payload_len == 2:
                chart.version = payload.u16()

        elif record_type == HEADER_CELL_NAME:
            chart.name = payload.cstr(payload_len)

        elif record_type == HEADER_CELL_PUBLISHDATE:
            chart.publish_date = payload.cstr(payload_len)

        elif record_type == HEADER_CELL_UPDATEDATE:
            chart.update_date = payload.cstr(payload_len)

        elif record_type == HEADER_CELL_NATIVESCALE:
            if payload_len == 4:
                chart.native_scale = payload.u32()

        elif record_type == HEADER_CELL_SOUNDINGDATUM:
            chart.sounding_datum = payload.cstr(payload_len)

        elif record_type == HEADER_CELL_EDITION or record_type == HEADER_CELL_UPDATE:
            pass  # not needed for rendering

        elif record_type == HEADER_CELL_SENCCREATEDATE:
            pass

        elif record_type == CELL_EXTENT_RECORD:
            if payload_len == 64:
                sw_lat, sw_lon = payload.f64(), payload.f64()
                nw_lat, nw_lon = payload.f64(), payload.f64()
                ne_lat, ne_lon = payload.f64(), payload.f64()
                se_lat, se_lon = payload.f64(), payload.f64()
                chart.extent = (nw_lat, nw_lon, se_lat, se_lon)

        elif record_type in (CELL_COVR_RECORD, CELL_NOCOVR_RECORD,
                              CELL_TXTDSC_INFO_FILE_RECORD,
                              FEATURE_GEOMETRY_RECORD_AREA_EXT,
                              VECTOR_EDGE_NODE_TABLE_EXT_RECORD,
                              VECTOR_CONNECTED_NODE_TABLE_EXT_RECORD):
            pass  # consumed but not used by this viewer

        elif record_type == FEATURE_ID_RECORD:
            type_code = payload.u16()
            _feature_id = payload.u16()
            _primitive = payload.u8()
            current_feature = Feature(type_code)
            chart.features.append(current_feature)

        elif record_type == FEATURE_ATTRIBUTE_RECORD:
            if current_feature is not None and payload_len >= 3:
                attr_code = payload.u16()
                value_type = payload.u8()
                name = ATTRIBUTE_CODES.get(attr_code)
                if value_type == 0 and payload.remaining() >= 4:
                    val = payload.u32()
                elif value_type == 2 and payload.remaining() >= 8:
                    val = payload.f64()
                elif value_type == 4:
                    val = payload.cstr(payload.remaining())
                else:
                    val = None
                if name and val is not None:
                    current_feature.attributes[name] = val

        elif record_type == FEATURE_GEOMETRY_RECORD_POINT:
            if current_feature is not None and payload_len == 16:
                lat = payload.f64()
                lon = payload.f64()
                current_feature.point = (lat, lon)

        elif record_type == FEATURE_GEOMETRY_RECORD_MULTIPOINT:
            if current_feature is not None and payload_len >= 36:
                payload.f64(); payload.f64(); payload.f64(); payload.f64()  # extent
                point_count = payload.u32()
                ref_lat, ref_lon = chart.center()
                pts = []
                for _ in range(point_count):
                    easting = payload.f32()
                    northing = payload.f32()
                    depth = payload.f32()
                    lat, lon = from_simple_mercator(easting, northing, ref_lat, ref_lon)
                    pts.append((lat, lon, depth))
                current_feature.multipoint = pts

        elif record_type == FEATURE_GEOMETRY_RECORD_LINE:
            if current_feature is not None and payload_len >= 36:
                payload.f64(); payload.f64(); payload.f64(); payload.f64()  # extent
                edge_count = payload.u32()
                elements = []
                for _ in range(edge_count):
                    start = payload.i32()
                    edge = payload.i32()
                    end = payload.i32()
                    direction = payload.i32()
                    elements.append(LineElement(start, edge, end, direction))
                current_feature.line_elements = elements

        elif record_type == FEATURE_GEOMETRY_RECORD_AREA:
            if current_feature is not None and payload_len >= 44:
                payload.f64(); payload.f64(); payload.f64(); payload.f64()  # extent
                contour_count = payload.u32()
                triprim_count = payload.u32()
                edge_count = payload.u32()

                # contour point-count array
                for _ in range(contour_count):
                    payload.i32()

                # skip tessellation triangle primitives
                for _ in range(triprim_count):
                    payload.u8()               # primitive type marker
                    nvert = payload.u32()
                    for _ in range(4):
                        payload.f64()          # bbox/extent doubles
                    payload.read(nvert * 2 * 4)  # nvert * (x,y) floats

                elements = []
                for _ in range(edge_count):
                    start = payload.i32()
                    edge = payload.i32()
                    end = payload.i32()
                    direction = payload.i32()
                    elements.append(LineElement(start, edge, end, direction))
                current_feature.polygon_elements = elements

        elif record_type == VECTOR_EDGE_NODE_TABLE_RECORD:
            n_count = payload.i32()
            ref_lat, ref_lon = chart.center()
            for _ in range(n_count):
                feature_index = payload.i32()
                point_count = payload.i32()
                positions = []
                for _ in range(point_count):
                    x = payload.f32()
                    y = payload.f32()
                    lat, lon = from_simple_mercator(x, y, ref_lat, ref_lon)
                    positions.append((lat, lon))
                chart.vector_edges[feature_index] = positions

        elif record_type == VECTOR_CONNECTED_NODE_TABLE_RECORD:
            n_count = payload.i32()
            ref_lat, ref_lon = chart.center()
            for _ in range(n_count):
                feature_index = payload.i32()
                x = payload.f32()
                y = payload.f32()
                lat, lon = from_simple_mercator(x, y, ref_lat, ref_lon)
                chart.connected_nodes[feature_index] = (lat, lon)

        else:
            if record_type == 0:
                break
            print(f"  [warn] unrecognized record type {record_type}, "
                  f"stopping (this chart version may use features this "
                  f"viewer doesn't support)")
            break

    return chart


# ---------------------------------------------------------------------------
# Geometry assembly (port of S57::buildGeometries in s57.cpp)
# ---------------------------------------------------------------------------

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
            feat.lines = build_geometries(feat.line_elements, chart.vector_edges,
                                           chart.connected_nodes)
        if feat.polygon_elements:
            feat.polygons = build_geometries(feat.polygon_elements, chart.vector_edges,
                                              chart.connected_nodes)


# ---------------------------------------------------------------------------
# Rendering
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
    "CoastLine": dict(kind="line", color="#5a4a2a", width=1.2, z=4),
    "ShorelineConstruction": dict(kind="line", color="#555555", width=1.0, z=4),
    "DepthContour": dict(kind="line", color="#7fa8c9", width=0.5, z=3),
    "Road": dict(kind="line", color="#cc6633", width=0.8, z=4),
    "Railway": dict(kind="line", color="#333333", width=0.8, z=4),
    "Canal": dict(kind="line", color="#4488cc", width=1.0, z=4),
    "River": dict(kind="line", color="#4488cc", width=1.0, z=4),
    "Pipeline": dict(kind="line", color="#996633", width=0.6, z=4),
    "CableOverhead": dict(kind="line", color="#996633", width=0.6, z=4),
    "RecommendedTrack": dict(kind="line", color="#cc00cc", width=0.8, z=5),
    "NavigationLine": dict(kind="line", color="#cc00cc", width=0.6, z=5),
    "RestrictedArea": dict(kind="poly_outline", edge="#cc3333", z=5),
    "AnchorageArea": dict(kind="poly_outline", edge="#3333cc", z=5),
    "CautionArea": dict(kind="poly_outline", edge="#cc9933", z=5),
    "TerritorialSeaArea": dict(kind="poly_outline", edge="#888888", z=5),
    "AdministrationArea": dict(kind="poly_outline", edge="#888888", z=5),
}

POINT_STYLE = {
    "BuoyLateral": dict(marker="^", color="red", size=30),
    "BeaconLateral": dict(marker="^", color="green", size=30),
    "Beacon": dict(marker="^", color="black", size=25),
    "BeaconIsolatedDanger": dict(marker="x", color="black", size=30),
    "Light": dict(marker="*", color="orange", size=60),
    "Pile": dict(marker="s", color="gray", size=20),
    "Pontoon": dict(marker="s", color="dimgray", size=20),
    "Obstruction": dict(marker="P", color="black", size=25),
    "UnderwaterRock": dict(marker="+", color="black", size=30),
    "Wreck": dict(marker="x", color="darkred", size=35),
    "Landmark": dict(marker="^", color="purple", size=25),
    "PilotBoardingPlace": dict(marker="o", color="blue", size=25),
}


def render_chart(chart, save_path=None, show=True, show_depths=False):
    try:
        import matplotlib.pyplot as plt
        from matplotlib.patches import Polygon as MplPolygon
    except ImportError:
        print("This viewer needs matplotlib. Install it with:\n"
              "    pip install matplotlib")
        sys.exit(1)

    fig, ax = plt.subplots(figsize=(11, 9))

    # group by z-order so fills draw under lines/points
    by_z = {}

    for feat in chart.features:
        style = STYLE.get(feat.type_name)

        if feat.polygons and style and style["kind"] in ("poly", "poly_outline"):
            for ring in feat.polygons:
                if len(ring) < 3:
                    continue
                pts = [(lon, lat) for lat, lon in ring]
                face = style.get("face", "none") if style["kind"] == "poly" else "none"
                edge = style.get("edge")
                patch = MplPolygon(pts, closed=True, facecolor=face,
                                    edgecolor=edge if edge else "none",
                                    linewidth=0.6 if edge else 0)
                by_z.setdefault(style["z"], []).append(("patch", patch))

        if feat.lines and style and style["kind"] == "line":
            for line in feat.lines:
                if len(line) < 2:
                    continue
                pts = [(lon, lat) for lat, lon in line]
                by_z.setdefault(style["z"], []).append(
                    ("line", pts, style["color"], style["width"]))

        if feat.type_name == "Sounding" and feat.multipoint:
            xs = [lon for lat, lon, d in feat.multipoint]
            ys = [lat for lat, lon, d in feat.multipoint]
            by_z.setdefault(3, []).append(("soundings", xs, ys, feat.multipoint))

        if feat.point and feat.type_name in POINT_STYLE:
            pstyle = POINT_STYLE[feat.type_name]
            lat, lon = feat.point
            by_z.setdefault(6, []).append(
                ("point", lon, lat, pstyle["marker"], pstyle["color"], pstyle["size"]))

    for z in sorted(by_z.keys()):
        for item in by_z[z]:
            if item[0] == "patch":
                ax.add_patch(item[1])
            elif item[0] == "line":
                _, pts, color, width = item
                xs = [p[0] for p in pts]
                ys = [p[1] for p in pts]
                ax.plot(xs, ys, color=color, linewidth=width, zorder=z)
            elif item[0] == "soundings":
                _, xs, ys, multipoint = item
                ax.scatter(xs, ys, s=2, color="#336688", zorder=z)
                if show_depths:
                    for (lat, lon, depth) in multipoint:
                        ax.annotate(f"{depth:.1f}", (lon, lat), fontsize=4,
                                    color="#1a3f5c", xytext=(1.5, 1.5),
                                    textcoords="offset points", zorder=z)
            elif item[0] == "point":
                _, x, y, marker, color, size = item
                ax.scatter([x], [y], marker=marker, color=color, s=size, zorder=z)

    if chart.extent:
        nw_lat, nw_lon, se_lat, se_lon = chart.extent
        ax.set_xlim(nw_lon, se_lon)
        ax.set_ylim(se_lat, nw_lat)

    ax.set_aspect(1.0 / max(math.cos(math.radians(chart.center()[0])), 0.01))
    ax.set_facecolor("#dff1f7")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    title = chart.name or "Chart"
    scale_txt = f"1:{chart.native_scale}" if chart.native_scale else ""
    ax.set_title(f"{title}  {scale_txt}".strip())

    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
        print(f"Saved to {save_path}")

    if show:
        plt.show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="View a decrypted OpenCPN chart (.oesu/.oesenc)")
    parser.add_argument("chart_file", help="Path to a decrypted .oesu or .oesenc file")
    parser.add_argument("--save", help="Save the rendered map to this image path (e.g. chart.png)")
    parser.add_argument("--no-show", action="store_true", help="Don't open an interactive window")
    parser.add_argument("--depths", action="store_true", help="Label soundings with their depth values")
    args = parser.parse_args()

    print(f"Parsing {args.chart_file} ...")
    chart = parse_chart(args.chart_file)
    print(f"  Name: {chart.name}  Version: {chart.version}  "
          f"Scale: 1:{chart.native_scale}  Features: {len(chart.features)}")
    print(f"  Vector edges: {len(chart.vector_edges)}  "
          f"Connected nodes: {len(chart.connected_nodes)}")

    build_all_geometry(chart)

    n_polys = sum(len(f.polygons) for f in chart.features)
    n_lines = sum(len(f.lines) for f in chart.features)
    n_soundings = sum(len(f.multipoint) for f in chart.features if f.type_name == "Sounding")
    print(f"  Polygons: {n_polys}  Lines: {n_lines}  Soundings: {n_soundings}")

    render_chart(chart, save_path=args.save, show=not args.no_show, show_depths=args.depths)


if __name__ == "__main__":
    main()

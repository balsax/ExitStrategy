#!/usr/bin/env python3
"""ochart_tiles.py - Draw web-map tiles straight from the decrypted O-charts
(.oesu, OSENC v201) files in ~/ocharts/exported, on demand, for the Chart tab's
"O-Charts" base source (the siloed /api/charts/ocharts/* block in
dashboard_api.py).

Why on-demand instead of the old pre-built MBTiles pipeline (git f7a6384,
ochart_tools.py): pre-cutting even the 27 BVI/USVI charts took ~1.5 h with
gdal2tiles on this Pi (and an earlier, wider run ran it out of memory).
Drawing a tile from vector data takes well under a second once its chart is
parsed, so every one of the 338 charts is available at every zoom with no
build step. Rendered tiles are cached on disk (CACHE_DIR) so each tile is
only ever drawn once.

Chart selection per tile ("quilting", roughly as OpenCPN does it): charts are
tried finest scale first; a chart is skipped if it's far too detailed for the
zoom; we stop adding coarser charts once one covers the whole tile. They are
then drawn coarsest first, each clipped to its own coverage polygon, so the
most detailed chart wins wherever charts overlap.

Aids to navigation, lights, bridges, hazards, fairway/restricted/caution
areas and soundings are left out of the tiles (see OVERLAY_LAYERS) and served
as GeoJSON by features_for_view() instead, so the Chart tab's own overlay
checkboxes turn them on and off and they're clickable.

Only one tile is drawn at a time (RENDER_LOCK). Parsed charts are kept in an
LRU (MAX_PARSED_CHARTS); all 338 together measure ~540 MB resident, which
the 8 GB Pi can afford, so the limit is sized to hold most of them.

CLI (for testing without the web server):
    python3 ochart_tiles.py index
    python3 ochart_tiles.py tile <z> <x> <y> <out.png>
"""

import functools
import glob
import io
import math
import os
import pickle
import struct
import sys
import threading
import time
from collections import OrderedDict

import numpy as np
from PIL import Image, ImageDraw

import osenc_parse as op

SRC_DIR = os.environ.get('OCHARTS_SRC', '/home/mikemc/ocharts/exported')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RENDER_VERSION = 'v2'  # bump when the look changes, so old cached tiles are ignored
# Fixed path (like NCDS_DIR in dashboard_api.py), not BASE_DIR-relative, so the
# production copy of this file next to ~/dashboard_api.py shares dev's cache
# and ochart_prerender.py's output instead of starting from empty.
CACHE_ROOT = os.environ.get('OCHARTS_CACHE', '/home/mikemc/dashboard-dev/chart_data/ocharts_cache')
CACHE_DIR = os.path.join(CACHE_ROOT, RENDER_VERSION)
PARSED_DIR = os.path.join(CACHE_ROOT, 'parsed')  # pickled PreparedChart per chart
PARSED_FORMAT = 2  # bump when PreparedChart's fields change

MIN_ZOOM = 6
MAX_ZOOM = 18
MAX_PARSED_CHARTS = 200  # parsed charts kept in memory for tile drawing (all 338 = ~540 MB; was 5 on the 4 GB Pi)
SS = 2                 # supersampling factor (draw at 512px, shrink to 256 for smooth edges)
TILE = 256 * SS

RENDER_LOCK = threading.Lock()
_index = None
_index_lock = threading.Lock()
_parsed = OrderedDict()   # path -> PreparedChart (LRU), used under RENDER_LOCK
# Overlay feature lookups get their own lock and chart LRU, so they don't
# queue behind tile drawing when a new area is first opened.
FEATURE_LOCK = threading.Lock()
_parsed_features = OrderedDict()
MAX_PARSED_FEATURE_CHARTS = 40  # was 3 on the 4 GB Pi


# ---------------------------------------------------------------------------
# Web-mercator helpers. "World" coords are normalized to [0, 1] in both axes.
# ---------------------------------------------------------------------------

def world_xy(lat, lon):
    lat = np.clip(np.asarray(lat, dtype=np.float64), -85.05, 85.05)
    lon = np.asarray(lon, dtype=np.float64)
    x = (lon + 180.0) / 360.0
    s = np.sin(np.radians(lat))
    y = 0.5 - np.log((1 + s) / (1 - s)) / (4 * math.pi)
    return x, y


def tile_world_bounds(z, x, y):
    n = 2 ** z
    return x / n, y / n, (x + 1) / n, (y + 1) / n   # minx, miny, maxx, maxy


def tile_center_lat(z, y):
    n = 2 ** z
    yc = (y + 0.5) / n
    return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yc))))


def _ring_world(ring_latlon):
    arr = np.asarray(ring_latlon, dtype=np.float64)
    wx, wy = world_xy(arr[:, 0], arr[:, 1])
    return np.column_stack([wx, wy])


def _bbox(arr):
    return (float(arr[:, 0].min()), float(arr[:, 1].min()),
            float(arr[:, 0].max()), float(arr[:, 1].max()))


def _point_in_ring(px, py, ring):
    xs, ys = ring[:, 0], ring[:, 1]
    xj, yj = np.roll(xs, 1), np.roll(ys, 1)
    cond = ((ys > py) != (yj > py)) & (px < (xj - xs) * (py - ys) / np.where(yj - ys == 0, 1e-30, yj - ys) + xs)
    return bool(np.count_nonzero(cond) % 2)


def _ring_area(ring):
    x, y = ring[:, 0], ring[:, 1]
    return abs(float(np.dot(x, np.roll(y, 1)) - np.dot(y, np.roll(x, 1)))) / 2


# ---------------------------------------------------------------------------
# Index: header-only scan of every chart (name, scale, extent, coverage).
# Headers sit before any feature records, so this reads a few KB per file.
# ---------------------------------------------------------------------------

def _scan_header(path):
    info = {'path': path, 'id': os.path.splitext(os.path.basename(path))[0],
            'name': '', 'scale': 0, 'extent': None, 'cov_raw': []}
    with open(path, 'rb') as f:
        while True:
            h = f.read(6)
            if len(h) < 6:
                break
            rtype, rlen = struct.unpack('<HI', h)
            if rtype == 0 or rlen < 6:
                break
            if rtype in (op.FEATURE_ID_RECORD, op.VECTOR_EDGE_NODE_TABLE_RECORD,
                         op.VECTOR_CONNECTED_NODE_TABLE_RECORD):
                break
            payload = f.read(rlen - 6)
            if rtype == op.HEADER_CELL_NAME:
                info['name'] = payload.split(b'\x00', 1)[0].decode('latin-1', 'replace')
            elif rtype == op.HEADER_CELL_NATIVESCALE and len(payload) == 4:
                info['scale'] = struct.unpack('<I', payload)[0]
            elif rtype == op.CELL_EXTENT_RECORD and len(payload) == 64:
                v = struct.unpack('<8d', payload)
                info['extent'] = (v[2], v[3], v[6], v[7])  # nw_lat, nw_lon, se_lat, se_lon
            elif rtype == op.CELL_COVR_RECORD:
                info['cov_raw'].append(payload)
    return info


def get_index():
    global _index
    with _index_lock:
        if _index is not None:
            return _index
        charts = []
        for path in sorted(glob.glob(os.path.join(SRC_DIR, '*.oesu')) +
                           glob.glob(os.path.join(SRC_DIR, '*.oesenc'))):
            try:
                info = _scan_header(path)
            except OSError:
                continue
            if not info['extent'] or not info['scale']:
                continue
            nw_lat, nw_lon, se_lat, se_lon = info['extent']
            margin = (se_lat, nw_lat, nw_lon, se_lon)
            rings = [r for raw in info.pop('cov_raw') if (r := op.decode_coverage_ring(raw, margin))]
            if not rings:
                rings = [[(nw_lat, nw_lon), (nw_lat, se_lon), (se_lat, se_lon), (se_lat, nw_lon)]]
            info['cov'] = [_ring_world(r) for r in rings]
            allpts = np.vstack(info['cov'])
            info['wbbox'] = _bbox(allpts)
            info['bounds'] = {'south': se_lat, 'north': nw_lat, 'west': nw_lon, 'east': se_lon}
            charts.append(info)
        _index = charts
        return _index


def index_summary():
    charts = get_index()
    if not charts:
        return {'available': False}
    b = {'south': min(c['bounds']['south'] for c in charts),
         'north': max(c['bounds']['north'] for c in charts),
         'west': min(c['bounds']['west'] for c in charts),
         'east': max(c['bounds']['east'] for c in charts)}
    return {'available': True, 'chartCount': len(charts), 'bounds': b,
            'minZoom': MIN_ZOOM, 'maxZoom': MAX_ZOOM}


# ---------------------------------------------------------------------------
# Styling (S-52-ish day palette, same spirit as NOAA's paper-chart look)
# ---------------------------------------------------------------------------

LAND = (240, 226, 182, 255)
LAND_EDGE = (150, 130, 80, 255)
BUILT = (222, 205, 158, 255)
BUILDING = (190, 170, 120, 255)
DEEP = (255, 255, 255, 255)


def depth_fill(drval1):
    if drval1 is None:
        return (226, 239, 249, 255)
    if drval1 < 0:
        return (168, 207, 160, 255)     # dries at low water
    if drval1 < 2:
        return (127, 184, 234, 255)
    if drval1 < 5:
        return (165, 205, 242, 255)
    if drval1 < 10:
        return (201, 225, 246, 255)
    if drval1 < 20:
        return (226, 239, 249, 255)
    return DEEP


# type_code -> how to draw polygons (fill, edge) and lines (color, width px @256)
AREA_FILL = {
    42: None,                                   # DepthArea (per-feature fill, see depth_fill)
    154: ((238, 238, 238, 255), (180, 180, 180, 255)),   # UnsurveyedArea
    71: (LAND, None),                           # LandArea
    73: None,                                   # LandRegion: names only, no fill (would hide islands' detail)
    13: (BUILT, None),                          # BuiltUpArea
    12: (BUILDING, (120, 100, 60, 255)),        # single building
    69: ((188, 217, 232, 255), (127, 168, 201, 255)),    # Lake
    114: ((188, 217, 232, 255), None),          # River (area form)
    22: ((188, 217, 232, 255), None),           # Canal (area form)
    95: ((170, 170, 170, 255), (90, 90, 90, 255)),       # Pontoon
    122: ((170, 170, 170, 255), (90, 90, 90, 255)),      # ShorelineConstruction (area form)
}
AREA_ORDER = [154, 42, 71, 13, 12, 69, 114, 22, 122, 95]

LINE_STYLE = {
    43: ((110, 150, 185, 255), 0.6),    # DepthContour
    30: ((80, 66, 36, 255), 1.2),       # CoastLine
    122: ((70, 70, 70, 255), 1.0),      # ShorelineConstruction
    116: ((200, 110, 60, 255), 0.7),    # Road
    106: ((60, 60, 60, 255), 0.7),      # Railway
    114: ((70, 130, 200, 255), 0.8),    # River
    22: ((70, 130, 200, 255), 0.8),     # Canal
    94: ((150, 100, 50, 255), 0.6),     # Pipeline
    21: ((150, 100, 50, 255), 0.6),     # CableOverhead
    109: ((110, 110, 110, 255), 0.8),   # RecommendedTrack
    85: ((110, 110, 110, 255), 0.6),    # NavigationLine
}
OUTLINE_STYLE = {
    4: (200, 40, 150, 255),    # AnchorageArea
}

# Features that are NOT drawn into the tiles but served separately by
# features_for_view(), so the Chart tab's own checkboxes (Aids to Navigation,
# Bridges, Hazards, Fairways / Areas, Soundings) switch them on and off.
# type_code -> the S-57 layer key the frontend's CHART_LAYER_STYLERS use.
OVERLAY_LAYERS = {
    17: 'BOYLAT', 14: 'BOYCAR', 18: 'BOYSAW', 19: 'BOYSPP', 16: 'BOYISD',
    7: 'BCNLAT', 5: 'BCNCAR', 8: 'BCNSAW', 9: 'BCNSPP', 6: 'BCNISD',
    75: 'LIGHTS', 11: 'BRIDGE',
    159: 'WRECKS', 86: 'OBSTRN', 153: 'UWTROC',
    51: 'FAIRWY', 112: 'RESARE', 27: 'CTNARE',
}
OVERLAY_AREA_LAYERS = {'FAIRWY', 'RESARE', 'CTNARE'}   # sent as polygons; the rest as points

# osenc_parse attribute names -> the S-57 acronyms the frontend's popups know
ATTR_TO_S57 = {
    'ObjectName': 'OBJNAM', 'Colour': 'COLOUR', 'ColourPattern': 'COLPAT',
    'BuoyShape': 'BOYSHP', 'BeaconShape': 'BCNSHP',
    'CategoryOfLateralMark': 'CATLAM', 'CategoryOfCardinalMark': 'CATCAM',
    'CategoryOfSpecialPurposeMark': 'CATSPM', 'CategoryOfLight': 'CATLIT',
    'CategoryOfObstruction': 'CATOBS', 'CategoryOfWreck': 'CATWRK',
    'LightCharacteristic': 'LITCHR', 'SignalGroup': 'SIGGRP', 'SignalPeriod': 'SIGPER',
    'ValueOfNominalRange': 'VALNMR', 'LightVisibility': 'LITVIS',
    'SectorLimitOne': 'SECTR1', 'SectorLimitTwo': 'SECTR2', 'Orientation': 'ORIENT',
    'Height': 'HEIGHT', 'VerticalClearance': 'VERCLR',
    'ValueOfSounding': 'VALSOU', 'WaterLevelEffect': 'WATLEV', 'ExpositionOfSounding': 'EXPSOU',
    'QualityOfSounding': 'QUASOU', 'NatureOfSurface': 'NATSUR', 'DepthValue1': 'DRVAL1',
    'MarkNavigationalSystem': 'MARSYS', 'Information': 'INFORM',
    'Restriction': 'RESTRN', 'CategoryOfRestrictedArea': 'CATREA', 'CategoryOfBridge': 'CATBRG',
    'SignalSequence': 'SIGSEQ', 'Condition': 'CONDTN', 'RadarConspicuous': 'CONRAD',
    'TechniqueOfSoundingMeasurement': 'TECSOU',
    'Status': 'STATUS', 'ScaleMin': 'SCAMIN', 'SourceDate': 'SORDAT', 'SourceIndication': 'SORIND',
}


# ---------------------------------------------------------------------------
# Parsed + projected chart, kept in a small LRU
# ---------------------------------------------------------------------------

class PreparedChart:
    """One chart's features, projected to world coords as numpy arrays with
    bounding boxes, so a tile only touches the features it overlaps."""

    def __init__(self, info):
        chart = op.parse_chart(info['path'])
        op.build_all_geometry(chart)
        self.scale = info['scale']
        self.cov = info['cov']
        self.areas = {code: [] for code in AREA_ORDER}   # code -> [(rings_with_parity, bbox, attrs)]
        self.outlines = []   # (rings, bbox, colour)
        self.lines = []      # (lines, bbox, colour, width, order)
        self.points = []     # (x, y, code, attrs)
        self.soundings = []  # arrays: x, y, depth, lat, lon
        self.overlay = []    # (layer, geojson geometry, props, anchor_x, anchor_y) -- see OVERLAY_LAYERS
        snd_x, snd_y, snd_d, snd_lat, snd_lon = [], [], [], [], []

        for feat in chart.features:
            code = feat.type_code
            if code in OVERLAY_LAYERS:
                item = _overlay_item(OVERLAY_LAYERS[code], feat)
                if item:
                    self.overlay.append(item)
                continue
            if feat.polygons and (code in self.areas or code in OUTLINE_STYLE):
                rings = [_ring_world(r) for r in feat.polygons if len(r) >= 3]
                if not rings:
                    continue
                bbox = _bbox(np.vstack(rings))
                if code in OUTLINE_STYLE:
                    self.outlines.append((rings, bbox, OUTLINE_STYLE[code]))
                if code in self.areas:
                    self.areas[code].append((_with_parity(rings), bbox, feat.attributes))
            if feat.lines and code in LINE_STYLE:
                lines = [_ring_world(l) for l in feat.lines if len(l) >= 2]
                if lines:
                    colour, width = LINE_STYLE[code]
                    self.lines.append((lines, _bbox(np.vstack(lines)), colour, width))
            if feat.point:
                wx, wy = world_xy(feat.point[0], feat.point[1])
                self.points.append((float(wx), float(wy), code, feat.attributes))
            if feat.multipoint and code == 129:
                arr = np.asarray(feat.multipoint, dtype=np.float64)
                wx, wy = world_xy(arr[:, 0], arr[:, 1])
                snd_x.append(wx); snd_y.append(wy); snd_d.append(arr[:, 2])
                snd_lat.append(arr[:, 0]); snd_lon.append(arr[:, 1])

        # Deepest depth areas first so shallower ones paint over them.
        self.areas[42].sort(key=lambda a: -(a[2].get('DepthValue1') if isinstance(a[2].get('DepthValue1'), (int, float)) else -1e9))
        if snd_x:
            self.soundings = tuple(np.concatenate(a) for a in (snd_x, snd_y, snd_d, snd_lat, snd_lon))


def _overlay_item(layer, feat):
    """One overlay feature as (layer, GeoJSON geometry, S-57 props, anchor
    world x, anchor world y). The anchor decides which chart "owns" the
    feature where charts overlap (see features_for_view)."""
    props = {ATTR_TO_S57.get(k, k): v for k, v in feat.attributes.items()}
    if layer in OVERLAY_AREA_LAYERS:
        rings = [r for r in feat.polygons if len(r) >= 3]
        if not rings:
            return None
        parity = _with_parity([_ring_world(r) for r in rings])
        polys = []
        for ring_w, is_hole in parity:
            if is_hole:
                continue
            lat, lon = _world_to_latlon(ring_w[:, 0], ring_w[:, 1])
            polys.append([[[round(a, 6), round(b, 6)] for a, b in zip(lon.tolist(), lat.tolist())]])
        geom = {'type': 'Polygon', 'coordinates': polys[0]} if len(polys) == 1 \
            else {'type': 'MultiPolygon', 'coordinates': polys}
        pts = np.vstack([_ring_world(r) for r in rings])
        ax, ay = float(pts[:, 0].mean()), float(pts[:, 1].mean())
        return (layer, geom, props, ax, ay)
    # Point layers: use the point itself, or the middle of a line/area
    # (e.g. a wreck charted as an area, a bridge charted as a line).
    if feat.point:
        lat, lon = feat.point
    else:
        shape = (feat.lines or feat.polygons or [None])[0]
        if not shape:
            return None
        if feat.lines:
            lat, lon = shape[len(shape) // 2]
        else:
            arr = np.asarray(shape)
            lat, lon = float(arr[:, 0].mean()), float(arr[:, 1].mean())
    wx, wy = world_xy(lat, lon)
    geom = {'type': 'Point', 'coordinates': [round(lon, 6), round(lat, 6)]}
    return (layer, geom, props, float(wx), float(wy))


def _world_to_latlon(wx, wy):
    lon = np.asarray(wx) * 360.0 - 180.0
    lat = np.degrees(np.arctan(np.sinh(math.pi * (1 - 2 * np.asarray(wy)))))
    return lat, lon


def _with_parity(rings):
    """Even-odd fill for multi-ring polygons: a ring inside an odd number of
    other rings is a hole. Returns [(ring, is_hole)] largest first."""
    if len(rings) == 1:
        return [(rings[0], False)]
    rings = sorted(rings, key=_ring_area, reverse=True)
    boxes = [_bbox(r) for r in rings]
    out = []
    for i, r in enumerate(rings):
        px, py = r[0]
        depth = 0
        for j in range(i):
            b = boxes[j]
            if b[0] <= px <= b[2] and b[1] <= py <= b[3] and _point_in_ring(px, py, rings[j]):
                depth += 1
        out.append((r, depth % 2 == 1))
    return out


def _load_or_prepare(info):
    """Parsing a big chart takes ~2 s; loading the pickled result is much
    faster, so each chart is only ever parsed once."""
    pkl = os.path.join(PARSED_DIR, f"{info['id']}.v{PARSED_FORMAT}.pkl")
    try:
        if os.path.getmtime(pkl) >= os.path.getmtime(info['path']):
            with open(pkl, 'rb') as f:
                return pickle.load(f)
    except (OSError, pickle.UnpicklingError, EOFError, AttributeError):
        pass
    prepared = PreparedChart(info)
    try:
        os.makedirs(PARSED_DIR, exist_ok=True)
        tmp = f'{pkl}.{os.getpid()}.{threading.get_ident()}.tmp'  # tiles and features may race here
        with open(tmp, 'wb') as f:
            pickle.dump(prepared, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, pkl)
    except OSError:
        pass
    return prepared


def _get_prepared(info, cache=_parsed, limit=MAX_PARSED_CHARTS):
    path = info['path']
    if path in cache:
        cache.move_to_end(path)
        return cache[path]
    prepared = _load_or_prepare(info)
    cache[path] = prepared
    while len(cache) > limit:
        cache.popitem(last=False)
    return prepared


# ---------------------------------------------------------------------------
# Tile rendering
# ---------------------------------------------------------------------------

def _overlaps(b, t):
    return not (b[2] < t[0] or b[0] > t[2] or b[3] < t[1] or b[1] > t[3])


@functools.lru_cache(maxsize=50000)
def pick_charts(z, x, y):
    """Charts to draw for this tile, coarsest first. Cached: the chart set
    doesn't change while the server runs, and panning asks for the same
    tiles again and again. (Callers must not modify the returned list.)"""
    t = tile_world_bounds(z, x, y)
    lat = tile_center_lat(z, y)
    m_per_px = 156543.034 * math.cos(math.radians(lat)) / (2 ** z)
    display_scale = m_per_px / 0.00028   # scale the screen is showing at this zoom
    # Charts much more detailed than the zoom are skipped (too slow, too busy)
    # -- unless nothing coarser covers the tile at all, as when zoomed far out
    # over the eastern Caribbean, where the coarsest charts are 1:700,000.
    for detail_limit in (8, 32):
        candidates = [c for c in get_index() if _overlaps(c['wbbox'], t)
                      and c['scale'] >= display_scale / detail_limit]
        candidates.sort(key=lambda c: c['scale'])
        # Stop adding coarser charts once the finer ones already chosen cover
        # every sample point of the tile.
        sx = np.repeat(np.linspace(t[0], t[2], 5), 5)
        sy = np.tile(np.linspace(t[1], t[3], 5), 5)
        chosen = []
        for c in candidates:
            rings = [r for r in c['cov'] if _overlaps(_bbox(r), t)]
            if not rings:
                continue
            chosen.append(c)
            left = ~_points_in_rings(sx, sy, rings)
            sx, sy = sx[left], sy[left]
            if not len(sx):
                break
        if chosen:
            break
    return list(reversed(chosen)), display_scale


def render_tile(z, x, y):
    """PNG bytes for tile z/x/y, or None if no chart covers it."""
    if z < MIN_ZOOM or z > MAX_ZOOM:
        return None
    charts, display_scale = pick_charts(z, x, y)
    if not charts:
        return None

    t = tile_world_bounds(z, x, y)
    n = 2 ** z
    pad = 24 / (256 * n)  # symbol/label overhang, in world units
    tp = (t[0] - pad, t[1] - pad, t[2] + pad, t[3] + pad)

    def to_px(arr):
        return [tuple(p) for p in ((arr * n - (x, y)) * TILE).tolist()]

    tile = Image.new('RGBA', (TILE, TILE), (0, 0, 0, 0))
    drew_any = False
    for info in charts:
        pc = _get_prepared(info)
        layer = Image.new('RGBA', (TILE, TILE), (0, 0, 0, 0))
        d = ImageDraw.Draw(layer)
        for ring in pc.cov:
            d.polygon(to_px(ring), fill=DEEP)

        for code in AREA_ORDER:
            for rings, bbox, attrs in pc.areas[code]:
                if not _overlaps(bbox, t):
                    continue
                if code == 42:
                    dv = attrs.get('DepthValue1')
                    fill, edge = depth_fill(dv if isinstance(dv, (int, float)) else None), None
                else:
                    fill, edge = AREA_FILL[code]
                if len(rings) == 1:
                    d.polygon(to_px(rings[0][0]), fill=fill)
                else:
                    mask = Image.new('L', (TILE, TILE), 0)
                    md = ImageDraw.Draw(mask)
                    for ring, is_hole in rings:
                        md.polygon(to_px(ring), fill=0 if is_hole else 255)
                    layer.paste(Image.new('RGBA', (TILE, TILE), fill), (0, 0), mask)
                if edge:
                    for ring, _ in rings:
                        pts = to_px(ring)
                        d.line(pts + [pts[0]], fill=edge, width=SS)

        for lines, bbox, colour, width in pc.lines:
            if _overlaps(bbox, t):
                for line in lines:
                    d.line(to_px(line), fill=colour, width=max(1, round(width * SS)))

        for rings, bbox, colour in pc.outlines:
            if _overlaps(bbox, t):
                for ring in rings:
                    pts = to_px(ring)
                    _dashed(d, pts + [pts[0]], colour, SS, 6 * SS, TILE)

        # Soundings, aids, hazards etc. are not drawn here -- see OVERLAY_LAYERS.
        detail = display_scale <= pc.scale * 4   # zoomed in enough for this chart's symbols
        if detail:
            for wx, wy, code, attrs in pc.points:
                if tp[0] <= wx <= tp[2] and tp[1] <= wy <= tp[3]:
                    px, py = (wx * n - x) * TILE, (wy * n - y) * TILE
                    _draw_point(d, px, py, code, attrs)

        # Clip to the chart's coverage. (CELL_NOCOVR rings aren't subtracted: in these
        # files they enclose the whole chart rather than marking holes.)
        mask = Image.new('L', (TILE, TILE), 0)
        md = ImageDraw.Draw(mask)
        for ring in pc.cov:
            md.polygon(to_px(ring), fill=255)
        if mask.getbbox() is None:
            continue
        tile.paste(layer, (0, 0), mask)
        drew_any = True

    if not drew_any:
        return None
    out = tile.resize((256, 256), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, format='PNG', optimize=False)
    return buf.getvalue()


def _clip_segment(x0, y0, x1, y1, lo, hi):
    """Liang-Barsky: the part of a segment inside the square [lo, hi], or None."""
    t0, t1 = 0.0, 1.0
    dx, dy = x1 - x0, y1 - y0
    for p, q in ((-dx, x0 - lo), (dx, hi - x0), (-dy, y0 - lo), (dy, hi - y0)):
        if p == 0:
            if q < 0:
                return None
            continue
        r = q / p
        if p < 0:
            t0 = max(t0, r)
        else:
            t1 = min(t1, r)
        if t0 > t1:
            return None
    return x0 + dx * t0, y0 + dy * t0, x0 + dx * t1, y0 + dy * t1


def _dashed(d, pts, colour, width, dash, size):
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        # Only dash the part on this tile: zoomed in close, one segment of an
        # area outline can be millions of pixels long.
        clipped = _clip_segment(x0, y0, x1, y1, -dash, size + dash)
        if clipped is None:
            continue
        x0, y0, x1, y1 = clipped
        seg = math.hypot(x1 - x0, y1 - y0)
        if seg == 0:
            continue
        steps = int(seg // dash)
        for i in range(0, steps + 1, 2):
            a, b = i * dash / seg, min(1.0, (i + 1) * dash / seg)
            d.line([(x0 + (x1 - x0) * a, y0 + (y1 - y0) * a),
                    (x0 + (x1 - x0) * b, y0 + (y1 - y0) * b)], fill=colour, width=width)


def _draw_point(d, px, py, code, attrs):
    s = SS
    if code == 74:  # Landmark
        r = 3 * s
        d.ellipse([px - r, py - r, px + r, py + r], fill=(20, 20, 20, 255))
    elif code == 91:  # PilotBoardingPlace
        r = 4 * s
        d.ellipse([px - r, py - r, px + r, py + r], outline=(210, 40, 160, 255), width=s)


# ---------------------------------------------------------------------------
# Overlay features (aids, lights, bridges, hazards, areas, soundings) for a view
# ---------------------------------------------------------------------------

MAX_VIEW_TILES = 400        # refuse absurdly large requests
SOUNDING_CELL_PX = (28, 14)  # at most one sounding per this many screen pixels


def _points_in_rings(xs, ys, rings):
    """Vectorized even-odd point-in-polygon: True where a point is inside any ring."""
    inside = np.zeros(len(xs), dtype=bool)
    for ring in rings:
        b = _bbox(ring)
        m = (xs >= b[0]) & (xs <= b[2]) & (ys >= b[1]) & (ys <= b[3]) & ~inside
        if not m.any():
            continue
        idx = np.nonzero(m)[0]
        ax, ay = ring[:, 0], ring[:, 1]
        bx, by = np.roll(ax, 1), np.roll(ay, 1)
        # Points x edges at once, in chunks so the matrix stays ~1M cells.
        step = max(1, 1_000_000 // len(ring))
        for s in range(0, len(idx), step):
            chunk = idx[s:s + step]
            py = ys[chunk][:, None]
            px = xs[chunk][:, None]
            with np.errstate(divide='ignore', invalid='ignore'):
                cross = ((ay > py) != (by > py)) & (px < (bx - ax) * (py - ay) / (by - ay) + ax)
            inside[chunk[np.count_nonzero(cross, axis=1) % 2 == 1]] = True
    return inside


def features_for_view(z, west, south, east, north):
    """{'layers': {LAYER: FeatureCollection}} for everything in OVERLAY_LAYERS
    (plus SOUNDG from zoom 14) inside the given lat/lon box, using the same
    charts the base tiles at zoom z show. Where charts overlap, a feature comes
    only from the most detailed one covering its spot, so nothing is doubled
    up. Returns None if the box is unreasonably large."""
    z = max(MIN_ZOOM, min(MAX_ZOOM, int(z)))
    n = 2 ** z
    (wx0, wx1), (wy1, wy0) = (v.tolist() for v in world_xy([south, north], [west, east]))
    tx0, tx1 = int(max(wx0, 0) * n), int(min(wx1, 0.999999) * n)
    ty0, ty1 = int(max(wy0, 0) * n), int(min(wy1, 0.999999) * n)
    if tx1 < tx0 or ty1 < ty0 or (tx1 - tx0 + 1) * (ty1 - ty0 + 1) > MAX_VIEW_TILES:
        return None
    view = (wx0, wy0, wx1, wy1)

    chosen = {}
    for tx in range(tx0, tx1 + 1):
        for ty in range(ty0, ty1 + 1):
            for c in pick_charts(z, tx, ty)[0]:
                chosen[c['id']] = c
    ordered = sorted(chosen.values(), key=lambda c: c['scale'])   # most detailed first

    layers = {}
    snd = []
    with FEATURE_LOCK:   # the LRU of parsed charts isn't thread-safe
        for i, info in enumerate(ordered):
            pc = _get_prepared(info, _parsed_features, MAX_PARSED_FEATURE_CHARTS)
            finer = [r for c in ordered[:i] for r in c['cov'] if _overlaps(_bbox(r), view)]

            items = [it for it in pc.overlay
                     if view[0] <= it[3] <= view[2] and view[1] <= it[4] <= view[3]]
            if items:
                xs = np.array([it[3] for it in items])
                ys = np.array([it[4] for it in items])
                keep = ~_points_in_rings(xs, ys, finer) if finer else np.ones(len(items), bool)
                for it, k in zip(items, keep):
                    if k:
                        layers.setdefault(it[0], []).append(
                            {'type': 'Feature', 'geometry': it[1], 'properties': it[2]})

            if z >= 14 and len(pc.soundings):
                sx, sy, sd, slat, slon = pc.soundings
                sel = (sx >= view[0]) & (sx <= view[2]) & (sy >= view[1]) & (sy <= view[3])
                if finer and sel.any():
                    idx = np.nonzero(sel)[0]
                    sel[idx[_points_in_rings(sx[idx], sy[idx], finer)]] = False
                if sel.any():
                    snd.append((sx[sel], sy[sel], sd[sel], slat[sel], slon[sel]))

    if snd:
        sx, sy, sd, slat, slon = (np.concatenate(a) for a in zip(*snd))
        # Thin to one per screen cell, keeping the shallowest (the one that matters).
        order = np.argsort(sd)
        cx = (sx[order] * n * 256 // SOUNDING_CELL_PX[0]).astype(np.int64)
        cy = (sy[order] * n * 256 // SOUNDING_CELL_PX[1]).astype(np.int64)
        _, first = np.unique(cx * 10_000_000 + cy, return_index=True)
        keep = order[first]
        layers['SOUNDG'] = [
            {'type': 'Feature',
             'geometry': {'type': 'Point', 'coordinates': [round(lo, 6), round(la, 6), round(d, 2)]},
             'properties': {'DEPTH': round(d, 2)}}
            for la, lo, d in zip(slat[keep].tolist(), slon[keep].tolist(), sd[keep].tolist())]

    return {'layers': {k: {'type': 'FeatureCollection', 'features': v} for k, v in layers.items()},
            'charts': [c['id'] for c in ordered]}


# ---------------------------------------------------------------------------
# Cached entry point used by dashboard_api.py
# ---------------------------------------------------------------------------

class _NewestFirstLock:
    """One tile is drawn at a time, and when the drawer frees up, the most
    recently requested tile goes next. Panning or zooming leaves a pile of
    requests for tiles no longer on screen; newest-first means the view the
    person is looking at now isn't stuck behind them. A request that waits
    longer than `timeout` gives up (acquire returns False)."""

    def __init__(self):
        self._cond = threading.Condition()
        self._busy = False
        self._waiting = []   # tickets in arrival order; the last one goes next
        self._next_ticket = 0

    def acquire(self, timeout):
        with self._cond:
            ticket = self._next_ticket
            self._next_ticket += 1
            self._waiting.append(ticket)
            deadline = time.monotonic() + timeout
            while self._busy or self._waiting[-1] != ticket:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._waiting.remove(ticket)
                    self._cond.notify_all()
                    return False
                self._cond.wait(remaining)
            self._waiting.remove(ticket)
            self._busy = True
            return True

    def release(self):
        with self._cond:
            self._busy = False
            self._cond.notify_all()


TILE_QUEUE = _NewestFirstLock()
TILE_WAIT_SECONDS = 60
BUSY = object()   # get_tile() result when the request waited too long


def get_tile(z, x, y):
    """PNG bytes for z/x/y (from the disk cache when possible), None if no
    chart covers it, or BUSY if it waited too long for its turn."""
    path = os.path.join(CACHE_DIR, str(z), str(x), f'{y}.png')
    empty = path[:-4] + '.empty'
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return f.read()
    if os.path.exists(empty):
        return None
    if not TILE_QUEUE.acquire(TILE_WAIT_SECONDS):
        return BUSY
    try:
        with RENDER_LOCK:
            if os.path.exists(path):  # drawn by another request while we waited
                with open(path, 'rb') as f:
                    return f.read()
            data = render_tile(z, x, y)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if data is None:
                open(empty, 'w').close()
                return None
            tmp = f'{path}.{os.getpid()}.tmp'   # unique: ochart_prerender.py may write too
            with open(tmp, 'wb') as f:
                f.write(data)
            os.replace(tmp, path)
            return data
    finally:
        TILE_QUEUE.release()


def _cli():
    if len(sys.argv) >= 2 and sys.argv[1] == 'index':
        for c in sorted(get_index(), key=lambda c: c['scale']):
            b = c['bounds']
            print(f"{c['id']:<16} 1:{c['scale']:<9} {c['name'][:40]:<40} "
                  f"N{b['north']:.3f} S{b['south']:.3f} W{b['west']:.3f} E{b['east']:.3f}")
        print(index_summary())
    elif len(sys.argv) == 6 and sys.argv[1] == 'tile':
        z, x, y = (int(v) for v in sys.argv[2:5])
        data = render_tile(z, x, y)
        if data is None:
            print('no chart covers this tile')
        else:
            with open(sys.argv[5], 'wb') as f:
                f.write(data)
            print(f'wrote {sys.argv[5]}')
    else:
        print(__doc__)


if __name__ == '__main__':
    # Run through the imported module so pickled charts reference
    # ochart_tiles.PreparedChart (what the server loads), not __main__'s.
    import ochart_tiles
    ochart_tiles._cli()

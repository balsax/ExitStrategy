#!/usr/bin/env python3
"""
chart_inventory.py - Scan decrypted .oesu/.oesenc chart files and report
their name, native scale, and geographic extent -- optionally filtered to
charts that overlap a given bounding box (e.g. to check whether a chart
set actually contains harbor-detail coverage for a specific area like the
BVI).

Usage:
    python chart_inventory.py <chart_dir>
    python chart_inventory.py <chart_dir> --bbox 18.2,18.8,-64.9,-64.2
    python chart_inventory.py <chart_dir> --bbox 18.2,18.8,-64.9,-64.2 --sort scale
"""

import argparse
import glob
import os
import struct
import sys


HEADER_CELL_NAME = 2
HEADER_CELL_NATIVESCALE = 7
CELL_EXTENT_RECORD = 100


def read_header_info(path):
    """Fast partial parse: just pulls name, native scale, and extent,
    skipping all feature/geometry records for speed."""
    name = ""
    native_scale = 0
    extent = None  # (nw_lat, nw_lon, se_lat, se_lon)

    with open(path, "rb") as f:
        data = f.read()

    pos = 0
    n = len(data)
    while pos + 6 <= n:
        record_type, record_length = struct.unpack_from("<HI", data, pos)
        pos += 6
        if record_type == 0:
            break
        payload_len = record_length - 6
        if payload_len < 0 or pos + payload_len > n:
            break
        payload = data[pos:pos + payload_len]
        pos += payload_len

        if record_type == HEADER_CELL_NAME:
            name = payload.split(b"\x00", 1)[0].decode("latin-1", errors="replace")
        elif record_type == HEADER_CELL_NATIVESCALE and payload_len == 4:
            native_scale = struct.unpack("<I", payload)[0]
        elif record_type == CELL_EXTENT_RECORD and payload_len == 64:
            vals = struct.unpack("<8d", payload)
            sw_lat, sw_lon, nw_lat, nw_lon, ne_lat, ne_lon, se_lat, se_lon = vals
            extent = (nw_lat, nw_lon, se_lat, se_lon)
            # Headers always come before feature records in this format,
            # and we have everything we need -- stop early for speed.
            break

    return name, native_scale, extent


def bbox_overlaps(extent, bbox):
    nw_lat, nw_lon, se_lat, se_lon = extent
    south, north, west, east = bbox
    chart_west, chart_east = nw_lon, se_lon
    chart_south, chart_north = se_lat, nw_lat
    return not (chart_east < west or chart_west > east or
                chart_north < south or chart_south > north)


def find_chart_files(input_dir):
    files = []
    for pat in ("**/*.oesu", "**/*.oesenc"):
        files.extend(glob.glob(os.path.join(input_dir, pat), recursive=True))
    return sorted(set(files))


def main():
    ap = argparse.ArgumentParser(description="Inventory decrypted OpenCPN charts")
    ap.add_argument("chart_dir")
    ap.add_argument("--bbox", help="south,north,west,east in decimal degrees "
                                    "(e.g. 18.2,18.8,-64.9,-64.2 for the BVI)")
    ap.add_argument("--sort", choices=["scale", "name", "path"], default="scale")
    args = ap.parse_args()

    bbox = None
    if args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        if len(parts) != 4:
            print("--bbox must be south,north,west,east")
            sys.exit(1)
        bbox = tuple(parts)

    files = find_chart_files(args.chart_dir)
    if not files:
        print(f"No .oesu/.oesenc files found under {args.chart_dir}")
        sys.exit(1)

    print(f"Scanning {len(files)} chart file(s)...")
    results = []
    skipped = 0
    for path in files:
        try:
            name, scale, extent = read_header_info(path)
        except Exception:
            skipped += 1
            continue
        if extent is None:
            skipped += 1
            continue
        if bbox and not bbox_overlaps(extent, bbox):
            continue
        results.append((os.path.basename(path), name, scale, extent))

    if args.sort == "scale":
        results.sort(key=lambda r: r[2] if r[2] else 10**9)
    elif args.sort == "name":
        results.sort(key=lambda r: r[1])
    else:
        results.sort(key=lambda r: r[0])

    label = "matching bbox" if bbox else "total"
    print(f"\n{len(results)} chart(s) {label} (skipped {skipped} unreadable/no-extent)\n")
    print(f"{'File':<22} {'Cell name':<12} {'Scale':>10}   Extent (N/S/E/W)")
    print("-" * 90)
    for fname, name, scale, extent in results:
        nw_lat, nw_lon, se_lat, se_lon = extent
        scale_txt = f"1:{scale}" if scale else "?"
        print(f"{fname:<22} {name:<12} {scale_txt:>10}   "
              f"N{nw_lat:.3f} S{se_lat:.3f} E{se_lon:.3f} W{nw_lon:.3f}")

    if bbox and results:
        detailed = [r for r in results if r[2] and r[2] <= 25000]
        print(f"\n{len(detailed)} of those are harbor-detail scale (<=1:25,000).")


if __name__ == "__main__":
    main()

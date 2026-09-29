#!/usr/bin/env python3
"""ochart_prerender.py - Draw the O-Charts tiles for zoomed-out levels ahead
of time, so panning between islands on the Chart tab doesn't wait for tiles
to be drawn on first view. Writes into the same disk cache the dashboard
serves from (ochart_tiles.CACHE_DIR) and skips tiles already there, so it
can be stopped and re-run at any time.

Only tiles that some chart suited to that zoom actually covers are drawn:
a tile counts if it overlaps a chart no more than 4x coarser than the zoom's
display scale (so the 1:3,000,000 / 1:20,000,000 overview charts don't make
it draw thousands of empty-ocean or inland tiles at zoom 11).

Run it at low priority; the dashboard keeps serving meanwhile:
    nice -n 19 python3 ochart_prerender.py            # zooms 6-11
    nice -n 19 python3 ochart_prerender.py 6 9        # a different range
    python3 ochart_prerender.py --count               # just count tiles
    nice -n 19 python3 ochart_prerender.py 13 14 --workers=3   # parallel drawing
"""

import math
import multiprocessing
import os
import sys
import threading
import time

import ochart_tiles as ot


def tile_range(wbbox, z):
    n = 2 ** z
    x0, y0, x1, y1 = wbbox
    return (int(max(x0, 0) * n), int(min(x1, 0.999999) * n),
            int(max(y0, 0) * n), int(min(y1, 0.999999) * n))


def tiles_for_zoom(z):
    tiles = set()
    for c in ot.get_index():
        x0, x1, y0, y1 = tile_range(c['wbbox'], z)
        for y in range(y0, y1 + 1):
            lat = ot.tile_center_lat(z, y)
            display_scale = 156543.034 * math.cos(math.radians(lat)) / (2 ** z) / 0.00028
            if c['scale'] > display_scale * 4:
                break   # far too coarse for this zoom -- other charts cover what matters
            for x in range(x0, x1 + 1):
                tiles.add((x, y))
    return sorted(tiles)


def cached(z, x, y):
    path = os.path.join(ot.CACHE_DIR, str(z), str(x), f'{y}.png')
    return os.path.exists(path) or os.path.exists(path[:-4] + '.empty')


def save(z, x, y, data):
    path = os.path.join(ot.CACHE_DIR, str(z), str(x), f'{y}.png')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if data is None:
        open(path[:-4] + '.empty', 'w').close()
        return
    tmp = f'{path}.{os.getpid()}.{threading.get_ident()}.tmp'
    with open(tmp, 'wb') as f:
        f.write(data)
    os.replace(tmp, path)


CHUNK = 100   # tiles per work unit; neighbours share charts, so keep them together


def draw_chunk(job):
    """Worker: draw the not-yet-cached tiles of one chunk. Returns count drawn."""
    z, tiles = job
    drawn = 0
    for x, y in tiles:
        if not cached(z, x, y):
            save(z, x, y, ot.render_tile(z, x, y))
            drawn += 1
    return drawn


def main_parallel(plan, workers):
    start = time.time()
    drawn = 0
    for z, tiles in plan:
        todo = [t for t in tiles if not cached(z, *t)]
        jobs = [(z, todo[i:i + CHUNK]) for i in range(0, len(todo), CHUNK)]
        zdrawn = 0
        with multiprocessing.get_context('fork').Pool(workers) as pool:
            for i, n in enumerate(pool.imap_unordered(draw_chunk, jobs), 1):
                zdrawn += n
                if i % 5 == 0 or i == len(jobs):
                    rate = (drawn + zdrawn) / (time.time() - start)
                    print(f'z{z}: {zdrawn}/{len(todo)} drawn, {rate:.1f} tiles/s', flush=True)
        drawn += zdrawn
        print(f'z{z} finished ({time.time() - start:.0f}s so far)', flush=True)
    print(f'done: drew {drawn} tiles in {time.time() - start:.0f}s', flush=True)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    zmin, zmax = (int(args[0]), int(args[1])) if len(args) == 2 else (6, 11)
    plan = [(z, tiles_for_zoom(z)) for z in range(zmin, zmax + 1)]
    for z, tiles in plan:
        todo = sum(1 for x, y in tiles if not cached(z, x, y))
        print(f'z{z}: {len(tiles)} tiles, {todo} not drawn yet', flush=True)
    if '--count' in sys.argv:
        return
    workers = next((int(a.split('=', 1)[1]) for a in sys.argv if a.startswith('--workers=')), 1)
    if workers > 1:
        return main_parallel(plan, workers)

    start = time.time()
    done = drawn = 0
    total = sum(len(t) for _, t in plan)
    for z, tiles in plan:
        for x, y in tiles:
            done += 1
            if cached(z, x, y):
                continue
            save(z, x, y, ot.render_tile(z, x, y))
            drawn += 1
            if drawn % 200 == 0:
                rate = drawn / (time.time() - start)
                print(f'{done}/{total} checked, {drawn} drawn, {rate:.1f} tiles/s, '
                      f'at z{z}', flush=True)
        print(f'z{z} finished ({time.time() - start:.0f}s so far)', flush=True)
    print(f'done: drew {drawn} tiles in {time.time() - start:.0f}s', flush=True)


if __name__ == '__main__':
    main()

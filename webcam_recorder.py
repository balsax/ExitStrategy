#!/usr/bin/env python3
"""Rolling 24 h recorder for the two IP cameras (Webcams tab playback).

One ffmpeg per camera pulls the RTSP main stream and writes 5-minute MP4
files, copying the H.264 video as-is (no re-encode, so very little CPU) and
converting the camera's G.711 audio to AAC so browsers can play it. Files are
fragmented MP4, so whatever was written before a crash or power cut still
plays. Names are the segment's UTC start time: <dir>/<cam id>/YYYYmmdd-HHMMSS.mp4

Every minute a pruner deletes files older than retention_hours, and deletes
the oldest files early if the drive's free space drops below min_free_gb, so
this can never fill the OS drive. Finished files are rewritten with a normal
MP4 index (see finalize) so the browser player can seek in them.

The same ffmpeg also keeps a rolling HLS feed in RAM (live_dir) for the
tab's full-resolution Live view.

Config (with the camera login, so kept out of git): ~/.config/webcams/cameras.json
Runs as the user service webcam-recorder.service (see ops/webcam-recorder.service).
"""
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

CONFIG_PATH = os.path.expanduser('~/.config/webcams/cameras.json')
STALL_SECONDS = 60      # restart ffmpeg if its current file stops growing this long
PRUNE_EVERY = 60

log = logging.getLogger('webcam_recorder')
stop_event = threading.Event()


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def segment_start(fname):
    """UTC start time from a segment's file name, or None if it isn't one."""
    try:
        return datetime.strptime(fname[:15], '%Y%m%d-%H%M%S').replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def newest_file(cam_dir):
    files = [f for f in os.listdir(cam_dir) if f.endswith('.mp4')]
    return os.path.join(cam_dir, max(files)) if files else None


def record_camera(cam_id, cam, cfg):
    cam_dir = os.path.join(cfg['recordings_dir'], cam_id)
    os.makedirs(cam_dir, exist_ok=True)
    cmd = [
        # 'error': the cameras' audio timestamps jitter a little, which ffmpeg fixes
        # but warns about every few seconds -- not worth filling the journal with
        'ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin',
        '-rtsp_transport', 'tcp', '-timeout', '10000000',   # 10 s socket timeout (µs)
        '-i', cam['rtsp'],
        '-map', '0:v', '-map', '0:a?',
        '-c:v', 'copy', '-c:a', 'aac', '-b:a', '32k',
        '-f', 'segment', '-segment_time', str(cfg.get('segment_seconds', 300)),
        '-segment_atclocktime', '1', '-reset_timestamps', '1', '-strftime', '1',
        '-segment_format', 'mp4',
        '-segment_format_options', 'movflags=+frag_keyframe+empty_moov+default_base_moof',
        os.path.join(cam_dir, '%Y%m%d-%H%M%S.mp4'),
    ]
    live_dir = cfg.get('live_dir')
    if live_dir:
        # Second output from the same camera connection: a short rolling HLS
        # playlist for the Webcams tab's full-resolution Live view. Video only
        # (the live view is muted), stream copy, kept in RAM (tmpfs) so it adds
        # no SSD writes. Segments can only cut at keyframes, which these cameras
        # send every 4 s, so the live picture runs several seconds behind.
        cam_live = os.path.join(live_dir, cam_id)
        shutil.rmtree(cam_live, ignore_errors=True)
        os.makedirs(cam_live, exist_ok=True)
        cmd += [
            '-map', '0:v', '-c:v', 'copy', '-an',
            '-f', 'hls', '-hls_time', '2', '-hls_list_size', '4',
            '-hls_flags', 'delete_segments+omit_endlist+temp_file',
            '-hls_segment_filename', os.path.join(cam_live, 'seg%06d.ts'),
            os.path.join(cam_live, 'index.m3u8'),
        ]
    env = dict(os.environ, TZ='UTC')    # strftime file names in UTC (no DST gaps/repeats)
    backoff = 5
    while not stop_event.is_set():
        log.info('%s: starting ffmpeg', cam['name'])
        started = time.time()
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        threading.Thread(target=_pipe_log, args=(cam['name'], proc.stderr), daemon=True).start()
        last_size, last_change = -1, time.time()
        while proc.poll() is None and not stop_event.is_set():
            time.sleep(5)
            f = newest_file(cam_dir)
            size = os.path.getsize(f) if f and os.path.exists(f) else -1
            if (f, size) != last_size:
                last_size, last_change = (f, size), time.time()
            elif time.time() - last_change > STALL_SECONDS:
                log.warning('%s: no new video for %ds, restarting ffmpeg', cam['name'], STALL_SECONDS)
                break
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if stop_event.is_set():
            return
        backoff = 5 if time.time() - started > 120 else min(backoff * 2, 60)
        log.warning('%s: ffmpeg exited (%s), retrying in %ds', cam['name'], proc.returncode, backoff)
        stop_event.wait(backoff)


def _pipe_log(name, stream):
    for line in stream:
        log.info('%s ffmpeg: %s', name, line.rstrip())


def finalize(cfg):
    """Rewrite finished segments as regular (indexed) MP4s.

    The fragmented files ffmpeg writes survive a crash, but have no index, so
    browsers can't tell their length and seek badly. Once a camera has moved on
    to its next file, remux the previous one (stream copy, a second or two) with
    the index at the front, and swap it in atomically. Finished files are
    marked by the .done list so each is only processed once.
    """
    for cam_id in cfg['cameras']:
        cam_dir = os.path.join(cfg['recordings_dir'], cam_id)
        if not os.path.isdir(cam_dir):
            continue
        done_path = os.path.join(cam_dir, '.done')
        try:
            with open(done_path) as f:
                done = set(f.read().split())
        except FileNotFoundError:
            done = set()
        names = sorted(f for f in os.listdir(cam_dir) if f.endswith('.mp4') and segment_start(f))
        for name in names[:-1]:                     # the newest one is still being written
            if name in done:
                continue
            src = os.path.join(cam_dir, name)
            tmp = os.path.join(cam_dir, '.' + name + '.tmp')
            r = subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin', '-y',
                                '-i', src, '-map', '0', '-c', 'copy', '-movflags', '+faststart',
                                '-f', 'mp4', tmp], capture_output=True, text=True)
            if r.returncode == 0 and os.path.getsize(tmp) > 0:
                os.replace(tmp, src)
            else:
                log.warning('finalize %s failed, keeping fragmented file: %s', src, r.stderr.strip()[-300:])
                if os.path.exists(tmp):
                    os.remove(tmp)
            done.add(name)
        keep = done & set(names)                    # forget pruned files
        with open(done_path, 'w') as f:
            f.write('\n'.join(sorted(keep)))


def prune(cfg):
    root = cfg['recordings_dir']
    cutoff = time.time() - cfg.get('retention_hours', 24) * 3600 - cfg.get('segment_seconds', 300)
    files = []
    for cam_id in cfg['cameras']:
        cam_dir = os.path.join(root, cam_id)
        if not os.path.isdir(cam_dir):
            continue
        names = sorted(f for f in os.listdir(cam_dir) if f.endswith('.mp4') and segment_start(f))
        for i, f in enumerate(names):
            # the newest file per camera is the one being written; never touch it
            files.append((segment_start(f).timestamp(), os.path.join(cam_dir, f), i == len(names) - 1))
    files.sort()
    for start, path, current in files:
        if start < cutoff and not current:
            _remove(path, 'older than retention')
    min_free = cfg.get('min_free_gb', 25) * 1024 ** 3
    for start, path, current in files:
        if shutil.disk_usage(root).free >= min_free:
            break
        if not current and os.path.exists(path):
            _remove(path, 'low disk space')


def _remove(path, why):
    try:
        os.remove(path)
        log.info('deleted %s (%s)', path, why)
    except FileNotFoundError:
        pass


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
    cfg = load_config()
    os.makedirs(cfg['recordings_dir'], exist_ok=True)
    signal.signal(signal.SIGTERM, lambda *a: stop_event.set())
    signal.signal(signal.SIGINT, lambda *a: stop_event.set())
    threads = [threading.Thread(target=record_camera, args=(cid, cam, cfg), daemon=True)
               for cid, cam in cfg['cameras'].items()]
    for t in threads:
        t.start()
    while not stop_event.is_set():
        try:
            finalize(cfg)
        except Exception:
            log.exception('finalize failed')
        try:
            prune(cfg)
        except Exception:
            log.exception('prune failed')
        stop_event.wait(PRUNE_EVERY)
    for t in threads:
        t.join(15)


if __name__ == '__main__':
    main()

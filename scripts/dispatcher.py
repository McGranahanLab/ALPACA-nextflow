#!/usr/bin/env python3
"""Dispatcher: move segments from pool to per-worker queue dirs.

This runs concurrently with workers and fills each worker's queue when empty.
"""
import os
import time
import argparse
import shutil
import traceback


def list_csv(path):
    try:
        return [f for f in os.listdir(path) if f.endswith(".csv")]
    except Exception:
        return []


def move_files(src_dir, dst_dir, files):
    moved = []
    for f in files:
        s = os.path.join(src_dir, f)
        d = os.path.join(dst_dir, f)
        try:
            os.makedirs(os.path.dirname(d), exist_ok=True)
            os.rename(s, d)
            moved.append(f)
        except Exception:
            # try copy fallback
            try:
                tmp = d + ".tmp"
                shutil.copy2(s, tmp)
                os.replace(tmp, d)
                try:
                    os.remove(s)
                except Exception:
                    pass
                moved.append(f)
            except Exception:
                traceback.print_exc()
                continue
    return moved


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pool-dir", required=True)
    p.add_argument(
        "--in-progress-dir", required=True, help="base dir for worker subdirs"
    )
    p.add_argument("--outputs-dir", required=True)
    p.add_argument("--workers", type=int, required=True)
    p.add_argument("--segments-per-claim", type=int, default=1)
    p.add_argument("--poll-interval", type=float, default=1.0)
    p.add_argument("--max-idle", type=int, default=30)
    args = p.parse_args()

    pool = args.pool_dir
    workers = [f"worker_{i}" for i in range(1, args.workers + 1)]

    # ensure per-worker queue and in_progress dirs exist and emit a ready token
    for w in workers:
        q = os.path.join(args.in_progress_dir, w, "queue")
        ip = os.path.join(args.in_progress_dir, w, "in_progress")
        os.makedirs(q, exist_ok=True)
        os.makedirs(ip, exist_ok=True)

    idle = 0
    while True:
        try:
            pool_files = list_csv(pool)
            if not pool_files:
                idle += 1
                if idle > args.max_idle:
                    # done
                    break
                time.sleep(args.poll_interval)
                continue
            idle = 0

            # for each worker, if queue empty, move up to segments-per-claim from pool
            for w in workers:
                qdir = os.path.join(args.in_progress_dir, w, "queue")
                qfiles = list_csv(qdir)
                if qfiles:
                    continue
                # claim files for this worker
                to_move = pool_files[: args.segments_per_claim]
                if not to_move:
                    break
                move_files(pool, qdir, to_move)
                # refresh pool_files
                pool_files = list_csv(pool)
        except Exception:
            traceback.print_exc()
            time.sleep(args.poll_interval)

if __name__ == "__main__":
    main()

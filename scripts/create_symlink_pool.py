#!/usr/bin/env python3
"""
Create a lightweight pool directory of symlinks to segment tables (csv/tsv/parquet) under a cohort.
Each worker will claim segments from this pool.

Usage:
  python3 scripts/create_symlink_pool.py --cohort_dir dev/multi-tumour --pool_dir dev/multi-tumour/pool

By default this walks <cohort_dir>/input/<tumour> and splits input table into single file for each segment.
Tehn it symlinks each such file into <pool_dir> and prefixes each filename with the tumour directory name to avoid name collisions.

"""
import os
import argparse
import pandas as pd
from pathlib import Path
from utils import read_any_table, write_table, find_table


def split_to_segments(tumour_dir: str, segments_filter: set[str] | None = None) -> list[str]:
    segments_dir_path = f"{tumour_dir}/segments"
    df_path = find_table(tumour_dir, "ALPACA_input_table")
    if df_path is None:
        raise FileNotFoundError(f"No ALPACA_input_table.[csv|tsv|parquet] found in {tumour_dir}")
    os.makedirs(segments_dir_path, exist_ok=True)
    # segment files keep the format of the tumour's input table (ALPACA reads only csv/parquet, so tsv becomes csv)
    df, file_type = read_any_table(df_path)
    if file_type == "tsv":
        file_type = "csv"
    tumour_id = df["tumour_id"].iloc[0]
    assert len(df["tumour_id"].unique()) == 1, "Found multiple tumour ids. In tummour mode only one tumour_id is allowed per input table"
    segments = []
    for segment, segment_df in df.groupby("segment"):
        if segments_filter and str(segment) not in segments_filter:
            continue
        segment_df_path = (
            f"{segments_dir_path}/ALPACA_input_table_{tumour_id}_{segment}.{file_type}"
        )
        write_table(segment_df, segment_df_path, file_type)
        segments.append(segment_df_path)
    return segments


p = argparse.ArgumentParser()
p.add_argument(
    "--input_dir",
    required=False,
)
p.add_argument("--pool_dir", required=True)
p.add_argument(
    "--done_dir",
    required=False,
    help="Optional path to done directory to skip already-processed basenames",
)
p.add_argument(
    "--tumours",
    required=False,
    default=None,
    help="Optional comma-separated list of tumour dirs to include (names only)",
)
p.add_argument(
    "--segments",
    required=False,
    default=None,
    help="Optional comma-separated list of segment names to include",
)
args = p.parse_args()

input_dir = Path(args.input_dir)
pool_dir = Path(args.pool_dir)
done_dir = Path(args.done_dir) if args.done_dir else None
if args.tumours:
    tumours_filter = set([t.strip() for t in args.tumours.split(",") if t.strip()])
else:
    tumours_filter = None
if args.segments:
    segments_filter = set([s.strip() for s in args.segments.split(",") if s.strip()])
else:
    segments_filter = None

if not input_dir.exists():
    raise SystemExit(f"Input dir does not exist: {input_dir}")

pool_dir.mkdir(parents=True, exist_ok=True)


count = 0
skipped = 0
done_basenames = set()
if done_dir and done_dir.exists():
    for root, _, files in os.walk(str(done_dir)):
        for fn in files:
            done_basenames.add(fn)
for tumour_entry in input_dir.iterdir():
    if not tumour_entry.is_dir():
        continue
    tumour_name = tumour_entry.name
    if tumours_filter and tumour_name not in tumours_filter:
        continue
    try:
        segment_paths = split_to_segments(str(tumour_entry), segments_filter)
    except Exception as e:
        print(f"Warning: split_to_segments failed for {tumour_entry}: {e}")
        segment_paths = []

    for src_path in segment_paths:
        src = Path(src_path)
        # sanitize filename: ensure no whitespaces or newlines
        f = src.name.strip()
        # skip if already present in done_dir (by basename)
        if f in done_basenames:
            skipped += 1
            continue
        dst = pool_dir / f
        try:
            if dst.exists() or dst.is_symlink():
                dst.unlink()
            os.symlink(src.resolve(), dst)
            count += 1
        except Exception as e:
            print(f"Warning: failed to create symlink for {src}: {e}")
print(f"Created {count} symlinks in pool: {pool_dir} (skipped {skipped} already-done)")

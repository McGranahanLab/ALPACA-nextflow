#!/usr/bin/env python3
"""
Create a lightweight pool directory of symlinks to segment CSVs under a cohort.
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


def split_to_segments(tumour_dir: str) -> list[str]:
    segments_dir_path = f"{tumour_dir}/segments"
    df_path = f"{tumour_dir}/ALPACA_input_table.csv"
    os.makedirs(segments_dir_path, exist_ok=True)
    df = pd.read_csv(df_path)
    tumour_id = df["tumour_id"].iloc[0]
    assert len(
        df["tumour_id"].unique()
    ), "Found multiple tumour ids. In tummour mode only one tumour_id is allowed per input csv"
    segments = []
    for segment, segment_df in df.groupby("segment"):
        segment_df_path = (
            f"{segments_dir_path}/ALPACA_input_table_{tumour_id}_{segment}.csv"
        )
        segment_df.to_csv(segment_df_path, index=False)
        segments.append(segment_df_path)
    return segments


p = argparse.ArgumentParser()
p.add_argument("--cohort_dir", required=True)
p.add_argument("--pool_dir", required=True)
p.add_argument(
    "--tumours",
    required=False,
    default=None,
    help="Optional comma-separated list of tumour dirs to include (names only)",
)
args = p.parse_args()

cohort_dir = Path(args.cohort_dir)
pool_dir = Path(args.pool_dir)
if args.tumours:
    tumours_filter = set([t.strip() for t in args.tumours.split(",") if t.strip()])
else:
    tumours_filter = None

if not cohort_dir.exists():
    raise SystemExit(f"Cohort dir does not exist: {cohort_dir}")

pool_dir.mkdir(parents=True, exist_ok=True)

input_root = cohort_dir / "input"
if not input_root.exists():
    # if user passed a directory that already contains tumour subdirs directly, use cohort_dir
    input_root = cohort_dir

count = 0
for tumour_entry in input_root.iterdir():
    if not tumour_entry.is_dir():
        continue
    tumour_name = tumour_entry.name
    if tumours_filter and tumour_name not in tumours_filter:
        continue
    if split_to_segments:
        try:
            segment_paths = split_to_segments(str(tumour_entry))
        except Exception as e:
            print(f"Warning: split_to_segments failed for {tumour_entry}: {e}")
            segment_paths = []

    for src_path in segment_paths:
        src = Path(src_path)
        # sanitize filename: ensure no whitespaces or newlines
        f = src.name.strip()
        dst = pool_dir / f
        try:
            if dst.exists() or dst.is_symlink():
                dst.unlink()
            os.symlink(src.resolve(), dst)
            count += 1
        except Exception as e:
            print(f"Warning: failed to create symlink for {src}: {e}")

print(f"Created {count} symlinks in pool: {pool_dir}")

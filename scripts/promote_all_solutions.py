#!/usr/bin/env python3
"""Promote ALPACA's `--output_all_solutions` outputs to the user's output dir.

Workers write per-segment optimal solutions to
`${outputs_dir}/segment_outputs/`, and when ALPACA is run with
`--output_all_solutions` it additionally writes to
`${outputs_dir}/segment_outputs/all_solutions/<segment>/`:

    all_<tumour>_<segment>.csv          # candidate solutions (elbow search)
    all_max_<tumour>_<segment>.csv      # unconstrained max solution
    <tumour>_<segment>_elbow_table.csv  # elbow-search metadata
    <tumour>_<segment>_elbow_plot.png

Without this script the entire tree stays inside the scratch `alpaca-work/`
dir and is deleted by the pipeline's cleanup step. This script mirrors the
tree into the user's `OUTPUT_DIR/all_solutions/` and additionally
concatenates the two "all" CSV families into cohort-level tables in
`OUTPUT_DIR/cohort_results/`.

The script is a no-op (exit 0) if the source tree doesn't exist, so it's
safe to run unconditionally in `mergeSegments` regardless of whether
`--output_all_solutions` was passed to ALPACA.
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
from pathlib import Path
from typing import List

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--segments-dir",
        required=True,
        help="Path to segment_outputs/ (i.e. ALPACA's --output_directory in per-segment mode).",
    )
    p.add_argument(
        "--output-dir",
        required=True,
        help="User-facing output directory (OUTPUT_DIR).",
    )
    return p.parse_args()


def concat_pattern(files: List[Path], out_path: Path) -> None:
    if not files:
        return
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception as exc:
            print(f"WARN: could not read {f}: {exc}", file=sys.stderr)
    if not frames:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(frames, ignore_index=True).to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(frames)} segment(s))")


def main() -> int:
    args = parse_args()
    src = Path(args.segments_dir) / "all_solutions"
    if not src.is_dir():
        print(f"No all_solutions/ tree at {src}; nothing to promote.")
        return 0

    dst = Path(args.output_dir) / "all_solutions"
    dst.mkdir(parents=True, exist_ok=True)
    # `copytree(dirs_exist_ok=True)` merges into an existing tree instead of
    # failing — needed because RESTART=1 runs may rerun the pipeline.
    shutil.copytree(src, dst, dirs_exist_ok=True)
    print(f"Promoted {src} -> {dst}")

    cohort = Path(args.output_dir) / "cohort_results"
    # `all_<tumour>_<segment>.csv` (candidate solutions per segment)
    optimal_files = sorted(
        Path(p)
        for p in glob.glob(str(dst / "*" / "all_*.csv"))
        if not os.path.basename(p).startswith("all_max_")
    )
    concat_pattern(optimal_files, cohort / "all_solutions_combined.csv")

    # `all_max_<tumour>_<segment>.csv` (unconstrained max per segment)
    max_files = sorted(Path(p) for p in glob.glob(str(dst / "*" / "all_max_*.csv")))
    concat_pattern(max_files, cohort / "all_max_combined.csv")

    return 0


if __name__ == "__main__":
    sys.exit(main())

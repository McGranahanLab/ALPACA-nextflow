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

The promoted layout is:

    ${OUTPUT_DIR}/all_solutions/
        <tumour>/
            all_solutions/
                all_<tumour>.csv
                all_max_<tumour>.csv
                <tumour>_elbow_table.csv
            elbow_plots/
                <tumour>_<segment>_elbow_plot.png

"""

from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd


def parse_file_tumour_and_segment(path: Path) -> Tuple[str, str, str]:
    stem = path.stem

    if stem.endswith("_elbow_table"):
        stem = stem[: -len("_elbow_table")]
        tumour, segment = stem.split("_", 1)
        return tumour, segment, "elbow_table"

    if stem.endswith("_elbow_plot"):
        stem = stem[: -len("_elbow_plot")]
        tumour, segment = stem.split("_", 1)
        return tumour, segment, "elbow_plot"

    if stem.startswith("all_max_"):
        suffix = stem[len("all_max_") :]
        tumour, segment = suffix.split("_", 1)
        return tumour, segment, "all_max"

    if stem.startswith("all_"):
        suffix = stem[len("all_") :]
        tumour, segment = suffix.split("_", 1)
        return tumour, segment, "all"

    raise ValueError(f"Could not parse tumour/segment from file name: {path}")


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


def promote_all_solutions(segments_dir: Path | str, output_dir: Path | str) -> None:
    src = Path(segments_dir) / "all_solutions"
    if not src.is_dir():
        print(f"No all_solutions/ tree at {src}; nothing to promote.")
        return

    dst = Path(output_dir) / "all_solutions"
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)

    tumour_to_files: Dict[str, Dict[str, List[Path]]] = {}
    for segment_dir in sorted(src.iterdir()):
        if not segment_dir.is_dir():
            continue
        for f in sorted(segment_dir.iterdir()):
            if not f.is_file():
                continue
            try:
                tumour, segment, modality = parse_file_tumour_and_segment(f)
            except ValueError:
                continue
            tumour_bucket = tumour_to_files.setdefault(
                tumour, {"all": [], "all_max": [], "elbow_table": [], "elbow_plot": []}
            )
            if (
                f.name.startswith("all_")
                and f.suffix == ".csv"
                and not f.name.startswith("all_max_")
            ):
                tumour_bucket["all"].append(f)
            elif f.name.startswith("all_max_") and f.suffix == ".csv":
                tumour_bucket["all_max"].append(f)
            elif f.name.endswith("_elbow_table.csv"):
                tumour_bucket["elbow_table"].append(f)
            elif f.name.endswith("_elbow_plot.png"):
                tumour_bucket["elbow_plot"].append(f)

    for tumour, groups in sorted(tumour_to_files.items()):
        tumour_dir = dst / tumour
        all_dir = tumour_dir / "all_solutions"
        plot_dir = tumour_dir / "elbow_plots"
        all_dir.mkdir(parents=True, exist_ok=True)
        plot_dir.mkdir(parents=True, exist_ok=True)

        concat_pattern(groups["all"], all_dir / f"all_{tumour}.csv")
        concat_pattern(groups["all_max"], all_dir / f"all_max_{tumour}.csv")
        concat_pattern(groups["elbow_table"], all_dir / f"{tumour}_elbow_table.csv")

        for src_plot in sorted(groups["elbow_plot"]):
            dst_plot = plot_dir / src_plot.name
            shutil.copy2(src_plot, dst_plot)
            print(f"Copied {src_plot} -> {dst_plot}")

        print(f"Promoted tumour {tumour} -> {tumour_dir}")

    print(f"Promoted {src} -> {dst}")


def main() -> int:
    args = parse_args()
    promote_all_solutions(args.segments_dir, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())

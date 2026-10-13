#!/usr/bin/env python3
"""Promote ALPACA's `--output_all_solutions` outputs to the user's output dir.

Workers write per-segment optimal solutions to
`${outputs_dir}/segment_outputs/`, and when ALPACA is run with
`--output_all_solutions` it additionally writes to
`${outputs_dir}/segment_outputs/all_solutions/<tumour>/<segment>/`
(the worker moves ALPACA's raw per-segment output there to avoid different
tumours colliding on a shared segment id):

    all_<tumour>_<segment>.<ext>          # candidate solutions (elbow search)
    all_max_<tumour>_<segment>.<ext>      # unconstrained max solution
    <tumour>_<segment>_elbow_table.<ext>  # elbow-search metadata
    <tumour>_<segment>_elbow_plot.png

where <ext> is csv or parquet (ALPACA's --output_format). The promoted layout is:

    ${OUTPUT_DIR}/all_solutions/
        <tumour>/
            all_solutions/
                all_<tumour>.<ext>
                all_max_<tumour>.<ext>
                <tumour>_elbow_table.<ext>
            elbow_plots/
                <tumour>_<segment>_elbow_plot.png

"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from utils import read_any_table, write_table, is_table_file, strip_table_ext, with_table_ext, common_table_type


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
    """Concatenate segment tables into out_path; the extension follows the inputs' format."""
    if not files:
        return
    frames = []
    file_types = set()
    for f in files:
        try:
            df, file_type = read_any_table(str(f))
            frames.append(df)
            file_types.add(file_type)
        except Exception as exc:
            print(f"WARN: could not read {f}: {exc}", file=sys.stderr)
    if not frames:
        return
    out_path = Path(with_table_ext(str(out_path), common_table_type(file_types)))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_table(pd.concat(frames, ignore_index=True), str(out_path))
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
    for tumour_dir in sorted(src.iterdir()):
        if not tumour_dir.is_dir():
            continue
        tumour = tumour_dir.name
        tumour_bucket = tumour_to_files.setdefault(
            tumour, {"all": [], "all_max": [], "elbow_table": [], "elbow_plot": []}
        )
        for segment_dir in sorted(tumour_dir.iterdir()):
            if not segment_dir.is_dir():
                continue
            for f in sorted(segment_dir.iterdir()):
                if not f.is_file():
                    continue
                if is_table_file(f.name) and strip_table_ext(f.name).endswith("_elbow_table"):
                    tumour_bucket["elbow_table"].append(f)
                elif is_table_file(f.name) and f.name.startswith("all_max_"):
                    tumour_bucket["all_max"].append(f)
                elif is_table_file(f.name) and f.name.startswith("all_"):
                    tumour_bucket["all"].append(f)
                elif f.name.endswith("_elbow_plot.png"):
                    tumour_bucket["elbow_plot"].append(f)

    for tumour, groups in sorted(tumour_to_files.items()):
        tumour_dir = dst / tumour
        all_dir = tumour_dir / "all_solutions"
        plot_dir = tumour_dir / "elbow_plots"
        all_dir.mkdir(parents=True, exist_ok=True)
        plot_dir.mkdir(parents=True, exist_ok=True)

        concat_pattern(groups["all"], all_dir / f"all_{tumour}")
        concat_pattern(groups["all_max"], all_dir / f"all_max_{tumour}")
        concat_pattern(groups["elbow_table"], all_dir / f"{tumour}_elbow_table")

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

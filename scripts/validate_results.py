#!/usr/bin/env python3
import argparse
import csv
import os
from pathlib import Path

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("expected_list")
    p.add_argument("actual_list")
    p.add_argument(
        "--input-dir",
        default="",
        help="Cohort input directory (for all_solutions checks)",
    )
    p.add_argument(
        "--output-dir",
        default="",
        help="User-facing output directory (for all_solutions checks)",
    )
    p.add_argument(
        "--restrict-tumours",
        default="",
        help="Optional comma-separated list of tumour dirs to restrict to",
    )
    p.add_argument(
        "--restrict-segments",
        default="",
        help="Optional comma-separated list of segment names to restrict to",
    )
    return p.parse_args()


def validate_segment_lists(expected_list, actual_list):
    expected = (
        set(Path(expected_list).read_text().splitlines())
        if Path(expected_list).exists()
        else set()
    )
    actual = (
        set(Path(actual_list).read_text().splitlines())
        if Path(actual_list).exists()
        else set()
    )
    expected = {
        s.replace("ALPACA_input_table_", "").replace(".csv", "") for s in expected
    }
    actual = {
        s.replace("optimal_", "").replace("all_", "").replace(".csv", "")
        for s in actual
    }
    return sorted(expected - actual)


def expected_segments_by_tumour(input_dir, tumours_filter, segments_filter):
    by_tumour = {}
    if not input_dir or not os.path.isdir(input_dir):
        return by_tumour
    for tumour_id in sorted(os.listdir(input_dir)):
        if tumour_id == ".DS_Store":
            continue
        if tumours_filter and tumour_id not in tumours_filter:
            continue
        table_path = os.path.join(input_dir, tumour_id, "ALPACA_input_table.csv")
        if not os.path.isfile(table_path):
            continue
        df = pd.read_csv(table_path)
        segments = set(df["segment"].astype(str))
        if segments_filter:
            segments &= segments_filter
        by_tumour[tumour_id] = segments
    return by_tumour


def validate_all_solutions(output_dir, input_dir, tumours_filter, segments_filter):
    """Check per-tumour all_solutions outputs (only produced with --output_all_solutions)."""
    rows = []
    if not output_dir or not (Path(output_dir) / "all_solutions").is_dir():
        return rows

    all_solutions_root = Path(output_dir) / "all_solutions"
    expected_by_tumour = expected_segments_by_tumour(
        input_dir, tumours_filter, segments_filter
    )

    for tumour_id, expected_segments in sorted(expected_by_tumour.items()):
        tumour_solutions_dir = all_solutions_root / tumour_id / "all_solutions"

        all_csv = tumour_solutions_dir / f"all_{tumour_id}.csv"
        if not all_csv.exists():
            rows.append(
                {
                    "tumour_id": tumour_id,
                    "check": "missing_all_solutions_file",
                    "details": str(all_csv),
                }
            )
        else:
            all_df = pd.read_csv(all_csv)
            present_segments = (
                set(all_df["segment"].astype(str))
                if "segment" in all_df.columns
                else set()
            )
            missing = sorted(expected_segments - present_segments)
            if missing:
                rows.append(
                    {
                        "tumour_id": tumour_id,
                        "check": "segments_missing_from_all_solutions",
                        "details": ", ".join(missing),
                    }
                )

        elbow_csv = tumour_solutions_dir / f"{tumour_id}_elbow_table.csv"
        if not elbow_csv.exists():
            rows.append(
                {
                    "tumour_id": tumour_id,
                    "check": "missing_elbow_table_file",
                    "details": str(elbow_csv),
                }
            )
        else:
            elbow_df = pd.read_csv(elbow_csv)
            if "optimal_complexity" not in elbow_df.columns:
                rows.append(
                    {
                        "tumour_id": tumour_id,
                        "check": "elbow_table_missing_optimal_complexity_column",
                        "details": str(elbow_csv),
                    }
                )
            else:
                unique_values = sorted(
                    set(elbow_df["optimal_complexity"].dropna().astype(str))
                )
                if len(unique_values) != 1:
                    rows.append(
                        {
                            "tumour_id": tumour_id,
                            "check": "elbow_table_optimal_complexity_not_unique",
                            "details": ", ".join(unique_values),
                        }
                    )
    return rows


def write_all_solutions_report(rows, out_path="all_solutions_validation_report.csv"):
    # write header even when there are no rows, so the file always exists as a declared output
    header = ["tumour_id", "check", "details"]
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row["tumour_id"], row["check"], row["details"]])


def main():
    args = parse_args()

    missing = validate_segment_lists(args.expected_list, args.actual_list)
    if missing:
        Path("missing_segments.txt").write_text("\n".join(missing) + "\n")
        Path("validation_done.token").write_text("failed")
        print("Missing segments: %d" % len(missing))
    else:
        Path("validation_done.token").write_text("done")
        print("Validation OK")

    tumours_filter = {t.strip() for t in args.restrict_tumours.split(",") if t.strip()}
    segments_filter = {
        s.strip() for s in args.restrict_segments.split(",") if s.strip()
    }
    rows = validate_all_solutions(
        args.output_dir, args.input_dir, tumours_filter, segments_filter
    )
    write_all_solutions_report(rows)
    if rows:
        print(f"all_solutions validation found {len(rows)} issue(s)")
    else:
        print("all_solutions validation OK")


if __name__ == "__main__":
    main()

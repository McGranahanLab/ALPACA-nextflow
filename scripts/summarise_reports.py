#!/usr/bin/env python3

import argparse
import json
import csv
import os
import glob
import sys
import pandas as pd


def process_ci_reports(dirpath, delete=False, outpath=None):
    pattern = os.path.join(dirpath, "*_ci_report.json")
    files = sorted(glob.glob(pattern))
    if outpath is None:
        # write to cwd
        outpath = "ci_modified_report.csv"

    header = [
        "tumour_id",
        "segment",
        "affected_sample",
        "affected_allele",
        "min_ci",
        "timestamp",
        "source_file",
    ]

    # write header even if no files
    with open(outpath, "w", newline="") as csvf:
        writer = csv.writer(csvf)
        writer.writerow(header)
        for fp in files:
            try:
                with open(fp) as fh:
                    data = json.load(fh)
            except Exception as e:
                print(f"Warning: failed to read {fp}: {e}", file=sys.stderr)
                continue

            tumour = data.get("tumour_id")
            segment = data.get("segment")
            min_ci = data.get("min_ci")
            ts = data.get("timestamp")
            affected_samples = data.get("affected_samples") or []
            affected_alleles = data.get("affected_alleles") or []

            # produce a row per sample x allele combination
            if not affected_samples and not affected_alleles:
                writer.writerow(
                    [tumour, segment, "", "", min_ci, ts, os.path.basename(fp)]
                )
                continue

            for s in affected_samples:
                if affected_alleles:
                    for a in affected_alleles:
                        writer.writerow(
                            [tumour, segment, s, a, min_ci, ts, os.path.basename(fp)]
                        )
                else:
                    writer.writerow(
                        [tumour, segment, s, "", min_ci, ts, os.path.basename(fp)]
                    )

            if delete and bool(files):
                try:
                    os.remove(fp)
                except Exception as e:
                    print(f"Warning: failed to remove {fp}: {e}", file=sys.stderr)


def process_monoclonal_reports(dirpath, delete=False, outpath=None):
    pattern = os.path.join(dirpath, "*_monoclonal_samples_report.csv")
    files = sorted(glob.glob(pattern))
    if outpath is None:
        outpath = "monoclonal_samples_report.csv"
    # each file is a csv with header, concatenate them:
    if not files:
        combined_df = pd.DataFrame(columns=['tumour_id', 'sample', 'segment', 'cpnA', 'cpnB', 'distance_to_integer_A', 'distance_to_integer_B'])
    else:
        dfs = []
        for fp in files:
            try:
                df = pd.read_csv(fp)
                dfs.append(df)
            except Exception as e:
                print(f"Warning: failed to read {fp}: {e}", file=sys.stderr)
                continue
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(outpath, index=False)
    if delete and bool(files):
        try:
            os.remove(fp)
        except Exception as e:
            print(f"Warning: failed to remove {fp}: {e}", file=sys.stderr)


def process_elbow_increase_reports(dirpath, delete, outpath):
    pattern = os.path.join(dirpath, "*_elbow_increase_report.csv")
    files = sorted(glob.glob(pattern))
    if outpath is None:
        outpath = "elbow_increase_report.csv"
    # each file is a csv with header, concatenate them or make an empty dataframe
    if not files:
        combined_df = pd.DataFrame(columns=['complexity', 'D_score', 'CI_score', 'allowed_complexity', 'issue', 'tumour_id', 'segment'])
    else:
        dfs = []
        for fp in files:
            try:
                df = pd.read_csv(fp)
                dfs.append(df)
            except Exception as e:
                print(f"Warning: failed to read {fp}: {e}", file=sys.stderr)
                continue
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(outpath, index=False)
    if delete and bool(files):
        try:
            os.remove(fp)
        except Exception as e:
            print(f"Warning: failed to remove {fp}: {e}", file=sys.stderr)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("reports_dir", help="Directory containing report JSON files")
    p.add_argument(
        "--delete", action="store_true", help="Delete JSON files after processing"
    )
    p.add_argument("--out", default=None, help="Output CSV path (optional)")
    args = p.parse_args()

    if not os.path.isdir(args.reports_dir):
        print(
            f"Error: reports_dir '{args.reports_dir}' is not a directory",
            file=sys.stderr,
        )
        sys.exit(2)
    process_ci_reports(args.reports_dir, delete=args.delete, outpath=None)
    process_monoclonal_reports(args.reports_dir, delete=args.delete, outpath=None)
    process_elbow_increase_reports(args.reports_dir, delete=args.delete, outpath=None)

if __name__ == "__main__":
    main()

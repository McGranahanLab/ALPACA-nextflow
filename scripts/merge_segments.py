#!/usr/bin/env python3
import os
import argparse
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--segments-dir", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    seg_dfs = []
    files = os.listdir(args.segments_dir)
    for f in files:
        if not f.endswith(".csv"):
            continue
        if "combined" in f:
            continue
        df = pd.read_csv(os.path.join(args.segments_dir, f))
        seg_dfs.append(df)
    final = pd.concat(seg_dfs, ignore_index=True)
    final.to_csv(args.out, index=False)


if __name__ == "__main__":
    main()

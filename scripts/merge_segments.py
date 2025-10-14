#!/usr/bin/env python3
import os
import argparse
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--segments-dir", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--input-dir", required=True)
    args = p.parse_args()

    seg_dfs = []
    files = os.listdir(args.segments_dir)
    for f in files:
        if not f.endswith(".csv"):
            continue
        if "combined" in f:
            continue
        if 'report' in f:
            continue
        df = pd.read_csv(os.path.join(args.segments_dir, f))
        seg_dfs.append(df)
    final = pd.concat(seg_dfs, ignore_index=True)
    # before saving, ensure that all the expected segments are present
    segments_out = set(final['tumour_id'] + '_' + final['segment'].astype(str))
    input_dfs = []
    for tumour_id in [x for x in os.listdir(args.input_dir) if x != '.DS_Store']:
        try:
            tumour_df = pd.read_csv(os.path.join(args.input_dir, tumour_id, 'ALPACA_input_table.csv'))
            input_dfs.append(tumour_df)
        except Exception as e:
            print(f"Error reading {tumour_id}: {e}")
    input_df = pd.concat(input_dfs)
    segments_in = set(input_df['tumour_id'] + '_' + input_df['segment'].astype(str))
    missing_segments = segments_in - segments_out
    if len(missing_segments) == 0:
        final.to_csv(args.out, index=False)
        # write merged_segments.txt, each segment on a new line:
        with open(os.path.join('merged_segments.txt'), 'w') as f:
            for seg in sorted(segments_in):
                f.write(f"{seg}\n")
    else:
        print(f"Missing {len(missing_segments)} segments in output: {missing_segments}")
        # throw error
        raise ValueError(f"Missing {len(missing_segments)} segments in output: {missing_segments}")


if __name__ == "__main__":
    main()

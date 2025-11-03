import pandas as pd
import argparse
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--combined_file", required=True, help="Path to combined cohort CSV file")
    p.add_argument("--tumour_results", required=True, help="Directory to output per-tumour CSV files")
    args = p.parse_args()
    df = pd.read_csv(args.combined_file)
    for tumour_id, tumour_df in df.groupby("tumour_id"):
        tumour_dir = os.path.join(args.tumour_results, f"{tumour_id}")
        os.makedirs(tumour_dir, exist_ok=True)
        outpath = f"{tumour_dir}/ALPACA_output_{tumour_id}.csv"
        tumour_df.to_csv(outpath, index=False)


if __name__ == "__main__":
    main()
import pandas as pd
import argparse
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tumour_results_dir", required=True, help="")
    p.add_argument("--output_path", required=True, help="")
    args = p.parse_args()
    dfs = []
    for tumour_id in os.listdir(args.tumour_results_dir):
        tumour_dir = os.path.join(args.tumour_results_dir, tumour_id)
        ccd_tumour = os.path.join(tumour_dir, "clone_copy_number_diversity_scores.csv")
        df = pd.read_csv(ccd_tumour)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    main()
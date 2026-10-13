import glob

import pandas as pd
import argparse
import os
from utils import read_any_table, write_table, is_table_file, with_table_ext, common_table_type


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tumour_results_dir", required=True, help="")
    p.add_argument("--output_path", required=True, help="Output path; the extension is set to match the per-tumour CCD tables' format")
    args = p.parse_args()
    dfs = []
    file_types = set()
    for tumour_id in os.listdir(args.tumour_results_dir):
        tumour_dir = os.path.join(args.tumour_results_dir, tumour_id)
        ccd_tumour_files = [
            f for f in glob.glob(os.path.join(tumour_dir, "clone_copy_number_diversity_scores.*")) if is_table_file(f)
        ]
        if not ccd_tumour_files:
            print(f"Warning: no clone_copy_number_diversity_scores table found in {tumour_dir}")
            continue
        # if a previous run left the table in another format, use the most recent one
        df, file_type = read_any_table(max(ccd_tumour_files, key=os.path.getmtime))
        dfs.append(df)
        file_types.add(file_type)
    combined_df = pd.concat(dfs, ignore_index=True)
    write_table(combined_df, with_table_ext(args.output_path, common_table_type(file_types)))


if __name__ == "__main__":
    main()

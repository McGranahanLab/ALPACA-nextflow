import glob

import pandas as pd
import argparse
import os
from utils import read_any_table, write_table, is_table_file, with_table_ext, common_table_type
from tumour_classification import read_classification


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tumour_results_dir", required=True, nargs="+", help="One or more directories with per-tumour results")
    p.add_argument("--classification", default=None, help="Tumour classification table; excluded and single-clone tumours are skipped")
    p.add_argument("--output_path", required=True, help="Output path; the extension is set to match the per-tumour CCD tables' format")
    args = p.parse_args()
    classification = read_classification(args.classification).set_index("tumour_id")
    dfs = []
    file_types = set()
    tumour_dirs = [
        os.path.join(results_dir, tumour_id)
        for results_dir in args.tumour_results_dir
        if os.path.isdir(results_dir)
        for tumour_id in sorted(os.listdir(results_dir))
    ]
    for tumour_dir in tumour_dirs:
        tumour_id = os.path.basename(tumour_dir)
        if tumour_id in classification.index:
            row = classification.loc[tumour_id]
            if not row["included"] or row["n_clones"] == 1:
                # CCD of a single-clone tumour is 0 by construction
                continue
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
    if not dfs:
        print("Warning: no tumours contributed CCD scores; writing an empty table")
        combined_df = pd.DataFrame(columns=["tumour_id", "CCD"])
        file_types = {"csv"}
    else:
        combined_df = pd.concat(dfs, ignore_index=True)
    write_table(combined_df, with_table_ext(args.output_path, common_table_type(file_types)))


if __name__ == "__main__":
    main()

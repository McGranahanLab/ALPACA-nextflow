import pandas as pd
import argparse
import os
from utils import read_any_table, write_table, find_table, table_type, with_table_ext, TABLE_EXTENSIONS
from tumour_classification import read_classification, is_edge_case


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--combined_file", required=True, help="Path to combined cohort table (csv/tsv/parquet)")
    p.add_argument("--tumour_results", required=True, help="Directory to output per-tumour tables")
    p.add_argument("--input_dir", default=None, help="Cohort input directory; per-tumour outputs follow the format of each tumour's ALPACA_input_table")
    p.add_argument("--classification", default=None, help="Tumour classification table written by merge_segments.py")
    p.add_argument("--edge_case_results", default=None, help="Directory for single clone / single sample tumours")
    args = p.parse_args()
    df, combined_type = read_any_table(args.combined_file)
    classification = read_classification(args.classification)
    edge_case_tumours = set(classification.loc[classification["category"].map(is_edge_case), "tumour_id"])
    for tumour_id, tumour_df in df.groupby("tumour_id"):
        results_dir = args.tumour_results
        if args.edge_case_results and str(tumour_id) in edge_case_tumours:
            results_dir = args.edge_case_results
        tumour_dir = os.path.join(results_dir, f"{tumour_id}")
        os.makedirs(tumour_dir, exist_ok=True)
        file_type = combined_type
        input_table = find_table(os.path.join(args.input_dir, f"{tumour_id}"), "ALPACA_input_table") if args.input_dir else None
        if input_table:
            # ALPACA reads only csv/parquet, so tsv inputs produce csv outputs
            file_type = "parquet" if table_type(input_table) == "parquet" else "csv"
        outpath = f"{tumour_dir}/ALPACA_output_{tumour_id}.{file_type}"
        # drop an output left in another format by a previous run, so downstream steps find a single file
        for other_type in TABLE_EXTENSIONS.values():
            stale = with_table_ext(outpath, other_type)
            if stale != outpath and os.path.exists(stale):
                os.remove(stale)
        write_table(tumour_df, outpath, file_type)


if __name__ == "__main__":
    main()

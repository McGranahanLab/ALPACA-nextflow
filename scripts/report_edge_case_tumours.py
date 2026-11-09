#!/usr/bin/env python3
"""Report single clone / single sample tumours at tumour and cohort level.

Writes, for every such tumour, warning_<category>.txt and edge_case_report.csv into
its directory under --edge_case_results, and a cohort summary into --reports_dir:

  single_clone_or_sample_report.csv   one row per affected tumour
  single_clone_or_sample_report.txt   the same, as readable text
"""
import argparse
import os
from datetime import datetime

import pandas as pd
from alpaca.edge_cases import (
    CATEGORY_DESCRIPTIONS,
    EDGE_CASE_CATEGORIES,
    load_segment_reports,
    write_tumour_edge_case_outputs,
)
from tumour_classification import is_edge_case, read_classification

ENV_SWITCHES = {
    "single_clone": "INCLUDE_SINGLE_CLONE",
    "single_sample": "INCLUDE_SINGLE_SAMPLE",
    "single_clone_and_single_sample": "INCLUDE_SINGLE_CLONE_AND_SINGLE_SAMPLE",
}


def cohort_status(row):
    if row["included"]:
        status = "INCLUDED in cohort_results/all_tumours_combined and the cohort-level analysis"
        if row["n_clones"] == 1:
            status += " (not in cohort_ccd_results: CCD of a single-clone tumour is 0 by construction)"
    else:
        status = "EXCLUDED from cohort_results/all_tumours_combined and the cohort-level analysis"
    return f"{status} [{ENV_SWITCHES[row['category']]}={int(row['included'])}]"


def flag_counts(segment_reports):
    counts = {}
    for flags in segment_reports["flags"]:
        for flag in filter(None, str(flags).split(";")):
            counts[flag] = counts.get(flag, 0) + 1
    return ";".join(f"{k}:{v}" for k, v in sorted(counts.items()))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--classification", required=True)
    p.add_argument("--segment_report", required=True, help="Combined edge_case_report.csv from summarise_reports.py")
    p.add_argument("--input_dir", required=True)
    p.add_argument("--edge_case_results", required=True)
    p.add_argument("--reports_dir", required=True)
    args = p.parse_args()

    classification = read_classification(args.classification)
    affected = classification[classification["category"].map(is_edge_case)]
    rows = []
    for _, row in affected.iterrows():
        tumour_id = str(row["tumour_id"])
        status = cohort_status(row)
        tumour_out = os.path.join(args.edge_case_results, tumour_id)
        write_tumour_edge_case_outputs(
            os.path.join(args.input_dir, tumour_id),
            args.segment_report,
            tumour_out,
            tumour_id=tumour_id,
            cohort_status=status,
        )
        segment_reports = load_segment_reports(args.segment_report, tumour_id)
        rows.append(
            {
                "tumour_id": tumour_id,
                "category": row["category"],
                "n_clones": int(row["n_clones"]),
                "n_samples": int(row["n_samples"]),
                "included_in_all_tumours_combined": bool(row["included"]),
                "included_in_cohort_ccd": bool(row["included"]) and int(row["n_clones"]) > 1,
                "segments": len(segment_reports),
                "flagged_segments": int((segment_reports["flags"] != "").sum()),
                "flag_counts": flag_counts(segment_reports),
                "results_dir": tumour_out,
            }
        )
    report = pd.DataFrame(
        rows,
        columns=[
            "tumour_id", "category", "n_clones", "n_samples", "included_in_all_tumours_combined",
            "included_in_cohort_ccd", "segments", "flagged_segments", "flag_counts", "results_dir",
        ],
    )
    os.makedirs(args.reports_dir, exist_ok=True)
    report.to_csv(os.path.join(args.reports_dir, "single_clone_or_sample_report.csv"), index=False)

    lines = [
        "Single clone / single sample tumours",
        f"generated: {datetime.now().isoformat(timespec='seconds')}",
        f"tumours in cohort: {len(classification)}, affected: {len(report)}",
        "",
        "Every affected tumour was solved and has its results, warning_<category>.txt and",
        f"edge_case_report.csv in {args.edge_case_results}/<tumour>/. The pipeline.env switches",
        "decide whether it is part of cohort_results/all_tumours_combined and the cohort-level analysis.",
    ]
    for category in EDGE_CASE_CATEGORIES:
        subset = report[report["category"] == category]
        lines += ["", f"== {category} ({len(subset)} tumours) [{ENV_SWITCHES[category]}]", CATEGORY_DESCRIPTIONS[category]]
        for _, r in subset.iterrows():
            status = "included" if r["included_in_all_tumours_combined"] else "EXCLUDED"
            lines.append(
                f"  {r['tumour_id']}: {status}; clones={r['n_clones']} samples={r['n_samples']}; "
                f"flagged segments {r['flagged_segments']}/{r['segments']}"
                + (f" ({r['flag_counts']})" if r["flag_counts"] else "")
            )
    with open(os.path.join(args.reports_dir, "single_clone_or_sample_report.txt"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"single clone / single sample tumours: {len(report)} (see {args.reports_dir})")


if __name__ == "__main__":
    main()

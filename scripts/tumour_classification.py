"""Classify cohort tumours as standard / single clone / single sample.

The categories come from ALPACA (alpaca.edge_cases) so both run modes agree.
The classification table is written by merge_segments.py and read by the
analysis steps:

    tumour_id, category, n_clones, n_samples, included
"""
import os

import pandas as pd
from alpaca.edge_cases import (
    SINGLE_CLONE,
    SINGLE_CLONE_AND_SINGLE_SAMPLE,
    SINGLE_SAMPLE,
    STANDARD,
    classify_tumour,
    read_cp_table,
)

CLASSIFICATION_COLUMNS = ["tumour_id", "category", "n_clones", "n_samples", "included"]


def classify_cohort(input_dir, tumour_ids, include):
    """include: {category: bool} for the three edge-case categories; standard tumours are always included."""
    rows = []
    for tumour_id in sorted(tumour_ids):
        cp_table = read_cp_table(os.path.join(input_dir, tumour_id))
        category = classify_tumour(cp_table)
        rows.append(
            {
                "tumour_id": tumour_id,
                "category": category,
                "n_clones": len(cp_table.index),
                "n_samples": len(cp_table.columns),
                "included": category == STANDARD or bool(include.get(category, False)),
            }
        )
    return pd.DataFrame(rows, columns=CLASSIFICATION_COLUMNS)


def read_classification(path):
    """Read the classification table; returns an empty table if the file is missing."""
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=CLASSIFICATION_COLUMNS)
    df = pd.read_csv(path, dtype={"tumour_id": str})
    df["included"] = df["included"].astype(str).str.lower().isin(["true", "1"])
    return df


def is_edge_case(category):
    return category in (SINGLE_CLONE, SINGLE_SAMPLE, SINGLE_CLONE_AND_SINGLE_SAMPLE)

#!/usr/bin/env python3
"""Write small synthetic ALPACA tumours covering the single clone / single sample cases.

Usage:
  python scripts/make_synthetic_test_data.py --out_dir test_data/input

Creates one tumour directory per scenario (ALPACA_input_table.csv, ci_table.csv,
cp_table.csv, tree_paths.json). Observed copy numbers are the proportion-weighted
mix of known integer clone copy numbers, except where a scenario overrides them
to provoke a warning flag:

  SYN-Standard                     3 clones, 2 samples (no special handling)
  SYN-SingleClone                  1 clone, 3 samples; one segment whose samples
                                   share no common integer
  SYN-SingleSample                 4 clones, 1 sample; one clonal-looking and one
                                   subclonal segment
  SYN-SingleCloneSingleSample      1 clone, 1 sample; one non-integer segment
"""
import argparse
import json
from pathlib import Path

import pandas as pd

CI_HALF_WIDTH = 0.2
SEGMENTS = ["1_1000000_5000000", "2_1000000_5000000", "3_1000000_5000000"]

SCENARIOS = {
    "SYN-Standard": {
        "tree": [["clone1", "clone2"], ["clone1", "clone3"]],
        "proportions": {
            "S1": {"clone1": 0.5, "clone2": 0.5, "clone3": 0.0},
            "S2": {"clone1": 0.4, "clone2": 0.0, "clone3": 0.6},
        },
        "truth": {
            SEGMENTS[0]: {"clone1": (2, 1), "clone2": (3, 1), "clone3": (2, 1)},
            SEGMENTS[1]: {"clone1": (1, 1), "clone2": (1, 1), "clone3": (1, 0)},
            SEGMENTS[2]: {"clone1": (2, 2), "clone2": (2, 2), "clone3": (2, 2)},
        },
    },
    "SYN-SingleClone": {
        "tree": [["clone1"]],
        "proportions": {s: {"clone1": 1.0} for s in ("S1", "S2", "S3")},
        "truth": {
            SEGMENTS[0]: {"clone1": (2, 1)},
            SEGMENTS[1]: {"clone1": (1, 1)},
            SEGMENTS[2]: {"clone1": (3, 1)},
        },
        # samples disagree: no integer lies in every sample's CI for allele A
        "observed": {SEGMENTS[2]: {"S1": (2.0, 1.0), "S2": (3.0, 1.0), "S3": (2.5, 1.0)}},
    },
    "SYN-SingleSample": {
        "tree": [["clone1", "clone2", "clone3"], ["clone1", "clone4"]],
        "proportions": {"S1": {"clone1": 0.4, "clone2": 0.2, "clone3": 0.1, "clone4": 0.3}},
        "truth": {
            # clonal: the sample's CI admits a single integer, so heterogeneity is restricted
            SEGMENTS[0]: {"clone1": (2, 1), "clone2": (2, 1), "clone3": (2, 1), "clone4": (2, 1)},
            # subclonal: several assignments explain the single observation
            SEGMENTS[1]: {"clone1": (2, 1), "clone2": (3, 1), "clone3": (4, 1), "clone4": (2, 0)},
            SEGMENTS[2]: {"clone1": (1, 1), "clone2": (1, 1), "clone3": (1, 0), "clone4": (2, 1)},
        },
    },
    "SYN-SingleCloneSingleSample": {
        "tree": [["clone1"]],
        "proportions": {"S1": {"clone1": 1.0}},
        "truth": {
            SEGMENTS[0]: {"clone1": (2, 1)},
            SEGMENTS[1]: {"clone1": (1, 0)},
            SEGMENTS[2]: {"clone1": (2, 1)},
        },
        # non-integer observation in a monoclonal sample
        "observed": {SEGMENTS[2]: {"S1": (2.5, 1.0)}},
    },
}


def write_tumour(out_dir: Path, tumour_id: str, scenario: dict) -> None:
    tumour_dir = out_dir / tumour_id
    tumour_dir.mkdir(parents=True, exist_ok=True)
    (tumour_dir / "tree_paths.json").write_text(json.dumps(scenario["tree"]))
    proportions = scenario["proportions"]
    pd.DataFrame(proportions).rename_axis("clone").reset_index().to_csv(
        tumour_dir / "cp_table.csv", index=False
    )
    overrides = scenario.get("observed", {})
    rows, ci_rows = [], []
    for segment, truth in scenario["truth"].items():
        for sample, props in proportions.items():
            cpn_a = sum(props[c] * a for c, (a, _) in truth.items())
            cpn_b = sum(props[c] * b for c, (_, b) in truth.items())
            cpn_a, cpn_b = overrides.get(segment, {}).get(sample, (cpn_a, cpn_b))
            rows.append(
                {"tumour_id": tumour_id, "sample": sample, "segment": segment, "cpnA": cpn_a, "cpnB": cpn_b}
            )
            ci_rows.append(
                {
                    "segment": segment,
                    "sample": sample,
                    "lower_CI_A": max(cpn_a - CI_HALF_WIDTH, 0),
                    "upper_CI_A": cpn_a + CI_HALF_WIDTH,
                    "lower_CI_B": max(cpn_b - CI_HALF_WIDTH, 0),
                    "upper_CI_B": cpn_b + CI_HALF_WIDTH,
                    "tumour_id": tumour_id,
                    "ci_value": 0.5,
                }
            )
    pd.DataFrame(rows).to_csv(tumour_dir / "ALPACA_input_table.csv", index=False)
    pd.DataFrame(ci_rows).to_csv(tumour_dir / "ci_table.csv", index=False)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out_dir", required=True, help="Cohort input directory to write tumour folders into")
    args = p.parse_args()
    out_dir = Path(args.out_dir)
    for tumour_id, scenario in SCENARIOS.items():
        write_tumour(out_dir, tumour_id, scenario)
        print(f"wrote {out_dir / tumour_id}")


if __name__ == "__main__":
    main()

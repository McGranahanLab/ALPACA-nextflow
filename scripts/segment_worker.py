#!/usr/bin/env python3
"""Worker that claims segment CSVs from a pool and runs ALPACA in segment mode.

This script is designed for Nextflow workers or long-lived local workers.
"""
import os
import sys
import time
import subprocess
import shutil
import argparse
import traceback


def claim_segment(pool_dir, in_progress_dir, done_dir):
    # Look for CSV files directly under pool_dir and atomically claim by
    # moving them into the worker-specific in_progress_dir.
    # If a basename already exists in done_dir, remove the pool entry and skip it.
    for segment_file in os.listdir(pool_dir):
        if not segment_file.endswith(".csv"):
            continue

        # If this segment already exists in done_dir, remove it from pool and skip
        try:
            already = False
            for root, _, files in os.walk(done_dir):
                if segment_file in files:
                    already = True
                    break
            if already:
                try:
                    os.remove(os.path.join(pool_dir, segment_file))
                    print(
                        f"Skipping already-done segment {segment_file}; removed pool entry"
                    )
                except Exception:
                    pass
                continue
        except Exception:
            # if done_dir scanning fails, proceed to attempt claim
            pass

        src = os.path.join(pool_dir, segment_file)
        dst = os.path.join(in_progress_dir, segment_file)
        try:
            os.rename(src, dst)
            return dst
        except FileNotFoundError:
            # someone else may have claimed it
            continue
        except OSError:
            # skip problematic entries
            continue
    return None


def run_alpaca_on_segment(claimed_paths, args):
    """
    Run ALPACA on a list of claimed segment files (all located in the worker_in_progress dir).
    The function assumes claimed_paths is a list of absolute paths. It derives the tumour
    from the filenames and builds a single ALPACA invocation with multiple --input_files.
    """
    if not claimed_paths:
        raise ValueError("no claimed paths provided")

    # All paths are expected to be in the worker_in_progress dir; use basenames for --input_files
    basenames = [os.path.basename(p) for p in claimed_paths]
    first = basenames[0]
    tumour = first.replace("ALPACA_input_table_", "").split("_", 1)[0]
    tumour_cohort_dir = os.path.join(args.cohort_dir, "input", tumour)

    cmd = [
        sys.executable,
        "-m",
        "alpaca.__main__",
        "run",
        "--mode",
        "segment",
        "--input_tumour_directory",
        tumour_cohort_dir,
        "--input_data_directory",
        args.worker_in_progress,
        "--input_files",
    ]
    # append each input file as a separate argument
    cmd += basenames
    cmd += [
        "--output_directory",
        args.outputs_dir,
        "--cpus",
        str(args.cpus),
    ]
    if args.gurobi_logs:
        cmd += ["--gurobi_logs", args.gurobi_logs]
    if args.debug:
        cmd += ["--debug"]
    if args.output_all_solutions:
        cmd += ["--output_all_solutions"]
    print("Running:", " ".join(cmd))
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--cohort_dir", required=True)
    p.add_argument("--pool-dir", required=True)
    p.add_argument("--in-progress-dir", required=True)
    p.add_argument(
        "--worker-id",
        required=False,
        default=None,
        help="Optional worker id used to create worker-specific in_progress subdir",
    )
    p.add_argument("--done-dir", required=True)
    p.add_argument("--failed-dir", required=True)
    p.add_argument("--outputs-dir", required=True)
    p.add_argument("--cpus", default=1, type=int)
    p.add_argument("--poll-interval", default=2, type=int)
    p.add_argument("--backoff", default=2, type=float)
    p.add_argument(
        "--segments-per-claim",
        default=1,
        type=int,
        help="How many segments to claim and pass to ALPACA in one invocation",
    )
    p.add_argument("--debug", action="store_true")
    p.add_argument("--output_all_solutions", action="store_true")
    p.add_argument("--gurobi_logs", default="")
    p.add_argument("--max-retries", default=2, type=int)
    args = p.parse_args()

    # create a per-worker in_progress directory to avoid cross-worker races
    if args.worker_id:
        worker_in_progress = os.path.join(
            args.in_progress_dir, f"worker_{args.worker_id}"
        )
    else:
        # fallback to pid-based worker dir
        worker_in_progress = os.path.join(args.in_progress_dir, f"worker_{os.getpid()}")
    os.makedirs(worker_in_progress, exist_ok=True)
    # expose the per-worker in_progress dir as the working in_progress dir
    args.worker_in_progress = worker_in_progress

    os.makedirs(args.in_progress_dir, exist_ok=True)
    os.makedirs(args.done_dir, exist_ok=True)
    os.makedirs(args.failed_dir, exist_ok=True)
    os.makedirs(args.outputs_dir, exist_ok=True)
    idle_cycles = 0
    while True:
        # Claim up to N segments into the worker-specific in_progress area
        claimed_paths = []
        for i in range(args.segments_per_claim):
            claimed = claim_segment(
                args.pool_dir, args.worker_in_progress, args.done_dir
            )
            if claimed is None:
                break
            claimed_paths.append(claimed)

        if not claimed_paths:
            idle_cycles += 1
            if idle_cycles > 5:
                print("No work found, exiting worker.")
                break
            time.sleep(args.poll_interval)
            continue

        idle_cycles = 0
        retries = 0
        success = False
        # Group claimed files by tumour so we can invoke ALPACA once per tumour
        groups = {}
        for p in claimed_paths:
            bn = os.path.basename(p)
            tumour = bn.replace("ALPACA_input_table_", "").split("_", 1)[0]
            groups.setdefault(tumour, []).append(p)

        # For each group (tumour), run ALPACA once with multiple input files
        group_results = {}
        for tumour, paths in groups.items():
            retries = 0
            success = False
            while retries <= args.max_retries and not success:
                try:
                    res = run_alpaca_on_segment(paths, args)
                    print(res.stdout)
                    if res.returncode == 0:
                        success = True
                    else:
                        print(
                            "ALPACA returned non-zero:\n", res.stderr, file=sys.stderr
                        )
                except Exception as e:
                    print("Exception when running ALPACA:", e, file=sys.stderr)
                    traceback.print_exc()
                if not success:
                    retries += 1
                    time.sleep(args.backoff * retries)
            group_results[tumour] = success

        # Move each processed claimed file to done or failed depending on its group's result
        for p in claimed_paths:
            rel = os.path.relpath(p, args.worker_in_progress)
            tumour = (
                os.path.basename(p).replace("ALPACA_input_table_", "").split("_", 1)[0]
            )
            success = group_results.get(tumour, False)
            if success:
                dest = os.path.join(args.done_dir, rel)
            else:
                dest = os.path.join(args.failed_dir, rel)

            os.makedirs(os.path.dirname(dest), exist_ok=True)
            try:
                shutil.move(p, dest)
            except FileNotFoundError:
                print(
                    f"Warning: file not found when moving '{p}' -> '{dest}'; maybe processed by another worker",
                    file=sys.stderr,
                )
            except OSError as e:
                print(f"Error moving '{p}' -> '{dest}': {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

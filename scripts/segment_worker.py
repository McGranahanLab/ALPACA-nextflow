#!/usr/bin/env python3
import os
import sys
import errno
import time
import subprocess
import shutil
import argparse
import traceback
import json
import socket
from datetime import datetime
import shlex
import csv


def get_tumour_id_from_seg_file(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                if row[0]=="tumour_id":
                    j=0
                    continue
                else:
                    for j in range(len(row)):
                        if row[j]=="tumour_id":
                            break
                    else:
                        raise ValueError(f"tumour_id column not found in {file_path}")
            if i == 1:
                return row[j]
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
    tumour = get_tumour_id_from_seg_file(claimed_paths[0])
    tumour_cohort_dir = os.path.join(args.input_dir, tumour)
    tumour_in_progress = os.path.join(args.worker_in_progress, 'in_progress')
    segment_solution_output_dir = os.path.join(args.outputs_dir, "segment_outputs")
    
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
        tumour_in_progress,
        "--input_files",
    ]
    # append each input file as a separate argument
    cmd += basenames
    cmd += [
        "--output_directory",
        segment_solution_output_dir,
        "--cpus",
        str(args.cpus),
    ]
    # parse extra alpaca args (a single quoted string) into tokens
    if getattr(args, "alpaca_args", None):
        extra = shlex.split(args.alpaca_args)
        cmd += extra
    print("Running:", " ".join(cmd))
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input_dir", required=False, help="Input directory")
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
        "--max-idle-seconds",
        default=600,
        type=int,
        help="If the worker sees no new work for this many seconds it will exit and emit its done token.",
    )
    p.add_argument(
        "--segments-per-claim",
        default=1,
        type=int,
        help="How many segments to claim and pass to ALPACA in one invocation",
    )
    p.add_argument("--log-level", default=0, type=int)
    p.add_argument("--max-retries", default=2, type=int)
    p.add_argument(
        "--alpaca-args",
        dest="alpaca_args",
        default="",
        help='Extra arguments (quoted) to append to the alpaca command, e.g. "--debug --two_objectives 1"',
    )

    args = p.parse_args()
    # create a per-worker in_progress directory to avoid cross-worker races
    worker_in_progress = os.path.join(
            args.in_progress_dir, f"worker_{args.worker_id}")
    # expose the per-worker in_progress dir as the working in_progress dir
    args.worker_in_progress = worker_in_progress

    os.makedirs(args.in_progress_dir, exist_ok=True)
    os.makedirs(args.done_dir, exist_ok=True)
    os.makedirs(args.failed_dir, exist_ok=True)
    os.makedirs(args.outputs_dir, exist_ok=True)
    # ensure subdirs for segment outputs and worker logs
    segment_out_dir = os.path.join(args.outputs_dir, "segment_outputs")
    worker_logs_dir = os.path.join(args.outputs_dir, "worker_logs")
    os.makedirs(segment_out_dir, exist_ok=True)
    os.makedirs(worker_logs_dir, exist_ok=True)
    # create reports directory:
    reports_dir = os.path.join(args.outputs_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    # ensure per-worker queue and in_progress subdirs
    worker_queue_dir = os.path.join(worker_in_progress, "queue")
    worker_active_dir = os.path.join(worker_in_progress, "in_progress")
    os.makedirs(worker_queue_dir, exist_ok=True)
    os.makedirs(worker_active_dir, exist_ok=True)

    # Initiate a log to record all the file paths and operations perfomed by this worker
    worker_log = {
        "worker_id": args.worker_id or f"pid_{os.getpid()}",
        "hostname": socket.gethostname(),
        "start_time": datetime.now().isoformat() + "Z",
        "pool_snapshots": [],  # list of {ts, files}
        "claims": [],  # list of {ts, basename, claimed_path}
        "alpaca_runs": [],  # list of {ts, tumour, input_files, returncode, success, stdout_snip, stderr_snip}
        "moves": [],  # list of {ts, basename, src, dest, result}
    }
    # Record the provided path params
    worker_log["params"] = {
        "in_progress_dir": args.in_progress_dir,
        "outputs_dir": args.outputs_dir,
        "failed_dir": args.failed_dir,
    }

    # diagnostic heartbeat file (updated each loop)
    heartbeat_path = os.path.join(
        worker_logs_dir, f"worker_{worker_log['worker_id']}.heartbeat"
    )

    while True:
        # update heartbeat
        try:
            with open(heartbeat_path, "w") as hf:
                hf.write(datetime.now().isoformat() + "Z")
        except Exception:
            pass

        try:
            q_entries = sorted(os.listdir(worker_queue_dir))
        except Exception:
            q_entries = []
        q_sample = [f for f in q_entries if f.endswith(".csv")]
        msg = f"Inspecting queue_dir={worker_queue_dir!r} total_entries={len(q_entries)} csv_sample_count={len(q_sample)}"
        print(msg)
        worker_log.setdefault("messages", []).append(
            {"ts": datetime.now().isoformat() + "Z", "msg": msg, "csv_sample": q_sample}
        )
        worker_log.setdefault("queue_snapshots", []).append(
            {"ts": datetime.now().isoformat() + "Z", "files": q_entries}
        )

        # Move up to N segments from queue -> in_progress for local processing
        claimed_paths = []

        # helper to move a queue file into the in_progress dir (atomic rename preferred)
        def move_queue_file(basename):
            src = os.path.join(worker_queue_dir, basename)
            dst = os.path.join(worker_active_dir, basename)
            for attempt in range(3):
                try:
                    os.rename(src, dst)
                    return dst
                except FileNotFoundError:
                    return None
                except OSError as e:
                    err = getattr(e, "errno", None)
                    if err == errno.EXDEV:
                        tmp = None
                        try:
                            tmp = dst + f".tmp.{os.getpid()}"
                            shutil.copy2(src, tmp)
                            try:
                                with open(tmp, "rb") as tf:
                                    try:
                                        os.fsync(tf.fileno())
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            os.replace(tmp, dst)
                            try:
                                os.remove(src)
                            except Exception:
                                pass
                            return dst
                        except Exception:
                            try:
                                if tmp and os.path.exists(tmp):
                                    os.remove(tmp)
                            except Exception:
                                pass
                            time.sleep(0.05)
                            continue
                    else:
                        time.sleep(0.02)
                        continue
            return None

        # try to claim up to segments_per_claim files
        to_claim = [f for f in q_entries if f.endswith(".csv")][
            : args.segments_per_claim
        ]
        for bn in to_claim:
            moved = move_queue_file(bn)
            if moved:
                worker_log["claims"].append(
                    {
                        "ts": datetime.now().isoformat() + "Z",
                        "basename": os.path.basename(moved),
                        "path": moved,
                        "from_queue": True,
                    }
                )
                claimed_paths.append(moved)

        # Always flush the worker log after attempting claims so it's available
        try:
            outname = f"worker_{worker_log['worker_id']}.done.log"
            outpath = os.path.join(worker_logs_dir, outname)
            tmp = outpath + f".tmp.{os.getpid()}.{int(time.time()*1000)}"
            with open(tmp, "w") as fh:
                json.dump(worker_log, fh, indent=2)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except Exception:
                    pass
            os.replace(tmp, outpath)
        except Exception:
            traceback.print_exc()
        print("Claimed paths:", claimed_paths)
        if not claimed_paths:
            if "last_work_ts" not in worker_log:
                worker_log["last_work_ts"] = time.time()
            if len(worker_log.get("claims", [])) > 0:
                worker_log["last_work_ts"] = time.time()
            idle_seconds = time.time() - worker_log.get("last_work_ts", time.time())
            # exit if idle time is too long or if nothing else is left to claim and worker confirms that dispatcher exited
            dispatcher_done_path = os.path.exists(os.path.join(args.outputs_dir, "dispatcher.done"))
            timeout_reached = idle_seconds > args.max_idle_seconds
            if timeout_reached or dispatcher_done_path:
                try:
                    diag = {
                        "ts": datetime.now().isoformat() + "Z",
                        "idle_seconds": idle_seconds,
                        "queue_snapshot": worker_log.get("queue_snapshots", [])[-5:],
                    }
                    diag_path = os.path.join(
                        worker_logs_dir, f"worker_{worker_log['worker_id']}.stuck.json"
                    )
                    with open(diag_path, "w") as df:
                        json.dump(diag, df, indent=2)
                except Exception:
                    pass
                if timeout_reached:
                    exit_msg = "No work found for extended period, exiting worker and writing diagnostics."
                elif dispatcher_done_path:
                    exit_msg = "Dispatcher done file detected and no work found, exiting worker."
                print(exit_msg)
                worker_log.setdefault("messages", []).append(exit_msg)
                break
            time.sleep(args.poll_interval)
            continue

        # reset idle marker since we got work
        worker_log["last_work_ts"] = time.time()
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
                    # record ALPACA invocation result
                    worker_log["alpaca_runs"].append(
                        {
                            "ts": datetime.now().isoformat() + "Z",
                            "tumour": tumour,
                            "input_files": [os.path.basename(p) for p in paths],
                            "returncode": res.returncode,
                            "stdout_snip": (res.stdout[:2000] if res.stdout else ""),
                            "stderr_snip": (res.stderr[:2000] if res.stderr else ""),
                        }
                    )
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
            tumour = (
                os.path.basename(p).replace("ALPACA_input_table_", "").split("_", 1)[0]
            )
            success = group_results.get(tumour, False)
            if success:
                dest = os.path.join(args.done_dir, os.path.basename(p))
            else:
                dest = os.path.join(args.failed_dir, os.path.basename(p))

            os.makedirs(os.path.dirname(dest), exist_ok=True)
            try:
                shutil.move(p, dest)
                worker_log["moves"].append(
                    {
                        "ts": datetime.now().isoformat() + "Z",
                        "basename": os.path.basename(p),
                        "src": p,
                        "dest": dest,
                        "result": "moved",
                    }
                )
            except FileNotFoundError:
                print(
                    f"Warning: file not found when moving '{p}' -> '{dest}'; maybe processed by another worker",
                    file=sys.stderr,
                )
                worker_log["moves"].append(
                    {
                        "ts": datetime.now().isoformat() + "Z",
                        "basename": os.path.basename(p),
                        "src": p,
                        "dest": dest,
                        "result": "not_found",
                    }
                )
            except OSError as e:
                print(f"Error moving '{p}' -> '{dest}': {e}", file=sys.stderr)
                worker_log["moves"].append(
                    {
                        "ts": datetime.now().isoformat() + "Z",
                        "basename": os.path.basename(p),
                        "src": p,
                        "dest": dest,
                        "result": f"error: {e}",
                    }
                )

        # Flush worker log to outputs dir after each batch so it's available
        try:
            outname = f"worker_{worker_log['worker_id']}.done.log"
            outpath = os.path.join(args.outputs_dir, outname)
            tmp = outpath + f".tmp.{os.getpid()}.{int(time.time()*1000)}"
            with open(tmp, "w") as fh:
                json.dump(worker_log, fh, indent=2)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except Exception:
                    pass
            os.replace(tmp, outpath)
        except Exception:
            # best-effort, don't fail the worker for logging problems
            traceback.print_exc()


if __name__ == "__main__":
    main()

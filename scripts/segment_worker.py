#!/usr/bin/env python3
"""Worker that claims segment CSVs from a pool and runs ALPACA in segment mode.

This script is designed for Nextflow workers or long-lived local workers.
"""
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


def claim_segment(pool_dir, in_progress_dir, done_dir, done_basenames=None):
    # Look for CSV files directly under pool_dir and atomically claim by
    # moving them into the worker-specific in_progress_dir.
    # If a basename already exists in done_dir, remove the pool entry and skip it.
    try:
        pool_entries = os.listdir(pool_dir)
    except Exception:
        pool_entries = []
    for segment_file in pool_entries:
        if not segment_file.endswith(".csv"):
            continue

        # If this segment already exists in done_dir, remove it from pool and skip
        try:
            # consult cached done basenames if provided to avoid expensive walks
            if done_basenames is not None:
                if segment_file in done_basenames:
                    try:
                        os.remove(os.path.join(pool_dir, segment_file))
                        print(
                            f"Skipping already-done segment {segment_file}; removed pool entry (cache)"
                        )
                    except Exception:
                        pass
                    continue
            else:
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
        # try a few times per entry to handle transient races
        for attempt in range(3):
            try:
                # record dev ids if possible for debugging (same-FS requirement)
                try:
                    sdev = os.stat(src).st_dev
                except Exception:
                    sdev = None
                try:
                    ddev = os.stat(os.path.dirname(dst)).st_dev
                except Exception:
                    ddev = None
                # attempt atomic rename (fast path)
                os.rename(src, dst)
                return dst
            except FileNotFoundError:
                # someone else may have claimed it
                break
            except OSError as e:
                err = getattr(e, "errno", None)
                msg = str(e)
                # if cross-device link error, try copy+replace fallback
                if err == errno.EXDEV:
                    tmp = None
                    try:
                        # copy to a temp file in in_progress_dir then atomic replace
                        tmp = os.path.join(
                            in_progress_dir, f".{segment_file}.tmp.{os.getpid()}"
                        )
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
                        # remove original src if still exists
                        try:
                            os.remove(src)
                        except Exception:
                            pass
                        return dst
                    except Exception as e2:
                        # log fallback failure
                        try:
                            dbg_path = os.path.join(
                                in_progress_dir, f"claim_error.{segment_file}.log"
                            )
                            with open(dbg_path, "a") as df:
                                df.write(
                                    f"fallback failed: {e2} primary_errno={err} primary_msg={msg} sdev={sdev} ddev={ddev}\n"
                                )
                        except Exception:
                            pass
                        # cleanup temp
                        try:
                            if tmp and os.path.exists(tmp):
                                os.remove(tmp)
                        except Exception:
                            pass
                        # retry a couple times
                        time.sleep(0.1)
                        continue
                else:
                    # non-EXDEV OSError: log diagnostics and retry a bit
                    try:
                        dbg_path = os.path.join(
                            in_progress_dir, f"claim_error.{segment_file}.log"
                        )
                        with open(dbg_path, "a") as df:
                            df.write(
                                f"rename failed: src={src} dst={dst} errno={err} msg={msg} sdev={sdev} ddev={ddev}\n"
                            )
                    except Exception:
                        pass
                    time.sleep(0.05)
                    continue
        # end attempts for this file -> move to next file
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
    # idle tracking is now handled via last_work_ts in worker_log
    # cache of done basenames to avoid rescanning the done_dir on each claim
    done_basenames = set()
    done_cache_last = 0
    done_cache_ttl = 30.0  # seconds
    # Initiate a log to record all the file paths and operations perfomed by this worker
    worker_log = {
        "worker_id": args.worker_id or f"pid_{os.getpid()}",
        "hostname": socket.gethostname(),
        "start_time": datetime.now().isoformat() + "Z",
        "pool_snapshots": [],  # list of {ts, files}
        "claims": [],  # list of {ts, basename, claimed_path}
        "alapaca_runs": [],  # list of {ts, tumour, input_files, returncode, success, stdout_snip, stderr_snip}
        "moves": [],  # list of {ts, basename, src, dest, result}
    }
    # Record the provided path params
    worker_log["params"] = {
        "pool_dir": args.pool_dir,
        "in_progress_dir": args.in_progress_dir,
        "outputs_dir": args.outputs_dir,
        "done_dir": args.done_dir,
        "failed_dir": args.failed_dir,
        "cohort_dir": args.cohort_dir,
    }

    # diagnostic heartbeat file (updated each loop) so external tooling can see worker is alive
    heartbeat_path = os.path.join(
        worker_logs_dir, f"worker_{worker_log['worker_id']}.heartbeat"
    )

    # Do quick existence/listing checks immediately and flush the log so it's
    # available even if the worker finds no work.
    path_checks = {}
    for name, path_val in worker_log["params"].items():
        try:
            exists = os.path.exists(path_val)
            entries = []
            if exists and os.path.isdir(path_val):
                try:
                    entries = os.listdir(path_val)
                except Exception:
                    entries = []
            path_checks[name] = {"exists": exists, "entry_count": len(entries)}
        except Exception as e:
            path_checks[name] = {"error": str(e)}
    worker_log["initial_path_checks"] = path_checks

    # flush initial worker log immediately
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
    # print an explicit startup message about where this worker will look
    try:
        pool_chk = worker_log["initial_path_checks"].get("pool_dir", {})
        msg = (
            f"Worker starting: looking at pool_dir={args.pool_dir!r} exists={pool_chk.get('exists')}"
            f" entry_count={pool_chk.get('entry_count')}"
        )
        print(msg)
        worker_log.setdefault("messages", []).append(
            {
                "ts": datetime.now().isoformat() + "Z",
                "msg": msg,
            }
        )
    except Exception:
        pass
    while True:
        # update heartbeat
        try:
            with open(heartbeat_path, "w") as hf:
                hf.write(datetime.now().isoformat() + "Z")
        except Exception:
            pass

        # refresh done_dir cache periodically
        try:
            nowt = time.time()
            if nowt - done_cache_last > done_cache_ttl:
                new_done = set()
                try:
                    for root, _, files in os.walk(args.done_dir):
                        for f in files:
                            new_done.add(f)
                except Exception:
                    new_done = done_basenames
                done_basenames = new_done
                done_cache_last = nowt
        except Exception:
            pass
        # Inspect per-worker queue instead of global pool
        try:
            q_entries = sorted(os.listdir(worker_queue_dir))
        except Exception:
            q_entries = []
        q_sample = [f for f in q_entries if f.endswith(".csv")][:20]
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
                        "done_count": len(done_basenames),
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
                    worker_log["alapaca_runs"].append(
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

#!/usr/bin/env python3
import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tumour_classification import (
    SINGLE_CLONE,
    SINGLE_CLONE_AND_SINGLE_SAMPLE,
    SINGLE_SAMPLE,
    classify_cohort,
)
from utils import (
    TABLE_EXTENSIONS,
    common_table_type,
    find_table,
    is_table_file,
    read_any_table,
    strip_table_ext,
    with_table_ext,
    write_table,
)

# "Oct-13 09:44:50.018 [Task submitter] DEBUG nextflow.executor.GridTaskHandler - [SLURM] submitted
#  process workerTask (167) > jobId: 60633732; workDir: /path/work/0b/cdc631dacf..."
SUBMITTED_RE = re.compile(
    r"submitted process (?P<name>[\w-]+) \((?P<run>[^)]*)\) > jobId: (?P<job>\d+); workDir: (?P<dir>\S+)"
)

SEGMENT_FILE_PREFIX = "ALPACA_input_table_"

# error signatures looked for in ALPACA stderr / slurm logs: (label, regex)
ERROR_SIGNATURES: List[Tuple[str, "re.Pattern[str]"]] = [
    (
        "out_of_memory",
        re.compile(
            r"MemoryError|out of memory|OOM|oom-kill|Cannot allocate memory|std::bad_alloc|\bKilled\b",
            re.IGNORECASE,
        ),
    ),
    (
        "time_limit",
        re.compile(r"DUE TO TIME LIMIT|TIME LIMIT|TIMEOUT", re.IGNORECASE),
    ),
    (
        "gurobi_license",
        re.compile(
            r"license|licence|GRB_ERROR|Error code 1000[0-9]|Restricted license|size-limited",
            re.IGNORECASE,
        ),
    ),
    ("infeasible", re.compile(r"infeasible", re.IGNORECASE)),
    ("python_exception", re.compile(r"Traceback \(most recent call last\)")),
    (
        "missing_file",
        re.compile(r"FileNotFoundError|No such file or directory", re.IGNORECASE),
    ),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--segments-dir", required=True)
    parser.add_argument(
        "--out",
        required=True,
        help="Output path; the extension is set to match the segment outputs' format",
    )
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--nextflow-log", dest="nextflow_log", default="")
    parser.add_argument("--work-dir", dest="work_dir", default="work")
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--worker-cpus", type=int, default=0)
    parser.add_argument("--worker-label", default="worker_high")
    parser.add_argument("--profile-config", default="")
    parser.add_argument("--profile-name", default="")
    parser.add_argument("--outputs-dir", default="", help="ALPACA work 'outputs' dir")
    parser.add_argument("--pool-dir", default="")
    parser.add_argument("--in-progress-dir", default="")
    parser.add_argument("--done-dir", default="")
    parser.add_argument("--failed-dir", default="")
    parser.add_argument("--max-idle-seconds", type=int, default=0)
    parser.add_argument("--segments-per-claim", type=int, default=0)
    parser.add_argument(
        "--restrict-tumours",
        default="",
        help="Optional comma-separated list of tumour dirs to restrict to",
    )
    parser.add_argument(
        "--restrict-segments",
        default="",
        help="Optional comma-separated list of segment names to restrict to",
    )
    for category in (SINGLE_CLONE, SINGLE_SAMPLE, SINGLE_CLONE_AND_SINGLE_SAMPLE):
        parser.add_argument(
            f"--include-{category.replace('_', '-')}",
            dest=f"include_{category}",
            type=int,
            choices=[0, 1],
            default=0,
            help=f"Include {category.replace('_', ' ')} tumours in the combined cohort table",
        )
    return parser.parse_args()


def write_fresh_table(df: pd.DataFrame, path: str, file_type: str) -> str:
    """Write df as <path stem>.<file_type>, removing copies left in other formats by a previous run
    so downstream steps find a single file."""
    out_path = with_table_ext(path, file_type)
    for other_type in TABLE_EXTENSIONS.values():
        stale = with_table_ext(path, other_type)
        if stale != out_path and os.path.exists(stale):
            os.remove(stale)
    write_table(df, out_path)
    return out_path


def write_combined_tables(final: pd.DataFrame, args, file_type: str) -> None:
    """Write the cohort table without excluded single clone / single sample tumours.

    The unfiltered table and the tumour classification go to the outputs dir, where
    the analysis step uses them to produce per-tumour results for every tumour.
    Both cohort tables are written in file_type (the segment outputs' format).
    """
    include = {c: bool(getattr(args, f"include_{c}")) for c in (SINGLE_CLONE, SINGLE_SAMPLE, SINGLE_CLONE_AND_SINGLE_SAMPLE)}
    tumour_ids = final["tumour_id"].astype(str).unique()
    classification = classify_cohort(args.input_dir, tumour_ids, include)
    side_dir = args.outputs_dir or os.path.dirname(os.path.abspath(args.out))
    classification.to_csv(os.path.join(side_dir, "tumour_classification.csv"), index=False)
    write_fresh_table(final, os.path.join(side_dir, "all_tumours_unfiltered"), file_type)
    included = set(classification.loc[classification["included"], "tumour_id"])
    excluded = classification.loc[~classification["included"], "tumour_id"].tolist()
    if excluded:
        print(f"Excluding {len(excluded)} single clone / single sample tumour(s) from the combined table: {excluded}")
    out_path = write_fresh_table(final[final["tumour_id"].astype(str).isin(included)], args.out, file_type)
    print(f"Wrote merged segments to {out_path}")


# --------------------------------------------------------------------------- #
# profile config / time helpers
# --------------------------------------------------------------------------- #
def parse_worker_resources(
    config_path: str, worker_label: str
) -> Tuple[Dict[str, Optional[str]], Optional[str]]:
    """Return ({memory,time,cpus}, problem). `problem` explains why values are missing."""
    result: Dict[str, Optional[str]] = {"memory": None, "time": None, "cpus": None}
    if not config_path:
        return result, "no profile config path was passed to the merge step"
    cfg_path = Path(config_path)
    try:
        text = cfg_path.read_text()
    except OSError as exc:
        return result, (
            f"profile config {config_path} is not readable from inside the merge task ({exc}); "
            "if USE_CONTAINER=1 the nextflow/ directory must be bound into the container"
        )
    label_re = re.compile(
        rf"withLabel:\s*['\"]{re.escape(worker_label)}['\"]\s*{{", re.IGNORECASE
    )
    match = label_re.search(text)
    if not match:
        return result, f"label '{worker_label}' not found in {config_path}"
    start = match.end()
    brace_level = 1
    idx = start
    while idx < len(text) and brace_level > 0:
        if text[idx] == "{":
            brace_level += 1
        elif text[idx] == "}":
            brace_level -= 1
        idx += 1
    block = text[start : idx - 1]
    for key in ("memory", "time", "cpus"):
        value = extract_assignment(block, key)
        if value:
            result[key] = value
    return result, None


def extract_assignment(block: str, key: str) -> Optional[str]:
    pattern = re.compile(rf"{key}\s*=\s*['\"]?([^'\"\n]+)['\"]?", re.IGNORECASE)
    match = pattern.search(block)
    if match:
        return match.group(1).strip()
    return None


def parse_time_to_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    cleaned = value.strip().lower().replace(" ", "")
    match = re.match(r"(?P<num>[0-9]+(?:\.[0-9]+)?)(?P<unit>[a-z]+)", cleaned)
    if not match:
        return None
    num = float(match.group("num"))
    unit = match.group("unit")
    factors = {
        "s": 1,
        "sec": 1,
        "secs": 1,
        "m": 60,
        "min": 60,
        "mins": 60,
        "h": 3600,
        "hr": 3600,
        "hrs": 3600,
        "d": 86400,
        "day": 86400,
        "days": 86400,
    }
    for key, factor in factors.items():
        if unit.startswith(key):
            return num * factor
    return None


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


# --------------------------------------------------------------------------- #
# Nextflow task discovery
# --------------------------------------------------------------------------- #
def parse_nextflow_log(log_path: str, process_name: str) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """Single pass over .nextflow.log; returns (task entries, problem)."""
    entries: List[Dict[str, str]] = []
    if not log_path:
        return entries, "no nextflow log path was passed to the merge step"
    seen = set()
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if "submitted process" not in line or process_name not in line:
                    continue
                match = SUBMITTED_RE.search(line)
                if not match or match.group("name") != process_name:
                    continue
                work_dir = match.group("dir")
                if work_dir in seen:
                    continue
                seen.add(work_dir)
                entries.append(
                    {
                        "run_id": match.group("run"),
                        "job_id": match.group("job"),
                        "work_path": work_dir,
                    }
                )
    except OSError as exc:
        return [], (
            f"nextflow log {log_path} is not readable from inside the merge task ({exc}); "
            "if USE_CONTAINER=1 the directory holding .nextflow.log must be bound into the container"
        )
    if not entries:
        return entries, f"no '{process_name}' submissions found in {log_path} (log rotated by a later run?)"
    return entries, None


def read_exit_code(work_path: Path) -> Optional[int]:
    try:
        exit_text = (work_path / ".exitcode").read_text().strip()
        if exit_text:
            return int(exit_text)
    except (OSError, ValueError):
        return None
    return None


def read_trace_value(work_path: Path, key: str) -> Optional[str]:
    try:
        for line in (work_path / ".command.trace").read_text().splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip() or None
    except OSError:
        return None
    return None


def read_realtime_seconds(work_path: Path) -> Optional[float]:
    value = read_trace_value(work_path, "realtime")
    if value:
        try:
            return float(value) / 1000.0  # Nextflow stores realtime in ms
        except ValueError:
            return None
    return None


def tail_text(path: Path, max_bytes: int = 4000) -> str:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            return handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return ""


def classify_text(text: str) -> List[str]:
    return [label for label, rx in ERROR_SIGNATURES if rx.search(text)]


def gather_worker_statuses(worker_entries: List[Dict[str, str]]) -> List[Dict[str, object]]:
    statuses: List[Dict[str, object]] = []
    for entry in worker_entries:
        work_path = Path(entry["work_path"])
        if not work_path.exists():
            continue
        exit_code = read_exit_code(work_path)
        status: Dict[str, object] = {
            "run_id": entry.get("run_id"),
            "job_id": entry.get("job_id"),
            "work_path": str(work_path),
            "log_path": str(work_path / ".command.log"),
            "err_path": str(work_path / ".command.err"),
            "exit_code": exit_code,
            "runtime_seconds": read_realtime_seconds(work_path),
            "labels": [],
        }
        if exit_code != 0:
            text = tail_text(work_path / ".command.err") + tail_text(
                work_path / ".command.log"
            )
            status["labels"] = classify_text(text)
        statuses.append(status)
    return statuses


def build_worker_summary(
    statuses: List[Dict[str, object]], allocated_seconds: Optional[float]
) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "total": len(statuses),
        "completed": 0,
        "failed": 0,
        "timed_out": [],
        "failures": [],
        "exit_codes": Counter(),
    }
    runtimes: List[float] = []
    for status in statuses:
        exit_code = status.get("exit_code")
        runtime = status.get("runtime_seconds")
        if runtime is not None:
            runtimes.append(runtime)
        summary["exit_codes"][exit_code] += 1
        if exit_code == 0:
            summary["completed"] += 1
        else:
            summary["failed"] += 1
            summary["failures"].append(status)
        if allocated_seconds and runtime and runtime >= allocated_seconds * 0.98:
            status["timed_out"] = True
            summary["timed_out"].append(status)
    if runtimes:
        summary["runtime_min"] = min(runtimes)
        summary["runtime_max"] = max(runtimes)
    return summary


# --------------------------------------------------------------------------- #
# Segment-level diagnosis (works without the Nextflow log: only needs the
# ALPACA work directories, which are always bound into the container)
# --------------------------------------------------------------------------- #
def segment_stem(segment_id: str) -> str:
    """Segment input file name without its table extension (segments may be csv or parquet)."""
    return f"{SEGMENT_FILE_PREFIX}{segment_id}"


def segment_file_exists(directory: str, segment_id: str) -> bool:
    # lexists: pool entries are symlinks that may dangle once the segment moved on
    stem = segment_stem(segment_id)
    return any(os.path.lexists(os.path.join(directory, f"{stem}{ext}")) for ext in TABLE_EXTENSIONS)


def index_worker_dirs(in_progress_dir: str) -> Dict[str, Tuple[str, str]]:
    """basename without table extension -> (worker_dir_name, 'queue'|'active') for every file left in worker dirs."""
    index: Dict[str, Tuple[str, str]] = {}
    if not in_progress_dir or not os.path.isdir(in_progress_dir):
        return index
    for worker in os.listdir(in_progress_dir):
        for sub, kind in (("queue", "queue"), ("in_progress", "active")):
            sub_dir = os.path.join(in_progress_dir, worker, sub)
            try:
                for name in os.listdir(sub_dir):
                    if is_table_file(name):
                        index[strip_table_ext(name)] = (worker, kind)
            except OSError:
                continue
    return index


def load_worker_logs(outputs_dir: str) -> Dict[str, Dict[str, object]]:
    """worker_<id> -> parsed worker log (+ mtime info), preferring the most complete copy."""
    logs: Dict[str, Dict[str, object]] = {}
    if not outputs_dir:
        return logs
    out = Path(outputs_dir)
    candidates = list(out.glob("worker_*.done.log")) + list(
        (out / "worker_logs").glob("worker_*.done.log")
    )
    for path in candidates:
        wid = path.name[: -len(".done.log")]  # worker_12
        try:
            data = json.loads(path.read_text())
            mtime = path.stat().st_mtime
        except (OSError, ValueError):
            continue
        prev = logs.get(wid)
        if prev and prev["mtime"] >= mtime:
            continue
        data["mtime"] = mtime
        hb = out / "worker_logs" / f"{wid}.heartbeat"
        try:
            data["heartbeat_mtime"] = hb.stat().st_mtime
        except OSError:
            data["heartbeat_mtime"] = None
        logs[wid] = data
    return logs


def worker_exit_reason(log: Dict[str, object]) -> str:
    for msg in reversed(log.get("messages", [])):
        text = msg if isinstance(msg, str) else None
        if text and "exiting worker" in text:
            return text
    return ""


def diagnose_segments(
    missing_segments: List[str], args, worker_logs: Dict[str, Dict[str, object]]
) -> Dict[str, object]:
    """Locate every missing segment and group them by what happened to it."""
    in_progress_index = index_worker_dirs(args.in_progress_dir)
    states: Dict[str, List[str]] = defaultdict(list)
    detail: Dict[str, str] = {}
    for seg in missing_segments:
        stem = segment_stem(seg)
        if stem in in_progress_index:
            worker, kind = in_progress_index[stem]
            state = "stranded_in_queue" if kind == "queue" else "claimed_not_finished"
            detail[seg] = worker
        elif args.failed_dir and segment_file_exists(args.failed_dir, seg):
            state = "failed"
        elif args.done_dir and segment_file_exists(args.done_dir, seg):
            state = "done_but_no_output"
        elif args.pool_dir and segment_file_exists(args.pool_dir, seg):
            state = "never_dispatched"
        else:
            state = "untracked"
        states[state].append(seg)

    # ALPACA non-zero runs recorded by workers (stderr snippets)
    failed_runs: Dict[str, Dict[str, object]] = {}
    alpaca_runs = 0
    alpaca_failed_runs = 0
    for wid, log in worker_logs.items():
        for run in log.get("alpaca_runs", []):
            alpaca_runs += 1
            if run.get("returncode") == 0:
                continue
            alpaca_failed_runs += 1
            for fn in run.get("input_files", []):
                failed_runs[strip_table_ext(fn)] = {
                    "worker": wid,
                    "returncode": run.get("returncode"),
                    "stderr": run.get("stderr_snip", ""),
                }

    # workers that left segments behind
    stranded_workers: Dict[str, Dict[str, object]] = {}
    for seg in states.get("stranded_in_queue", []) + states.get(
        "claimed_not_finished", []
    ):
        worker = detail[seg]
        info = stranded_workers.setdefault(
            worker, {"segments": 0, "claims": None, "lifetime": None, "reason": ""}
        )
        info["segments"] += 1
        log = worker_logs.get(worker)
        if log:
            info["claims"] = len(log.get("claims", []))
            info["reason"] = worker_exit_reason(log)
            start = log.get("start_time")
            end = log.get("heartbeat_mtime")
            if start and end:
                try:
                    start_ts = datetime.fromisoformat(start.rstrip("Z")).timestamp()
                    info["lifetime"] = end - start_ts
                except ValueError:
                    pass
    return {
        "states": states,
        "failed_runs": failed_runs,
        "alpaca_runs": alpaca_runs,
        "alpaca_failed_runs": alpaca_failed_runs,
        "stranded_workers": stranded_workers,
        "workers_with_logs": len(worker_logs),
    }


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def ensure_report_path(output_dir: str) -> Path:
    if output_dir:
        report_dir = Path(output_dir).expanduser().resolve() / "reports"
    else:
        report_dir = Path.cwd()
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / "FAILED_RUN_REPORT.txt"


def _bullets(lines: List[str], text: str, indent: str = "") -> None:
    lines.append(f"{indent}- {text}")


def build_findings(context: Dict[str, object]) -> List[Dict[str, object]]:
    """Turn raw evidence into ranked findings: title, evidence, concrete actions."""
    diag = context.get("diagnosis") or {}
    states: Dict[str, List[str]] = diag.get("states", {})
    summary = context.get("worker_summary") or {}
    max_idle = context.get("max_idle_seconds") or 0
    workers = context.get("workers") or 0
    findings: List[Dict[str, object]] = []

    # 1. segments left behind in worker queues / claimed but unfinished
    queued = states.get("stranded_in_queue", [])
    claimed = states.get("claimed_not_finished", [])
    if queued or claimed:
        sw = diag.get("stranded_workers", {})
        idle_exits = [
            w
            for w, i in sw.items()
            if i.get("lifetime") is not None
            and max_idle
            and i["lifetime"] <= max_idle + 60
            and not i.get("claims")
        ]
        evidence = [
            f"{len(queued)} segment(s) are still sitting in worker queue dirs "
            f"({context.get('in_progress_dir') or 'alpaca-work/in_progress'}/worker_N/queue) "
            "although every worker task has already exited.",
        ]
        if claimed:
            evidence.append(
                f"{len(claimed)} segment(s) were claimed by a worker but never finished "
                "(worker killed mid-run: OOM, walltime or node failure - see Slurm findings)."
            )
        evidence.append(
            f"{len(sw)} worker(s) left segments behind; "
            f"{len(idle_exits)} of them exited without ever claiming work."
        )
        examples = sorted(
            sw.items(), key=lambda kv: kv[1]["segments"], reverse=True
        )[:3]
        for worker, info in examples:
            evidence.append(
                f"e.g. {worker}: left {info['segments']} segment(s), claimed {info['claims']} in total, "
                f"alive {format_duration(info['lifetime'])}"
                + (f", last message: {info['reason']!r}" if info["reason"] else "")
            )
        cause = "workers exited before the dispatcher handed them work"
        actions = [
            "Workers quit after MAX_IDLE_SECONDS "
            f"(= {max_idle or 'unknown'}s) without work and, with segments_per_claim={context.get('segments_per_claim') or '?'}"
            f" and {workers} workers, the dispatcher fills worker queues only gradually. "
            "A worker that started while its queue was empty exited, then the dispatcher filled the dead queue.",
            "In nextflow/pipeline.env raise MAX_IDLE_SECONDS well above the dispatcher fill time "
            "and the longest segment runtime, e.g. MAX_IDLE_SECONDS=900 (currently "
            f"{max_idle or 'unknown'}).",
            "Optionally lower WORKERS so that each worker gets several batches "
            "(fewer, longer-lived workers are less sensitive to queue-fill races).",
            "Keep RESTART=1 in pipeline.env: this clears alpaca-work/in_progress and failed/, "
            "rebuilds the pool from segments that are NOT in alpaca-work/done/, and reruns only the missing "
            f"{len(queued) + len(claimed)} segment(s) (completed segments are skipped).",
            "Resubmit with: sbatch slurm_submit.sbatch",
        ]
        findings.append(
            {"title": f"Workers exited early ({cause})", "evidence": evidence, "actions": actions}
        )

    # 2. ALPACA failures
    failed = states.get("failed", [])
    if failed:
        failed_runs = diag.get("failed_runs", {})
        label_counter: Counter = Counter()
        samples: Dict[str, str] = {}
        for seg in failed:
            run = failed_runs.get(segment_stem(seg))
            text = run["stderr"] if run else ""
            labels = classify_text(text) or ["unclassified"]
            for label in labels:
                label_counter[label] += 1
                samples.setdefault(label, f"{seg}: " + " | ".join(text.strip().splitlines()[-3:])[:400])
        evidence = [
            f"{len(failed)} segment(s) are in the failed dir ({context.get('failed_dir')}); "
            "ALPACA returned non-zero for all retries.",
            "Error classes: " + ", ".join(f"{k} x{v}" for k, v in label_counter.most_common()),
        ]
        evidence += [f"sample [{k}] {v}" for k, v in samples.items()]
        actions = [
            "Inspect per-segment stderr in alpaca-work/outputs/worker_logs/worker_<id>.done.log "
            "(key 'alpaca_runs' -> 'stderr_snip') and the ALPACA logs/ dir under the worker work dir.",
        ]
        if "out_of_memory" in label_counter:
            actions.append(
                "OOM signature found: raise 'memory' of withLabel 'worker_high' in nextflow/slurm.conf "
                f"(currently {context.get('worker_memory') or 'unknown'}), e.g. to 16 GB."
            )
        if "gurobi_license" in label_counter:
            actions.append(
                "Gurobi licence problem: check GUROBI_LICENSE_FILE in pipeline.env exists on compute nodes "
                "and is bound into the container; a size-limited licence cannot solve large segments."
            )
        if "infeasible" in label_counter:
            actions.append(
                "Infeasible models: inspect the segment input tables and --min_ci / "
                "--homo_del_strong_evidence_threshold in ALPACA_ARGS."
            )
        if "python_exception" in label_counter or "unclassified" in label_counter:
            actions.append(
                "Python exception / unknown error: reproduce one segment by hand, e.g. "
                f"alpaca run --mode segment --input_tumour_directory <input_dir>/<tumour> "
                f"--input_data_directory <dir with the segment table> --input_files {segment_stem(failed[0])}.<csv|parquet> "
                "--output_directory /tmp/test " + (context.get("alpaca_args") or "")
            )
        actions.append(
            "After fixing, keep RESTART=1 and resubmit (failed/ is cleared and the segments are retried)."
        )
        findings.append(
            {"title": "ALPACA failed on some segments", "evidence": evidence, "actions": actions}
        )

    # 3. done but no output
    done_no_out = states.get("done_but_no_output", [])
    if done_no_out:
        findings.append(
            {
                "title": "Segments marked done but no output row was produced",
                "evidence": [
                    f"{len(done_no_out)} segment(s) are in the done dir but absent from segment_outputs "
                    "(ALPACA exited 0 without writing a result, or the output csv was removed/overwritten)."
                ],
                "actions": [
                    "Inspect alpaca-work/outputs/segment_outputs/ and ALPACA logs for these segments.",
                    "RESTART=1 skips anything in done/, so remove them first: "
                    "while read s; do rm -f alpaca-work/done/ALPACA_input_table_${s}.*; done "
                    "< <path to missing_segments.tsv, state=done_but_no_output>",
                    "Then resubmit with sbatch slurm_submit.sbatch.",
                ],
            }
        )

    # 4. never dispatched / untracked
    never = states.get("never_dispatched", [])
    untracked = states.get("untracked", [])
    if never:
        findings.append(
            {
                "title": "Segments never dispatched",
                "evidence": [
                    f"{len(never)} segment(s) are still in the pool dir: dispatcher exited "
                    "(DISPATCHER_MAX_IDLE_CYCLES reached) or all workers died before they were handed out."
                ],
                "actions": [
                    "Raise DISPATCHER_MAX_IDLE_CYCLES / check alpaca-work/outputs/dispatcher.done and the "
                    "runDispatcher task log; then rerun with RESTART=1.",
                ],
            }
        )
    if untracked:
        findings.append(
            {
                "title": "Segments not found in pool, queue, done or failed",
                "evidence": [
                    f"{len(untracked)} segment(s) from the input tables have no trace in the work dirs: "
                    "they were never put in the pool (RESTRICT_TO_* filter?), or their files were deleted."
                ],
                "actions": [
                    "Check RESTRICT_TO_TUMOURS / RESTRICT_TO_SEGMENTS in pipeline.env and the preparePool "
                    "task log (scripts/create_symlink_pool.py).",
                    "Rerun with RESTART=1 so the pool is rebuilt.",
                ],
            }
        )

    # 5. Slurm / Nextflow level problems
    failures = summary.get("failures", [])
    if failures:
        codes = Counter(f.get("exit_code") for f in failures)
        evidence = [
            f"{len(failures)} of {summary.get('total')} worker task(s) exited non-zero: "
            + ", ".join(f"exit {c if c is not None else 'none'} x{n}" for c, n in codes.items())
        ]
        actions = []
        label_all = Counter(l for f in failures for l in f.get("labels", []))
        if 137 in codes or "out_of_memory" in label_all:
            actions.append(
                "Exit 137 / OOM: raise 'memory' for withLabel 'worker_high' in nextflow/slurm.conf "
                f"(currently {context.get('worker_memory') or 'unknown'}) and the sbatch --mem if needed."
            )
        if 140 in codes or 143 in codes or "time_limit" in label_all:
            actions.append(
                "Exit 140/143 / time limit: raise 'time' for withLabel 'worker_high' in nextflow/slurm.conf "
                f"(currently {context.get('worker_time') or 'unknown'})."
            )
        if not actions:
            actions.append("Inspect the task logs listed in the 'Problematic worker logs' section.")
        findings.append(
            {"title": "Worker tasks failed at Slurm level", "evidence": evidence, "actions": actions}
        )
    timed_out = summary.get("timed_out", [])
    if timed_out:
        findings.append(
            {
                "title": "Workers hit their time allocation",
                "evidence": [
                    f"{len(timed_out)} worker(s) ran >=98% of the allocated {context.get('worker_time')}."
                ],
                "actions": [
                    "Increase 'time' for withLabel 'worker_high' in nextflow/slurm.conf, "
                    "or lower SEGMENTS_PER_CLAIM so work is rebalanced between workers."
                ],
            }
        )
    return findings


def generate_failure_report(
    report_path: Path,
    missing_segments: List[str],
    context: Dict[str, object],
) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    diag = context.get("diagnosis") or {}
    states: Dict[str, List[str]] = diag.get("states", {})
    summary = context.get("worker_summary") or {}
    findings = build_findings(context)

    lines = [
        "FAILED RUN REPORT",
        "=================",
        f"Timestamp: {timestamp}",
        "",
        "Summary:",
        f"- Run failed because {len(missing_segments)} segment(s) were missing from the merged output.",
    ]
    if findings:
        lines.append(f"- Most likely cause: {findings[0]['title']}")
    else:
        lines.append(
            "- Cause could not be determined automatically (no trace of the missing segments was found); "
            "see 'Diagnostics availability' below."
        )
    if missing_segments:
        lines.append("- What happened to the missing segments:")
        labels = {
            "stranded_in_queue": "left in a worker queue (never started)",
            "claimed_not_finished": "claimed by a worker but never finished",
            "failed": "ALPACA failed (in failed dir)",
            "done_but_no_output": "marked done but no output row",
            "never_dispatched": "still in the pool (never dispatched)",
            "untracked": "no trace in any work dir",
        }
        for key, label in labels.items():
            if states.get(key):
                lines.append(f"    {len(states[key]):>6}  {label}")
        preview = ", ".join(sorted(missing_segments)[:10])
        lines.append(f"- Missing segment examples: {preview}")
    if context.get("missing_list_path"):
        lines.append(f"- Full list (segment<TAB>state<TAB>worker): {context['missing_list_path']}")
    lines.append("")

    lines.append("WHAT TO CHANGE / INSPECT BEFORE RERUNNING")
    lines.append("-----------------------------------------")
    if not findings:
        lines.append("1. Open the Nextflow log and worker logs listed under 'Log locations'.")
        lines.append("2. Rerun with RESTART=1 (completed segments in alpaca-work/done are skipped).")
    for number, finding in enumerate(findings, 1):
        lines.append(f"{number}. {finding['title']}")
        for item in finding["evidence"]:
            lines.append(f"   evidence: {item}")
        for step, action in enumerate(finding["actions"], 1):
            lines.append(f"   [{number}.{step}] {action}")
        lines.append("")
    if findings:
        lines.append(
            "Then rerun: sbatch slurm_submit.sbatch (RESTART=1 keeps finished work; set RESTART=0 only "
            "to recompute everything)."
        )
        lines.append("")

    lines.append("Log locations:")
    if context.get("merge_log_path"):
        lines.append(f"- Merge process log: {context['merge_log_path']}")
    lines.append(f"- Nextflow log: {context.get('nextflow_log') or 'unknown'}")
    if context.get("outputs_dir"):
        lines.append(
            f"- Per-worker JSON logs (claims, ALPACA return codes, stderr): "
            f"{context['outputs_dir']}/worker_logs/worker_<id>.done.log"
        )
    if context.get("failed_dir"):
        lines.append(f"- Failed segment files: {context['failed_dir']}")
    lines.append("")

    lines.append("Worker configuration:")
    lines.append(f"- Profile: {context.get('profile_name') or 'unknown'}")
    lines.append(f"- Worker label: {context.get('worker_label') or 'worker_high'}")
    lines.append(f"- Workers requested: {context.get('workers') or 'unknown'}")
    lines.append(f"- CPUs per worker: {context.get('worker_cpus') or 'unknown'}")
    lines.append(f"- Memory per worker: {context.get('worker_memory') or 'unknown'}")
    lines.append(f"- Time per worker: {context.get('worker_time') or 'unknown'}")
    lines.append(f"- MAX_IDLE_SECONDS: {context.get('max_idle_seconds') or 'unknown'}")
    lines.append(f"- SEGMENTS_PER_CLAIM: {context.get('segments_per_claim') or 'unknown'}")
    lines.append("")

    lines.append("Worker execution summary:")
    if summary and summary.get("total"):
        lines.append(f"- Workers observed (Nextflow work dirs): {summary.get('total', 0)}")
        lines.append(f"- Exited 0: {summary.get('completed', 0)}")
        lines.append(f"- Exited non-zero: {summary.get('failed', 0)}")
        if summary.get("runtime_max") is not None:
            lines.append(
                f"- Observed runtime window: {format_duration(summary.get('runtime_min'))} - "
                f"{format_duration(summary.get('runtime_max'))}"
            )
        lines.append(
            "- Note: a worker exits 0 even when its segments failed; use the segment states above."
        )
    else:
        lines.append("- Nextflow task directories were not available (see 'Diagnostics availability').")
    if diag.get("workers_with_logs"):
        lines.append(
            f"- Worker JSON logs read: {diag['workers_with_logs']}; ALPACA invocations: "
            f"{diag.get('alpaca_runs', 0)} ({diag.get('alpaca_failed_runs', 0)} non-zero, incl. retried)"
        )
    lines.append("")

    failure_paths = []
    for failure in summary.get("failures", [])[:5]:
        failure_paths.append(
            f"workerTask ({failure.get('run_id') or 'unknown'}) - exit code "
            f"{failure.get('exit_code') if failure.get('exit_code') is not None else 'none (no .exitcode)'}"
            f" - {failure.get('err_path')} ; {failure.get('log_path')}"
            + (f" [{', '.join(failure['labels'])}]" if failure.get("labels") else "")
        )
    if failure_paths:
        lines.append("Problematic worker logs (limited to 5):")
        lines.extend(f"- {entry}" for entry in failure_paths)
        lines.append("")

    problems = context.get("diagnostic_problems") or []
    if problems:
        lines.append("Diagnostics availability:")
        lines.extend(f"- {p}" for p in problems)
        lines.append("")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    seg_dfs: List[pd.DataFrame] = []
    seg_types = set()
    for file_name in os.listdir(args.segments_dir):
        if not is_table_file(file_name):
            continue
        if any(token in file_name for token in ("combined", "report", "summary")):
            continue
        df, file_type = read_any_table(os.path.join(args.segments_dir, file_name))
        seg_dfs.append(df)
        seg_types.add(file_type)

    final = pd.concat(seg_dfs, ignore_index=True)

    if final["pred_CN_A"].isna().any() or final["pred_CN_B"].isna().any():
        nan_df = final[final["pred_CN_A"].isna() | final["pred_CN_B"].isna()]
        nan_df_head = nan_df[["tumour_id", "segment"]].head(5)
        raise ValueError(
            f"All values in 'pred_CN_A' or 'pred_CN_B' are NaN. Sample rows with NaN:\n{nan_df_head}"
        )

    segments_out = set(final["tumour_id"] + "_" + final["segment"].astype(str))

    tumours_filter = {t.strip() for t in args.restrict_tumours.split(",") if t.strip()}
    segments_filter = {
        s.strip() for s in args.restrict_segments.split(",") if s.strip()
    }

    input_dfs: List[pd.DataFrame] = []
    for tumour_id in [x for x in os.listdir(args.input_dir) if x != ".DS_Store"]:
        if tumours_filter and tumour_id not in tumours_filter:
            continue
        alpca_input_table_path = find_table(
            os.path.join(args.input_dir, tumour_id), "ALPACA_input_table"
        )
        if alpca_input_table_path is None:
            continue
        try:
            tumour_df, _ = read_any_table(alpca_input_table_path)
            input_dfs.append(tumour_df)
        except Exception as exc:
            print(f"Error reading {tumour_id}: {exc}")
    input_df = pd.concat(input_dfs)
    if segments_filter:
        input_df = input_df[input_df["segment"].astype(str).isin(segments_filter)]
    segments_in = set(input_df["tumour_id"] + "_" + input_df["segment"].astype(str))
    missing_segments = sorted(segments_in - segments_out)

    if not missing_segments:
        write_combined_tables(final, args, common_table_type(seg_types))
        with open("merged_segments.txt", "w", encoding="utf-8") as handle:
            for seg in sorted(segments_in):
                handle.write(f"{seg}\n")
        return

    print(f"Missing {len(missing_segments)} segments in output: {missing_segments[:50]} ...")

    report_path = ensure_report_path(args.output_dir)
    problems: List[str] = []

    # --- Nextflow level evidence (optional: depends on log/work dir visibility) ---
    merge_entries, problem = parse_nextflow_log(args.nextflow_log, "mergeSegments")
    merge_log_path = None
    if merge_entries:
        candidate = Path(merge_entries[-1]["work_path"]) / ".command.log"
        if candidate.exists():
            merge_log_path = str(candidate)
    worker_entries, problem = parse_nextflow_log(args.nextflow_log, "workerTask")
    if problem:
        problems.append(problem)
    worker_statuses = gather_worker_statuses(worker_entries)
    if worker_entries and not worker_statuses:
        problems.append(
            f"{len(worker_entries)} workerTask work dirs were listed in the log but none is readable "
            "from the merge task (work dir not bound into the container?)"
        )

    worker_resources, problem = parse_worker_resources(args.profile_config, args.worker_label)
    if problem:
        problems.append(problem)
    worker_memory = worker_resources.get("memory")
    worker_time = worker_resources.get("time")
    allocated_seconds = parse_time_to_seconds(worker_time)
    worker_summary = build_worker_summary(worker_statuses, allocated_seconds)

    # --- ALPACA work-dir evidence ---
    worker_logs = load_worker_logs(args.outputs_dir)
    if not worker_logs:
        problems.append(
            f"no worker JSON logs found under {args.outputs_dir or '<outputs-dir not passed>'}/worker_logs"
        )
    for label, path in (
        ("in-progress", args.in_progress_dir),
        ("done", args.done_dir),
        ("failed", args.failed_dir),
        ("pool", args.pool_dir),
    ):
        if not path:
            problems.append(f"--{label}-dir was not passed to merge_segments.py; segment states are limited")
    diagnosis = diagnose_segments(missing_segments, args, worker_logs)

    # machine readable list of missing segments and their state
    list_path = report_path.parent / "missing_segments.tsv"
    seg_state = {s: k for k, v in diagnosis["states"].items() for s in v}
    try:
        with open(list_path, "w", encoding="utf-8") as handle:
            handle.write("segment\tstate\n")
            for seg in missing_segments:
                handle.write(f"{seg}\t{seg_state.get(seg, 'unknown')}\n")
    except OSError as exc:
        problems.append(f"could not write {list_path}: {exc}")
        list_path = None

    context = {
        "merge_log_path": merge_log_path,
        "nextflow_log": args.nextflow_log or "unknown",
        "profile_name": args.profile_name or "unknown",
        "worker_label": args.worker_label,
        "workers": args.workers,
        "worker_cpus": args.worker_cpus or worker_resources.get("cpus"),
        "worker_memory": worker_memory,
        "worker_time": worker_time,
        "worker_summary": worker_summary,
        "diagnosis": diagnosis,
        "max_idle_seconds": args.max_idle_seconds,
        "segments_per_claim": args.segments_per_claim,
        "outputs_dir": args.outputs_dir,
        "in_progress_dir": args.in_progress_dir,
        "failed_dir": args.failed_dir,
        "missing_list_path": str(list_path) if list_path else None,
        "diagnostic_problems": problems,
        "alpaca_args": os.environ.get("ALPACA_ARGS", ""),
    }

    generate_failure_report(report_path, missing_segments, context)

    raise ValueError(
        f"Missing {len(missing_segments)} segments in output. See failure report at {report_path} for details."
    )


if __name__ == "__main__":
    main()

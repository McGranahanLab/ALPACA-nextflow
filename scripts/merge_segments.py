#!/usr/bin/env python3
import argparse
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


PROCESS_RE = re.compile(
    r"\[(?P<hash>[0-9a-f]{2}/[0-9a-f]+)\]\s+(?:Submitted|Cached)\s+process > (?P<name>[\w-]+)\s+\((?P<run>\d+)\)"
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--segments-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--nextflow-log", dest="nextflow_log", default="")
    parser.add_argument("--work-dir", dest="work_dir", default="work")
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--worker-cpus", type=int, default=0)
    parser.add_argument("--worker-label", default="worker_high")
    parser.add_argument("--profile-config", default="")
    parser.add_argument("--profile-name", default="")
    return parser.parse_args()


def parse_worker_resources(config_path: str, worker_label: str) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {"memory": None, "time": None, "cpus": None}
    if not config_path:
        return result
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        return result
    text = cfg_path.read_text()
    label_re = re.compile(rf"withLabel:\s*['\"]{re.escape(worker_label)}['\"]\s*{{", re.IGNORECASE)
    match = label_re.search(text)
    if not match:
        return result
    start = match.end()
    brace_level = 1
    idx = start
    while idx < len(text) and brace_level > 0:
        if text[idx] == '{':
            brace_level += 1
        elif text[idx] == '}':
            brace_level -= 1
        idx += 1
    block = text[start:idx - 1]
    for key in ("memory", "time", "cpus"):
        value = extract_assignment(block, key)
        if value:
            result[key] = value
    return result


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


def parse_nextflow_log(log_path: str, process_name: str) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    if not log_path:
        return entries
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if process_name not in line:
                    continue
                match = PROCESS_RE.search(line)
                if match and match.group("name") == process_name:
                    entries.append(
                        {
                            "hash": match.group("hash"),
                            "run_id": match.group("run"),
                            "line": line.strip(),
                        }
                    )
    except OSError:
        return []
    return entries


def read_exit_code(work_path: Path) -> Optional[int]:
    try:
        exit_text = (work_path / ".exitcode").read_text().strip()
        if exit_text:
            return int(exit_text)
    except (OSError, ValueError):
        return None
    return None


def read_realtime_seconds(work_path: Path) -> Optional[float]:
    trace_path = work_path / ".command.trace"
    try:
        for line in trace_path.read_text().splitlines():
            if line.startswith("realtime="):
                value = line.split("=", 1)[1].strip()
                if value:
                    millis = float(value)
                    # Nextflow stores realtime in milliseconds
                    return millis / 1000.0
    except (OSError, ValueError):
        return None
    return None


def gather_worker_statuses(worker_entries: List[Dict[str, str]], work_dir: str) -> List[Dict[str, Optional[str]]]:
    statuses: List[Dict[str, Optional[str]]] = []
    if not work_dir:
        return statuses
    work_root = Path(work_dir)
    for entry in worker_entries:
        work_path = work_root / entry["hash"]
        if not work_path.exists():
            continue
        statuses.append(
            {
                "run_id": entry.get("run_id"),
                "hash": entry.get("hash"),
                "work_path": str(work_path),
                "log_path": str(work_path / ".command.log"),
                "exit_code": read_exit_code(work_path),
                "runtime_seconds": read_realtime_seconds(work_path),
            }
        )
    return statuses


def build_worker_summary(statuses: List[Dict[str, Optional[str]]], allocated_seconds: Optional[float]) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "total": len(statuses),
        "completed": 0,
        "failed": 0,
        "timed_out": [],
        "failures": [],
    }
    runtimes: List[float] = []
    for status in statuses:
        exit_code = status.get("exit_code")
        runtime = status.get("runtime_seconds")
        if runtime is not None:
            runtimes.append(runtime)
        if exit_code == 0:
            summary["completed"] += 1
        else:
            summary["failed"] += 1
            summary["failures"].append(status)
        if allocated_seconds and runtime:
            if runtime >= allocated_seconds * 0.98:
                status["timed_out"] = True
                summary["timed_out"].append(status)
    if runtimes:
        summary["runtime_min"] = min(runtimes)
        summary["runtime_max"] = max(runtimes)
    return summary


def ensure_report_path(output_dir: str) -> Path:
    if output_dir:
        report_dir = Path(output_dir).expanduser().resolve() / "reports"
    else:
        report_dir = Path.cwd()
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / "FAILED_RUN_REPORT.txt"


def generate_failure_report(
    report_path: Path,
    missing_segments: List[str],
    context: Dict[str, object],
) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "FAILED RUN REPORT",
        "=================",
        f"Timestamp: {timestamp}",
        "",
        "Summary:",
        f"- Run failed because {len(missing_segments)} segment(s) were missing from the merged output.",
    ]
    if missing_segments:
        preview = ", ".join(sorted(missing_segments)[:10])
        lines.append(f"- Missing segment examples: {preview}")
    if context.get("merge_log_path"):
        lines.append(f"- Merge process log: {context['merge_log_path']}")
    if context.get("nextflow_log"):
        lines.append(f"- Nextflow log: {context['nextflow_log']}")
    lines.append("")
    lines.append("Worker configuration:")
    lines.append(f"- Profile: {context.get('profile_name') or 'unknown'}")
    lines.append(f"- Worker label: {context.get('worker_label') or 'worker_high'}")
    lines.append(f"- Workers requested: {context.get('workers') or 'unknown'}")
    lines.append(f"- CPUs per worker: {context.get('worker_cpus') or 'unknown'}")
    lines.append(f"- Memory per worker: {context.get('worker_memory') or 'unknown'}")
    lines.append(f"- Time per worker: {context.get('worker_time') or 'unknown'}")
    lines.append("")

    summary = context.get("worker_summary", {})
    if summary:
        lines.append("Worker execution summary:")
        lines.append(f"- Workers observed: {summary.get('total', 0)}")
        lines.append(f"- Completed successfully: {summary.get('completed', 0)}")
        lines.append(f"- Failed or exited early: {summary.get('failed', 0)}")
        runtime_min = format_duration(summary.get("runtime_min")) if summary.get("runtime_min") else "unknown"
        runtime_max = format_duration(summary.get("runtime_max")) if summary.get("runtime_max") else "unknown"
        lines.append(f"- Observed runtime window: {runtime_min} - {runtime_max}")
        timed_out = summary.get("timed_out", [])
        if timed_out:
            lines.append(
                f"- {len(timed_out)} worker(s) ran for the full requested time allocation; increase worker time/memory and re-run."
            )
        failures = summary.get("failures", [])
        if failures:
            lines.append("- Non-zero exit codes detected; inspect the logs listed below.")
    else:
        lines.append("Worker execution summary:")
        lines.append("- Unable to determine worker status from logs.")
    lines.append("")

    failure_paths = []
    for failure in summary.get("failures", [])[:5]:
        log_path = failure.get("log_path")
        exit_code = failure.get("exit_code")
        run_id = failure.get("run_id")
        if log_path:
            entry = f"workerTask ({run_id or 'unknown'}) - exit code {exit_code if exit_code is not None else 'unknown'} - {log_path}"
            failure_paths.append(entry)
    if failure_paths:
        lines.append("Problematic worker logs (limited to 5):")
        lines.extend(f"- {entry}" for entry in failure_paths)
        lines.append("")

    lines.append("Next steps:")
    lines.append("- Fix the upstream issue (missing segments or worker failures) and rerun the pipeline.")
    lines.append("- Keep this report for debugging or attach it to bug reports.")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()

    seg_dfs: List[pd.DataFrame] = []
    for file_name in os.listdir(args.segments_dir):
        if not file_name.endswith(".csv"):
            continue
        if any(token in file_name for token in ("combined", "report", "summary")):
            continue
        df = pd.read_csv(os.path.join(args.segments_dir, file_name))
        seg_dfs.append(df)

    final = pd.concat(seg_dfs, ignore_index=True)

    if final['pred_CN_A'].isna().any() or final['pred_CN_B'].isna().any():
        nan_df = final[final['pred_CN_A'].isna() | final['pred_CN_B'].isna()]
        nan_df_head = nan_df[['tumour_id', 'segment']].head(5)
        raise ValueError(f"All values in 'pred_CN_A' or 'pred_CN_B' are NaN. Sample rows with NaN:\n{nan_df_head}")

    segments_out = set(final['tumour_id'] + '_' + final['segment'].astype(str))
    input_dfs: List[pd.DataFrame] = []
    for tumour_id in [x for x in os.listdir(args.input_dir) if x != '.DS_Store']:
        try:
            tumour_df = pd.read_csv(os.path.join(args.input_dir, tumour_id, 'ALPACA_input_table.csv'))
            input_dfs.append(tumour_df)
        except Exception as exc:
            print(f"Error reading {tumour_id}: {exc}")
    input_df = pd.concat(input_dfs)
    segments_in = set(input_df['tumour_id'] + '_' + input_df['segment'].astype(str))
    missing_segments = sorted(segments_in - segments_out)

    if not missing_segments:
        final.to_csv(args.out, index=False)
        with open('merged_segments.txt', 'w', encoding='utf-8') as handle:
            for seg in sorted(segments_in):
                handle.write(f"{seg}\n")
        return

    print(f"Missing {len(missing_segments)} segments in output: {missing_segments}")

    report_path = ensure_report_path(args.output_dir)
    merge_entries = parse_nextflow_log(args.nextflow_log, "mergeSegments")
    merge_log_path = None
    if merge_entries:
        latest_merge = merge_entries[-1]
        merge_hash = latest_merge.get('hash')
        if merge_hash:
            merge_log_candidate = Path(args.work_dir) / merge_hash / '.command.log'
            if merge_log_candidate.exists():
                merge_log_path = str(merge_log_candidate)

    worker_entries = parse_nextflow_log(args.nextflow_log, "workerTask")
    worker_statuses = gather_worker_statuses(worker_entries, args.work_dir)

    worker_resources = parse_worker_resources(args.profile_config, args.worker_label)
    worker_memory = worker_resources.get('memory')
    worker_time = worker_resources.get('time')
    allocated_seconds = parse_time_to_seconds(worker_time)
    worker_summary = build_worker_summary(worker_statuses, allocated_seconds)

    context = {
        "merge_log_path": merge_log_path,
        "nextflow_log": args.nextflow_log or "unknown",
        "profile_name": args.profile_name or "unknown",
        "worker_label": args.worker_label,
        "workers": args.workers,
        "worker_cpus": args.worker_cpus or worker_resources.get('cpus'),
        "worker_memory": worker_memory,
        "worker_time": worker_time,
        "worker_summary": worker_summary,
    }

    generate_failure_report(report_path, missing_segments, context)

    raise ValueError(
        f"Missing {len(missing_segments)} segments in output. See failure report at {report_path} for details."
    )


if __name__ == "__main__":
    main()

# ALPACA-nextflow

## Overview
ALPACA-nextflow wraps the ALPACA copy-number inference toolkit in a fault-tolerant Nextflow pipeline. It parallelises segment-level ALPACA runs across workers, merges the solved segments into cohort-level results, generates QC summaries, and performs basic validation/cleanup so repeated executions are reproducible.

## Repository Layout
- nextflow/main.nf – orchestrates pool preparation, dispatcher, workers, merge, validation, analysis, reports, and cleanup stages.
- nextflow/run_nextflow.sh – entrypoint that loads a shell config (e.g. pipeline.env) and builds the Nextflow command.
- nextflow/pipeline.env – example shell config; edit paths/parameters before running.
- scripts/*.py – helper utilities (worker loop, dispatcher, pool builder, merging, report summarising, CCD aggregation, tumour splitting, validation).
- scripts/analyse_failed_run_helper.sh – quick diagnostics for stalled/failed executions.

## Requirements
- Nextflow 23.10+ with Java 11 or newer.
- Python 3.9+ with pandas plus the ALPACA Python package available on $PATH or within the specified conda env.
- SLURM cluster if using the `slurm` profile (otherwise the default `local` executor suffices).

## Quickstart
1. Create/edit `nextflow/pipeline.env` with paths that exist in your environment. The example values point to `../ALPACA-model/tests/...` and must be updated for real runs.
2. Prepare aand activate conda env that includes ALPACA.
3. Launch the pipeline from the repo root:

```bash
./nextflow/run_nextflow.sh pipeline.env
```

Pass additional Nextflow flags after the env file, e.g. `./nextflow/run_nextflow.sh pipeline.env -resume -with-trace`.

## Configuration knobs
`run_nextflow.sh` sources the chosen env file and forwards the variables as Nextflow parameters:

| Variable | Purpose |
| --- | --- |
| `INPUT_DIR` / `OUTPUT_DIR` | Cohort input directory (one tumour subdir each) and final delivery location. |
| `ALPACA_WORK` | Scratch workspace that stores pool, in-progress, done, failed, and worker outputs. Safe to delete between runs (unless debugging). |
| `NFX_REPORTS` | Folder for Nextflow HTML run reports (under `nextflow/` by default). |
| `ENV_PROFILE` | Nextflow profile (`local` or `slurm`). Profiles extend `nextflow/nextflow.config` and optionally `slurm.conf`. |
| `CONDA_ENV` | Path to a conda environment (YAML or `.env`) used by all processes. Leave empty to skip conda. |
| `WORKERS` / `CPUS` | Number of concurrent workers and ALPACA threads per worker. Workers map to separate Nextflow processes; adjust HPC queue requests accordingly. |
| `SEGMENTS_PER_CLAIM` | How many segment CSVs each worker requests per ALPACA invocation (batching reduces overhead). |
| `MAX_IDLE_SECONDS` | Worker exit timeout when no new queue entries arrive. |
| `DISPATCHER_POLL_INTERVAL_SECONDS` / `DISPATCHER_MAX_IDLE_CYCLES` | Controls how often the dispatcher checks the pool and when it emits `dispatcher.done`. |
| `RESTART` | Set to `1` to purge dispatcher/worker tokens and rebuild the pool before starting (use when re-running failed cohorts). |
| `DEBUG` | Keep `alpaca-work` contents if non-zero; otherwise the cleanup step removes intermediates after validation. |
| `DELETE_REPORTS` | If `1`, per-segment report JSON/CSVs are deleted after consolidation to save space. |
| `ALPACA_ARGS` | Extra flags forwarded untouched to the ALPACA CLI inside each worker (wrap multi-arg strings in quotes). |

## Process outline
- `preparePool` builds per-segment CSVs from tumour inputs (using `create_symlink_pool.py`), skips already completed segments, and emits the worker pool list.
- `runDispatcher` continuously moves CSV symlinks from the pool into per-worker queues based on `segments_per_claim` until the pool stays empty for `dispatcher_max_idle_cycles`.
- `workerTask` executes `segment_worker.py`, which repeatedly consumes queued segments, runs the ALPACA CLI (optionally batched per tumour), and triages results into `done` or `failed` directories while persisting verbose JSON logs and heartbeat files.
- `mergeSegments` concatenates all per-segment ALPACA outputs into `all_tumours_combined.csv` and asserts full coverage against the cohort inputs.
- `analysis` splits the cohort file back into tumour-level outputs, runs `alpaca ancestor-delta` and `alpaca ccd`, and aggregates CCD metrics (`combine_ccd_results.py`).
- `summariseReports` collects CI, monoclonal, elbow, and run-gap reports, writing consolidated CSVs into both `outputs_dir` and the final report folder.
- `validateResults` compares the expected vs merged segment lists to detect any missing work before downstream cleanup.
- `cleanup` copies merged artefacts to `output_dir/cohort_results` and removes `alpaca_work` unless debug mode is active.

## Outputs
- `${ALPACA_WORK}/outputs/segment_outputs` – raw per-segment ALPACA CSVs plus worker logs, heartbeats, and dispatcher/worker tokens.
- `${OUTPUT_DIR}/reports` – consolidated CI/monoclonal/elbow/run-gap CSVs plus a copy of the env file used for the run.
- `${OUTPUT_DIR}/tumour_results` – per-tumour ALPACA outputs (split from the merged cohort CSV).
- `${OUTPUT_DIR}/cohort_results` – `all_tumours_combined.csv`, CCD cohort summary, and merged artefacts copied post-validation.
- `nextflow/reports/report_<timestamp>.html` – standard Nextflow execution report captured by `run_nextflow.sh`.

## Monitoring & recovery
- Worker progress can be tracked via `${ALPACA_WORK}/outputs/worker_*.done` tokens and JSON logs under `worker_logs/`.
- Restarting a failed run: set `RESTART=1` to clear dispatcher/worker tokens and rebuild the pool without deleting successfully completed segments.
- For quick triage use `scripts/analyse_failed_run_helper.sh`, which summarises counts in `done`, `failed`, `in_progress`, and `pool` along with representative file paths.

## Troubleshooting tips
- If validation fails, inspect `missing_segments.txt` emitted by `validateResults` and re-run with `RESTART=1` after fixing the underlying issue.
- Ensure the ALPACA CLI plus pandas are importable inside the configured environment; `segment_worker.py` shells out via `python -m alpaca.__main__ run`.
- When running on SLURM, adjust `slurm.conf` (memory/time per label) and confirm `$NXF_OPTS` contains the correct `-work-dir` and cache paths if needed.
- Use `scripts/analyse_failed_run_helper.sh` to identify and troubleshoot failed workers. 

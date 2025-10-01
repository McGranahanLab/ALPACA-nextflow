#!/usr/bin/env bash
# Run Nextflow with repository-local work/cache directories under nextflow/
# Usage: ./nextflow/run_nextflow.sh [nextflow args]

set -euo pipefail

RUN_CONFIG=$1

THIS_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd "$THIS_SCRIPT_DIR/.." >/dev/null 2>&1 && pwd -P)"
cd $REPO_ROOT || exit 1
# load user config if present
if [ -f "$REPO_ROOT/nextflow/$RUN_CONFIG" ]; then
	# shellcheck disable=SC1090
	source "$REPO_ROOT/nextflow/$RUN_CONFIG"
fi

mkdir -p "$ALPACA_WORK" "$POOL_DIR" "$IN_PROGRESS_DIR" "$DONE_DIR" "$FAILED_DIR" "$OUTPUTS_DIR" "$NFX_REPORTS"

SCRIPT_DIR="$(realpath "${SCRIPT_DIR:-$REPO_ROOT}")"

POOL_DIR="$(realpath "${POOL_DIR:-$REPO_ROOT/dev/multi-tumour/pool}")"
IN_PROGRESS_DIR="$(realpath "${IN_PROGRESS_DIR:-$REPO_ROOT/dev/multi-tumour/in_progress}")"
DONE_DIR="$(realpath "${DONE_DIR:-$REPO_ROOT/dev/multi-tumour/done}")"
FAILED_DIR="$(realpath "${FAILED_DIR:-$REPO_ROOT/dev/multi-tumour/failed}")"
OUTPUTS_DIR="$(realpath "${OUTPUTS_DIR:-$REPO_ROOT/dev/multi-tumour/outputs}")"
COHORT_DIR="$(realpath "${COHORT_DIR:-$REPO_ROOT/dev/multi-tumour/cohort}")"
ALPACA_WORK="$(realpath "${ALPACA_WORK:-$REPO_ROOT/dev/alpaca-work}")"
NFX_REPORTS="$(realpath "${NFX_REPORTS:-$REPO_ROOT/nextflow/reports}")"

# build args from env settings
timestamp=$(date +%Y%m%d_%H%M%S)
NXF_ARGS=( run main.nf -profile "${ENV_PROFILE:-local}" )
NXF_ARGS+=( --alpaca_work_dir "${ALPACA_WORK}" )
NXF_ARGS+=( --pool_dir "${POOL_DIR}" )
NXF_ARGS+=( --cohort_dir "${COHORT_DIR}" )
NXF_ARGS+=( --in_progress_dir "${IN_PROGRESS_DIR}" )
NXF_ARGS+=( --done_dir "${DONE_DIR}" )
NXF_ARGS+=( --failed_dir "${FAILED_DIR}" )
NXF_ARGS+=( --outputs_dir "${OUTPUTS_DIR}" )
NXF_ARGS+=( --script_dir "${SCRIPT_DIR}" )
NXF_ARGS+=( --workers "${WORKERS:-4}" )
NXF_ARGS+=( --cpus "${CPUS:-1}" )
NXF_ARGS+=( --debug "${DEBUG:-0}" )
NXF_ARGS+=( --conda_env "${CONDA_ENV:-}" )
NXF_ARGS+=( --segments_per_claim "${SEGMENTS_PER_CLAIM:-1}" )
NXF_ARGS+=( --worker_logs "${WORKER_LOGS:-0}" )
NXF_ARGS+=( -with-report "${NFX_REPORTS}/report_${timestamp}.html" )
if [ -n "${ALPACA_ARGS:-}" ]; then
	NXF_ARGS+=( --alpaca_args "${ALPACA_ARGS}" )
fi

for a in "$@"; do
	NXF_ARGS+=( "$a" )
done

echo executing nextflow "${NXF_ARGS[@]}"
# persist configuration used for this run
mkdir -p "$COHORT_DIR/output/reports"
cp "$REPO_ROOT/nextflow/$RUN_CONFIG" "$COHORT_DIR/output/reports/used_config_${timestamp}.env"

pushd "$REPO_ROOT/nextflow" >/dev/null
nextflow "${NXF_ARGS[@]}"
EXIT_CODE=$?
popd >/dev/null
exit $EXIT_CODE

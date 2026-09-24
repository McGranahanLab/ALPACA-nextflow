#!/usr/bin/env bash
# Run Nextflow with repository-local work/cache directories under nextflow/
# Usage: ./nextflow/run_nextflow.sh [config-path] [extra nextflow args...]

set -euo pipefail

RUN_CONFIG="${1:-}"

if [[ -z "$RUN_CONFIG" ]]; then
	echo "Usage: $0 [config-path] [extra nextflow args...]" >&2
	exit 2
fi

THIS_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd "$THIS_SCRIPT_DIR/.." >/dev/null 2>&1 && pwd -P)"
cd "$REPO_ROOT" || exit 1

# Resolve config path:
# - absolute path
# - path relative to current repo root
# - bare filename under nextflow/
if [[ -f "$RUN_CONFIG" ]]; then
	RUN_CONFIG_PATH="$(realpath "$RUN_CONFIG")"
elif [[ -f "$REPO_ROOT/$RUN_CONFIG" ]]; then
	RUN_CONFIG_PATH="$(realpath "$REPO_ROOT/$RUN_CONFIG")"
elif [[ -f "$REPO_ROOT/nextflow/$RUN_CONFIG" ]]; then
	RUN_CONFIG_PATH="$(realpath "$REPO_ROOT/nextflow/$RUN_CONFIG")"
else
	echo "Config file not found: $RUN_CONFIG" >&2
	exit 2
fi

# shellcheck disable=SC1090
source "$RUN_CONFIG_PATH"

# Normalize Gurobi license path for container bind/use.
if [[ -n "${GUROBI_LICENSE_FILE:-}" ]]; then
	case "$GUROBI_LICENSE_FILE" in
		~/*)
			GUROBI_LICENSE_FILE="$HOME/${GUROBI_LICENSE_FILE#~/}"
			;;
	esac

	if [[ ! -f "$GUROBI_LICENSE_FILE" ]]; then
		echo "Gurobi license file not found: $GUROBI_LICENSE_FILE" >&2
		exit 2
	fi

	GUROBI_LICENSE_FILE="$(realpath "$GUROBI_LICENSE_FILE")"
fi

for required_var in ALPACA_WORK INPUT_DIR OUTPUT_DIR NFX_REPORTS; do
	if [[ -z "${!required_var:-}" ]]; then
		echo "Required variable '$required_var' is missing in $RUN_CONFIG_PATH" >&2
		exit 2
	fi
done

# Pool and intermediate directories (relative to repo root or absolute)
POOL_DIR="$ALPACA_WORK/pool"
IN_PROGRESS_DIR="$ALPACA_WORK/in_progress"
DONE_DIR="$ALPACA_WORK/done"
FAILED_DIR="$ALPACA_WORK/failed"
WORK_OUTPUTS_DIR="$ALPACA_WORK/outputs"
SCRIPT_DIR="scripts"

mkdir -p "$ALPACA_WORK" "$POOL_DIR" "$IN_PROGRESS_DIR" "$DONE_DIR" "$FAILED_DIR" "$WORK_OUTPUTS_DIR" "$NFX_REPORTS"

SCRIPT_DIR="$(realpath "${SCRIPT_DIR:-$REPO_ROOT}")"

POOL_DIR="$(realpath "${POOL_DIR}")"
IN_PROGRESS_DIR="$(realpath "${IN_PROGRESS_DIR}")"
DONE_DIR="$(realpath "${DONE_DIR}")"
FAILED_DIR="$(realpath "${FAILED_DIR}")"
WORK_OUTPUTS_DIR="$(realpath "${WORK_OUTPUTS_DIR}")"
INPUT_DIR="$(realpath "${INPUT_DIR}")"
OUTPUT_DIR="$(realpath "${OUTPUT_DIR}")"
ALPACA_WORK="$(realpath "${ALPACA_WORK}")"
NFX_REPORTS="$(realpath "${NFX_REPORTS}")"

# build args from env settings
timestamp=$(date +%Y%m%d_%H%M%S)
NXF_ARGS=( run main.nf -profile "${ENV_PROFILE:-local}" )
NXF_ARGS+=( --alpaca_work_dir "${ALPACA_WORK}" )
NXF_ARGS+=( --pool_dir "${POOL_DIR}" )
NXF_ARGS+=( --input_dir "${INPUT_DIR}" )
NXF_ARGS+=( --output_dir "${OUTPUT_DIR}" )
NXF_ARGS+=( --in_progress_dir "${IN_PROGRESS_DIR}" )
NXF_ARGS+=( --done_dir "${DONE_DIR}" )
NXF_ARGS+=( --failed_dir "${FAILED_DIR}" )
NXF_ARGS+=( --outputs_dir "${WORK_OUTPUTS_DIR}" )
NXF_ARGS+=( --script_dir "${SCRIPT_DIR}" )
NXF_ARGS+=( --workers "${WORKERS:-4}" )
NXF_ARGS+=( --cpus "${CPUS:-1}" )
NXF_ARGS+=( --debug "${DEBUG:-0}" )
NXF_ARGS+=( --use_container "${USE_CONTAINER:-0}" )
NXF_ARGS+=( --alpaca_container "${ALPACA_CONTAINER:-docker://wlippa/alpaca:1.0}" )
NXF_ARGS+=( --gurobi_license_file "${GUROBI_LICENSE_FILE:-}" )
NXF_ARGS+=( --segments_per_claim "${SEGMENTS_PER_CLAIM:-1}" )
NXF_ARGS+=( --worker_logs "${WORKER_LOGS:-0}" )
NXF_ARGS+=( --dispatcher_poll_interval_seconds "${DISPATCHER_POLL_INTERVAL_SECONDS:-${DISPATCHER_POLL_INTERVAL:-1}}" )
NXF_ARGS+=( --dispatcher_max_idle_cycles "${DISPATCHER_MAX_IDLE_CYCLES:-${DISPATCHER_MAX_IDLE:-30}}" )
NXF_ARGS+=( -with-report "${NFX_REPORTS}/report_${timestamp}.html" )
NXF_ARGS+=( --max_idle_seconds "${MAX_IDLE_SECONDS:-600}" )
NXF_ARGS+=( --delete_reports "${DELETE_REPORTS:-0}" )
NXF_ARGS+=( --restart "${RESTART:-0}" )

if [[ -n "${RESTRICT_TO_TUMOURS:-}" ]]; then
	NXF_ARGS+=( --restrict_to_tumours "${RESTRICT_TO_TUMOURS}" )
fi
if [[ -n "${RESTRICT_TO_SEGMENTS:-}" ]]; then
	NXF_ARGS+=( --restrict_to_segments "${RESTRICT_TO_SEGMENTS}" )
fi

PROFILE_CONF_PATH="$REPO_ROOT/nextflow/${ENV_PROFILE:-local}.conf"
if [ -f "$PROFILE_CONF_PATH" ]; then
	NXF_ARGS+=( --profile_config "$PROFILE_CONF_PATH" )
else
	NXF_ARGS+=( --profile_config "" )
fi
NXF_ARGS+=( --env_profile "${ENV_PROFILE:-local}" )

if [ -n "${ALPACA_ARGS:-}" ]; then
	NXF_ARGS+=( "--alpaca_args=${ALPACA_ARGS}" )
fi


for a in "${@:2}"; do
	NXF_ARGS+=( "$a" )
done

echo executing nextflow "${NXF_ARGS[@]}"
# persist configuration used for this run
mkdir -p "$OUTPUT_DIR/reports"
cp "$RUN_CONFIG_PATH" "$OUTPUT_DIR/reports/used_config_${timestamp}.env"

pushd "$REPO_ROOT/nextflow" >/dev/null
nextflow "${NXF_ARGS[@]}"
EXIT_CODE=$?
popd >/dev/null
exit $EXIT_CODE

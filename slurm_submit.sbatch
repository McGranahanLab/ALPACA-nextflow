#!/bin/bash
#SBATCH --job-name=nf_alpaca
#SBATCH --output=alpaca_slurm.out
#SBATCH --error=alpaca_slurm.err
#SBATCH --time=72:00:00
#SBATCH --mem=4G
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1



SCRIPT_PATH="${BASH_SOURCE[0]}"

if [[ -n "${SLURM_JOB_ID:-}" ]] && command -v scontrol >/dev/null 2>&1; then
	SLURM_COMMAND="$(scontrol show job "$SLURM_JOB_ID" -o | sed -n 's/.* Command=\([^ ]*\).*/\1/p')"
	if [[ -n "$SLURM_COMMAND" ]]; then
		if [[ "$SLURM_COMMAND" = /* ]]; then
			SCRIPT_PATH="$SLURM_COMMAND"
		elif [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
			SCRIPT_PATH="$SLURM_SUBMIT_DIR/$SLURM_COMMAND"
		fi
	fi
fi

if [[ "$SCRIPT_PATH" = /* ]]; then
	REPO_ROOT="$(cd -- "$(dirname -- "$SCRIPT_PATH")" >/dev/null 2>&1 && pwd -P)"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
	REPO_ROOT="$(cd -- "$SLURM_SUBMIT_DIR" >/dev/null 2>&1 && pwd -P)"
else
	REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
fi

echo "Repo Root set to: $REPO_ROOT"

ml Nextflow
ml Singularity/3.6.4
echo "Modules loaded"
bash "$REPO_ROOT/nextflow/run_nextflow.sh" "$REPO_ROOT/nextflow/pipeline.env"

#!/bin/bash
#SBATCH --job-name=nf_alpaca
#SBATCH --output=alpaca_slurm.out
#SBATCH --error=alpaca_slurm.err
#SBATCH --time=72:00:00
#SBATCH --mem=4G
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1


# Absolute path to the directory containing this sbatch file.
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ml Nextflow
ml Singularity/3.6.4
bash "$REPO_ROOT/nextflow/run_nextflow.sh" "$REPO_ROOT/pipeline.env"
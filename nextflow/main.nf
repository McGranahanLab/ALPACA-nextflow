#!/usr/bin/env nextflow

// Pipeline parameters are defined in nextflow/pipeline.env
// Minimal fallbacks used below when necessary by the pipeline.
params.workers = params.workers ?: 4
params.cpus = params.cpus ?: 1
params.alpaca_args = params.alpaca_args ?: ''
params.worker_logs = params.worker_logs ?: 0
params.segments_per_claim = params.segments_per_claim ?: System.getenv('SEGMENTS_PER_CLAIM')?.toInteger() ?: 1
workflow {
    // Prepare the lightweight symlink pool once before starting workers - workers will claimed unsolved segments from here
    def pool = preparePool()
    def worker_input = pool.flatMap { segfile ->
    (1..params.workers).collect { wid -> [wid, segfile] }}
    // run workers in parallel
    def worker_done_ch = workerTask(worker_input)
    def all_workers_done = worker_done_ch.collect()
    // run merge after all workers finished
    def merge = mergeSegments(all_workers_done)
    // validate results: collect the prepared segments list (pool.out) and merged segments list (merge.out) and compare
    def expected_ch = pool.collect()
    def actual_ch = merge.collect()
    def (missing_ch, validation_token_ch) = validateResults(expected_ch, actual_ch)
    missing_ch.subscribe { println "validation missing file: ${it}" }
    // run cleanup only if validation emits a done token
    cleanup(validation_token_ch)
}


process workerTask {
    conda params.conda_env
    label 'worker_high'
    tag "$worker_id"
    cpus params.cpus
    input:
    tuple val(worker_id), file(segments_list)

    output:
    file 'worker_*.done'

    script:
    """
        python3 ${params.script_dir}/segment_worker.py \
            --cohort_dir ${params.cohort_dir} \
            --pool-dir ${params.pool_dir} \
            --in-progress-dir ${params.in_progress_dir} \
            --worker-id ${worker_id} \
            --done-dir ${params.done_dir} \
            --failed-dir ${params.failed_dir} \
            --outputs-dir ${params.outputs_dir} \
            --cpus ${task.cpus} \
            --segments-per-claim ${params.segments_per_claim} \
            --log-level ${params.worker_logs} \
                --alpaca-args '${params.alpaca_args}'
    # emit a simple local done token file so the workflow knows this worker finished
    echo "DONE ${worker_id}" > worker_${worker_id}.done
    hostname >> worker_${worker_id}.done
    # copy the token into the shared outputs dir for external visibility
    mkdir -p ${params.outputs_dir}
    cp worker_${worker_id}.done ${params.outputs_dir}/worker_${worker_id}.done || true
    """
}

process preparePool {
    // Use runtime-provided conda env if set
    conda params.conda_env
    label 'low'
    tag 'preparePool'
    cpus params.cpus

    output:
    file 'segments_to_process.txt'

    script:
    """
    python3 ${params.script_dir}/create_symlink_pool.py \
            --cohort_dir ${params.cohort_dir} \
            --pool_dir ${params.pool_dir}
        mkdir -p ${params.outputs_dir}
        # If there are leftover files in in_progress (previous failed run), move them back to the pool
        if [ -d "${params.in_progress_dir}" ]; then
            for fpath in \$(find "${params.in_progress_dir}" -type f -name '*.csv' -print); do
                bn=\$(basename "\$fpath")
                # move back to pool if not already present
                if [ ! -e "${params.pool_dir}/\$bn" ]; then
                    mkdir -p "${params.pool_dir}"
                    mv -n "\$fpath" "${params.pool_dir}/\$bn" || true
                else
                    # remove the leftover file to avoid duplicates
                    rm -f "\$fpath" || true
                fi
            done || true
        fi

        # Remove any pool entries already present in done_dir (skip reprocessing)
        if [ -d "${params.done_dir}" ]; then
            for ppath in "${params.pool_dir}"/*; do
                [ -e "\$ppath" ] || continue
                bn=\$(basename "\$ppath")
                # search done_dir recursively for a matching basename
                if find "${params.done_dir}" -type f -name "\$bn" -print -quit | grep -q .; then
                    rm -f "\$ppath" || true
                fi
            done || true
        fi

        # create a canonical list of segments the workers should process
        ls -1 ${params.pool_dir} | sort > segments_to_process.txt
        echo ready > pool_ready.txt
        echo ready > ${params.outputs_dir}/pool_ready.txt
    """

}

process mergeSegments {
    // Use runtime-provided conda env if set
    conda params.conda_env
    label 'low'
    tag 'mergeSegments'
    cpus params.cpus

    input:
    val(done_list)

    output:
    file 'merged_segments.txt'

    script:
    """
    mkdir -p ${params.outputs_dir}/merged
    python ${params.script_dir}/merge_segments.py \
            --segments-dir ${params.outputs_dir} \
            --out ${params.outputs_dir}/merged/all_tumours_combined.csv

    # emit a list of merged segment files (based on outputs_dir contents), portable
    ls -1 ${params.outputs_dir}/*.csv 2>/dev/null | xargs -n1 basename | sort > merged_segments.txt || true
    """
}

process validateResults {
    // Use runtime-provided conda env if set
    conda params.conda_env
    label 'low'
    tag 'validateResults'
    cpus params.cpus

    input:
    file expected_list
    file actual_list

    output:
    file 'missing_segments.txt'
    file 'validation_done.token'

    script:
    """
    python ${params.script_dir}/validate_results.py \
            ${expected_list} ${actual_list} || true


    if [ ! -f missing_segments.txt ]; then
        touch missing_segments.txt
    fi
    if [ -s missing_segments.txt ]; then
        echo failed > validation_done.token
    else
        echo done > validation_done.token
    fi
    """
}

process cleanup {
    // Use runtime-provided conda env if set
    conda params.conda_env
    label 'low'
    tag 'cleanup'
    cpus params.cpus

    input:
    file 'validation_done.token'

    script:
    """
        set -e
        if [ "\$(cat validation_done.token 2>/dev/null)" != "done" ]; then
            echo "validation failed, not running cleanup" > cleanup_failed.token
            exit 2
        fi
        mkdir -p ${params.cohort_dir}/output/cohort_results
        cp -r ${params.outputs_dir}/merged/* ${params.cohort_dir}/output/cohort_results/ || true
        # remove alpaca_work directory if it exists and debug mode is not set:
        if [ "${params.debug}" != "0" ] && [ "${params.debug}" != "false" ]; then
            echo "debug mode is on, not removing alpaca_work directory" > debug_mode.token
        else
            rm -rf ${params.alpaca_work_dir} || true
        fi
        echo done > cleanup_done.token
    """
    output:
    file 'cleanup_done.token'
}

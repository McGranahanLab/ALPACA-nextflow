#!/usr/bin/env nextflow

// Pipeline parameters are defined in nextflow/pipeline.env

// Print params to the console at pipeline startup
println "=== Pipeline params ==="
params.sort().each { k, v -> println "${k} = ${v}" }
workflow {
    // Prepare the lightweight symlink pool once before starting workers - workers will claimed unsolved segments from here
    def pool = preparePool()
    // once pool is ready, start the dispatcher; it monitors workers progress and tops up each workers queue
    runDispatcher(pool)
    def worker_input = pool.flatMap { segfile ->
    (1..params.workers).collect { wid -> [wid, segfile] }}
    // run workers in parallel
    def worker_done_ch = workerTask(worker_input)
    def all_workers_done = worker_done_ch.collect()
    // run merge after all workers finished
    def merge = mergeSegments(all_workers_done)
    // run the analysis:
    def analysis_token = analysis(merge)
    // once workers are done, run report summariser
    summariseReports(all_workers_done)
    // validate results: collect the prepared segments list (pool.out) and merged segments list (merge.out) and compare
    def expected_ch = pool.collect()
    def actual_ch = merge.collect()
    def (missing_ch, validation_token_ch) = validateResults(expected_ch, actual_ch)
    //missing_ch.subscribe { println "validation missing file: ${it}" }
    // run cleanup only if validation emits a done token
    cleanup(validation_token_ch,analysis_token)
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
            --input_dir ${params.input_dir} \
            --in-progress-dir ${params.in_progress_dir} \
            --worker-id ${worker_id} \
            --done-dir ${params.done_dir} \
            --failed-dir ${params.failed_dir} \
            --outputs-dir ${params.outputs_dir} \
            --cpus ${task.cpus} \
            --segments-per-claim ${params.segments_per_claim} \
            --max-idle-seconds ${params.max_idle_seconds} \
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


process runDispatcher {
    conda params.conda_env
    label 'worker_high'
    tag 'dispatcher'

    input:
    file pool_ready_token

    output:
    file 'dispatcher.done'

    script:
    """
    python3 ${params.script_dir}/dispatcher.py \
        --pool-dir ${params.pool_dir} \
        --in-progress-dir ${params.in_progress_dir} \
        --workers ${params.workers} \
        --segments-per-claim ${params.segments_per_claim} \
        --poll-interval-seconds ${params.dispatcher_poll_interval_seconds} \
        --max-idle-cycles ${params.dispatcher_max_idle_cycles}
    cp dispatcher.done ${params.outputs_dir}/dispatcher.done || true
    """
}


process preparePool {
    conda params.conda_env
    label 'low'
    tag 'preparePool'

    output:
    file 'segments_to_process.txt'

    script:
    """
    mkdir -p ${params.outputs_dir}

    # If restart requested, clear tokens and remove failed/in_progress before building pool
    if [ "${params.restart}" = "1" ]; then
        echo "Restart requested: clearing dispatcher and worker tokens and removing failed/in_progress directories"
        rm -f ${params.outputs_dir}/dispatcher.done || true
        rm -f ${params.outputs_dir}/worker_*.done || true
        # optional: archive old failed/in_progress (skipped here for speed)
        rm -rf ${params.failed_dir} || true
        rm -rf ${params.in_progress_dir} || true
        mkdir -p ${params.failed_dir} ${params.in_progress_dir}
    fi

    # Create the pool but skip files already present in done_dir to avoid reprocessing
    python3 ${params.script_dir}/create_symlink_pool.py \
        --input_dir ${params.input_dir} \
        --done_dir ${params.done_dir} \
        --pool_dir ${params.pool_dir}

    # create the list of segments the workers should process
    ls -1 ${params.pool_dir} | sort > segments_to_process.txt
    echo ready > pool_ready.done
    echo ready > ${params.outputs_dir}/pool_ready.done
    """

}

process mergeSegments {
    conda params.conda_env
    label 'low'
    tag 'mergeSegments'

    input:
    val(done_list)

    output:
    file 'merged_segments.txt'

    script:
    """
    mkdir -p ${params.outputs_dir}/merged
    python ${params.script_dir}/merge_segments.py \
            --segments-dir ${params.outputs_dir}/segment_outputs \
            --out ${params.outputs_dir}/merged/all_tumours_combined.csv \
            --input-dir ${params.input_dir}

    """
}



process validateResults {
    conda params.conda_env
    label 'low'
    tag 'validateResults'

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

process summariseReports {
    conda params.conda_env
    label 'low'
    tag 'summariseReports'

    input:
    val(done_list)

    output:
    file 'ci_modified_report.csv'

    script:
    """
    mkdir -p ${params.outputs_dir}/reports
    python ${params.script_dir}/summarise_reports.py ${params.outputs_dir}/segment_outputs ${params.delete_reports.toInteger() ? '--delete' : ''}
    # copy to alpaca-work for debugging
    cp ci_modified_report.csv ${params.outputs_dir}/reports/ci_modified_report.csv || true
    cp monoclonal_samples_report.csv ${params.outputs_dir}/reports/monoclonal_samples_report.csv || true
    cp elbow_increase_report.csv ${params.outputs_dir}/reports/elbow_increase_report.csv || true
    # copy to user-specified output directory
    mkdir -p ${params.output_dir}/reports
    cp ci_modified_report.csv ${params.output_dir}/reports/ci_modified_report.csv || true
    cp monoclonal_samples_report.csv ${params.output_dir}/reports/monoclonal_samples_report.csv || true
    cp elbow_increase_report.csv ${params.output_dir}/reports/elbow_increase_report.csv || true
    """
}

process cleanup {
    conda params.conda_env
    label 'low'
    tag 'cleanup'

    input:
    file 'validation_done.token'
    file 'analysis_done.token'

    script:
    """
        set -e
        if [ "\$(cat validation_done.token 2>/dev/null)" != "done" ]; then
            echo "validation failed, not running cleanup" > cleanup_failed.token
            exit 2
        fi
        mkdir -p ${params.output_dir}/cohort_results
        cp -r ${params.outputs_dir}/merged/* ${params.output_dir}/cohort_results/ || true
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

process analysis {
    conda params.conda_env
    label 'low'
    tag 'analysis'

    input:
    file 'merged_segments.txt' // start analysis after merging is done

    script:
    """
    # split combined cohort file into per-tumour files:
    tumour_results=${params.output_dir}/tumour_results
    mkdir -p \${tumour_results}
    python3 ${params.script_dir}/split_cohort_to_tumours.py \
        --combined_file ${params.outputs_dir}/merged/all_tumours_combined.csv \
        --tumour_results \${tumour_results}

    for tumour_dir in "\$tumour_results"/*; do
        tumour_id=\$(basename \${tumour_dir})
        input_tumour_directory=${params.input_dir}/\${tumour_id}
        # get cn change to ancestor:
        alpaca ancestor-delta \
            --output_directory "\${tumour_dir}" \
            --tumour_df_path "\${tumour_dir}/ALPACA_output_\${tumour_id}.csv" \
            --tree_path "\${input_tumour_directory}/tree_paths.json"

        # calculate clone copy number diversity:
        alpaca ccd \
            --output_directory "\${tumour_dir}" \
            --alpaca_output_path "\${tumour_dir}/ALPACA_output_\${tumour_id}.csv"
    done
    
    # combine CCD tables into a single cohort-level table:
    mkdir -p ${params.output_dir}/cohort_results
    python3 ${params.script_dir}/combine_ccd_results.py \
        --tumour_results_dir "\$tumour_results" \
        --output_path "${params.output_dir}/cohort_results/cohort_ccd_results.csv"

    echo done > analysis_done.token
    """
    output:
    file 'analysis_done.token'

}
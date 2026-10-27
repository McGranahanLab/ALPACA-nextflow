#!/usr/bin/env nextflow

// Pipeline parameters are defined in nextflow/pipeline.env

// Print params to the console at pipeline startup
println "=== Pipeline params ==="
params.sort().each { k, v -> println "${k} = ${v}" }
def use_container = params.use_container in [true, 1, '1', 'true', 'TRUE', 'yes', 'YES', 'on', 'ON']

// Gurobi is used either directly (--solver gurobi) or through Pyomo (--solver pyomo --pyomo_solver gurobi*).
// Defaults mirror ALPACA's argument parser: --solver pyomo, --pyomo_solver scip.
def alpaca_args = (params.alpaca_args ?: '').toString()
def alpaca_arg_value = { String name, String dflt ->
    def m = alpaca_args =~ /(?:^|\s)--${name}(?:=|\s+)["']?([^\s"']+)/
    m.find() ? m.group(1) : dflt
}
def alpaca_solver = alpaca_arg_value('solver', 'pyomo').toLowerCase()
def alpaca_pyomo_solver = alpaca_arg_value('pyomo_solver', 'scip').toLowerCase()
def uses_gurobi = alpaca_solver == 'gurobi' || (alpaca_solver == 'pyomo' && alpaca_pyomo_solver.startsWith('gurobi'))
def solver_description = alpaca_solver == 'gurobi' ? 'gurobi (Gurobi API)' : "pyomo (${alpaca_pyomo_solver})"
// written next to used_config_*.env (see run_nextflow.sh)
def licence_error_file = file("${params.output_dir}/reports/LICENCE_ERROR.txt")

workflow {
    // If Gurobi is the solver, make sure a licence can be acquired before doing any work
    def licence_ok
    if (uses_gurobi) {
        licence_ok = checkGurobiLicence().map { status, check_log ->
            if (status == 'OK') {
                licence_error_file.delete()  // drop a stale error file from a previous run
                return 'OK'
            }
            def check_output = check_log.text.trim()
            def message = """\
                |GUROBI LICENCE CHECK FAILED - pipeline aborted before processing any segments.
                |
                |Time:                ${new Date().format('yyyy-MM-dd HH:mm:ss')}
                |Solver:              ${solver_description}
                |ALPACA_ARGS:         ${alpaca_args}
                |GUROBI_LICENSE_FILE: ${params.gurobi_license_file ?: '(not set)'}
                |Use container:       ${use_container}
                |Check work dir:      ${check_log.parent}
                |
                |Output of check_gurobi_licence_server.py:
                |${check_output}
                |
                |Check that the licence server is reachable from the compute nodes, that the licence
                |file in pipeline.env (GUROBI_LICENSE_FILE) is valid, and that no other jobs hold all
                |available licence tokens. Alternatively switch solver in ALPACA_ARGS (e.g. --solver pyomo --pyomo_solver scip).
                |""".stripMargin()
            licence_error_file.parent.mkdirs()
            licence_error_file.text = message
            def banner = '!' * 100
            System.err.println("\n${banner}\n${banner}\n\n${message}\nDetails written to: ${licence_error_file}\n\n${banner}\n${banner}\n")
            error("Gurobi licence not available - see ${licence_error_file}")
        }
    } else {
        licence_ok = Channel.value('not_needed')
    }
    // Prepare the lightweight symlink pool once before starting workers - workers will claimed unsolved segments from here
    def pool = preparePool(licence_ok)
    // once pool is ready, start the dispatcher; it monitors workers progress and tops up each workers queue
    runDispatcher(pool)
    def worker_input = pool.flatMap { segfile ->
    (1..params.workers).collect { wid -> [wid, segfile] }}
    // run workers in parallel
    def worker_done_ch = workerTask(worker_input)
    def all_workers_done = worker_done_ch.collect()
    // run merge after all workers finished
    def merge = mergeSegments(all_workers_done)
    // once workers are done, run report summariser
    def summary = summariseReports(all_workers_done)
    // run the analysis (needs the summarised edge case reports for the warning files):
    def analysis_token = analysis(merge, summary)
    // validate results: collect the prepared segments list (pool.out) and merged segments list (merge.out) and compare
    def expected_ch = pool.collect()
    def actual_ch = merge.collect()
    def (missing_ch, validation_token_ch, all_solutions_report_ch) = validateResults(expected_ch, actual_ch)
    //missing_ch.subscribe { println "validation missing file: ${it}" }
    // run cleanup only if validation emits a done token
    cleanup(validation_token_ch,analysis_token)
}


process workerTask {
    container use_container ? params.alpaca_container : null
    label 'worker_high'
    tag "$worker_id"
    cpus params.cpus
    input:
    tuple val(worker_id), file(segments_list)

    output:
    file 'worker_*.done'

    script:
    """
        python ${params.script_dir}/segment_worker.py \
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
    container use_container ? params.alpaca_container : null
    label 'worker_high'
    tag 'dispatcher'

    input:
    file pool_ready_token

    output:
    file 'dispatcher.done'

    script:
    """
    python ${params.script_dir}/dispatcher.py \
        --pool-dir ${params.pool_dir} \
        --in-progress-dir ${params.in_progress_dir} \
        --workers ${params.workers} \
        --segments-per-claim ${params.segments_per_claim} \
        --poll-interval-seconds ${params.dispatcher_poll_interval_seconds} \
        --max-idle-cycles ${params.dispatcher_max_idle_cycles}
    cp dispatcher.done ${params.outputs_dir}/dispatcher.done || true
    """
}


process checkGurobiLicence {
    container use_container ? params.alpaca_container : null
    label 'low'
    tag 'checkGurobiLicence'

    output:
    tuple env(LICENCE_STATUS), path('licence_check.log')

    script:
    // always exit 0 and report the status instead: slurm.conf sets errorStrategy 'ignore',
    // which would silently swallow a failed check; the workflow aborts on a FAILED status
    """
    if python ${params.script_dir}/check_gurobi_licence_server.py > licence_check.log 2>&1; then
        LICENCE_STATUS=OK
    else
        LICENCE_STATUS=FAILED
        cat licence_check.log >&2
    fi
    """
}

process preparePool {
    container use_container ? params.alpaca_container : null
    label 'low'
    tag 'preparePool'

    input:
    val licence_ok

    output:
    file 'segments_to_process.txt'

    script:
    """
    mkdir -p ${params.outputs_dir}

    # If restart requested, clear tokens and remove failed/in_progress before building pool
    if [ "${params.restart}" = "1" ]; then
        echo "Restart requested: clearing dispatcher and worker tokens and cleaning failed/in_progress directories"
        rm -f ${params.outputs_dir}/dispatcher.done || true
        rm -f ${params.outputs_dir}/worker_*.done || true
        mkdir -p ${params.failed_dir} ${params.in_progress_dir}
        find ${params.failed_dir} -mindepth 1 -maxdepth 1 -exec rm -rf {} + || true
        find ${params.in_progress_dir} -mindepth 1 -maxdepth 1 -exec rm -rf {} + || true
    fi

    # Create the pool but skip files already present in done_dir to avoid reprocessing
    python ${params.script_dir}/create_symlink_pool.py \
        --input_dir ${params.input_dir} \
        --done_dir ${params.done_dir} \
        --pool_dir ${params.pool_dir} \
        --tumours '${params.restrict_to_tumours ?: ''}' \
        --segments '${params.restrict_to_segments ?: ''}'

    # create the list of segments the workers should process
    ls -1 ${params.pool_dir} | sort > segments_to_process.txt
    echo ready > pool_ready.done
    echo ready > ${params.outputs_dir}/pool_ready.done
    """

}

process mergeSegments {
    container use_container ? params.alpaca_container : null
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
            --segments-dir "${params.outputs_dir}/segment_outputs" \
            --out "${params.outputs_dir}/merged/all_tumours_combined.csv" \
            --input-dir "${params.input_dir}" \
            --output-dir "${params.output_dir}" \
            --nextflow-log "${workflow.launchDir}/.nextflow.log" \
            --work-dir "${workflow.workDir}" \
            --outputs-dir "${params.outputs_dir}" \
            --pool-dir "${params.pool_dir}" \
            --in-progress-dir "${params.in_progress_dir}" \
            --done-dir "${params.done_dir}" \
            --failed-dir "${params.failed_dir}" \
            --max-idle-seconds ${params.max_idle_seconds} \
            --segments-per-claim ${params.segments_per_claim} \
            --workers ${params.workers} \
            --worker-cpus ${params.cpus} \
            --worker-label worker_high \
            --profile-config "${params.profile_config ?: ''}" \
            --profile-name "${params.env_profile ?: ''}" \
            --restrict-tumours '${params.restrict_to_tumours ?: ''}' \
            --restrict-segments '${params.restrict_to_segments ?: ''}' \
            --include-single-clone ${params.include_single_clone} \
            --include-single-sample ${params.include_single_sample} \
            --include-single-clone-and-single-sample ${params.include_single_clone_and_single_sample}

    # If ALPACA was run with --output_all_solutions, promote the per-segment
    # all_solutions/ tree from the scratch work-dir to the user's output-dir
    # before cleanup deletes it. No-op when the flag wasn't used.
    python ${params.script_dir}/promote_all_solutions.py \
            --segments-dir "${params.outputs_dir}/segment_outputs" \
            --output-dir "${params.output_dir}"
    """
}



process validateResults {
    container use_container ? params.alpaca_container : null
    label 'low'
    tag 'validateResults'

    input:
    file expected_list
    file actual_list

    output:
    file 'missing_segments.txt'
    file 'validation_done.token'
    file 'all_solutions_validation_report.csv'

    script:
    """
    python ${params.script_dir}/validate_results.py \
            ${expected_list} ${actual_list} \
            --input-dir "${params.input_dir}" \
            --output-dir "${params.output_dir}" \
            --restrict-tumours '${params.restrict_to_tumours ?: ''}' \
            --restrict-segments '${params.restrict_to_segments ?: ''}' || true


    if [ ! -f missing_segments.txt ]; then
        touch missing_segments.txt
    fi
    if [ -s missing_segments.txt ]; then
        echo failed > validation_done.token
    else
        echo done > validation_done.token
    fi
    if [ ! -f all_solutions_validation_report.csv ]; then
        echo "tumour_id,check,details" > all_solutions_validation_report.csv
    fi
    # copy alongside the other run reports
    mkdir -p ${params.outputs_dir}/reports ${params.output_dir}/reports
    cp all_solutions_validation_report.csv ${params.outputs_dir}/reports/all_solutions_validation_report.csv || true
    cp all_solutions_validation_report.csv ${params.output_dir}/reports/all_solutions_validation_report.csv || true
    """
}

process summariseReports {
    container use_container ? params.alpaca_container : null
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
    cp run_gap_summary.csv ${params.outputs_dir}/reports/run_gap_summary.csv || true
    cp edge_case_report.csv ${params.outputs_dir}/reports/edge_case_report.csv || true
    # copy to user-specified output directory
    mkdir -p ${params.output_dir}/reports
    cp ci_modified_report.csv ${params.output_dir}/reports/ci_modified_report.csv || true
    cp monoclonal_samples_report.csv ${params.output_dir}/reports/monoclonal_samples_report.csv || true
    cp elbow_increase_report.csv ${params.output_dir}/reports/elbow_increase_report.csv || true
    cp run_gap_summary.csv ${params.output_dir}/reports/run_gap_summary.csv || true
    cp edge_case_report.csv ${params.output_dir}/reports/edge_case_report.csv || true
    """
}

process cleanup {
    container use_container ? params.alpaca_container : null
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
    container use_container ? params.alpaca_container : null
    label 'low'
    tag 'analysis'

    input:
    file 'merged_segments.txt' // start analysis after merging is done
    file 'ci_modified_report.csv' // and after the segment reports are summarised

    script:
    """
    # split the unfiltered cohort file into per-tumour files; single clone / single sample
    # tumours go to their own directory whether or not they are in all_tumours_combined:
    tumour_results=${params.output_dir}/tumour_results
    edge_case_results=${params.output_dir}/single_clone_or_sample
    mkdir -p \${tumour_results}
    # merge_segments.py writes the unfiltered table in the segment outputs' format (csv or parquet)
    combined_file=\$(ls ${params.outputs_dir}/all_tumours_unfiltered.{parquet,csv,tsv} 2>/dev/null | head -n 1)
    python ${params.script_dir}/split_cohort_to_tumours.py \
        --combined_file "\${combined_file}" \
        --tumour_results \${tumour_results} \
        --classification ${params.outputs_dir}/tumour_classification.csv \
        --edge_case_results \${edge_case_results}

    for tumour_dir in "\$tumour_results"/* "\$edge_case_results"/*; do
        [ -d "\${tumour_dir}" ] || continue
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
        
        # make plots: 
        input_tumour_directory=${params.input_dir}/\${tumour_id}
        HOME=/tmp
        alpaca plot-tumour \
            --input_directory "\${input_tumour_directory}" \
            --output_directory "\${tumour_dir}" \
            --plot_output_mode "${params.plot_output_mode}" \
            --heatmap_palette "magma" \
            --alpaca_cache_dir ${params.alpaca_cache_dir} \
            --genome_build ${params.genome_build} \
            --genome_cache_dir ${params.genome_cache_dir}
    done
    
    # combine CCD tables into a single cohort-level table (excluded and single-clone tumours are skipped):
    mkdir -p ${params.output_dir}/cohort_results
    python ${params.script_dir}/combine_ccd_results.py \
        --tumour_results_dir "\$tumour_results" "\$edge_case_results" \
        --classification ${params.outputs_dir}/tumour_classification.csv \
        --output_path "${params.output_dir}/cohort_results/cohort_ccd_results.csv"

    # per-tumour warning_*.txt files and the cohort-level single clone / single sample report:
    python ${params.script_dir}/report_edge_case_tumours.py \
        --classification ${params.outputs_dir}/tumour_classification.csv \
        --segment_report ${params.outputs_dir}/reports/edge_case_report.csv \
        --input_dir ${params.input_dir} \
        --edge_case_results "\$edge_case_results" \
        --reports_dir ${params.output_dir}/reports

    echo done > analysis_done.token
    """
    output:
    file 'analysis_done.token'

}
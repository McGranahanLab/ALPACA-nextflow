# a few commands to help figure out what went wrong in a failed run


base_dir='..'
alpaca_work=$base_dir/alpaca-work
nextflow_work=$base_dir/nextflow/work

done_dir=$alpaca_work/done
failed_dir=$alpaca_work/failed
in_progress_dir=$alpaca_work/in_progress
pool_dir=$alpaca_work/pool
# count files in each directory:
done_count=$(ls -1q $done_dir/* 2>/dev/null | wc -l)
failed_count=$(ls -1q $failed_dir/* 2>/dev/null | wc -l)
in_progress_count=$(ls -1q $in_progress_dir/* 2>/dev/null | wc -l)
pool_count=$(ls -1q $pool_dir/* 2>/dev/null | wc -l)
# print summary:
echo "Summary of Alpaca work directories:"
echo "-----------------------------------"
echo "Done: $done_count files"
echo "Failed: $failed_count files"
echo "In Progress: $in_progress_count files"
echo "Pool: $pool_count files"
echo "-----------------------------------"

# identify examples of failed segment files:
if [ $failed_count -gt 0 ]; then
    echo "Examples of failed segment files:"
    exf=$(ls -1q $failed_dir/* | head -n 1)
    echo $exf
fi

if [ $in_progress_count -gt 0 ]; then
    echo "Examples of in_progress segment files:"
    exip=$(ls -1q $in_progress_dir/worker*/*/* | head -n 1)
    echo $exip
fi


# set accordingly:
ex=$exip
# find responsible worker:
# find worker directory by 'worker_' pattern:
worker=$(echo $ex | grep -oE 'worker_[0-9]+' | head -n 1)

segment_file=$(basename $ex)

# search nextflow work directory for files containing segment_file string, but exclude all files called segments_to_process.txt
grep -rl $segment_file $nextflow_work | grep -v 'segments_to_process.txt' | while read nf_file; do
    echo "-----------------------------------"
    echo "Nextflow work file: $nf_file"
    echo "-----------------------------------"
    echo "Contents:"
    head -n 20 $nf_file
    echo "-----------------------------------"
done
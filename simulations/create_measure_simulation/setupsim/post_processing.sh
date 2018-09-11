#!/bin/bash
#SBATCH -p short
#SBATCH -t 1:00:00

RESULT_DIR=$1
timestamp=$2
paramsetfile=$3

########################
# Setup stopos pool
# to run post processing
# in parallell.
########################

export STOPOS_POOL=pool_post_proc
stopos create
stopos add $paramsetfile

processes=`sara-get-num-cores`

for ((i=1; i<=processes; i++)); do
(
    while true; do
        # Move to the first item in the pool
        stopos next &> /dev/null

        # Check if there are more items in the pool
        if [ "$STOPOS_RC" != "OK" ]; then
            break
        fi

        # Get the next parameters from the pool
        params=( $STOPOS_VALUE )

        # Extract the parameters
        actual_key=${params[0]} # Key to parameter set in paramcombinations-file
        runindex=${params[1]} # Run index

        # Results file (without file-type)
        result_file="${RESULT_DIR}/${timestamp}_key_${actual_key}_run_${runindex}"

        # Run post processing
        analysis_file="${SIMULATION_DIR}/../analysis_sql_data.py"
        python3 $analysis_file --results-path "${results_file}.db" --no-plot --save-figs --save-output --analysis-folder "${result_file}_analysis"

        # Remove the parameters from the pool
        stopos remove -p $STOPOS_POOL
) &
done
wait

# Zip the results directory
zip -r "${RESULT_DIR}.zip" $RESULT_DIR

#!/bin/bash
#SBATCH -p normal
#SBATCH -t 24:00:00

RESULT_DIR=$1
timestamp=$2
paramsetfile=$3
RUNONCLUSTER=$4
OUTPUTDIRNAME=$5

############################
# Create a tmp CSV file with
# The keys of the paramsets.
# To be used by stopos
# and will be removed.
############################

tmp_key_file="${RESULT_DIR}/tmp_key_file_${SLURM_JOB_ID}.csv"
python3 "${SIMULATION_DIR}/readonly/make_key_pool.py" $paramsetfile $tmp_key_file


########################
# Setup stopos pool
# to run post processing
# in parallell.
########################

if [ "$RUNONCLUSTER" == 'y' ]; then

    export STOPOS_POOL=key_pool
    stopos create
    stopos add $tmp_key_file

    processes=`sara-get-num-cores`
else
    processes=1
fi

#######################
# Start post processing
#######################

echo "Starting post-processing..."

counter=0
for ((i=1; i<=processes; i++)); do
(
    while true; do
        ((counter=counter+1))
        # Check if there are more parameters to simulate
        if [ "$RUNONCLUSTER" == 'y' ]; then
            # Move to the first item in the pool
            stopos next &> /dev/null

            # Check if there are more items in the pool
            if [ "$STOPOS_RC" != "OK" ]; then
                break
            fi

            # Get the next parameters from the pool
            params=( $STOPOS_VALUE )
        else
            # Get the next line of parameters
            line=$(sed "${counter}q;d" $tmp_key_file)
            if [[ -z "$line" ]]; then
                break
            fi
            params=($line)
        fi

        # Extract the parameters
        actual_key=${params[0]} # Key to parameter set in paramcombinations-file

        # Results file (without run-index or file-type)
        filebasename="${RESULT_DIR}/${timestamp}_key_${actual_key}"

        echo "Analysing data from key ${actual_key}"

        # Run post processing
        analysis_file="${SIMULATION_DIR}/post_processing/analyse_single_key.py"
        python3 $analysis_file $filebasename

        if [ "$RUNONCLUSTER" == 'y' ]; then
            # Remove the parameters from the pool
            stopos remove -p $STOPOS_POOL
        fi
    done
) &
done
wait

# Clean up
rm $tmp_key_file

# Move to folder to not include absolute
cd $SIMULATION_DIR

# Zip the results directory
zip -r "${timestamp}_${OUTPUTDIRNAME}.zip" "${timestamp}_${OUTPUTDIRNAME}"

#!/bin/bash
#SBATCH -p staging
#SBATCH -t 1-00:00:00
#SBATCH -J archiving

# NOTES
# https://userinfo.surfsara.nl/systems/cartesius/usage/batch-usage#heading16

# change directory to the temporary directory of the computation job
JOB_ID=$1
TMP_DIR=$2
RESULT_DIR=$3
POST_PROC=$4
timestamp=$5
OUTPUTDIRNAME=$6
paramsetfile=$7
RUNONCLUSTER=$8

# Concatenate output CSV files and move to project space
mkdir -p $RESULT_DIR

mv $TMP_DIR/* $RESULT_DIR

# Move the slurm file
mv "${SIMULATION_DIR}/slurm-${JOB_ID}.out" $RESULT_DIR


# Cleanup
rmdir $TMP_DIR

# Run post-processing
if [ "$POST_PROC" == "y" ]; then
    outfile="${RESULT_DIR}/post_proc_output.out"
    post_proc_file="${SIMULATION_DIR}/setupsim/post_processing.sh"
    if [ "$RUNONCLUSTER" == 'y' ]; then
        sbatch $post_proc_file $RESULT_DIR $timestamp $paramsetfile
    else
        $post_proc_file $RESULT_DIR $timestamp $paramsetfile
    fi
else
    # Move to folder to not include absolute
    cd $SIMULATION_DIR

    # Zip the results directory
    zip -r "${timestamp}\_${OUTPUTDIRNAME}.zip" "${timestamp}\_${OUTPUTDIRNAME}"
fi
